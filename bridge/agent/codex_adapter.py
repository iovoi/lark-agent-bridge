"""CodexAdapter — drives the OpenAI Codex CLI (``codex exec --json``) per turn.

Unlike ClaudeAdapter (one long-lived bidirectional subprocess speaking the
stream-json control protocol), ``codex exec`` is one-shot: each turn spawns a
fresh process, feeds the prompt over stdin, and reads JSONL events from stdout
until the turn completes. Continuity across turns is provided by
``codex exec resume <session-id>`` using the thread id captured from
``thread.started``.

Exec mode has no interactive approval round-trip (no ``can_use_tool`` control
protocol), so the codex sandbox policy (default ``workspace-write`` via
FEISHU_CODEX_SANDBOX, passed as a ``-c sandbox_mode="…"`` config override so it
also works on ``exec resume``) is the safety boundary — approximated for UX by
the escalation card: when a command fails BECAUSE of the sandbox, the adapter
routes an escalation request through the same middle-layer
``ApprovalCallback`` the claude adapter uses (Lark approval card); on
allow/approve_all it bumps the tier one step (read-only → workspace-write →
danger-full-access) and retries the turn once on the same thread.

The JSONL event shapes are codex's experimental ``--json`` output (verified
against codex-cli 0.147): ``thread.started`` / ``turn.started`` /
``item.started`` / ``item.updated`` / ``item.completed`` with Responses-API
item payloads / ``turn.completed`` (usage) / ``turn.failed`` / ``error``.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from typing import Any, Optional

from .. import config as _config
from . import (
    ApprovalCallback,
    DoneEvent,
    Emit,
    ErrorEvent,
    SystemEvent,
    TextEvent,
    ThinkingEvent,
    ToolResultEvent,
    ToolUseEvent,
    UsageEvent,
)

# Item types (Responses API payloads) we surface as tool activity.
_TOOL_ITEM_NAMES = {
    "command_execution": "shell",
    "file_change": "file_change",
    "mcp_tool_call": "mcp_tool_call",
    "web_search": "web_search",
    "todo_list": "todo_list",
}

# Sandbox tiers, low → high; escalation moves one step up this order.
_ESCALATION_ORDER = ("read-only", "workspace-write", "danger-full-access")

# Substrings (lowercased, OR-ed) marking a FAILED command_execution as
# "blocked by the sandbox" — the escalation-card trigger. First two verified
# live on Windows codex.exe (workspace-write, out-of-workdir write); the rest
# cover codex's own strings and Linux Landlock EPERM.
_DENY_SIGNATURES = (
    "access to the path",  # + "is denied" checked pairwise below (Windows ACL)
    "blocked by the sandbox",
    "sandbox policy",
    "operation not permitted",
    "operation not allowed",
)


def _looks_sandbox_denied(item: dict[str, Any], extra_patterns: tuple = ()) -> bool:
    """True if a completed command_execution item failed BECAUSE of the sandbox
    (not an ordinary failure like command-not-found). Never raises."""
    try:
        if item.get("type") != "command_execution" or item.get("status") != "failed":
            return False
        out = str(item.get("aggregated_output") or item.get("output") or "").lower()
        if "access to the path" in out and "is denied" in out:
            return True  # Windows sandboxed-write denial (verified live)
        sigs = _DENY_SIGNATURES + tuple(extra_patterns)
        return any(s in out for s in sigs)
    except Exception:
        return False


def _reasoning_text(item: dict[str, Any]) -> str:
    """Join an item's reasoning summaries into one text blob."""
    parts: list[str] = []
    for key in ("summary", "content"):
        for entry in item.get(key) or []:
            if isinstance(entry, dict):
                txt = entry.get("text")
                if txt:
                    parts.append(str(txt))
            elif isinstance(entry, str) and entry:
                parts.append(entry)
    return "\n".join(parts)


def _map_item(item: dict[str, Any], *, started: bool) -> list:
    """Translate one Responses-API item into AgentEvents.

    ``started=True`` emits the ToolUseEvent as soon as the item opens (so the
    chat sees activity early); ``started=False`` (item.completed) emits the
    text/reasoning plus the matching ToolResultEvent."""
    itype = item.get("type")
    out: list = []
    if itype == "agent_message":
        if not started and item.get("text"):
            out.append(TextEvent(text=str(item["text"])))
        return out
    if itype == "reasoning":
        if not started:
            txt = _reasoning_text(item)
            if txt:
                out.append(ThinkingEvent(text=txt))
        return out
    name = _TOOL_ITEM_NAMES.get(str(itype))
    if name is None:
        return out
    iid = str(item.get("id") or f"{itype}_{id(item):x}")
    if started:
        inp: dict[str, Any] = {}
        for key in ("command", "arguments", "changes", "todos", "queries"):
            if item.get(key) is not None:
                inp[key] = item[key]
        out.append(ToolUseEvent(id=iid, name=name, input=inp))
        return out
    # completed: outcome as the tool result
    output: Any = None
    for key in ("aggregated_output", "output"):
        if item.get(key) is not None:
            output = item[key]
            break
    if output is None:
        output = {k: item[k] for k in ("command", "changes", "arguments") if item.get(k) is not None}
    if not isinstance(output, str):
        try:
            output = json.dumps(output, ensure_ascii=False)
        except Exception:
            output = str(output)
    status = item.get("status")
    exit_code = item.get("exit_code")
    is_error = status not in (None, "completed", "success") or (
        isinstance(exit_code, int) and exit_code != 0
    )
    out.append(ToolResultEvent(id=iid, output=str(output), is_error=bool(is_error)))
    return out


def _map_event(evt: dict[str, Any], session_id_ref: dict[str, Any]) -> tuple[list, bool]:
    """Translate one codex JSONL event into (AgentEvents, turn_done?).

    ``session_id_ref`` is mutated to capture the thread id from thread.started."""
    etype = evt.get("type")
    out: list = []
    done = False
    if etype == "thread.started":
        sid = evt.get("thread_id") or evt.get("session_id")
        if sid:
            session_id_ref["session_id"] = sid
        out.append(SystemEvent(session_id=sid))
    elif etype == "item.started":
        item = evt.get("item")
        if isinstance(item, dict):
            out.extend(_map_item(item, started=True))
    elif etype in ("item.completed", "item.updated"):
        item = evt.get("item")
        if isinstance(item, dict):
            out.extend(_map_item(item, started=False))
    elif etype == "turn.completed":
        usage = evt.get("usage") or {}
        out.append(UsageEvent(
            input_tokens=int(usage.get("input_tokens", 0) or 0),
            output_tokens=int(usage.get("output_tokens", 0) or 0),
        ))
        out.append(DoneEvent(session_id=session_id_ref.get("session_id"), reason="normal"))
        done = True
    elif etype in ("turn.failed", "error"):
        msg = evt.get("message")
        if not msg and isinstance(evt.get("error"), dict):
            msg = evt["error"].get("message")
        out.append(ErrorEvent(message=str(msg or etype)))
        out.append(DoneEvent(session_id=session_id_ref.get("session_id"), reason="error"))
        done = True
    return out, done


def _build_codex_argv(
    codex_bin: str,
    *,
    sandbox: str,
    extra_args: list[str],
    resume: Optional[str],
) -> list[str]:
    """Construct the codex argv for one turn. The prompt itself goes via stdin
    (``-``), never argv, so prompt text can't be read as flags."""
    argv = [
        # npm-installed CLIs on Windows are .cmd shims CreateProcess can't exec.
        *(
            [os.environ.get("COMSPEC", "cmd.exe"), "/c", codex_bin]
            if os.name == "nt" and codex_bin.lower().endswith((".cmd", ".bat"))
            else [codex_bin]
        ),
        "exec",
    ]
    # Sandbox via -c config override, NOT -s: `exec resume` (a clap subcommand)
    # accepts -c/--json/--skip-git-repo-check but rejects -s — passing it made
    # every resumed turn die with rc=2 "unexpected argument '-s' found".
    common = [
        "--json",
        "--skip-git-repo-check",
        "-c", f'sandbox_mode="{sandbox}"',
        *extra_args,
        "-",
    ]
    if resume:
        # `exec resume` defines its own flags (clap subcommand), so they go after it.
        argv += ["resume", str(resume), *common]
    else:
        argv += common
    return argv


class CodexAdapter:
    """One ``codex exec`` subprocess per turn; the runtime treats it as an
    AgentAdapter (same duck type as ClaudeAdapter)."""

    def __init__(
        self,
        cfg: "_config.BridgeConfig",
        *,
        resume: str | None = None,
        approval_callback: ApprovalCallback | None = None,
        stderr_sink=None,
    ) -> None:
        self._cfg = cfg
        # codex exec has no approval round-trip; the sandbox is the boundary.
        self._approval_callback = approval_callback
        self._resume = resume
        self._session_id_ref: dict[str, Any] = {"session_id": resume}
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._stderr_sink = stderr_sink
        self._stderr_buf = bytearray()  # bounded tail of codex stderr (error reporting)
        self._started = False
        # Effective sandbox tier (escalates with user approval; see run_turn).
        self._sandbox = cfg.codex_sandbox
        self._escalated = False  # approve_all: tier stays high for this adapter

    @property
    def session_id(self) -> str | None:
        return self._session_id_ref.get("session_id")

    async def start(self) -> None:
        # Nothing to keep alive between turns (unlike claude's persistent transport).
        self._started = True

    async def run_turn(self, prompt: str, emit: Emit, on_frame: Any = None) -> dict[str, Any]:
        """Run one turn; if a command was denied by the sandbox, offer an
        escalation approval card (same middle-layer ApprovalCallback the
        claude adapter uses) and on allow retry the turn once, one tier up."""
        result: dict[str, Any] = {}
        base_tier = self._cfg.codex_sandbox
        for attempt in range(2):  # original + at most one escalated rerun
            result, denial = await self._run_once(prompt, emit, on_frame)
            if attempt or denial is None:
                break
            verdict = await self._ask_escalation(denial)
            if verdict is None:
                break  # denied / no callback / already at top tier
            try:
                idx = _ESCALATION_ORDER.index(self._sandbox)
                self._sandbox = _ESCALATION_ORDER[min(idx + 1, len(_ESCALATION_ORDER) - 1)]
            except ValueError:
                self._sandbox = "danger-full-access"
            self._escalated = verdict == "approve_all"
            prompt = (prompt + f"\n\n[sandbox escalated to {self._sandbox}. "
                      "Retry the previously blocked operation now.]")
        if not self._escalated:
            # Plain "allow" is per-turn (claude Approve semantics): the rerun
            # ran elevated; subsequent turns drop back to the base tier.
            # Only "approve_all" keeps the chat escalated.
            self._sandbox = base_tier
        return result

    async def _ask_escalation(self, denial: dict[str, Any]) -> Optional[str]:
        """Route a sandbox denial through the approval card. Returns the
        verdict ("allow"/"approve_all") or None (= don't escalate)."""
        if self._approval_callback is None or self._escalated:
            return None
        if self._sandbox == _ESCALATION_ORDER[-1]:
            return None  # nowhere higher to go
        try:
            verdict = await self._approval_callback("sandbox-escalation", denial)
        except Exception as e:  # never wedge the turn on a UI failure
            print(f"[codex] escalation card error: {e!r}", file=sys.stderr)
            return None
        return verdict if verdict in ("allow", "approve_all") else None

    async def _run_once(self, prompt: str, emit: Emit, on_frame: Any = None) -> tuple[dict[str, Any], Optional[dict[str, Any]]]:
        """Spawn one codex exec process for the prompt; returns (result_info,
        first_sandbox_denial_or_None)."""
        argv = _build_codex_argv(
            self._cfg.codex_bin,
            sandbox=self._sandbox,
            extra_args=list(self._cfg.codex_extra_args),
            resume=self._resume,
        )
        popen_kwargs: dict[str, Any] = {
            "stdin": asyncio.subprocess.PIPE,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "cwd": str(self._cfg.workdir),
        }
        if os.name == "nt":
            CREATE_NEW_PROCESS_GROUP = 0x00000200
            CREATE_NO_WINDOW = 0x08000000
            popen_kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
        else:
            popen_kwargs["start_new_session"] = True

        self._proc = await asyncio.create_subprocess_exec(*argv, **popen_kwargs)
        self._stderr_buf = bytearray()
        if self._proc.stderr is not None:
            # Always drain (buffer + optional sink): a full stderr pipe would
            # deadlock codex, and the tail is how failures get reported.
            self._stderr_task = asyncio.create_task(self._drain_stderr())
        # The prompt rides on stdin ("-" argv), then EOF so codex starts the turn.
        self._proc.stdin.write((prompt + "\n").encode("utf-8"))
        await self._proc.stdin.drain()
        self._proc.stdin.close()

        result_info: dict[str, Any] = {"session_id": self.session_id}
        done = False
        emitted = False
        denial: Optional[dict[str, Any]] = None
        while not done:
            raw = await self._proc.stdout.readline()
            if not raw:
                break  # EOF without turn.completed — treat as end of turn
            line = raw.decode("utf-8", "replace").strip()
            if not line or not line.startswith("{"):
                continue
            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue
            if on_frame is not None:
                on_frame()  # heartbeat per event (covers long command executions)
            events, done = _map_event(evt, self._session_id_ref)
            for agent_evt in events:
                emitted = True
                await emit(agent_evt)
            if (denial is None and evt.get("type") == "item.completed"
                    and isinstance(evt.get("item"), dict)
                    and _looks_sandbox_denied(evt["item"], tuple(self._cfg.codex_deny_patterns))):
                item = evt["item"]
                denial = {
                    "command": str(item.get("command") or "")[:300],
                    "error": str(item.get("aggregated_output") or item.get("output") or "")[:300],
                }
        rc = await self._proc.wait()
        if not done and (rc != 0 or not emitted):
            # codex died before finishing (e.g. bad resume id, auth failure):
            # surface it instead of a silent empty turn (Done-emoji-only bug).
            tail = bytes(self._stderr_buf[-500:]).decode("utf-8", "replace").strip()
            msg = f"codex exited (rc={rc})"
            if tail:
                msg += f": {tail}"
            await emit(ErrorEvent(message=msg))
        if not done:
            await emit(DoneEvent(session_id=self.session_id, reason="error" if rc != 0 else "eof"))
        if self._resume is None and self.session_id:
            # later turns resume this thread
            self._resume = self.session_id
        result_info.update(session_id=self.session_id, exit_code=rc)
        return result_info, denial

    async def interrupt(self) -> None:
        # No control protocol in exec mode — terminate the in-flight turn's tree.
        proc = self._proc
        if proc is None or proc.returncode is not None:
            return
        try:
            if os.name == "nt":
                from ..transport import _taskkill

                _taskkill(proc.pid, force=True)
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    async def kill(self) -> None:
        # In exec mode interrupt() is already a hard tree-kill.
        await self.interrupt()
        self._started = False

    async def stop(self) -> None:
        await self.interrupt()
        if self._stderr_task is not None:
            self._stderr_task.cancel()
            self._stderr_task = None
        self._started = False

    async def _drain_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        try:
            while True:
                chunk = await self._proc.stderr.read(4096)
                if not chunk:
                    break
                self._stderr_buf += chunk
                if len(self._stderr_buf) > 8192:  # keep only the tail
                    del self._stderr_buf[:-8192]
                if self._stderr_sink is not None:
                    try:
                        self._stderr_sink.write(chunk.decode("utf-8", "replace"))
                        self._stderr_sink.flush()
                    except Exception:
                        pass
        except (asyncio.CancelledError, RuntimeError):
            pass
