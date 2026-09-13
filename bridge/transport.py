"""Hand-rolled transport over the Claude Code streaming subprocess.

Spawns ``claude -p --input-format stream-json --output-format stream-json --verbose ...``
once, keeps it alive, and speaks the bidirectional NDJSON protocol ourselves (no Agent
SDK dependency). Two layers ride over the one stdio pipe:

* **Stream frames** (``system``/``assistant``/``user``/``result``) — yielded by
  :meth:`Transport.events` until the turn's ``result`` frame.
* **Control protocol** — outbound ``control_request`` (with a correlation ``request_id``)
  awaiting a matching ``control_response`` (:meth:`request`, used for ``initialize`` and
  ``interrupt``), and inbound ``control_request`` from claude (``can_use_tool`` permission
  prompts) routed to ``permission_handler``.

The control-message shapes mirror the open-source Python Agent SDK; they are
semi-documented and version-sensitive (see PRD §4.5/§4.6 and log.md). Correctness
against a *real* claude is validated by the manual smoke test (T8.1); unit tests use
the fake-claude stub (T7) and injected streams.

Cross-platform: process group / kill uses POSIX ``start_new_session`` + ``killpg`` and
Windows ``CREATE_NEW_PROCESS_GROUP`` + ``taskkill /T``. No PTY, no tmux.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from typing import Any, Awaitable, Callable, Optional

# Sentinel pushed onto the stream queue when claude's stdout closes (EOF).
class _EOF:
    __slots__ = ()


_EOF = _EOF()

# Permission handler contract: given the inbound control_request's `request` dict
# (contains tool_name / input for can_use_tool), return a response body such as
# {"behavior": "allow"} or {"behavior": "deny", "message": "...", "interrupt": True}.
PermissionHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class _LineFramer:
    """Reassemble newline-delimited JSON lines from arbitrary text chunks."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = ""

    def feed(self, chunk: str) -> list[str]:
        """Feed a chunk; return the complete (newline-terminated) lines it yielded."""
        self._buf += chunk
        if "\n" not in self._buf:
            return []
        parts = self._buf.split("\n")
        self._buf = parts.pop()  # last, possibly-incomplete fragment
        return [p for p in parts if p]  # drop empty lines between frames


def _build_claude_argv(
    claude_bin: str,
    *,
    session_id: Optional[str],
    resume: Optional[str],
    permission_prompt_tool: bool,
    permission_mode: Optional[str],
    skip_permissions: bool,
    add_dirs: list[str],
    extra_args: list[str],
) -> list[str]:
    """Construct the claude argv. ``--flag=value`` form is used for values that may
    start with ``-`` to avoid flag injection (mirrors the SDK)."""
    argv = [
        # npm-installed claude on Windows is a .cmd shim, which CreateProcess
        # cannot exec directly — route it through cmd.exe /c.
        *(
            [os.environ.get("COMSPEC", "cmd.exe"), "/c", claude_bin]
            if os.name == "nt" and claude_bin.lower().endswith((".cmd", ".bat"))
            else [claude_bin]
        ),
        "-p",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
    ]
    if permission_prompt_tool:
        argv += ["--permission-prompt-tool", "stdio"]
    if skip_permissions:
        argv.append("--dangerously-skip-permissions")
    elif permission_mode:
        argv += ["--permission-mode", permission_mode]
    if resume:
        argv.append(f"--resume={resume}")
    elif session_id:
        argv.append(f"--session-id={session_id}")
    for d in add_dirs:
        argv.append(f"--add-dir={d}")
    argv.extend(extra_args)
    return argv


class Transport:
    """Owns one long-lived claude subprocess + framing + the control protocol."""

    def __init__(
        self,
        claude_bin: str,
        *,
        cwd: str,
        env: Optional[dict[str, str]] = None,
        session_id: Optional[str] = None,
        resume: Optional[str] = None,
        permission_prompt_tool: bool = True,
        permission_mode: Optional[str] = None,
        skip_permissions: bool = False,
        add_dirs: Optional[list[str]] = None,
        extra_args: Optional[list[str]] = None,
        permission_handler: Optional[PermissionHandler] = None,
        stderr_sink: Any = None,
        initialize_timeout: float = 30.0,
        request_timeout: float = 300.0,
    ) -> None:
        self.claude_bin = claude_bin
        self.cwd = cwd
        self.env = env or {}
        self.session_id = session_id
        self.resume = resume
        self.permission_prompt_tool = permission_prompt_tool
        self.permission_mode = permission_mode
        self.skip_permissions = skip_permissions
        self.add_dirs = add_dirs or []
        self.extra_args = extra_args or []
        self.permission_handler = permission_handler
        self.stderr_sink = stderr_sink  # file-like for claude stderr (observability)
        self.initialize_timeout = initialize_timeout
        self.request_timeout = request_timeout

        self._argv = _build_claude_argv(
            claude_bin,
            session_id=session_id,
            resume=resume,
            permission_prompt_tool=permission_prompt_tool,
            permission_mode=permission_mode,
            skip_permissions=skip_permissions,
            add_dirs=self.add_dirs,
            extra_args=self.extra_args,
        )
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._stdin: Optional[asyncio.StreamWriter] = None
        self._stdout: Optional[asyncio.StreamReader] = None
        self._framer = _LineFramer()
        self._pending: dict[str, asyncio.Future] = {}
        self._stream_q: Optional[asyncio.Queue] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._req_counter = 0
        self._closing = False

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        """Spawn the subprocess and begin the stdout/stderr reader loops."""
        env = dict(os.environ)
        env.update(self.env)
        env.setdefault("CLAUDE_CODE_ENTRYPOINT", "pipe-bridge")
        env.pop("CLAUDECODE", None)  # don't inherit a parent claude context

        popen_kwargs: dict[str, Any] = {
            "stdin": asyncio.subprocess.PIPE,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "cwd": self.cwd,
            "env": env,
        }
        if os.name == "nt":
            # CREATE_NO_WINDOW, not DETACHED_PROCESS: a hidden console the
            # claude process and its grandchildren inherit — DETACHED leaves
            # the child console-less and any console grandchild pops a visible
            # empty cmd window on the desktop.
            CREATE_NEW_PROCESS_GROUP = 0x00000200
            CREATE_NO_WINDOW = 0x08000000
            popen_kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
        else:
            popen_kwargs["start_new_session"] = True

        self._proc = await asyncio.create_subprocess_exec(*self._argv, **popen_kwargs)
        self._stdin = self._proc.stdin
        self._stdout = self._proc.stdout
        self._stream_q = asyncio.Queue()
        self._reader_task = asyncio.create_task(self._read_loop())
        if self.stderr_sink is not None:
            self._stderr_task = asyncio.create_task(self._drain_stderr())

    async def initialize(self) -> dict[str, Any]:
        """Send the ``initialize`` control_request and await its response.

        Best-effort against real claude (the handshake payload is semi-documented);
        callers should tolerate a timeout and proceed, validating via smoke test."""
        return await self.request({"subtype": "initialize"}, timeout=self.initialize_timeout)

    # ------------------------------------------------------------------ outbound

    async def request(self, payload: dict[str, Any], timeout: Optional[float] = None) -> dict[str, Any]:
        """Send a control_request and await its control_response (correlated by id)."""
        if self._stdin is None:
            raise RuntimeError("Transport not started")
        self._req_counter += 1
        rid = f"req_{self._req_counter}_{os.urandom(4).hex()}"
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[rid] = fut
        await self._write({"type": "control_request", "request_id": rid, "request": payload})
        try:
            return await asyncio.wait_for(fut, timeout if timeout is not None else self.request_timeout)
        except asyncio.TimeoutError:
            self._pending.pop(rid, None)
            raise

    async def send_user_turn(self, text: str) -> None:
        """Write one user-turn NDJSON line (the prompt). The same process serves many turns."""
        await self._write(
            {
                "type": "user",
                "session_id": "",
                "message": {"role": "user", "content": text},
                "parent_tool_use_id": None,
            }
        )

    async def interrupt(self) -> None:
        """Best-effort graceful interrupt via the control protocol (no process kill)."""
        try:
            await self.request({"subtype": "interrupt"}, timeout=10.0)
        except (asyncio.TimeoutError, RuntimeError):
            pass

    async def kill(self) -> None:
        """Force-kill the whole process tree NOW (wedged turn; the graceful path failed).
        The read loop then sees EOF, which unblocks ``events()`` so the turn finalizes."""
        if self._proc is None:
            return
        self._closing = True
        self._kill_tree()
        try:
            await asyncio.wait_for(self._proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------ inbound

    async def events(self):
        """Yield stream frames for the current turn until ``result`` or EOF."""
        if self._stream_q is None:
            raise RuntimeError("Transport not started")
        while True:
            frame = await self._stream_q.get()
            if frame is _EOF:
                return
            yield frame
            if isinstance(frame, dict) and frame.get("type") == "result":
                return

    async def _read_loop(self) -> None:
        try:
            while True:
                chunk = await self._stdout.read(4096)
                if not chunk:
                    break
                for line in self._framer.feed(chunk.decode("utf-8", "replace")):
                    await self._dispatch_line(line)
        except (asyncio.CancelledError, RuntimeError):
            pass
        finally:
            # Drain any unresolved waiters and unblock events().
            for rid, fut in list(self._pending.items()):
                if not fut.done():
                    fut.set_exception(RuntimeError("claude stream closed"))
                self._pending.pop(rid, None)
            await self._stream_q.put(_EOF)

    async def _drain_stderr(self) -> None:
        assert self._proc is not None and self.stderr_sink is not None
        try:
            while True:
                chunk = await self._proc.stderr.read(4096)
                if not chunk:
                    break
                try:
                    self.stderr_sink.write(chunk.decode("utf-8", "replace"))
                    self.stderr_sink.flush()
                except Exception:
                    pass
        except (asyncio.CancelledError, RuntimeError):
            pass

    async def _dispatch_line(self, line: str) -> None:
        s = line.strip()
        if not s or not s.startswith("{"):
            return  # skip non-JSON noise on stdout
        try:
            frame = json.loads(s)
        except json.JSONDecodeError:
            return
        ftype = frame.get("type")
        if ftype == "control_response":
            resp = frame.get("response", {}) or {}
            rid = resp.get("request_id")
            fut = self._pending.pop(rid, None) if rid else None
            if fut is not None and not fut.done():
                fut.set_result(resp)
        elif ftype == "control_request":
            await self._handle_inbound_control(frame)
        else:
            await self._stream_q.put(frame)

    async def _handle_inbound_control(self, frame: dict[str, Any]) -> None:
        rid = frame.get("request_id")
        req = frame.get("request", {}) or {}
        subtype = req.get("subtype")
        body: dict[str, Any] = {}
        if subtype == "can_use_tool" and self.permission_handler is not None:
            try:
                body = await self.permission_handler(req)
            except Exception as e:  # never let a handler bug wedge the turn
                body = {"behavior": "deny", "message": f"approval handler error: {e!r}"}
            if not isinstance(body, dict) or "behavior" not in body:
                body = {"behavior": "deny", "message": "invalid approval response"}
        # Reply to the CLI's control_request with a control_response carrying its id.
        await self._write(
            {
                "type": "control_response",
                "response": {"subtype": "success", "request_id": rid, "response": body},
            }
        )

    # ------------------------------------------------------------------ low-level

    async def _write(self, obj: dict[str, Any]) -> None:
        assert self._stdin is not None
        data = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
        self._stdin.write(data)
        await self._stdin.drain()

    @property
    def alive(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    async def close(self) -> None:
        """Tear down: stdin EOF -> wait -> SIGTERM -> wait -> SIGKILL."""
        if self._proc is None:
            return
        if self._closing:
            return
        self._closing = True
        try:
            if self._stdin is not None:
                self._stdin.close()
        except Exception:
            pass
        try:
            await asyncio.wait_for(self._proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            self._terminate_tree()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self._kill_tree()
        for t in (self._reader_task, self._stderr_task):
            if t is not None:
                t.cancel()
        # atexit-style: nothing; caller owns the process lifetime.

    def _terminate_tree(self) -> None:
        assert self._proc is not None
        if os.name == "nt":
            _taskkill(self._proc.pid, force=False)
        else:
            try:
                os.killpg(os.getpgid(self._proc.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                try:
                    self._proc.terminate()
                except ProcessLookupError:
                    pass

    def _kill_tree(self) -> None:
        assert self._proc is not None
        if os.name == "nt":
            _taskkill(self._proc.pid, force=True)
        else:
            try:
                os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                try:
                    self._proc.kill()
                except ProcessLookupError:
                    pass


def _taskkill(pid: int, *, force: bool) -> None:
    """Windows whole-tree termination (no psutil dependency)."""
    import subprocess

    try:
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T"] + (["/F"] if force else []),
            capture_output=True,
            timeout=10,
        )
    except Exception:
        pass
