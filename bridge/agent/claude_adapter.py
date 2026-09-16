"""ClaudeAdapter — drives one :class:`bridge.transport.Transport` per scope.

Maps raw Claude Code stream-json frames to :mod:`bridge.agent` events, captures the
session id (for `--resume` after a crash/restart), and routes inbound ``can_use_tool``
prompts through the allowlist + the chat-layer approval callback.
"""
from __future__ import annotations

import asyncio
import sys
from typing import Any

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
from ..transport import Transport


def _map_frame(frame: dict[str, Any], session_id_ref: dict[str, Any]) -> list:
    """Translate one raw stream-json frame into zero or more AgentEvents.

    ``session_id_ref`` is mutated to capture the session id from system/init."""
    ftype = frame.get("type")
    out: list = []

    if ftype == "system":
        data = frame.get("data") or frame  # system/init nests under 'data' in some versions
        subtype = frame.get("subtype") or data.get("subtype")
        if subtype == "init":
            sid = data.get("session_id") or frame.get("session_id")
            if sid:
                session_id_ref["session_id"] = sid
            out.append(SystemEvent(session_id=sid, cwd=data.get("cwd"), model=data.get("model")))
        return out

    if ftype == "assistant":
        content = ((frame.get("message") or {}).get("content")) or []
        for block in content:
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype == "text" and block.get("text"):
                out.append(TextEvent(text=block["text"]))
            elif btype == "thinking" and block.get("thinking"):
                out.append(ThinkingEvent(text=block["thinking"]))
            elif btype == "tool_use" and block.get("id") and block.get("name"):
                out.append(ToolUseEvent(id=block["id"], name=block["name"], input=block.get("input") or {}))
        return out

    if ftype == "user":
        content = ((frame.get("message") or {}).get("content")) or []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                out.append(ToolResultEvent(
                    id=str(block.get("tool_use_id", "")),
                    output=_stringify(block.get("content")),
                    is_error=bool(block.get("is_error")),
                ))
        return out

    if ftype == "result":
        usage = frame.get("usage") or {}
        cost = frame.get("total_cost_usd")
        out.append(UsageEvent(
            input_tokens=int(usage.get("input_tokens", 0) or 0),
            output_tokens=int(usage.get("output_tokens", 0) or 0),
            cost_usd=float(cost) if cost is not None else 0.0,
        ))
        out.append(DoneEvent(session_id=frame.get("session_id"), reason="normal"))
        return out

    return out


def _stringify(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    import json as _json
    try:
        return _json.dumps(content, ensure_ascii=False)
    except Exception:
        return str(content)


class ClaudeAdapter:
    """Owns one Transport per scope; the runtime treats it as an AgentAdapter."""

    def __init__(
        self,
        cfg: "_config.BridgeConfig",
        *,
        resume: str | None = None,
        approval_callback: ApprovalCallback | None = None,
        stderr_sink=None,
    ) -> None:
        self._cfg = cfg
        self._approval_callback = approval_callback
        self._session_id_ref: dict[str, Any] = {"session_id": resume}
        # If approvals are available, use the stdio permission tool; otherwise bypass.
        approvals_on = approval_callback is not None
        self._transport = Transport(
            cfg.claude_bin,
            cwd=str(cfg.workdir),
            resume=resume,
            permission_prompt_tool=approvals_on,
            skip_permissions=not approvals_on,
            add_dirs=[str(cfg.workdir)],
            permission_handler=self._on_can_use_tool,
            stderr_sink=stderr_sink,
        )
        self._started = False
        # Per-turn "approve all" escalation: once the user picks "Approve all (turn)" on an
        # approval card, subsequent can_use_tool prompts in THIS turn auto-allow (no card).
        # Reset at the start of each turn.
        self._approve_all = False

    @property
    def session_id(self) -> str | None:
        return self._session_id_ref.get("session_id")

    async def start(self) -> None:
        if self._started:
            return
        await self._transport.start()
        self._started = True
        try:
            await self._transport.initialize()
        except asyncio.TimeoutError:
            print("[claude] initialize handshake timed out (continuing)", file=sys.stderr)
        except RuntimeError as e:
            print(f"[claude] initialize error: {e!r} (continuing)", file=sys.stderr)

    async def run_turn(self, prompt: str, emit: Emit, on_frame: Any = None) -> dict[str, Any]:
        self._approve_all = False  # per-turn: re-confirm approvals each turn
        await self._transport.send_user_turn(prompt)
        result_info: dict[str, Any] = {"session_id": self.session_id}
        async for frame in self._transport.events():
            if on_frame is not None:
                on_frame()  # heartbeat per frame (covers thinking_tokens, which emit no event)
            for evt in _map_frame(frame, self._session_id_ref):
                await emit(evt)
            if isinstance(frame, dict) and frame.get("type") == "result":
                usage = frame.get("usage") or {}
                result_info.update(
                    cost_usd=frame.get("total_cost_usd"),
                    input_tokens=usage.get("input_tokens"),
                    output_tokens=usage.get("output_tokens"),
                )
        result_info["session_id"] = self.session_id
        return result_info

    async def interrupt(self) -> None:
        await self._transport.interrupt()

    async def kill(self) -> None:
        """Hard-kill the wedged subprocess; the scope drops the adapter so the next
        message builds a fresh process (fresh thread, like a bridge restart)."""
        await self._transport.kill()
        self._started = False

    async def stop(self) -> None:
        await self._transport.close()
        self._started = False

    async def _on_can_use_tool(self, req: dict[str, Any]) -> dict[str, Any]:
        tool = str(req.get("tool_name", ""))
        if tool in self._cfg.auto_approve_tools:
            return {"behavior": "allow"}
        if self._approve_all:
            # User escalated to "Approve all (turn)" earlier this turn.
            return {"behavior": "allow"}
        if self._approval_callback is None:
            return {"behavior": "allow"}  # defensive; bypass mode won't prompt anyway
        verdict = await self._approval_callback(tool, req.get("input") or {})
        if verdict == "approve_all":
            self._approve_all = True  # auto-allow the rest of this turn
            return {"behavior": "allow"}
        if verdict == "allow":
            return {"behavior": "allow"}
        if verdict == "deny_stop":
            return {"behavior": "deny", "message": "user denied", "interrupt": True}
        return {"behavior": "deny", "message": "user denied"}
