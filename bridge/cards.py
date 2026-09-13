"""Interactive-card rendering for the streaming card and the approval card.

Uses the Feishu v1 interactive-card schema (broad client support; ``update_multi`` so
in-place updates push to all clients). The streaming card shows a compact tool log +
the partial answer while running, and the final answer when done. The approval card
has three buttons (Approve / Deny / Deny+stop) carrying JSON ``value`` objects the
runtime resolves on ``card.action.trigger``.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # avoid a circular import at runtime (lark.py imports from this module)
    from .lark import Lark as LarkBridge

# Card header templates (colors).
_TPL = {
    "working": "blue",
    "waiting_approval": "orange",
    "done": "green",
    "error": "red",
    "stopped": "grey",
}
_EMOJI = {
    "working": "🤖",
    "waiting_approval": "⏸",
    "done": "✅",
    "error": "⚠️",
    "stopped": "⏹",
}
_TITLE = {
    "working": "Working…",
    "waiting_approval": "Waiting for approval",
    "done": "Done",
    "error": "Error",
    "stopped": "Stopped",
}


@dataclass
class CardState:
    phase: str = "working"
    prompt: str = ""
    status: str = ""
    answer: str = ""
    tools: list[str] = field(default_factory=list)
    usage: str = ""
    scope: str = ""


def _md(content: str) -> dict:
    return {"tag": "lark_md", "content": content}


def _plain(content: str) -> dict:
    return {"tag": "plain_text", "content": content}


def render_streaming_card(state: CardState, *, with_stop: bool = True) -> dict:
    """Render the streaming card JSON for ``state``."""
    elements: list[dict] = []
    if state.prompt:
        elements.append({"tag": "div", "text": _md(f"**You asked:** {_truncate(state.prompt, 400)}")})
    if state.status:
        elements.append({"tag": "div", "text": _md(f"{_EMOJI.get(state.phase,'•')} {state.status}")})
    elements.append({"tag": "hr"})
    body = state.answer.strip() if state.answer else ("_" if state.phase == "working" else "")
    elements.append({"tag": "div", "text": _md(body) or _plain(" ")})
    if state.tools:
        elements.append({"tag": "note", "elements": [_plain("tools: " + " · ".join(state.tools))]})
    if state.usage:
        elements.append({"tag": "note", "elements": [_plain(state.usage)]})
    if with_stop and state.phase in ("working", "waiting_approval"):
        elements.append({
            "tag": "action",
            "actions": [{
                "tag": "button", "type": "danger",
                "text": _plain("⏹ Stop"),
                "value": {"v": "stop", "s": state.scope},
            }],
        })
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "title": _plain(f"{_EMOJI.get(state.phase,'•')} {_TITLE.get(state.phase, state.phase)}"),
            "template": _TPL.get(state.phase, "blue"),
        },
        "elements": elements,
    }


def render_approval_card(*, tool: str, summary: str, context: str, token: str, scope: str) -> dict:
    """Three-button approval card. Button values carry the approval token."""
    body = [
        {"tag": "div", "text": _md("Claude wants to run a tool.")},
        {"tag": "div", "text": _md(f"**Tool:** `{tool}`")},
    ]
    if summary:
        body.append({"tag": "div", "text": _md(f"```\n{_truncate(summary, 800)}\n```")})
    if context:
        body.append({"tag": "note", "elements": [_plain("task: " + _truncate(context, 200))]})
    body.append({"tag": "note", "elements": [_plain(
        "Tip: if the buttons don't respond, reply in chat: approve / deny / stop")]})
    body.append({
        "tag": "action",
        "actions": [
            {"tag": "button", "type": "primary", "text": _plain("✓ Approve"),
             "value": {"v": "approve", "t": token, "s": scope}},
            {"tag": "button", "type": "primary", "text": _plain("✓ Approve all (turn)"),
             "value": {"v": "approve_all", "t": token, "s": scope}},
            {"tag": "button", "type": "default", "text": _plain("✕ Deny"),
             "value": {"v": "deny", "t": token, "s": scope}},
            {"tag": "button", "type": "danger", "text": _plain("✕ Deny + stop"),
             "value": {"v": "deny_stop", "t": token, "s": scope}},
        ],
    })
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"title": _plain("⚠️ Approval needed"), "template": "orange"},
        "elements": body,
    }


def render_approval_card_resolved(*, tool: str, chosen: str, summary: str, context: str) -> dict:
    """Render an approval card AFTER a verdict: header reflects the choice, and only the
    button that was clicked is retained (the others removed) so the user sees what they picked.

    chosen is the internal verdict: 'allow' | 'deny' | 'deny_stop'."""
    info = {
        "allow": ("✓ Approved", "green", "Approve", "primary"),
        "approve_all": ("✓ Approved (all this turn)", "green", "Approve all (turn)", "primary"),
        "deny": ("✕ Denied", "grey", "Deny", "default"),
        "deny_stop": ("✕ Denied + stopped", "grey", "Deny + stop", "danger"),
    }.get(chosen, ("Resolved", "grey", chosen, "default"))
    title, template, label, btype = info
    elements = [{"tag": "div", "text": _md(f"**Tool:** `{tool}`")}]
    if summary:
        elements.append({"tag": "div", "text": _md(f"```\n{_truncate(summary, 400)}\n```")})
    elements.append({"tag": "note", "elements": [_plain(f"you clicked: {label}")]})
    # Retain only the chosen button (disabled-looking: same label, no action changes state).
    elements.append({"tag": "action", "actions": [
        {"tag": "button", "type": btype, "text": _plain(f"{label}  ✓"),
         "value": {"v": "noop"}, "disabled": True},
    ]})
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"title": _plain(title), "template": template},
        "elements": elements,
    }


def render_stuck_card(*, scope: str, seconds: int, prompt: str) -> dict:
    """Stuck-turn decision card. The watchdog posts it when a turn shows no stream
    activity for ``FEISHU_STUCK_TIMEOUT`` seconds: the graceful control-protocol
    interrupt may not break a wedged agent process, so the user decides."""
    body = [
        {"tag": "div", "text": _md(
            f"No activity for **{seconds}s**. The agent process may be wedged "
            "(e.g. a hung API stream). Choose an action:"
        )},
    ]
    if prompt:
        body.append({"tag": "note", "elements": [_plain("task: " + _truncate(prompt, 200))]})
    body.append({"tag": "note", "elements": [_plain(
        "Tip: if the buttons don't respond, /stop tries a graceful interrupt; "
        "`feishu-bridge stop && feishu-bridge up` restarts from the CLI")]})
    body.append({
        "tag": "action",
        "actions": [
            {"tag": "button", "type": "default", "text": _plain("⏳ Keep waiting"),
             "value": {"v": "stuck_wait", "s": scope}},
            {"tag": "button", "type": "danger", "text": _plain("🔪 Kill turn"),
             "value": {"v": "stuck_kill", "s": scope}},
            {"tag": "button", "type": "primary", "text": _plain("🔄 Restart bridge"),
             "value": {"v": "stuck_restart", "s": scope}},
        ],
    })
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"title": _plain("⚠️ Turn appears stuck"), "template": "red"},
        "elements": body,
    }


def render_stuck_card_resolved(*, chosen: str) -> dict:
    """Render the stuck card AFTER the user chose (or the turn recovered/ended)."""
    info = {
        "wait": ("⏳ Keeping waiting", "blue", "Keep waiting"),
        "kill": ("🔪 Turn killed", "grey", "Kill turn"),
        "restart": ("🔄 Bridge restarting…", "orange", "Restart bridge"),
        "recovered": ("✅ Turn recovered on its own", "green", "Recovered"),
        "ended": ("⏹ Turn ended", "grey", "Turn ended"),
        "ignored": ("⏹ Turn already ended", "grey", "Turn ended"),
    }.get(chosen, ("Resolved", "grey", chosen))
    title, template, label = info
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"title": _plain(title), "template": template},
        "elements": [
            {"tag": "note", "elements": [_plain(f"you clicked: {label}")]},
            {"tag": "action", "actions": [
                {"tag": "button", "type": "default", "text": _plain(f"{label}  ✓"),
                 "value": {"v": "noop"}, "disabled": True},
            ]},
        ],
    }


def _truncate(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


class StreamingCard:
    """One interactive card per turn; throttled in-place updates; fail-soft."""

    def __init__(self, lark: "LarkBridge", chat_id: str, scope: str, throttle_ms: int) -> None:
        self._lark = lark
        self._chat_id = chat_id
        self._scope = scope
        self._throttle = throttle_ms / 1000.0
        self.msg_id: str | None = None
        self._last_send = 0.0
        self._dirty = False
        self._last_state: CardState | None = None

    async def create(self, state: CardState) -> None:
        self._last_state = state
        self.msg_id = self._lark.send_card(self._chat_id, render_streaming_card(state, with_stop=True))
        self._last_send = time.monotonic()
        self._dirty = False

    async def update(self, state: CardState) -> None:
        self._last_state = state
        self._dirty = True
        now = time.monotonic()
        if now - self._last_send >= self._throttle:
            self._flush()

    async def finalize(self, state: CardState) -> bool:
        state.phase = state.phase or "done"
        self._last_state = state
        self._dirty = True
        return self._flush(final=True)

    def flush_pending(self) -> None:
        """Push any throttled-but-pending update (e.g. on interrupt)."""
        if self._dirty:
            self._flush()

    def _flush(self, *, final: bool = False) -> bool:
        if self.msg_id is None or self._last_state is None:
            return False
        ok = self._lark.update_card(self.msg_id, render_streaming_card(self._last_state, with_stop=not final))
        self._last_send = time.monotonic()
        self._dirty = False
        return bool(ok)
