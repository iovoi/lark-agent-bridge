"""Bridge runtime: wires ingest -> scopes -> agent, all on one asyncio loop.

The websocket (in a daemon thread) calls :meth:`on_message` / :meth:`on_card_action`;
these are thread-safe trampolines that schedule the real work onto the loop. The loop
owns all async state (scopes, semaphore, approval futures). ``run`` is the foreground
entry used by the supervisor (``feishu-bridge run``).
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Awaitable, Callable, Optional

from . import access, ingest, session_store  # noqa: F401 (session_store used by scope)
from .approvals import ApprovalManager
from .config import BridgeConfig
from .ingest import is_stale
from .lark import Lark
from .scope import ScopeRunner


def _verdict_from_text(text: str) -> str | None:
    """Map a user reply to an approval verdict (allow/deny/deny_stop), or None if unrecognized."""
    t = (text or "").strip().lower()
    if t in ("approve", "yes", "y", "ok", "1", "允许", "同意"):
        return "allow"
    if t in ("all", "approve all", "approveall", "approve-all"):
        return "approve_all"
    if t in ("stop", "deny_stop", "abort", "cancel", "3", "取消", "停止"):
        return "deny_stop"
    if t in ("deny", "no", "n", "2", "拒绝"):
        return "deny"
    return None


class Runtime:
    def __init__(self, cfg: BridgeConfig) -> None:
        self.cfg = cfg
        self.lark = Lark(cfg)
        self.approvals = ApprovalManager(self.lark, cfg)
        self.scopes: dict[str, ScopeRunner] = {}
        self._sem = asyncio.Semaphore(cfg.max_concurrent)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.boot_time = time.time()

    # ---- thread-safe trampolines (called from the websocket thread) -----------

    def on_message(self, evt: dict) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._schedule, self._handle_message, evt)

    def on_card_action(self, action: dict) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._schedule, self._handle_card_action, action)

    def _schedule(self, coro_fn: Callable[[dict], Awaitable[None]], arg: dict) -> None:
        assert self._loop is not None
        asyncio.create_task(coro_fn(arg))

    # ---- handlers (on the loop) ----------------------------------------------

    async def _handle_message(self, evt: dict) -> None:
        if is_stale(evt.get("create_time"), self.boot_time):
            return
        if not access.allowed(evt.get("open_id"), evt.get("chat_id")):
            return
        text = (evt.get("text") or "").strip()
        scope = self._scope_of(evt)
        chat_id = evt.get("chat_id") or ""
        runner = self.scopes.get(scope)
        if runner is None:
            runner = ScopeRunner(scope, chat_id, self.cfg, self.lark, self.approvals,
                                 restart_cb=self.request_restart)
            self.scopes[scope] = runner

        if text == "/stop":
            ok = await runner.request_stop()
            if not ok:
                self.lark.send_text(chat_id, "(nothing to stop)")
            return

        # Reply-based approval resolution: a fallback for when card-action buttons aren't
        # delivered to the bot (the app hasn't subscribed to card.action.trigger). While an
        # approval is pending in this scope, a reply of approve/deny/stop resolves it.
        if self.approvals.pending_for_scope(scope) is not None:
            verdict = _verdict_from_text(text)
            print(f"[reply-approval {scope}] text={text!r} -> verdict={verdict!r}",
                  file=sys.stderr, flush=True)
            if verdict:
                self.approvals.resolve_for_scope(scope, verdict)
                self.lark.send_text(chat_id, f"(approval: {verdict})")
                return

        async with self._sem:
            await runner.handle_message(evt)

    async def _handle_card_action(self, action: dict) -> None:
        val = action.get("value") or {}
        print(f"[card-action] received; value={val!r} message_id={action.get('message_id')}",
              file=sys.stderr, flush=True)
        if not isinstance(val, dict):
            return
        verb = val.get("v")
        token = val.get("t")
        scope = val.get("s")
        if verb == "stop":
            runner = self.scopes.get(scope) if scope else None
            if runner is not None:
                await runner.request_stop()
        elif verb in ("stuck_wait", "stuck_kill", "stuck_restart"):
            runner = self.scopes.get(scope) if scope else None
            if runner is not None:
                await runner.resolve_stuck(verb)
        elif verb in ("approve", "deny", "deny_stop", "approve_all") and token:
            self.approvals.resolve(token, verb)

    # ---- self-restart (stuck card "Restart bridge") ---------------------------

    def request_restart(self) -> None:
        """Spawn a replacement bridge process and exit this one. Called from the
        runtime loop; ``os._exit`` skips asyncio teardown (a wedged turn may be
        holding it). The replacement takes over the pidfile."""
        from . import supervisor

        print("[runtime] restart requested — respawning bridge", file=sys.stderr, flush=True)
        try:
            pid = supervisor.respawn()
            print(f"[runtime] replacement bridge started (pid {pid}); exiting",
                  file=sys.stderr, flush=True)
        except Exception as e:
            print(f"[runtime] respawn failed: {e!r} — NOT exiting", file=sys.stderr, flush=True)
            return
        os._exit(0)

    @staticmethod
    def _scope_of(evt: dict) -> str:
        cid = evt.get("chat_id") or "unknown"
        tid = evt.get("thread_id")
        return f"{cid}:{tid}" if tid else cid

    # ---- lifecycle -----------------------------------------------------------

    async def _main(self, no_ws: bool) -> None:
        self._loop = asyncio.get_running_loop()
        if not no_ws:
            print("[runtime] starting Feishu websocket…", file=sys.stderr)
            ingest.start_ws(self.on_message, self.on_card_action)
        else:
            print("[runtime] --no-ws: websocket not started", file=sys.stderr)
        await asyncio.Event().wait()  # run forever

    def run(self, *, no_ws: bool = False) -> int:
        try:
            asyncio.run(self._main(no_ws))
        except KeyboardInterrupt:
            pass
        return 0


def run_forever(*, no_ws: bool = False) -> int:
    """Console entry: load config, build a Runtime, run it."""
    return Runtime(BridgeConfig.load()).run(no_ws=no_ws)
