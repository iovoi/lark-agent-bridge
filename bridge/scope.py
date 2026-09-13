"""Per-scope turn orchestration.

A :class:`ScopeRunner` owns one chat's state: single-flight (reject a 2nd message with a
``/stop`` hint), the OnIt→Done emoji cycle, a streaming card, the lazy-started agent
adapter (:class:`~bridge.agent.ClaudeAdapter` or ``CodexAdapter``, per ``cfg.agent``;
resumed by the stored session id), approval delegation, and a
stuck watchdog. The runtime creates one per scope.
"""
from __future__ import annotations

import asyncio
import sys
import time
from typing import Any, Callable, Optional

from .agent import (
    AgentAdapter,
    DoneEvent,
    ErrorEvent,
    TextEvent,
    ThinkingEvent,
    ToolResultEvent,
    ToolUseEvent,
    UsageEvent,
)
from .agent.claude_adapter import ClaudeAdapter  # noqa: F401 (re-exported for tests)
from .agent import make_adapter
from .approvals import ApprovalManager
from .cards import CardState, StreamingCard, render_stuck_card, render_stuck_card_resolved
from .config import BridgeConfig
from .lark import Lark
from . import session_store
from .watchdog import StuckWatchdog


class ScopeRunner:
    def __init__(
        self,
        scope: str,
        chat_id: str,
        cfg: BridgeConfig,
        lark: Lark,
        approvals: ApprovalManager,
        *,
        adapter_factory: Optional[Callable[[], Any]] = None,
        restart_cb: Optional[Callable[[], None]] = None,
    ) -> None:
        self.scope = scope
        self.chat_id = chat_id
        self.cfg = cfg
        self.lark = lark
        self.approvals = approvals
        self._adapter_factory = adapter_factory or self._default_adapter_factory
        # Runtime hook to self-restart the whole bridge (stuck card "Restart bridge").
        self._restart_cb = restart_cb

        self._busy = False
        self._stop_flag = False
        self._adapter: Optional[AgentAdapter] = None
        self._card: Optional[StreamingCard] = None
        self._state: Optional[CardState] = None
        self._watchdog: Optional[StuckWatchdog] = None
        self._stuck_card_msg: Optional[str] = None
        self._stuck_open = False

    def _default_adapter_factory(self) -> AgentAdapter:
        # stderr of the agent CLI goes to a log file (observability): a codex
        # resume failure or claude crash otherwise vanishes with the pipe.
        stderr_sink = None
        try:
            from .supervisor import RUN_DIR

            RUN_DIR.mkdir(parents=True, exist_ok=True)
            stderr_sink = open(RUN_DIR / "agent-stderr.log", "ab", buffering=0)
        except Exception:
            stderr_sink = None
        return make_adapter(
            self.cfg,
            # Fresh thread per bridge start by default (FEISHU_RESUME_SESSIONS=1
            # opts back into cross-restart continuity). Identical for claude and
            # codex; within a running bridge, turns of a chat still share context.
            resume=(
                session_store.get_session_id(self.scope, agent=self.cfg.agent)
                if self.cfg.resume_sessions else None
            ),
            approval_callback=self._approval_cb,
            stderr_sink=stderr_sink,
        )

    # ------------------------------------------------------------------ entry

    async def handle_message(self, evt: dict) -> None:
        if self._busy:
            await self._reject(evt)
            return
        self._busy = True
        try:
            await self._run_turn(evt)
        finally:
            self._busy = False

    async def request_stop(self) -> bool:
        """``/stop``: interrupt the active turn. Returns True if a turn was active."""
        if not self._busy:
            return False
        self._stop_flag = True
        if self._state is not None:
            self._state.status = "Stopping…"
        if self._card is not None and self._state is not None:
            await self._card.update(self._state)
        if self._adapter is not None:
            try:
                await self._adapter.interrupt()
            except Exception as e:  # never wedge a /stop
                print(f"[scope {self.scope}] interrupt error: {e!r}", file=sys.stderr)
        return True

    # ------------------------------------------------------------------ pieces

    async def _reject(self, evt: dict) -> None:
        # Stamp Done on the rejected message (no OnIt to remove) + hint /stop.
        self.lark.swap_to_done(evt.get("message_id", ""), None)
        self.lark.send_text(
            evt.get("chat_id", self.chat_id),
            "(still working on your last message — send /stop to cancel)",
        )

    async def _run_turn(self, evt: dict) -> None:
        message_id = evt.get("message_id", "")
        onit = self.lark.stamp_onit(message_id)
        prompt = (evt.get("text") or "").strip()
        print(f"[turn {self.scope}] start: {prompt[:150]!r}", file=sys.stderr, flush=True)

        self._state = CardState(prompt=prompt, phase="working", status="Starting…", scope=self.scope)
        # The progress card is DEFERRED: only created if the turn is still running after
        # card_defer_sec (the OnIt emoji acknowledges receipt meanwhile). Once created it
        # updates every card_interval_sec with excerpts of the agent output.
        self._card = StreamingCard(self.lark, evt.get("chat_id", self.chat_id),
                                   self.scope, self.cfg.card_throttle_ms)
        self._turn_start = time.monotonic()

        if self._adapter is None:
            self._adapter = self._adapter_factory()
            await self._adapter.start()

        self._stop_flag = False
        self._stuck_card_msg = None
        self._stuck_open = False
        wd = StuckWatchdog(self.cfg.stuck_timeout, self._on_stuck,
                           is_approval_pending=lambda: self.approvals.has_pending)
        wd.start()
        self._watchdog = wd
        self._card_task = asyncio.create_task(self._card_loop())
        result: dict = {}
        try:
            result = await self._adapter.run_turn(prompt, self._emit, on_frame=wd.bump)
        except Exception as e:
            self._state.phase = "error"
            self._state.status = f"error: {e}"
            print(f"[turn {self.scope}] ERROR: {e!r}", file=sys.stderr, flush=True)
        finally:
            wd.stop()
            self._watchdog = None
            self._card_task.cancel()
            if self._stuck_open:  # turn ended with the stuck card unanswered (e.g. via /stop)
                self._stuck_open = False
                self._resolve_stuck_card("ended")
        await self._finalize(result, message_id, onit)

    async def _card_loop(self) -> None:
        """Defer the progress card until card_defer_sec; then update it every card_interval_sec."""
        try:
            await asyncio.sleep(self.cfg.card_defer_sec)
            if self._card is None or self._state is None:
                return
            await self._card.create(self._state)
            print(f"[turn {self.scope}] progress card shown after {self.cfg.card_defer_sec}s",
                  file=sys.stderr, flush=True)
            while True:
                await asyncio.sleep(self.cfg.card_interval_sec)
                if self._card.msg_id is not None and self._state is not None:
                    await self._card.update(self._state)
        except asyncio.CancelledError:
            return

    async def _finalize(self, result: dict, message_id: str, onit) -> None:
        if self._stop_flag:
            self._state.phase = "stopped"
            self._state.status = "Stopped"
        elif self._state.phase != "error":
            self._state.phase = "done"
        cost = result.get("cost_usd")
        if isinstance(cost, (int, float)):
            self._state.usage = f"💰 ${cost:.4f}"
        answer = (self._state.answer or "").strip()
        if self._card is not None and self._card.msg_id is not None:
            # The progress card becomes a Done STATUS indicator only — the full result is
            # delivered as a separate bot text message below, not crammed into the card.
            if self._state.phase == "done":
                self._state.answer = "✅ Done — result in the reply below."
            await self._card.finalize(self._state)
        # The actual result is ALWAYS sent as a normal bot message.
        if answer:
            self.lark.send_text(self.chat_id, answer[:4000])
        self.lark.swap_to_done(message_id, onit)
        if self._adapter is not None and self._adapter.session_id:
            session_store.set_session_id(
                self.scope, self._adapter.session_id, str(self.cfg.workdir), agent=self.cfg.agent
            )

    async def _emit(self, event) -> None:
        if self._watchdog is not None:
            self._watchdog.bump()
        if self._stuck_open:
            # Activity resumed on its own (e.g. a slow API finally answered) —
            # dismiss the stuck card so it doesn't sit in chat looking actionable.
            self._stuck_open = False
            print(f"[turn {self.scope}] activity resumed — dismissing stuck card",
                  file=sys.stderr, flush=True)
            self._resolve_stuck_card("recovered")
        if self._state is None or self._card is None:
            return
        st = self._state
        if isinstance(event, TextEvent):
            st.answer += event.text
            st.status = "Writing…"
        elif isinstance(event, ThinkingEvent):
            st.status = "Thinking…"
        elif isinstance(event, ToolUseEvent):
            if event.name not in st.tools:
                st.tools.append(event.name)
            st.status = f"Using {event.name}"
            print(f"[turn {self.scope}] tool_use {event.name}: "
                  + repr(event.input)[:200], file=sys.stderr, flush=True)
        elif isinstance(event, ToolResultEvent):
            st.status = "Continuing…" if not event.is_error else "Tool error"
        elif isinstance(event, UsageEvent):
            pass
        elif isinstance(event, ErrorEvent):
            st.phase = "error"
            st.status = event.message
            print(f"[turn {self.scope}] ERROR: {event.message}", file=sys.stderr, flush=True)
        elif isinstance(event, DoneEvent):
            print(f"[turn {self.scope}] done; tools={st.tools}; "
                  f"answer_len={len(st.answer)}", file=sys.stderr, flush=True)
        # NOTE: do not push a card update per event — the deferred _card_loop updates the
        # progress card on a fixed cadence (card_interval_sec); we only accumulate state here.

    async def _approval_cb(self, tool: str, inp: dict) -> str:
        # Pause the turn on an approval card; resolved by a card-action tap, reply, or timeout.
        print(f"[turn {self.scope}] approval requested: {tool}", file=sys.stderr, flush=True)
        if self._state is not None:
            self._state.status = f"⏸ Waiting for approval: {tool}"
        if self._card is not None and self._card.msg_id is not None:
            await self._card.update(self._state)
        return await self.approvals.request(
            chat_id=self.chat_id,
            scope=self.scope,
            tool=tool,
            inp=inp,
            context=(self._state.prompt[:200] if self._state else ""),
        )

    async def _on_stuck(self) -> None:
        """Watchdog fired: no stream events for ``stuck_timeout`` seconds. Log it and
        ASK the user (card) instead of silently interrupting — the graceful control
        interrupt cannot break a process wedged on e.g. a hung API stream."""
        secs = self.cfg.stuck_timeout
        print(f"[turn {self.scope}] watchdog: no stream events for {secs}s — posting stuck card",
              file=sys.stderr, flush=True)
        try:
            if self._state is not None:
                self._state.status = f"⚠️ no activity for {secs}s — see the stuck card in chat"
                if self._card is not None:
                    await self._card.update(self._state)
            self._stuck_card_msg = self.lark.send_card(
                self.chat_id,
                render_stuck_card(scope=self.scope, seconds=secs,
                                  prompt=self._state.prompt if self._state else ""),
            )
            self._stuck_open = True
        except Exception as e:  # never wedge the watchdog task
            print(f"[scope {self.scope}] stuck card failed: {e!r}", file=sys.stderr, flush=True)

    async def resolve_stuck(self, verb: str) -> None:
        """User tapped a button on the stuck card: wait / kill / restart."""
        if not self._stuck_open:
            return  # stale tap (turn already recovered or ended)
        self._stuck_open = False
        if verb == "stuck_wait":
            print(f"[turn {self.scope}] stuck card: user chose to keep waiting; "
                  f"watchdog re-armed", file=sys.stderr, flush=True)
            self._resolve_stuck_card("wait")
            if self._watchdog is not None:
                self._watchdog.start()  # fires again (new card) if still silent
        elif verb == "stuck_kill":
            print(f"[turn {self.scope}] stuck card: user chose KILL — killing agent process",
                  file=sys.stderr, flush=True)
            self._resolve_stuck_card("kill")
            self._stop_flag = True
            if self._state is not None:
                self._state.status = "Killed (stuck)"
            if self._adapter is not None:
                try:
                    await self._adapter.kill()
                except Exception as e:
                    print(f"[scope {self.scope}] kill error: {e!r}", file=sys.stderr, flush=True)
            # EOF unblocks run_turn, which finalizes as Stopped; the adapter is dropped
            # so the NEXT message builds a fresh process instead of writing to a corpse.
            self._adapter = None
        elif verb == "stuck_restart":
            print(f"[turn {self.scope}] stuck card: user chose RESTART BRIDGE",
                  file=sys.stderr, flush=True)
            self._resolve_stuck_card("restart")
            if self._restart_cb is not None:
                self._restart_cb()
            else:
                self.lark.send_text(
                    self.chat_id, "(restart unavailable here — use: feishu-bridge stop && feishu-bridge up)")
        else:
            self._stuck_open = True  # unknown verb: leave the card actionable

    def _resolve_stuck_card(self, chosen: str) -> None:
        if self._stuck_card_msg is None:
            return
        msg_id, self._stuck_card_msg = self._stuck_card_msg, None
        try:
            self.lark.update_card(msg_id, render_stuck_card_resolved(chosen=chosen))
        except Exception:
            pass
