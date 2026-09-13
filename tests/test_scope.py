"""ScopeRunner orchestration tests (no real claude, no real Lark).

FakeAdapter + FakeLark verify: OnIt->Done emoji cycle + streaming-card lifecycle,
single-flight rejection (with /stop hint), and /stop interrupt.
"""
from __future__ import annotations

import asyncio

from bridge.agent import DoneEvent, TextEvent
from bridge.approvals import ApprovalManager
from bridge.config import BridgeConfig
from bridge.lark import Lark
from bridge.scope import ScopeRunner


class FakeLark(Lark):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.onit: list[str] = []
        self.done: list[str] = []
        self.deleted: list[str] = []
        self.cards: list[dict] = []
        self.updates: list[tuple] = []
        self.texts: list[str] = []

    def stamp_onit(self, mid):
        self.onit.append(mid)
        return "onit-rid"

    def swap_to_done(self, mid, onit_rid):
        if onit_rid:
            self.deleted.append(onit_rid)
        self.done.append(mid)
        return "done-rid"

    def send_text(self, cid, text):
        self.texts.append(text)
        return f"m-{len(self.texts)}"

    def send_card(self, cid, card):
        self.cards.append(card)
        return f"card-{len(self.cards)}"

    def update_card(self, mid, card):
        self.updates.append((mid, card))
        return True


class FakeAdapter:
    def __init__(self):
        self.interrupted = False
        self.started = False
        self.session_id = "s-fake"

    async def start(self):
        self.started = True

    async def run_turn(self, prompt, emit, on_frame=None):
        await emit(TextEvent(text="the answer"))
        await emit(DoneEvent(session_id="s-fake"))
        return {"session_id": "s-fake", "cost_usd": 0.002}

    async def interrupt(self):
        self.interrupted = True

    async def kill(self):
        pass

    async def stop(self):
        pass


class WedgedAdapter(FakeAdapter):
    """Hangs forever until kill() releases it (a turn stuck on a hung API stream)."""

    def __init__(self):
        super().__init__()
        self.killed = False
        self._release: asyncio.Event | None = None

    async def run_turn(self, prompt, emit, on_frame=None):
        self._release = asyncio.Event()
        await self._release.wait()
        return {"session_id": "s-fake"}

    async def kill(self):
        self.killed = True
        if self._release is not None:
            self._release.set()


class LateAdapter(FakeAdapter):
    """Silent past the stuck timeout, then recovers on its own."""

    async def run_turn(self, prompt, emit, on_frame=None):
        await asyncio.sleep(0.25)
        await emit(TextEvent(text="finally"))
        await emit(DoneEvent(session_id="s-fake"))
        return {"session_id": "s-fake"}


class SlowAdapter(FakeAdapter):
    async def run_turn(self, prompt, emit, on_frame=None):
        await asyncio.sleep(0.3)
        return {"session_id": "s-fake"}


def _cfg(tmp_path):
    cfg = BridgeConfig.load()
    cfg.workdir = tmp_path
    cfg.stuck_timeout = 999  # don't trip the watchdog in these tests
    return cfg


def test_short_turn_delivers_text_no_card(tmp_path):
    """A turn that finishes under card_defer_sec sends NO progress card; the OnIt emoji
    acknowledges receipt and the answer is delivered as a text message."""
    async def go():
        cfg = _cfg(tmp_path)  # card_defer_sec defaults to 60
        lark = FakeLark(cfg)
        runner = ScopeRunner("oc_1", "oc_1", cfg, lark, ApprovalManager(lark, cfg),
                             adapter_factory=lambda: FakeAdapter())
        await runner.handle_message({"message_id": "m1", "chat_id": "oc_1", "text": "hi"})
        assert lark.onit == ["m1"]            # OnIt stamped (the ack)
        assert "m1" in lark.done              # Done stamped
        assert "onit-rid" in lark.deleted     # OnIt removed on swap
        assert any("the answer" in t for t in lark.texts)  # answer delivered as text
        assert lark.cards == []               # no progress card (short turn)

    asyncio.run(go())


def test_long_turn_shows_progress_card(tmp_path):
    """A turn still running past card_defer_sec shows a progress card (updated on done)."""
    async def go():
        cfg = _cfg(tmp_path)
        cfg.card_defer_sec = 0      # show the card immediately
        cfg.card_interval_sec = 10
        lark = FakeLark(cfg)
        runner = ScopeRunner("oc_1b", "oc_1b", cfg, lark, ApprovalManager(lark, cfg),
                             adapter_factory=lambda: SlowAdapter())
        await runner.handle_message({"message_id": "m1", "chat_id": "oc_1b", "text": "hi"})
        assert len(lark.cards) >= 1           # progress card created
        assert len(lark.updates) >= 1         # finalized to done
        assert "m1" in lark.done

    asyncio.run(go())


def test_second_message_rejected_with_stop_hint(tmp_path):
    async def go():
        cfg = _cfg(tmp_path)
        lark = FakeLark(cfg)
        runner = ScopeRunner("oc_2", "oc_2", cfg, lark, ApprovalManager(lark, cfg),
                             adapter_factory=lambda: SlowAdapter())
        first = asyncio.create_task(
            runner.handle_message({"message_id": "m1", "chat_id": "oc_2", "text": "first"}))
        await asyncio.sleep(0.05)  # let the first turn become busy
        await runner.handle_message({"message_id": "m2", "chat_id": "oc_2", "text": "second"})
        assert any("still working" in t and "/stop" in t for t in lark.texts)
        assert "m2" in lark.done
        await first

    asyncio.run(go())


def test_stop_interrupts_active_turn(tmp_path):
    async def go():
        cfg = _cfg(tmp_path)
        lark = FakeLark(cfg)
        slow = SlowAdapter()
        runner = ScopeRunner("oc_3", "oc_3", cfg, lark, ApprovalManager(lark, cfg),
                             adapter_factory=lambda: slow)
        task = asyncio.create_task(
            runner.handle_message({"message_id": "m1", "chat_id": "oc_3", "text": "x"}))
        await asyncio.sleep(0.05)
        ok = await runner.request_stop()
        assert ok is True
        assert slow.interrupted is True
        await task

    asyncio.run(go())


def _stuck_cfg(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.card_defer_sec = 999   # no progress card; the stuck card is what we assert on
    cfg.stuck_timeout = 0.1
    return cfg


def test_stuck_turn_posts_decision_card_then_kill(tmp_path):
    """Watchdog fires -> stuck decision card in chat; Kill kills the adapter, drops it,
    and the turn finalizes as Stopped."""
    async def go():
        cfg = _stuck_cfg(tmp_path)
        wedged = WedgedAdapter()
        lark = FakeLark(cfg)
        runner = ScopeRunner("oc_k", "oc_k", cfg, lark, ApprovalManager(lark, cfg),
                             adapter_factory=lambda: wedged)
        task = asyncio.create_task(
            runner.handle_message({"message_id": "m1", "chat_id": "oc_k", "text": "x"}))
        await asyncio.sleep(0.3)                    # watchdog fires -> stuck card
        assert runner._stuck_open is True
        assert any("Turn appears stuck" in str(c) for c in lark.cards)
        assert any("Kill turn" in str(c) for c in lark.cards)

        await runner.resolve_stuck("stuck_kill")
        await task
        assert wedged.killed is True
        assert runner._adapter is None               # next message rebuilds the process
        assert "m1" in lark.done                     # turn finalized
        assert any("Turn killed" in str(c) for _, c in lark.updates)

    asyncio.run(go())


def test_stuck_wait_rearms_watchdog(tmp_path):
    """Keep waiting re-arms the watchdog: still silent -> a fresh stuck card."""
    async def go():
        cfg = _stuck_cfg(tmp_path)
        wedged = WedgedAdapter()
        lark = FakeLark(cfg)
        runner = ScopeRunner("oc_w", "oc_w", cfg, lark, ApprovalManager(lark, cfg),
                             adapter_factory=lambda: wedged)
        task = asyncio.create_task(
            runner.handle_message({"message_id": "m1", "chat_id": "oc_w", "text": "x"}))
        await asyncio.sleep(0.3)
        assert sum("Turn appears stuck" in str(c) for c in lark.cards) == 1

        await runner.resolve_stuck("stuck_wait")
        assert any("Keeping waiting" in str(c) for _, c in lark.updates)
        await asyncio.sleep(0.3)                     # fires AGAIN after re-arm
        assert sum("Turn appears stuck" in str(c) for c in lark.cards) == 2

        await runner.resolve_stuck("stuck_kill")
        await task

    asyncio.run(go())


def test_stuck_recovery_dismisses_card(tmp_path):
    """If the turn recovers on its own, the stuck card is marked recovered and a
    late button tap becomes a no-op."""
    async def go():
        cfg = _stuck_cfg(tmp_path)
        lark = FakeLark(cfg)
        runner = ScopeRunner("oc_r", "oc_r", cfg, lark, ApprovalManager(lark, cfg),
                             adapter_factory=lambda: LateAdapter())
        task = asyncio.create_task(
            runner.handle_message({"message_id": "m1", "chat_id": "oc_r", "text": "x"}))
        await task                                    # stuck fired at ~0.1s, recovered at 0.25s
        assert runner._stuck_open is False
        assert any("recovered on its own" in str(c) for _, c in lark.updates)
        assert any("finally" in t for t in lark.texts)  # the answer still delivered
        await runner.resolve_stuck("stuck_kill")       # late tap: no-op
        assert runner._adapter is not None

    asyncio.run(go())


def test_stuck_restart_invokes_callback(tmp_path):
    async def go():
        cfg = _stuck_cfg(tmp_path)
        wedged = WedgedAdapter()
        lark = FakeLark(cfg)
        calls: list[str] = []
        runner = ScopeRunner("oc_x", "oc_x", cfg, lark, ApprovalManager(lark, cfg),
                             adapter_factory=lambda: wedged,
                             restart_cb=lambda: calls.append("restart"))
        task = asyncio.create_task(
            runner.handle_message({"message_id": "m1", "chat_id": "oc_x", "text": "x"}))
        await asyncio.sleep(0.3)
        await runner.resolve_stuck("stuck_restart")
        assert calls == ["restart"]
        assert any("restarting" in str(c) for _, c in lark.updates)
        # release the wedged turn so the test can end
        wedged._release.set()
        await task

    asyncio.run(go())
