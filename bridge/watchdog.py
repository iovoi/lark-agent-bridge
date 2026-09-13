"""Stuck-turn watchdog.

Runs as a background task during a turn. If no stream event has arrived for
``stuck_timeout`` seconds AND no approval is pending (which legitimately blocks the
turn waiting for a human), it fires the ``on_stuck`` callback (the scope uses it to
interrupt the turn and report). Paused while an approval card is outstanding.
"""
from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable


class StuckWatchdog:
    def __init__(
        self,
        timeout: float,
        on_stuck: Callable[[], Awaitable[None]],
        *,
        is_approval_pending: Callable[[], bool] = lambda: False,
        tick: float = 5.0,
    ) -> None:
        self._timeout = timeout
        self._on_stuck = on_stuck
        self._is_approval_pending = is_approval_pending
        # Poll no slower than half the timeout, so a small timeout doesn't wait a
        # full default tick (5s) before its first check.
        self._tick = max(min(tick, timeout / 2.0), 0.05)
        self._task: asyncio.Task | None = None
        self._last_event = time.monotonic()
        self._fired = False

    def bump(self) -> None:
        """Call on every stream event to prove the turn is making progress."""
        self._last_event = time.monotonic()

    async def _run(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._tick)
                if self._fired:
                    return
                if self._is_approval_pending():
                    self._last_event = time.monotonic()  # waiting on a human is not "stuck"
                    continue
                if (time.monotonic() - self._last_event) > self._timeout:
                    self._fired = True
                    await self._on_stuck()
                    return
        except asyncio.CancelledError:
            return

    def start(self) -> None:
        self._last_event = time.monotonic()
        self._fired = False
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None
