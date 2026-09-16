# Tasks — stuck-watchdog-user-decision

- [x] `bridge/cards.py`: `render_stuck_card` / `render_stuck_card_resolved`
- [x] `bridge/transport.py`: `Transport.kill()` (force tree-kill, EOF unblocks `events()`)
- [x] `bridge/agent/*`: `kill()` on the adapter protocol + claude/codex impls
- [x] `bridge/scope.py`: `_on_stuck` logs + posts the decision card (no silent
      auto-interrupt); `resolve_stuck(wait|kill|restart)`; recovery/turn-end
      card dismissal; adapter dropped after kill
- [x] `bridge/runtime.py`: route `stuck_wait|stuck_kill|stuck_restart` card
      actions; `request_restart()` (respawn + `_exit`); `restart_cb` injected
      into scopes
- [x] `bridge/supervisor.py`: extract `_spawn()`, add `respawn()`
- [x] `bridge/watchdog.py`: tick auto-scales to `min(tick, timeout/2)` so small
      timeouts don't wait a full 5s tick before the first check
- [x] tests: decision card + kill, wait re-arm, self-recovery dismissal,
      restart callback (`tests/test_scope.py`)
- [x] full suite green (62 passed)
