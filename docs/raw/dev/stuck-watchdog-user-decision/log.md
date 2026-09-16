# Implementation log — stuck-watchdog-user-decision

- 2026-09-14 — incident that motivated this: bridge turn wedged ~7 min on a
  zero-byte streamed response from the API relay (ANTHROPIC_BASE_URL
  `api.z.ai`). Bridge + claude subprocess alive; claude idle in
  `do_epoll_wait` on one ESTAB socket (keepalive only, Recv-Q 0). Watchdog
  (180s) could not have rescued it anyway: its only tool was the control
  interrupt, which times out and is swallowed. Turn eventually self-recovered
  and completed — but the user saw "Working…" with zero feedback.
- Design choice: on stuck, **ask, don't act**. A silent auto-kill risks losing
  a turn that would have recovered (exactly tonight's case); a visible card
  gives the user the same escalation ladder the CLI has (wait / kill / restart).
- Restart from inside a running bridge: `supervisor.respawn()` spawns the
  replacement (same detached flags as `up`, rewrites the pidfile) and
  `Runtime.request_restart` then `os._exit(0)` — skipping asyncio teardown,
  which a wedged turn may be holding.
- Kill path relies on the transport's EOF semantics: `Transport.kill` force-
  kills the tree, `_read_loop` sees EOF, pushes the sentinel, `events()`
  returns, the turn finalizes as Stopped. Scope then nulls `_adapter` so the
  next message re-runs the factory (fresh process; fresh thread unless
  `FEISHU_RESUME_SESSIONS=1`).
- Subtle fix along the way: the watchdog's poll `tick` defaulted to a flat 5s,
  so a small `stuck_timeout` still waited 5s for its first check. Tick now
  scales to `min(tick, timeout/2)`.
- Tests run with `uv run --with pytest --with lark-oapi==1.7.1 python -m
  pytest tests/ -q` (the installed venv has no dev deps) — 62 passed.
- Noticed in passing (not addressed): `tests/fake_claude.py` has a stray
  mode-only chmod (100644→100755) in the working tree.
