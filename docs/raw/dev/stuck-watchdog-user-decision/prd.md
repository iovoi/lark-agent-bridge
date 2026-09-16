# PRD — Stuck-turn watchdog: ask the user instead of failing silently

## Problem

When a turn hangs with no stream events (observed 2026-09-14: the API relay at
`api.z.ai` accepted the request but streamed zero bytes for ~7 minutes), the old
`_on_stuck` handler silently:

1. set the turn to "stopped" and sent a control-protocol `interrupt` — which
   **times out after 10s and swallows the exception** (`transport.py`), so a
   process wedged on a network read is never actually interrupted;
2. logged **nothing**, leaving `bridge.log` blank past "progress card shown".

The user sees "Working…" forever; the only recovery is an external
`feishu-bridge stop && up`.

## Requirements

1. **Log when the watchdog fires** — a stderr line with the scope and idle time.
2. **Ask the user** — post a Feishu decision card when `FEISHU_STUCK_TIMEOUT`
   (default 180s) is exceeded, with three actions:
   - **Keep waiting** — re-arm the watchdog (fires again, new card, if still silent).
   - **Kill turn** — hard-kill the agent process tree; the EOF finalizes the turn
     as Stopped; the adapter is dropped so the next message builds a fresh
     process instead of writing to a corpse.
   - **Restart bridge** — spawn a replacement bridge process (supervisor
     `respawn()`, takes over the pidfile) and `os._exit(0)` the current one.
3. **Self-healing display** — if the turn recovers on its own (first stream
   event arrives), the stuck card is re-rendered as "recovered" so it doesn't
   sit in chat looking actionable; late button taps are no-ops.
4. Works for both agents: `kill()` added to the `AgentAdapter` protocol
   (claude: `Transport.kill` force-kills the tree; codex: exec-mode interrupt
   already is a hard kill).

## Non-goals

- No automatic kill/restart — the user decides.
- No change to `/stop` (still the graceful control-protocol interrupt).
- No new env vars; the existing `FEISHU_STUCK_TIMEOUT` governs the trigger.
