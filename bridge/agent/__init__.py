"""Agent layer abstraction.

``AgentAdapter`` is the pluggable boundary: the runtime talks to any adapter the same
way. ``ClaudeAdapter`` (the only implementation for now) drives the hand-rolled
:class:`bridge.transport.Transport`. A future non-Claude (or SDK-backed) adapter can
slot in without touching the runtime.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol, Union


# --- AgentEvent union (the normalized stream the runtime/cards consume) ---------

@dataclass
class SystemEvent:
    session_id: str | None = None
    cwd: str | None = None
    model: str | None = None


@dataclass
class TextEvent:
    text: str


@dataclass
class ThinkingEvent:
    text: str


@dataclass
class ToolUseEvent:
    id: str
    name: str
    input: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResultEvent:
    id: str
    output: str = ""
    is_error: bool = False


@dataclass
class UsageEvent:
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0


@dataclass
class DoneEvent:
    session_id: str | None = None
    reason: str = "normal"


@dataclass
class ErrorEvent:
    message: str
    reason: str = "error"


AgentEvent = Union[
    SystemEvent, TextEvent, ThinkingEvent, ToolUseEvent, ToolResultEvent, UsageEvent, DoneEvent, ErrorEvent
]

# A callback the adapter calls to surface approval requests to the chat layer.
# Returns one of "allow" / "deny" / "deny_stop".
ApprovalCallback = Callable[[str, dict[str, Any]], Awaitable[str]]

# A callback the adapter calls to forward each AgentEvent to the runtime/cards.
Emit = Callable[[AgentEvent], Awaitable[None]]


class AgentAdapter(Protocol):
    """What the runtime expects of any agent backend."""

    async def start(self) -> None: ...
    async def run_turn(self, prompt: str, emit: Emit) -> dict[str, Any]: ...
    async def interrupt(self) -> None: ...
    async def kill(self) -> None: ...
    async def stop(self) -> None: ...


def make_adapter(cfg: Any, *, resume: str | None = None,
                 approval_callback: ApprovalCallback | None = None,
                 stderr_sink: Any = None) -> AgentAdapter:
    """Build the agent backend selected by ``cfg.agent`` ("claude" | "codex")."""
    if cfg.agent == "codex":
        from .codex_adapter import CodexAdapter

        return CodexAdapter(cfg, resume=resume, approval_callback=approval_callback,
                            stderr_sink=stderr_sink)
    from .claude_adapter import ClaudeAdapter

    return ClaudeAdapter(cfg, resume=resume, approval_callback=approval_callback,
                         stderr_sink=stderr_sink)
