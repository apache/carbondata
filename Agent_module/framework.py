"""
framework.py
============
Core abstractions: AgentStatus, AgentMessage, BaseAgent.

BaseAgent subclasses implement `_process_impl` and inherit a lifecycle
wrapper (`process`) that updates status, records history, and dispatches
callbacks. Callbacks may be sync or async.
"""

from __future__ import annotations

import inspect
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Union


class AgentStatus(Enum):
    IDLE = "idle"
    PROCESSING = "processing"
    ERROR = "error"
    STOPPED = "stopped"


@dataclass
class AgentMessage:
    content: Any
    sender: str
    timestamp: float = field(default_factory=time.time)
    message_type: str = "text"
    metadata: dict[str, Any] = field(default_factory=dict)


CallbackResult = Union[None, Awaitable[None]]
AgentCallback = Callable[[AgentMessage, AgentMessage], CallbackResult]


class BaseAgent(ABC):
    """Base class for all agents."""

    def __init__(self, name: str, description: str = "") -> None:
        self.name = name
        self.description = description
        self.status = AgentStatus.IDLE
        self.message_history: list[AgentMessage] = []
        self._callbacks: list[AgentCallback] = []

    async def process(self, message: AgentMessage) -> AgentMessage:
        """Dispatch *message* to `_process_impl` with lifecycle bookkeeping."""
        self.status = AgentStatus.PROCESSING
        self.message_history.append(message)

        try:
            result = await self._process_impl(message)
        except Exception as exc:
            self.status = AgentStatus.ERROR
            return AgentMessage(
                content=f"Error: {exc}",
                sender=self.name,
                message_type="error",
            )

        self.status = AgentStatus.IDLE
        await self._notify_callbacks(message, result)
        return result

    @abstractmethod
    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        """Agent-specific processing logic."""

    def add_callback(self, callback: AgentCallback) -> None:
        self._callbacks.append(callback)

    async def _notify_callbacks(
        self, input_msg: AgentMessage, output_msg: AgentMessage
    ) -> None:
        """Fire every registered callback. Callbacks may be sync or async."""
        for cb in self._callbacks:
            try:
                result = cb(input_msg, output_msg)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass

    def get_recent_messages(self, count: int = 10) -> list[AgentMessage]:
        return self.message_history[-count:]
