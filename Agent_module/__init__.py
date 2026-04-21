"""
Agent_module
============
Multi-agent framework with an async context store.

Public API:

    from Agent_module import (
        AgentMessage, AgentStatus, BaseAgent,
        AgentManager,
        EchoAgent, CalculatorAgent, WeatherAgent, TranslatorAgent, ChatAgent,
        ContextStore, ContextNamespace, ContextRecord, ContextEntry, ContextMessage,
    )
"""

from .framework import AgentMessage, AgentStatus, BaseAgent
from .manager import AgentManager
from .agents import (
    EchoAgent,
    CalculatorAgent,
    WeatherAgent,
    TranslatorAgent,
    ChatAgent,
)
from .context_store import (
    ContextStore,
    ContextNamespace,
    ContextRecord,
    ContextEntry,
    ContextMessage,
)

__all__ = [
    "AgentMessage",
    "AgentStatus",
    "BaseAgent",
    "AgentManager",
    "EchoAgent",
    "CalculatorAgent",
    "WeatherAgent",
    "TranslatorAgent",
    "ChatAgent",
    "ContextStore",
    "ContextNamespace",
    "ContextRecord",
    "ContextEntry",
    "ContextMessage",
]
