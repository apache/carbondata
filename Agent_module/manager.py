"""
manager.py
==========
AgentManager — registers agents, routes messages, and collects statistics.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from .framework import AgentMessage, BaseAgent


class AgentManager:
    """Registry and dispatcher for a set of BaseAgent instances."""

    def __init__(self) -> None:
        self.agents: dict[str, BaseAgent] = {}
        self.conversation_log: list[dict[str, Any]] = []

    def register_agent(self, agent: BaseAgent) -> None:
        """Register *agent*. Re-registering by the same name replaces the entry."""
        self.agents[agent.name] = agent
        agent.add_callback(self._log_interaction)

    async def send_message(
        self,
        agent_name: str,
        content: Any,
        metadata: dict[str, Any] | None = None,
    ) -> AgentMessage:
        """Send *content* to a specific agent and return its response."""
        if agent_name not in self.agents:
            raise ValueError(f"Agent '{agent_name}' not found")

        message = AgentMessage(
            content=content,
            sender="User",
            metadata=metadata or {},
        )
        return await self.agents[agent_name].process(message)

    async def broadcast_message(
        self,
        content: Any,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, AgentMessage]:
        """Send *content* to every registered agent concurrently."""
        message = AgentMessage(
            content=content,
            sender="Broadcast",
            metadata=metadata or {},
        )

        names = list(self.agents.keys())
        responses = await asyncio.gather(
            *(self.agents[n].process(message) for n in names),
            return_exceptions=True,
        )

        results: dict[str, AgentMessage] = {}
        for name, response in zip(names, responses):
            if isinstance(response, Exception):
                results[name] = AgentMessage(
                    content=f"Error: {response}",
                    sender=name,
                    message_type="error",
                )
            else:
                results[name] = response
        return results

    def find_agent_by_capability(self, keyword: str) -> list[str]:
        """Return names of agents whose name or description contains *keyword*."""
        needle = keyword.lower()
        return [
            name
            for name, agent in self.agents.items()
            if needle in agent.name.lower() or needle in agent.description.lower()
        ]

    def _log_interaction(
        self, input_msg: AgentMessage, output_msg: AgentMessage
    ) -> None:
        self.conversation_log.append({
            "timestamp": time.time(),
            "input": {
                "content": input_msg.content,
                "sender": input_msg.sender,
                "type": input_msg.message_type,
            },
            "output": {
                "content": output_msg.content,
                "sender": output_msg.sender,
                "type": output_msg.message_type,
            },
            "processing_time": output_msg.timestamp - input_msg.timestamp,
        })

    def get_agent_statuses(self) -> dict[str, str]:
        return {name: agent.status.value for name, agent in self.agents.items()}

    def get_system_stats(self) -> dict[str, Any]:
        total_messages = sum(
            len(agent.message_history) for agent in self.agents.values()
        )
        return {
            "total_agents": len(self.agents),
            "total_messages_processed": total_messages,
            "conversation_log_entries": len(self.conversation_log),
            "agent_statuses": self.get_agent_statuses(),
        }
