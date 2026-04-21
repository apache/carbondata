"""
agent_demo.py
=============
End-to-end demo of the agent system: single-agent usage, manager routing,
broadcast, capability discovery, and system stats.

Run:
    python3 -m Agent_module.demos.agent_demo
"""

from __future__ import annotations

import asyncio

from Agent_module import (
    AgentManager,
    AgentMessage,
    CalculatorAgent,
    ChatAgent,
    EchoAgent,
    TranslatorAgent,
    WeatherAgent,
)


async def demo_single_agents() -> None:
    print("=== Single Agent Demo ===\n")

    print("1. EchoAgent:")
    r = await EchoAgent().process(AgentMessage(content="Hello, world!", sender="User"))
    print(f"  {r.content}\n")

    print("2. CalculatorAgent:")
    r = await CalculatorAgent().process(AgentMessage(content="(15 + 25) * 2", sender="User"))
    print(f"  {r.content}\n")

    print("3. WeatherAgent:")
    r = await WeatherAgent().process(AgentMessage(content="Tokyo", sender="User"))
    print(f"  {r.content}\n")

    print("4. TranslatorAgent:")
    r = await TranslatorAgent().process(AgentMessage(
        content="Hello, how are you?",
        sender="User",
        metadata={"target_language": "french"},
    ))
    print(f"  {r.content}\n")

    print("5. ChatAgent:")
    r = await ChatAgent().process(AgentMessage(content="Hello! What can you do?", sender="User"))
    print(f"  {r.content}\n")


async def demo_agent_system() -> None:
    print("\n=== Agent System Demo ===\n")

    manager = AgentManager()
    for agent in (EchoAgent(), CalculatorAgent(), WeatherAgent(), TranslatorAgent(), ChatAgent()):
        manager.register_agent(agent)

    print(f"Registered {len(manager.agents)} agents: {', '.join(manager.agents)}")

    print("\n1. CalculatorAgent via Manager:")
    r = await manager.send_message("CalculatorAgent", "45 + 17 * 3")
    print(f"  {r.content}")

    print("\n2. WeatherAgent via Manager:")
    r = await manager.send_message("WeatherAgent", "London")
    print(f"  {r.content}")

    print("\n3. Broadcast:")
    responses = await manager.broadcast_message("Hello everyone!")
    for name, resp in responses.items():
        print(f"  {name}: {str(resp.content)[:60]}...")

    print("\n4. Capability discovery:")
    print(f"  calculation -> {manager.find_agent_by_capability('calculation')}")
    print(f"  weather     -> {manager.find_agent_by_capability('weather')}")

    print("\n5. System stats:")
    for k, v in manager.get_system_stats().items():
        print(f"  {k}: {v}")


async def main() -> None:
    await demo_single_agents()
    await demo_agent_system()


if __name__ == "__main__":
    asyncio.run(main())
