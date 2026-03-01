"""
03_agent_manager.py
===================
Tests AgentManager: registration, unicast, broadcast, capability
discovery, system statistics, and concurrent execution.

Run:
    python3 Agent_module/Example/03_agent_manager.py
"""

import asyncio
import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))

from _loader import load

_m = load("Framework", "Implementation", "Manager", "Chat_Agent")
BaseAgent       = _m.BaseAgent
AgentMessage    = _m.AgentMessage
AgentStatus     = _m.AgentStatus
AgentManager    = _m.AgentManager
EchoAgent       = _m.EchoAgent
CalculatorAgent = _m.CalculatorAgent
WeatherAgent    = _m.WeatherAgent
TranslatorAgent = _m.TranslatorAgent
ChatAgent       = _m.ChatAgent

# ============================================================================
# Test helpers
# ============================================================================

_passed = 0
_failed = 0


def check(label: str, condition: bool) -> None:
    global _passed, _failed
    if condition:
        _passed += 1
        print(f"  PASS  {label}")
    else:
        _failed += 1
        print(f"  FAIL  {label}")


def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


# ============================================================================
# Helper agent
# ============================================================================

class TagAgent(BaseAgent):
    """Returns a fixed tag — useful for verifying broadcast routing."""

    def __init__(self, name: str, tag: str):
        super().__init__(name=name, description=f"Returns tag: {tag}")
        self.tag = tag

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        return AgentMessage(
            content=self.tag,
            sender=self.name,
            timestamp=time.time(),
        )


# ============================================================================
# Tests
# ============================================================================

async def test_registration() -> None:
    section("Agent registration")

    manager = AgentManager()
    a1 = EchoAgent()
    a2 = CalculatorAgent()

    manager.register_agent(a1)
    manager.register_agent(a2)

    check("two agents registered",         len(manager.agents) == 2)
    check("EchoAgent found by name",        "EchoAgent" in manager.agents)
    check("CalculatorAgent found by name",  "CalculatorAgent" in manager.agents)

    # Re-registering with the same name replaces the entry
    a3 = EchoAgent()
    manager.register_agent(a3)
    check("re-register keeps count at 2",   len(manager.agents) == 2)
    check("replaced agent is the new one",  manager.agents["EchoAgent"] is a3)


async def test_send_message() -> None:
    section("Unicast — send_message")

    manager = AgentManager()
    manager.register_agent(CalculatorAgent())
    manager.register_agent(WeatherAgent())

    r = await manager.send_message("CalculatorAgent", "6 * 7")
    check("calculator returns correct result",  "42" in r.content)
    check("sender is CalculatorAgent",          r.sender == "CalculatorAgent")

    r = await manager.send_message("WeatherAgent", "Tokyo")
    check("weather response contains Tokyo",    "Tokyo" in r.content)

    # Unknown agent should raise ValueError
    try:
        await manager.send_message("NonExistentAgent", "hello")
        check("ValueError raised for unknown agent", False)
    except ValueError:
        check("ValueError raised for unknown agent", True)


async def test_broadcast() -> None:
    section("Broadcast — broadcast_message")

    manager = AgentManager()
    manager.register_agent(TagAgent("Alpha", "alpha-reply"))
    manager.register_agent(TagAgent("Beta",  "beta-reply"))
    manager.register_agent(TagAgent("Gamma", "gamma-reply"))

    responses = await manager.broadcast_message("ping")

    check("all three agents responded",         len(responses) == 3)
    check("Alpha response correct",             responses["Alpha"].content == "alpha-reply")
    check("Beta response correct",              responses["Beta"].content  == "beta-reply")
    check("Gamma response correct",             responses["Gamma"].content == "gamma-reply")


async def test_broadcast_with_failure() -> None:
    section("Broadcast — partial failure handled gracefully")

    class FailingAgent(BaseAgent):
        def __init__(self):
            super().__init__(name="FailingAgent", description="always fails")

        async def _process_impl(self, m: AgentMessage) -> AgentMessage:
            raise RuntimeError("boom")

    manager = AgentManager()
    manager.register_agent(TagAgent("Good", "ok"))
    manager.register_agent(FailingAgent())

    responses = await manager.broadcast_message("test")

    check("both agents have a response entry",      len(responses) == 2)
    check("Good agent responded normally",           responses["Good"].content == "ok")
    # FailingAgent's error is caught inside BaseAgent.process(), not in broadcast
    check("FailingAgent returned an error message",  responses["FailingAgent"].message_type == "error")


async def test_capability_discovery() -> None:
    section("Capability discovery — find_agent_by_capability")

    manager = AgentManager()
    manager.register_agent(CalculatorAgent())   # description: "Performs mathematical calculations"
    manager.register_agent(WeatherAgent())      # description: "Provides weather information"
    manager.register_agent(TranslatorAgent())   # description: "Translates text between languages"

    calc_agents    = manager.find_agent_by_capability("calculation")
    weather_agents = manager.find_agent_by_capability("weather")
    trans_agents   = manager.find_agent_by_capability("translat")
    none_agents    = manager.find_agent_by_capability("nonexistent_xyz")

    check("calculator found by 'calculation'",  "CalculatorAgent" in calc_agents)
    check("weather found by 'weather'",         "WeatherAgent"    in weather_agents)
    check("translator found by 'translat'",     "TranslatorAgent" in trans_agents)
    check("empty list for no match",            none_agents == [])


async def test_system_stats() -> None:
    section("System statistics")

    manager = AgentManager()
    manager.register_agent(EchoAgent())
    manager.register_agent(CalculatorAgent())

    await manager.send_message("EchoAgent", "msg 1")
    await manager.send_message("EchoAgent", "msg 2")
    await manager.send_message("CalculatorAgent", "1 + 1")

    stats = manager.get_system_stats()

    check("total_agents is 2",                  stats["total_agents"] == 2)
    check("total_messages_processed is 3",      stats["total_messages_processed"] == 3)
    check("conversation_log_entries is 3",      stats["conversation_log_entries"] == 3)
    check("agent_statuses dict present",        "agent_statuses" in stats)
    check("EchoAgent status is idle",           stats["agent_statuses"]["EchoAgent"] == "idle")


async def test_metadata_routing() -> None:
    section("Metadata routing — TranslatorAgent target_language")

    manager = AgentManager()
    manager.register_agent(TranslatorAgent())

    r = await manager.send_message(
        "TranslatorAgent",
        "Good morning",
        metadata={"target_language": "german"}
    )
    check("german translation triggered",       "GERMAN" in r.content)
    check("original text present",              "Good morning" in r.content)


async def test_chat_agent() -> None:
    section("ChatAgent — basic intent detection")

    manager = AgentManager()
    manager.register_agent(ChatAgent())

    greetings = ["hello", "hi there", "hey", "greetings"]
    for g in greetings:
        r = await manager.send_message("ChatAgent", g, metadata={"user_id": "u1"})
        check(f"  greeting '{g}' gets a reply",      len(r.content) > 0)
        check(f"  message_type is chat_response",    r.message_type == "chat_response")

    r = await manager.send_message("ChatAgent", "what is your name?", metadata={"user_id": "u1"})
    check("question gets an answer",                 len(r.content) > 0)

    # Conversation count should increment in context
    r1 = await manager.send_message("ChatAgent", "hello", metadata={"user_id": "ctx_user"})
    # Read count1 immediately — the metadata dict is a live reference that will
    # be mutated by subsequent messages for the same user_id.
    count1 = r1.metadata.get("user_context", {}).get("conversation_count", 0)
    r2 = await manager.send_message("ChatAgent", "hello", metadata={"user_id": "ctx_user"})
    count2 = r2.metadata.get("user_context", {}).get("conversation_count", 0)
    check("conversation_count increments per user",  count2 == count1 + 1)


# ============================================================================
# Entry point
# ============================================================================

async def _run_all() -> bool:
    global _passed, _failed
    _passed = _failed = 0

    await test_registration()
    await test_send_message()
    await test_broadcast()
    await test_broadcast_with_failure()
    await test_capability_discovery()
    await test_system_stats()
    await test_metadata_routing()
    await test_chat_agent()

    total = _passed + _failed
    print(f"\n{'=' * 60}")
    print(f"  Result: {_passed}/{total} passed", "✓" if _failed == 0 else "✗")
    print(f"{'=' * 60}")
    return _failed == 0


def run() -> bool:
    print("\n" + "=" * 60)
    print("  03 — AgentManager")
    print("=" * 60)
    return asyncio.run(_run_all())


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
