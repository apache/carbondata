"""
02_framework_agents.py
======================
Tests BaseAgent subclassing, AgentStatus lifecycle, message history,
callbacks, and error handling from Framework.py and Implementation.py.

Run:
    python3 Agent_module/Example/02_framework_agents.py
"""

import asyncio
import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))

from _loader import load

_m = load("Framework", "Implementation")
BaseAgent   = _m.BaseAgent
AgentMessage = _m.AgentMessage
AgentStatus  = _m.AgentStatus
EchoAgent       = _m.EchoAgent
CalculatorAgent = _m.CalculatorAgent
WeatherAgent    = _m.WeatherAgent
TranslatorAgent = _m.TranslatorAgent

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


def msg(content: str, **meta) -> AgentMessage:
    return AgentMessage(content=content, sender="tester", timestamp=time.time(), metadata=meta)


# ============================================================================
# Custom agent used in tests
# ============================================================================

class UpperCaseAgent(BaseAgent):
    """Returns the message content uppercased."""

    def __init__(self):
        super().__init__(name="UpperCaseAgent", description="Converts text to uppercase")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        return AgentMessage(
            content=str(message.content).upper(),
            sender=self.name,
            timestamp=time.time(),
            message_type="uppercase_response",
        )


class BrokenAgent(BaseAgent):
    """Always raises an exception — tests error handling."""

    def __init__(self):
        super().__init__(name="BrokenAgent", description="Always fails")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        raise RuntimeError("intentional failure")


# ============================================================================
# Tests
# ============================================================================

async def test_custom_agent() -> None:
    section("Custom agent — UpperCaseAgent")

    agent = UpperCaseAgent()
    check("initial status is IDLE",     agent.status == AgentStatus.IDLE)
    check("name set correctly",         agent.name == "UpperCaseAgent")
    check("history starts empty",       len(agent.message_history) == 0)

    response = await agent.process(msg("hello world"))
    check("response content uppercased",    response.content == "HELLO WORLD")
    check("response sender is agent name",  response.sender == "UpperCaseAgent")
    check("message_type preserved",         response.message_type == "uppercase_response")
    check("status returns to IDLE",         agent.status == AgentStatus.IDLE)
    check("input added to history",         len(agent.message_history) == 1)

    # Send a second message
    await agent.process(msg("second message"))
    check("history grows with each message", len(agent.message_history) == 2)
    check("get_recent_messages(1) returns 1", len(agent.get_recent_messages(1)) == 1)
    check("get_recent_messages(5) returns 2", len(agent.get_recent_messages(5)) == 2)


async def test_error_handling() -> None:
    section("Error handling — BrokenAgent")

    agent = BrokenAgent()
    response = await agent.process(msg("trigger error"))

    check("response message_type is error",     response.message_type == "error")
    check("response content contains 'Error'",  "Error" in response.content)
    check("status set to ERROR",                agent.status == AgentStatus.ERROR)


async def test_callbacks() -> None:
    section("Callbacks")

    agent = UpperCaseAgent()
    log: list = []

    def record(input_msg: AgentMessage, output_msg: AgentMessage) -> None:
        log.append((input_msg.content, output_msg.content))

    agent.add_callback(record)
    await agent.process(msg("ping"))
    await agent.process(msg("pong"))

    check("callback fired for each message",    len(log) == 2)
    check("callback received correct input",    log[0][0] == "ping")
    check("callback received correct output",   log[0][1] == "PING")
    check("second callback entry correct",      log[1] == ("pong", "PONG"))

    # A crashing callback must not break the agent
    def bad_callback(i, o):
        raise ValueError("bad callback")

    agent.add_callback(bad_callback)
    response = await agent.process(msg("still works"))
    check("agent survives a crashing callback",     response.content == "STILL WORKS")


async def test_echo_agent() -> None:
    section("EchoAgent")

    agent = EchoAgent()
    response = await agent.process(msg("hello"))
    check("response starts with 'Echo:'",       response.content.startswith("Echo:"))
    check("original content echoed",            "hello" in response.content)
    check("message_type is echo_response",      response.message_type == "echo_response")


async def test_calculator_agent() -> None:
    section("CalculatorAgent")

    agent = CalculatorAgent()

    cases = [
        ("2 + 2",       "4"),
        ("10 * 5",      "50"),
        ("100 / 4",     "25.0"),
        ("(3 + 7) * 2", "20"),
    ]
    for expr, expected in cases:
        r = await agent.process(msg(expr))
        check(f"  {expr} = {expected}",     expected in r.content)

    # Invalid input should return an error message, not crash
    r = await agent.process(msg("__import__('os')"))
    check("invalid chars -> error message",     "error" in r.content.lower() or "Error" in r.content)

    r = await agent.process(msg("1 / 0"))
    check("division by zero -> error message",  "error" in r.content.lower() or "Error" in r.content)


async def test_weather_agent() -> None:
    section("WeatherAgent")

    agent = WeatherAgent()

    known_cities = ["New York", "London", "Tokyo", "Sydney"]
    for city in known_cities:
        r = await agent.process(msg(city))
        check(f"  {city}: contains city name",      city in r.content)
        check(f"  {city}: contains Temperature",    "Temperature" in r.content)
        check(f"  {city}: message_type correct",    r.message_type == "weather_report")

    # Unknown city should still return a response (simulated)
    r = await agent.process(msg("Atlantis"))
    check("unknown city returns simulated data",    "Atlantis" in r.content)
    check("simulated data has Temperature field",   "Temperature" in r.content)


async def test_translator_agent() -> None:
    section("TranslatorAgent")

    agent = TranslatorAgent()

    supported = ["spanish", "french", "german", "italian"]
    for lang in supported:
        r = await agent.process(msg("Hello", target_language=lang))
        check(f"  {lang}: lang name in response",   lang.upper() in r.content)
        check(f"  {lang}: message_type correct",    r.message_type == "translation")

    # Unsupported language
    r = await agent.process(msg("Hello", target_language="klingon"))
    check("unsupported language returns error text", "don't support" in r.content or "klingon" in r.content.lower())


# ============================================================================
# Entry point
# ============================================================================

async def _run_all() -> bool:
    global _passed, _failed
    _passed = _failed = 0

    await test_custom_agent()
    await test_error_handling()
    await test_callbacks()
    await test_echo_agent()
    await test_calculator_agent()
    await test_weather_agent()
    await test_translator_agent()

    total = _passed + _failed
    print(f"\n{'=' * 60}")
    print(f"  Result: {_passed}/{total} passed", "✓" if _failed == 0 else "✗")
    print(f"{'=' * 60}")
    return _failed == 0


def run() -> bool:
    print("\n" + "=" * 60)
    print("  02 — Framework Agents")
    print("=" * 60)
    return asyncio.run(_run_all())


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
