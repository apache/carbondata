"""Verify that BaseAgent dispatches both sync and async callbacks."""

from __future__ import annotations

import pytest

from Agent_module import AgentMessage, BaseAgent


class _UpperAgent(BaseAgent):
    def __init__(self):
        super().__init__(name="UpperAgent", description="uppercases")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        return AgentMessage(content=str(message.content).upper(), sender=self.name)


@pytest.mark.asyncio
async def test_sync_callback_fires():
    agent = _UpperAgent()
    log: list = []

    def cb(input_msg: AgentMessage, output_msg: AgentMessage) -> None:
        log.append((input_msg.content, output_msg.content))

    agent.add_callback(cb)
    await agent.process(AgentMessage(content="hi", sender="test"))

    assert log == [("hi", "HI")]


@pytest.mark.asyncio
async def test_async_callback_is_awaited():
    agent = _UpperAgent()
    log: list = []

    async def async_cb(input_msg: AgentMessage, output_msg: AgentMessage) -> None:
        log.append(output_msg.content)

    agent.add_callback(async_cb)
    await agent.process(AgentMessage(content="hi", sender="test"))

    # If the coroutine wasn't awaited, log would still be empty.
    assert log == ["HI"]


@pytest.mark.asyncio
async def test_sync_and_async_callbacks_both_fire():
    agent = _UpperAgent()
    log: list = []

    def sync_cb(i: AgentMessage, o: AgentMessage) -> None:
        log.append(("sync", o.content))

    async def async_cb(i: AgentMessage, o: AgentMessage) -> None:
        log.append(("async", o.content))

    agent.add_callback(sync_cb)
    agent.add_callback(async_cb)
    await agent.process(AgentMessage(content="x", sender="test"))

    assert ("sync", "X") in log
    assert ("async", "X") in log


@pytest.mark.asyncio
async def test_crashing_callback_does_not_break_agent():
    agent = _UpperAgent()
    survived = []

    def bad_sync(i, o):
        raise RuntimeError("sync boom")

    async def bad_async(i, o):
        raise RuntimeError("async boom")

    def good(i, o):
        survived.append(o.content)

    agent.add_callback(bad_sync)
    agent.add_callback(bad_async)
    agent.add_callback(good)

    response = await agent.process(AgentMessage(content="ok", sender="test"))
    assert response.content == "OK"
    assert survived == ["OK"]
