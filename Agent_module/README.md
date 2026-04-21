# Agent Module

A small multi-agent framework with a thread-safe async context store.

**Requirements:** Python 3.10+. No third-party packages required (pytest only for the test suite).

---

## Layout

```
Agent_module/
├── __init__.py           # Re-exports the public API
├── framework.py          # BaseAgent, AgentMessage, AgentStatus
├── manager.py            # AgentManager — registry + routing
├── agents.py             # Built-in agents (Echo/Calculator/Weather/Translator/Chat)
├── safe_eval.py          # AST-based arithmetic evaluator (no eval())
├── context_store.py      # Async context store + P2P channels + pub/sub
├── demos/
│   ├── agent_demo.py            # Agent system walkthrough
│   └── context_store_demo.py    # ContextStore walkthrough
├── examples/
│   ├── 01_data_models.py        # LLM-oriented agent data format
│   ├── agent_format_example.py  # Same, with a worked example
│   ├── 02_framework_agents.py   # Integration tests for agents
│   ├── 03_agent_manager.py      # Integration tests for the manager
│   ├── 04_context_store.py      # Integration tests for ContextStore
│   └── run_all.py               # Runs every example and prints a summary
└── tests/
    ├── test_safe_eval.py        # pytest: safe_eval arithmetic + rejections
    └── test_async_callbacks.py  # pytest: sync/async callback dispatch
```

---

## Quick Start

### Single agent

```python
import asyncio
from Agent_module import AgentMessage, CalculatorAgent

async def main():
    agent = CalculatorAgent()
    response = await agent.process(AgentMessage(
        content="(15 + 25) * 2",
        sender="User",
    ))
    print(response.content)   # (15 + 25) * 2 = 80

asyncio.run(main())
```

### Multiple agents via `AgentManager`

```python
import asyncio
from Agent_module import AgentManager, CalculatorAgent, WeatherAgent, ChatAgent

async def main():
    manager = AgentManager()
    manager.register_agent(CalculatorAgent())
    manager.register_agent(WeatherAgent())
    manager.register_agent(ChatAgent())

    print((await manager.send_message("CalculatorAgent", "3.14 * 2")).content)
    print((await manager.send_message("WeatherAgent", "Tokyo")).content)

    responses = await manager.broadcast_message("Hello!")
    for name, r in responses.items():
        print(f"{name}: {r.content[:60]}")

asyncio.run(main())
```

### Writing your own agent

Subclass `BaseAgent` and implement `_process_impl`:

```python
from Agent_module import AgentMessage, BaseAgent

class GreetingAgent(BaseAgent):
    def __init__(self):
        super().__init__(name="GreetingAgent", description="Says hi")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        name = message.metadata.get("user_name", "stranger")
        return AgentMessage(content=f"Hello, {name}!", sender=self.name)
```

### Shared context across agents

```python
import asyncio
from Agent_module import ContextStore, ContextNamespace

async def main():
    store = ContextStore()
    ns = ContextNamespace(store, "my_pipeline")

    await ns.put("task", {"id": "001", "desc": "Process data"}, writer="coordinator")
    await ns.put("status", "pending", writer="coordinator")

    task = await ns.get("task")
    await ns.put("status", "in_progress", writer="worker")
    await ns.put("result", {"processed": 1000}, writer="worker")
    await ns.put("status", "completed", writer="worker")

    for r in await ns.history("status"):
        print(f"v{r.version} [{r.writer}] -> {r.value}")

asyncio.run(main())
```

---

## Public API

### `framework.py`

| Symbol | Notes |
|---|---|
| `AgentStatus` | Enum: `IDLE`, `PROCESSING`, `ERROR`, `STOPPED` |
| `AgentMessage` | Dataclass — `content`, `sender`, `timestamp` (default=now), `message_type`, `metadata` |
| `BaseAgent` | Abstract base. Subclasses implement `async _process_impl`. `add_callback()` accepts **sync or async** callables. |

### `manager.py`

`AgentManager` — `register_agent`, `send_message`, `broadcast_message` (concurrent via `asyncio.gather`), `find_agent_by_capability`, `get_system_stats`.

### `agents.py`

| Agent | Summary |
|---|---|
| `EchoAgent` | Echoes every input |
| `CalculatorAgent` | Arithmetic via `safe_eval` — only `+ - * / // % **` and numeric literals |
| `WeatherAgent` | Simulated: four preset cities, random data for others |
| `TranslatorAgent` | Simulated; target language from `metadata.target_language` |
| `ChatAgent` | Rule-based, keeps per-user context |

### `context_store.py`

| Feature | Description |
|---|---|
| Async-first | Every read/write is `async`, protected by `asyncio.Lock` |
| Write history | Up to 50 records per key |
| P2P channels | `send` / `receive` / `inbox` FIFO queues between agents |
| Pub/sub | Subscribers notified after `put()`; sync + async callbacks |
| TTL | Lazy eviction on reads; batch via `cleanup_expired()` |
| Namespaces | Multi-tenant isolation; `ContextNamespace` is a scoped proxy |
| Merge | Last-write-wins by version number |
| Serialisation | `snapshot()`, `to_dict()`, `to_json()` |

Writers do not receive their own notifications. Callbacks fire **after** the store lock is released — safe from re-entrant deadlocks.

---

## Running

Run as a package (from the repo root):

```bash
# Demos
python3 -m Agent_module.demos.agent_demo
python3 -m Agent_module.demos.context_store_demo

# Integration tests
python3 Agent_module/examples/run_all.py

# Unit tests
pytest Agent_module/tests/ -v
```

---

## Notes on security

`CalculatorAgent` uses `safe_eval.py`, which parses the input with `ast.parse` and only accepts arithmetic nodes — attribute access, function calls, names, comprehensions, and comparisons are rejected *before* any code runs. It is safe to feed untrusted input.
