# Agent Module

A Python multi-agent framework providing an abstract agent base class, an orchestration manager, and a thread-safe async context sharing system.

---

## Directory Structure

```
Agent_module/
├── Framework.py                    # Core abstractions: BaseAgent, AgentMessage, AgentStatus
├── Manager.py                      # Multi-agent orchestration and routing
├── Implementation.py               # Built-in agents: Echo / Calculator / Weather / Translator
├── Chat_Agent.py                   # Rule-based conversational agent
├── ContextStore.py                 # Unified async context store (recommended)
├── SharedContext.py                # Synchronous shared memory (legacy)
├── ContextBridge.py                # Pub/sub bridge for SharedContext (legacy)
├── Agent_Demo.py                   # Full agent system demo
├── ContextStore_Demo.py            # ContextStore feature demo
├── SharedContext_Demo.py           # SharedContext feature demo
└── Example/
    └── Agent_format_example.py     # Complete LLM-oriented agent data structure example
```

---

## Core Modules

### Framework.py — Base Abstraction

Shared base class and data models for all agents.

```python
from Framework import BaseAgent, AgentMessage, AgentStatus
import time

class MyAgent(BaseAgent):
    def __init__(self):
        super().__init__(name="MyAgent", description="An example agent")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        return AgentMessage(
            content=f"Processed: {message.content}",
            sender=self.name,
            timestamp=time.time()
        )
```

**`AgentMessage` fields**

| Field | Type | Description |
|---|---|---|
| `content` | `Any` | Message payload |
| `sender` | `str` | Sender identifier |
| `timestamp` | `float` | Unix timestamp |
| `message_type` | `str` | Type label (default: `"text"`) |
| `metadata` | `dict` | Arbitrary additional data |

**`AgentStatus` enum**: `IDLE` / `PROCESSING` / `ERROR` / `STOPPED`

---

### Manager.py — Orchestration Layer

Register multiple agents with support for unicast, broadcast, and capability discovery.

```python
from Manager import AgentManager

manager = AgentManager()
manager.register_agent(MyAgent())

# Unicast to a specific agent
response = await manager.send_message("MyAgent", "hello")

# Broadcast to all agents concurrently (asyncio.gather)
responses = await manager.broadcast_message("ping")

# Discover agents by capability keyword
agents = manager.find_agent_by_capability("calculation")

# System statistics
stats = manager.get_system_stats()
```

---

### ContextStore.py — Async Context Store (Recommended)

The unified data exchange layer for multi-agent systems. Replaces the legacy `SharedContext` + `ContextBridge` pair.

#### Features

| Feature | Description |
|---|---|
| Async-first | All read/write operations are `async`, protected by `asyncio.Lock` |
| Write history | Each key retains the last 50 write records |
| P2P channels | `send` / `receive` / `inbox` FIFO queues between agents |
| Pub/sub | Subscribers are notified after every `put()`; supports sync and async callbacks |
| TTL eviction | Lazy expiry on reads; batch eviction via `cleanup_expired()` |
| Namespace isolation | Multi-tenancy support; `ContextNamespace` provides a scoped proxy view |
| Merge strategy | Last-write-wins by version number; supports cross-store merging |
| Serialization | Full snapshot and restore via `to_dict()` / `to_json()` |

#### State Read / Write

```python
from ContextStore import ContextStore, ContextNamespace

store = ContextStore()
ns = ContextNamespace(store, "pipeline_v1")

# Write a value
record = await ns.put("status", "running", writer="coordinator")

# Read the current value
value = await ns.get("status")

# Read the full entry (current record + history)
entry = await ns.get_entry("status")

# Write history, newest-first
history = await ns.history("status")

# Write with TTL (seconds)
await ns.put("heartbeat", {"alive": True}, writer="worker", ttl=30)

# Write with tags
await ns.put("result", data, writer="worker", tags=["output"])

# Query by writer and/or tags
entries = await ns.query(writer="worker", tags=["output"])
```

#### P2P Message Channels

```python
# Send a message from coordinator to worker
await store.send("coordinator", "worker", {"cmd": "run"})

# Receive the next message from a specific channel
msg = await store.receive("coordinator", "worker")

# Collect all pending messages for worker (across all senders, sorted by time)
inbox = await store.inbox("worker")
```

#### Pub/Sub

```python
async def on_change(key: str, record) -> None:
    print(f"Changed: {key} = {record.value}")

# Subscribe to all keys in the namespace
ns.subscribe("monitor", on_change)

# Subscribe to specific keys only
ns.subscribe("monitor", on_change, keys=["status", "result"])

# Unsubscribe
ns.unsubscribe("monitor")
```

> Writers do not receive their own notifications. Callbacks fire after the lock is released to prevent re-entrant deadlocks.

#### Maintenance and Serialization

```python
# Remove all expired entries
removed = await ns.cleanup_expired()

# Deep-copy snapshot of current state
snapshot = await ns.snapshot()

# Merge another ContextStore (last-write-wins by version)
merged = await store.merge(other_store, namespace="pipeline_v1")

# Store statistics
stats = await store.stats()

# Full JSON serialization
json_str = await store.to_json()
```

---

### Built-in Agents

| Agent | Description |
|---|---|
| `EchoAgent` | Echoes back every received message |
| `CalculatorAgent` | Evaluates math expressions (character whitelist filter) |
| `WeatherAgent` | Simulated weather lookup (4 preset cities; random data for others) |
| `TranslatorAgent` | Simulated translation; target language set via `metadata.target_language` |
| `ChatAgent` | Rule-based conversational agent with greeting / Q&A / routing intent detection |

---

## Quick Start

### Single Agent

```python
import asyncio
import time
from Framework import AgentMessage
from Implementation import CalculatorAgent

async def main():
    agent = CalculatorAgent()
    response = await agent.process(AgentMessage(
        content="(15 + 25) * 2",
        sender="User",
        timestamp=time.time()
    ))
    print(response.content)  # (15 + 25) * 2 = 80

asyncio.run(main())
```

### Multi-Agent Orchestration

```python
import asyncio
from Manager import AgentManager
from Implementation import CalculatorAgent, WeatherAgent
from Chat_Agent import ChatAgent

async def main():
    manager = AgentManager()
    manager.register_agent(CalculatorAgent())
    manager.register_agent(WeatherAgent())
    manager.register_agent(ChatAgent())

    result = await manager.send_message("CalculatorAgent", "3.14 * 2")
    print(result.content)

    weather = await manager.send_message("WeatherAgent", "Tokyo")
    print(weather.content)

asyncio.run(main())
```

### Shared Context Between Agents

```python
import asyncio
from ContextStore import ContextStore, ContextNamespace

async def main():
    store = ContextStore()
    ns = ContextNamespace(store, "my_pipeline")

    # Coordinator assigns a task
    await ns.put("task", {"id": "001", "desc": "Process data"}, writer="coordinator")
    await ns.put("status", "pending", writer="coordinator")

    # Worker processes the task
    task = await ns.get("task")
    await ns.put("status", "in_progress", writer="worker")
    await ns.put("result", {"processed": 1000}, writer="worker")
    await ns.put("status", "completed", writer="worker")

    # Inspect write history
    for r in await ns.history("status"):
        print(f"v{r.version} [{r.writer}] -> {r.value}")

asyncio.run(main())
```

---

## Running the Demos

```bash
# Agent system demo (single agent + multi-agent broadcast)
python3 Agent_Demo.py

# ContextStore feature demo (pub/sub, channels, TTL, merge, serialization)
python3 ContextStore_Demo.py

# SharedContext feature demo (legacy synchronous API)
python3 SharedContext_Demo.py

# Agent data structure format demo (LLM-oriented)
python3 Example/Agent_format_example.py
```

---

## Module Evolution

```
SharedContext.py + ContextBridge.py   <- Legacy (synchronous, not thread-safe)
            |
            v  refactored
        ContextStore.py               <- Current recommendation (async, thread-safe)
```

`SharedContext` and `ContextBridge` are retained for reference. New development should use `ContextStore` directly.

---

## Notes

- **`ContextBridge.py`**: Loads its dependency via `importlib` using a `.python` file extension, which does not match the current `.py` extension. Direct import will fail. Refer to `SharedContext_Demo.py` for the correct loading pattern if you need the legacy API.
- **`CalculatorAgent`**: Uses a character whitelist before calling `eval()`. Suitable only for trusted input. Replace with a dedicated math parser library for production use.
- **`SharedContext`** is not thread-safe. Use `ContextStore` for any concurrent workload.
