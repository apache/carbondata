# Quick Start for CarbonData's Agent

CarbonData as Agent metadata storage , how to support Multi Agents work together. 

**Requirements**: Python 3.10+ — no third-party packages needed, stdlib only.

---

## Step 1 — Run a Built-in Agent

The simplest starting point: instantiate one of the built-in agents and send it a message.

```python
# step1_single_agent.py
import asyncio
import time
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from Framework import AgentMessage
from Implementation import CalculatorAgent, WeatherAgent, TranslatorAgent

async def main():
    # --- Calculator ---
    calc = CalculatorAgent()
    response = await calc.process(AgentMessage(
        content="(10 + 5) * 4",
        sender="User",
        timestamp=time.time()
    ))
    print(response.content)          # (10 + 5) * 4 = 60

    # --- Weather ---
    weather = WeatherAgent()
    response = await weather.process(AgentMessage(
        content="Tokyo",
        sender="User",
        timestamp=time.time()
    ))
    print(response.content)

    # --- Translator (target language via metadata) ---
    translator = TranslatorAgent()
    response = await translator.process(AgentMessage(
        content="Good morning",
        sender="User",
        timestamp=time.time(),
        metadata={"target_language": "french"}
    ))
    print(response.content)

asyncio.run(main())
```

---

## Step 2 — Write Your Own Agent

Subclass `BaseAgent` and implement `_process_impl`. That is the only requirement.

```python
# step2_custom_agent.py
import asyncio
import time
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from Framework import BaseAgent, AgentMessage

class GreetingAgent(BaseAgent):
    def __init__(self):
        super().__init__(
            name="GreetingAgent",
            description="Returns a personalised greeting"
        )

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        name = message.metadata.get("user_name", "stranger")
        return AgentMessage(
            content=f"Hello, {name}! You said: '{message.content}'",
            sender=self.name,
            timestamp=time.time(),
            message_type="greeting"
        )

async def main():
    agent = GreetingAgent()
    response = await agent.process(AgentMessage(
        content="Nice to meet you",
        sender="User",
        timestamp=time.time(),
        metadata={"user_name": "Alice"}
    ))
    print(response.content)
    # Hello, Alice! You said: 'Nice to meet you'

    print("Agent status:", agent.status.value)          # idle
    print("Messages processed:", len(agent.message_history))  # 1

asyncio.run(main())
```

---

## Step 3 — Manage Multiple Agents

`AgentManager` handles registration, routing, and broadcast. Use it when you have more than one agent.

```python
# step3_manager.py
import asyncio
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from Manager import AgentManager
from Implementation import CalculatorAgent, WeatherAgent, TranslatorAgent
from Chat_Agent import ChatAgent

async def main():
    manager = AgentManager()
    manager.register_agent(CalculatorAgent())
    manager.register_agent(WeatherAgent())
    manager.register_agent(TranslatorAgent())
    manager.register_agent(ChatAgent())

    # --- Unicast: send to one agent by name ---
    response = await manager.send_message("CalculatorAgent", "99 / 3")
    print(response.content)            # 99 / 3 = 33.0

    response = await manager.send_message("WeatherAgent", "London")
    print(response.content)

    # --- Broadcast: send to every agent concurrently ---
    print("\n--- Broadcast ---")
    responses = await manager.broadcast_message("Hello!")
    for agent_name, resp in responses.items():
        print(f"  {agent_name}: {resp.content[:60]}")

    # --- Capability discovery: find agents by keyword ---
    print("\n--- Discovery ---")
    print(manager.find_agent_by_capability("weather"))
    print(manager.find_agent_by_capability("calculation"))

    # --- System statistics ---
    print("\n--- Stats ---")
    stats = manager.get_system_stats()
    for k, v in stats.items():
        print(f"  {k}: {v}")

asyncio.run(main())
```

---

## Step 4 — Share State Between Agents with ContextStore

`ContextStore` is the recommended way for agents to exchange data. It is async, namespace-isolated, and keeps a full write history per key.

```python
# step4_context_store.py
import asyncio
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ContextStore import ContextStore, ContextNamespace

async def main():
    store = ContextStore()
    ns = ContextNamespace(store, "pipeline_v1")

    # --- coordinator writes a task ---
    await ns.put("task",   {"id": "t1", "desc": "process sales data"}, writer="coordinator")
    await ns.put("status", "pending",                                   writer="coordinator")

    # --- worker reads the task and updates state ---
    task = await ns.get("task")
    print("Task received:", task)

    await ns.put("status", "in_progress", writer="worker")
    await ns.put("result", {"rows": 5000, "errors": 0}, writer="worker", tags=["output"])
    await ns.put("status", "completed",   writer="worker")

    # --- read the final status ---
    print("Final status:", await ns.get("status"))

    # --- inspect the full write history for "status" (newest first) ---
    print("\nStatus history:")
    for record in await ns.history("status"):
        print(f"  v{record.version}  [{record.writer}]  ->  {record.value!r}")

    # --- query all entries written by "worker" ---
    print("\nWorker entries:")
    for entry in await ns.query(writer="worker"):
        print(f"  {entry.current.key}: {entry.current.value}")

asyncio.run(main())
```

---

## Step 5 — React to Changes with Pub/Sub

Subscribe a callback to be notified whenever a key is written.

```python
# step5_pubsub.py
import asyncio
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ContextStore import ContextStore, ContextNamespace, ContextRecord

store = ContextStore()
ns    = ContextNamespace(store, "pipeline_v1")

# Callback can be sync or async
async def on_status_change(key: str, record: ContextRecord) -> None:
    print(f"  [monitor] {key} -> {record.value!r}  (writer: {record.writer})")

async def main():
    # Subscribe to specific keys
    ns.subscribe("monitor", on_status_change, keys=["status"])

    print("Writing status updates...")
    await ns.put("status", "pending",     writer="coordinator")
    await ns.put("status", "in_progress", writer="worker")
    await ns.put("status", "completed",   writer="worker")

    # The writer does not receive its own notifications.
    # Subscribing with no keys list watches the entire namespace.
    ns.subscribe("logger", lambda k, r: print(f"  [logger] {k}={r.value}"))
    await ns.put("result", {"score": 0.98}, writer="worker")

asyncio.run(main())
```

Expected output:
```
Writing status updates...
  [monitor] status -> 'pending'     (writer: coordinator)
  [monitor] status -> 'in_progress' (writer: worker)
  [monitor] status -> 'completed'   (writer: worker)
  [logger] result={'score': 0.98}
```

---

## Step 6 — Send Direct Messages Between Agents

Use channels for point-to-point communication that does not go through the shared state.

```python
# step6_channels.py
import asyncio
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ContextStore import ContextStore

async def main():
    store = ContextStore()

    # coordinator sends two commands to worker
    await store.send("coordinator", "worker", {"cmd": "run_report", "format": "pdf"})
    await store.send("coordinator", "worker", {"cmd": "upload",     "dest": "s3://bucket"})

    # auditor sends a health check to worker
    await store.send("auditor", "worker", {"cmd": "health_check"})

    # worker drains its entire inbox (all senders, sorted by time)
    inbox = await store.inbox("worker")
    print(f"Worker has {len(inbox)} message(s):")
    for msg in inbox:
        print(f"  from={msg.sender:<15} payload={msg.payload}")

    # inbox is now empty
    print("Inbox after drain:", await store.inbox("worker"))

asyncio.run(main())
```

---

## Step 7 — Write Time-Limited Data with TTL

Entries with a TTL are automatically evicted after the specified number of seconds.

```python
# step7_ttl.py
import asyncio
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ContextStore import ContextStore, ContextNamespace

async def main():
    store = ContextStore()
    ns    = ContextNamespace(store, "pipeline_v1")

    # Write a heartbeat that expires in 3 seconds
    await ns.put("heartbeat", {"alive": True, "load": 0.42}, writer="worker", ttl=3)

    print("Immediate read:", await ns.get("heartbeat"))  # {'alive': True, 'load': 0.42}

    await asyncio.sleep(3.5)

    print("After 3.5 s:   ", await ns.get("heartbeat"))  # None  (expired)

    # Optional: batch-remove any remaining stale entries
    removed = await ns.cleanup_expired()
    print(f"Cleaned up {removed} expired key(s)")

asyncio.run(main())
```

---

## Step 8 — Connect Agents to ContextStore

Combine the agent framework with `ContextStore` to build a full pipeline.

```python
# step8_pipeline.py
import asyncio
import time
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from Framework import BaseAgent, AgentMessage
from ContextStore import ContextStore, ContextNamespace, ContextRecord

store = ContextStore()
ns    = ContextNamespace(store, "pipeline")


class CoordinatorAgent(BaseAgent):
    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        task = {"id": "t1", "desc": message.content}
        await ns.put("task",   task,      writer=self.name)
        await ns.put("status", "pending", writer=self.name)
        return AgentMessage(content=f"Task assigned: {task['id']}",
                            sender=self.name, timestamp=time.time())


class WorkerAgent(BaseAgent):
    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        task = await ns.get("task")
        if task is None:
            return AgentMessage(content="No task found",
                                sender=self.name, timestamp=time.time())

        await ns.put("status", "in_progress", writer=self.name)
        # Simulate work
        await asyncio.sleep(0.1)
        await ns.put("result", {"task_id": task["id"], "output": "done"},
                     writer=self.name, tags=["output"])
        await ns.put("status", "completed", writer=self.name)
        return AgentMessage(content="Work completed", sender=self.name, timestamp=time.time())


async def main():
    coordinator = CoordinatorAgent(name="coordinator")
    worker      = WorkerAgent(name="worker")

    # Monitor all context changes
    def log(key: str, record: ContextRecord) -> None:
        print(f"  [monitor] {key} = {record.value!r}")
    ns.subscribe("monitor", log)

    print("=== Coordinator assigns task ===")
    r = await coordinator.process(AgentMessage(
        content="Analyse Q1 sales", sender="orchestrator", timestamp=time.time()
    ))
    print(r.content)

    print("\n=== Worker processes task ===")
    r = await worker.process(AgentMessage(
        content="start", sender="orchestrator", timestamp=time.time()
    ))
    print(r.content)

    print("\n=== Final state ===")
    print("status:", await ns.get("status"))
    print("result:", await ns.get("result"))

    print("\n=== Status history ===")
    for rec in await ns.history("status"):
        print(f"  v{rec.version} [{rec.writer}] -> {rec.value!r}")

asyncio.run(main())
```

---

## Patterns at a Glance

| Goal | What to use |
|---|---|
| Build a new agent | Subclass `BaseAgent`, implement `_process_impl` |
| Run multiple agents | `AgentManager.register_agent()` + `send_message()` |
| Share data between agents | `ContextStore` / `ContextNamespace` — `put()` / `get()` |
| React to data changes | `ns.subscribe(agent_id, callback)` |
| Send agent-to-agent commands | `store.send()` / `store.inbox()` |
| Expire data automatically | `ns.put(..., ttl=<seconds>)` |
| Audit what was written | `ns.history(key)` |
| Filter entries by author/tag | `ns.query(writer=..., tags=[...])` |

---

## Legacy API (SharedContext)

If you need the older synchronous API, use `SharedContext` + `ContextBridge` directly. The `SharedContext_Demo.py` file shows the full pattern. Note that `ContextBridge` loads its dependency using a custom `importlib` loader — refer to the demo's `_load()` helper for the correct usage.

For all new development, use `ContextStore`.
