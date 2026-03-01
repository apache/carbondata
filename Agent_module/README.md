# Agent Module

Python 多智能体框架，提供 Agent 抽象基类、编排管理器与线程安全的上下文共享系统。

---

## 目录结构

```
Agent_module/
├── Framework.py                    # 核心抽象：BaseAgent、AgentMessage、AgentStatus
├── Manager.py                      # 多 Agent 编排与调度
├── Implementation.py               # 内置 Agent 实现（Echo / Calculator / Weather / Translator）
├── Chat_Agent.py                   # 对话智能体
├── ContextStore.py                 # 统一异步上下文存储（推荐使用）
├── SharedContext.py                # 同步共享内存（旧版）
├── ContextBridge.py                # Pub/Sub 桥接器（旧版）
├── Agent_Demo.py                   # Agent 系统完整演示
├── ContextStore_Demo.py            # ContextStore 功能演示
├── SharedContext_Demo.py           # SharedContext 功能演示
└── Example/
    └── Agent_format_example.py     # 面向 LLM 的完整 Agent 数据结构示例
```

---

## 核心模块

### Framework.py — 基础抽象层

所有 Agent 的公共基类与数据模型。

```python
from Framework import BaseAgent, AgentMessage, AgentStatus

class MyAgent(BaseAgent):
    def __init__(self):
        super().__init__(name="MyAgent", description="示例 Agent")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        return AgentMessage(
            content=f"已处理：{message.content}",
            sender=self.name,
            timestamp=time.time()
        )
```

**`AgentMessage` 字段**

| 字段 | 类型 | 说明 |
|---|---|---|
| `content` | `Any` | 消息内容 |
| `sender` | `str` | 发送方标识 |
| `timestamp` | `float` | Unix 时间戳 |
| `message_type` | `str` | 消息类型标签（默认 `"text"`） |
| `metadata` | `dict` | 任意附加数据 |

**`AgentStatus` 枚举**：`IDLE` / `PROCESSING` / `ERROR` / `STOPPED`

---

### Manager.py — 编排层

注册多个 Agent，支持单播、广播与能力发现。

```python
from Manager import AgentManager

manager = AgentManager()
manager.register_agent(MyAgent())

# 单播
response = await manager.send_message("MyAgent", "hello")

# 广播（asyncio.gather 并发）
responses = await manager.broadcast_message("ping")

# 按关键词发现 Agent
agents = manager.find_agent_by_capability("calculation")

# 系统统计
stats = manager.get_system_stats()
```

---

### ContextStore.py — 异步上下文存储（推荐）

多 Agent 间的统一数据交换层，替代旧版 `SharedContext` + `ContextBridge`。

#### 主要特性

| 特性 | 说明 |
|---|---|
| 异步优先 | 所有读写操作为 `async`，`asyncio.Lock` 保护 |
| 写入历史 | 每个 key 保留最近 50 条写入记录 |
| P2P 消息通道 | `send` / `receive` / `inbox` FIFO 队列 |
| Pub/Sub | 写入后自动通知订阅者，支持同步/异步回调 |
| TTL 惰性驱逐 | 读取时自动检查过期，也可调用 `cleanup_expired()` |
| 命名空间隔离 | 多租户支持，`ContextNamespace` 提供代理视图 |
| 合并策略 | last-write-wins（按版本号），支持跨 Store 合并 |
| 序列化 | `to_dict()` / `to_json()` 完整快照与恢复 |

#### 状态读写

```python
from ContextStore import ContextStore, ContextNamespace

store = ContextStore()
ns = ContextNamespace(store, "pipeline_v1")

# 写入
record = await ns.put("status", "running", writer="coordinator")

# 读取当前值
value = await ns.get("status")

# 读取完整条目（含历史）
entry = await ns.get_entry("status")

# 写入历史（newest-first）
history = await ns.history("status")

# 带 TTL（秒）写入
await ns.put("heartbeat", {"alive": True}, writer="worker", ttl=30)

# 带标签写入
await ns.put("result", data, writer="worker", tags=["output"])

# 按条件查询
entries = await ns.query(writer="worker", tags=["output"])
```

#### P2P 消息通道

```python
# 发送
await store.send("coordinator", "worker", {"cmd": "run"})

# 接收单条
msg = await store.receive("coordinator", "worker")

# 收取所有发往 worker 的消息（跨所有 sender，按时间排序）
inbox = await store.inbox("worker")
```

#### Pub/Sub

```python
async def on_change(key: str, record) -> None:
    print(f"变更: {key} = {record.value}")

# 订阅命名空间内所有 key
ns.subscribe("monitor", on_change)

# 订阅指定 key
ns.subscribe("monitor", on_change, keys=["status", "result"])

# 取消订阅
ns.unsubscribe("monitor")
```

> 写入方不会收到自己触发的通知。回调在锁释放后执行，避免死锁。

#### 维护与序列化

```python
# 清理过期条目
removed = await ns.cleanup_expired()

# 深拷贝快照
snapshot = await ns.snapshot()

# 合并另一个 ContextStore（last-write-wins）
merged = await store.merge(other_store, namespace="pipeline_v1")

# 统计信息
stats = await store.stats()

# 完整序列化
json_str = await store.to_json()
```

---

### 内置 Agent 实现

| Agent | 说明 |
|---|---|
| `EchoAgent` | 回显收到的消息 |
| `CalculatorAgent` | 数学表达式计算（字符白名单过滤） |
| `WeatherAgent` | 模拟天气查询（内置 4 个城市，其余随机生成） |
| `TranslatorAgent` | 模拟多语言翻译（通过 `metadata.target_language` 指定目标语言） |
| `ChatAgent` | 基于规则的对话智能体，支持问候/问答/路由意图识别 |

---

## 快速开始

### 单 Agent 使用

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

### 多 Agent 编排

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

### 多 Agent 共享上下文

```python
import asyncio
from ContextStore import ContextStore, ContextNamespace

async def main():
    store = ContextStore()
    ns = ContextNamespace(store, "my_pipeline")

    # coordinator 分配任务
    await ns.put("task", {"id": "001", "desc": "处理数据"}, writer="coordinator")
    await ns.put("status", "pending", writer="coordinator")

    # worker 处理任务
    task = await ns.get("task")
    await ns.put("status", "in_progress", writer="worker")
    await ns.put("result", {"processed": 1000}, writer="worker")
    await ns.put("status", "completed", writer="worker")

    # 查看写入历史
    for r in await ns.history("status"):
        print(f"v{r.version} [{r.writer}] → {r.value}")

asyncio.run(main())
```

---

## 运行演示

```bash
# Agent 系统演示（单 Agent + 多 Agent 广播）
python3 Agent_Demo.py

# ContextStore 功能演示（pub/sub / 通道 / TTL / merge / 序列化）
python3 ContextStore_Demo.py

# SharedContext 功能演示（旧版同步 API）
python3 SharedContext_Demo.py

# Agent 数据结构格式演示（面向 LLM）
python3 Example/Agent_format_example.py
```

---

## 模块演化说明

```
SharedContext.py + ContextBridge.py   ← 旧版（同步，非线程安全）
            ↓ 重构升级
        ContextStore.py               ← 当前推荐（异步，线程安全）
```

`SharedContext` 与 `ContextBridge` 保留作为参考，新开发应直接使用 `ContextStore`。

---

## 注意事项

- **`ContextBridge.py`**：内部通过 `importlib` 加载 `.python` 扩展名文件，与当前 `.py` 扩展名不匹配，直接导入会报错。如需使用旧版 API，请参考 `SharedContext_Demo.py` 中的加载方式。
- **`CalculatorAgent`**：使用字符白名单过滤后调用 `eval()`，仅适用于受信任环境。生产场景建议替换为安全的数学解析库。
- **`SharedContext`** 非线程安全，并发场景请使用 `ContextStore`。
