"""
01_data_models.py
=================
Tests every data model class used by the LLM-oriented Agent format:
  ToolDefinition, ToolCall, Message, Memory, Task, AgentState, AgentConfig, Agent

Run:
    python3 Agent_module/Example/01_data_models.py
"""

import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

# ============================================================================
# Data Models (self-contained)
# ============================================================================

@dataclass
class ToolDefinition:
    name: str
    description: str
    parameters: Dict[str, Any]
    required: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {
                    "type": "object",
                    "properties": self.parameters,
                    "required": self.required,
                },
            },
        }


@dataclass
class ToolCall:
    id: str = field(default_factory=lambda: f"call_{uuid4().hex[:16]}")
    name: str = ""
    arguments: Dict[str, Any] = field(default_factory=dict)
    result: Optional[Any] = None
    status: Literal["pending", "success", "failed"] = "pending"
    error: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "arguments": self.arguments,
            "result": self.result,
            "status": self.status,
            "error": self.error,
            "timestamp": self.timestamp,
        }


@dataclass
class Message:
    role: Literal["system", "user", "assistant", "tool"]
    content: str
    id: str = field(default_factory=lambda: f"msg_{uuid4().hex[:16]}")
    name: Optional[str] = None
    tool_calls: List[ToolCall] = field(default_factory=list)
    tool_call_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "id": self.id,
            "role": self.role,
            "content": self.content,
            "timestamp": self.timestamp,
        }
        if self.name:
            data["name"] = self.name
        if self.tool_calls:
            data["tool_calls"] = [tc.to_dict() for tc in self.tool_calls]
        if self.tool_call_id:
            data["tool_call_id"] = self.tool_call_id
        if self.metadata:
            data["metadata"] = self.metadata
        return data


@dataclass
class Memory:
    short_term: List[Message] = field(default_factory=list)
    long_term: List[Dict[str, Any]] = field(default_factory=list)
    working_memory: Dict[str, Any] = field(default_factory=dict)
    max_short_term: int = 20

    def add_to_short_term(self, message: Message) -> None:
        self.short_term.append(message)
        if len(self.short_term) > self.max_short_term:
            self.short_term.pop(0)

    def add_to_long_term(self, key: str, value: Any, importance: float = 0.5) -> None:
        self.long_term.append({
            "key": key,
            "value": value,
            "importance": importance,
            "timestamp": datetime.now().isoformat(),
        })

    def to_dict(self) -> Dict[str, Any]:
        return {
            "short_term": [m.to_dict() for m in self.short_term],
            "long_term": self.long_term,
            "working_memory": self.working_memory,
        }


@dataclass
class Task:
    id: str = field(default_factory=lambda: f"task_{uuid4().hex[:16]}")
    description: str = ""
    status: Literal["pending", "in_progress", "completed", "failed"] = "pending"
    priority: int = 0
    subtasks: List["Task"] = field(default_factory=list)
    result: Optional[Any] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "description": self.description,
            "status": self.status,
            "priority": self.priority,
            "subtasks": [st.to_dict() for st in self.subtasks],
            "result": self.result,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }


@dataclass
class AgentState:
    mode: Literal["idle", "thinking", "acting", "waiting"] = "idle"
    current_task: Optional[Task] = None
    task_queue: List[Task] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)
    iteration_count: int = 0
    max_iterations: int = 10
    last_action: Optional[str] = None
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "current_task": self.current_task.to_dict() if self.current_task else None,
            "task_queue": [t.to_dict() for t in self.task_queue],
            "context": self.context,
            "iteration_count": self.iteration_count,
            "max_iterations": self.max_iterations,
            "last_action": self.last_action,
            "last_updated": self.last_updated,
        }


@dataclass
class AgentConfig:
    model: str = "claude-sonnet-4-5"
    temperature: float = 0.7
    max_tokens: int = 4096
    top_p: float = 0.9
    enable_memory: bool = True
    enable_reasoning: bool = True
    reasoning_effort: Literal["low", "medium", "high"] = "medium"
    streaming: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "top_p": self.top_p,
            "enable_memory": self.enable_memory,
            "enable_reasoning": self.enable_reasoning,
            "reasoning_effort": self.reasoning_effort,
            "streaming": self.streaming,
        }


@dataclass
class Agent:
    id: str = field(default_factory=lambda: f"agent_{uuid4().hex[:12]}")
    name: str = "Assistant"
    role: str = "general_assistant"
    description: str = "A helpful AI assistant"
    system_prompt: str = "You are a helpful AI assistant."
    config: AgentConfig = field(default_factory=AgentConfig)
    tools: List[ToolDefinition] = field(default_factory=list)
    memory: Memory = field(default_factory=Memory)
    state: AgentState = field(default_factory=AgentState)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    version: str = "1.0.0"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_tool(self, tool: ToolDefinition) -> None:
        self.tools.append(tool)

    def add_message(self, message: Message) -> None:
        self.memory.add_to_short_term(message)

    def create_task(self, description: str, priority: int = 0) -> Task:
        task = Task(description=description, priority=priority)
        self.state.task_queue.append(task)
        self.state.task_queue.sort(key=lambda t: t.priority, reverse=True)
        return task

    def update_state(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            if hasattr(self.state, key):
                setattr(self.state, key, value)
        self.state.last_updated = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "role": self.role,
            "description": self.description,
            "system_prompt": self.system_prompt,
            "config": self.config.to_dict(),
            "tools": [t.to_dict() for t in self.tools],
            "memory": self.memory.to_dict(),
            "state": self.state.to_dict(),
            "created_at": self.created_at,
            "version": self.version,
            "metadata": self.metadata,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


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
# Tests
# ============================================================================

def test_tool_definition() -> None:
    section("ToolDefinition")
    tool = ToolDefinition(
        name="web_search",
        description="Search the web",
        parameters={"query": {"type": "string", "description": "search term"}},
        required=["query"],
    )
    d = tool.to_dict()
    check("type is 'function'",             d["type"] == "function")
    check("name preserved",                 d["function"]["name"] == "web_search")
    check("required list preserved",        d["function"]["parameters"]["required"] == ["query"])
    check("query parameter present",        "query" in d["function"]["parameters"]["properties"])


def test_tool_call() -> None:
    section("ToolCall")
    tc = ToolCall(name="execute_code", arguments={"code": "print(1)"}, result="1", status="success")
    d = tc.to_dict()
    check("id auto-generated",              tc.id.startswith("call_"))
    check("name preserved",                 d["name"] == "execute_code")
    check("status is success",              d["status"] == "success")
    check("result preserved",               d["result"] == "1")
    check("error is None by default",       d["error"] is None)

    failed_tc = ToolCall(name="bad_tool", status="failed", error="timeout")
    check("error field set",                failed_tc.to_dict()["error"] == "timeout")


def test_message() -> None:
    section("Message")
    tc = ToolCall(name="search", arguments={"q": "hello"}, status="success", result="found")

    user_msg = Message(role="user", content="Hello", metadata={"user_id": "u1"})
    d = user_msg.to_dict()
    check("id auto-generated",              user_msg.id.startswith("msg_"))
    check("role preserved",                 d["role"] == "user")
    check("content preserved",              d["content"] == "Hello")
    check("metadata included when set",     "metadata" in d)
    check("tool_calls absent when empty",   "tool_calls" not in d)

    asst_msg = Message(role="assistant", content="I'll search for that", tool_calls=[tc])
    d2 = asst_msg.to_dict()
    check("tool_calls included when set",   "tool_calls" in d2)
    check("tool_call name correct",         d2["tool_calls"][0]["name"] == "search")

    tool_msg = Message(role="tool", content="found", name="search", tool_call_id=tc.id)
    d3 = tool_msg.to_dict()
    check("tool_call_id preserved",         d3["tool_call_id"] == tc.id)
    check("name preserved",                 d3["name"] == "search")


def test_memory() -> None:
    section("Memory")
    mem = Memory(max_short_term=3)

    for i in range(5):
        mem.add_to_short_term(Message(role="user", content=f"msg {i}"))

    check("sliding window enforced (max 3)",    len(mem.short_term) == 3)
    check("oldest messages dropped",            mem.short_term[0].content == "msg 2")
    check("newest message retained",            mem.short_term[-1].content == "msg 4")

    mem.add_to_long_term("fact_1", "sky is blue", importance=0.9)
    check("long-term entry added",              len(mem.long_term) == 1)
    check("importance stored",                  mem.long_term[0]["importance"] == 0.9)

    mem.working_memory["current_topic"] = "testing"
    d = mem.to_dict()
    check("working_memory serialised",          d["working_memory"]["current_topic"] == "testing")
    check("short_term serialised",              len(d["short_term"]) == 3)
    check("long_term serialised",               len(d["long_term"]) == 1)


def test_task() -> None:
    section("Task")
    parent = Task(description="Write tests", priority=10)
    child  = Task(description="Write unit tests", priority=5)
    parent.subtasks.append(child)

    check("id auto-generated",              parent.id.startswith("task_"))
    check("default status is pending",      parent.status == "pending")
    check("subtask appended",               len(parent.subtasks) == 1)

    parent.status = "completed"
    parent.result = "All tests written"
    parent.completed_at = datetime.now().isoformat()
    d = parent.to_dict()
    check("status updated",                 d["status"] == "completed")
    check("result preserved",               d["result"] == "All tests written")
    check("completed_at set",               d["completed_at"] is not None)
    check("subtask serialised",             len(d["subtasks"]) == 1)


def test_agent_state() -> None:
    section("AgentState")
    state = AgentState()
    check("default mode is idle",           state.mode == "idle")
    check("task_queue starts empty",        len(state.task_queue) == 0)

    task = Task(description="do something")
    state.current_task = task
    state.mode = "acting"
    state.iteration_count = 2
    d = state.to_dict()
    check("mode updated",                   d["mode"] == "acting")
    check("iteration_count updated",        d["iteration_count"] == 2)
    check("current_task serialised",        d["current_task"]["description"] == "do something")


def test_agent_config() -> None:
    section("AgentConfig")
    cfg = AgentConfig(model="claude-opus-4", temperature=0.2, reasoning_effort="high")
    d = cfg.to_dict()
    check("model preserved",                d["model"] == "claude-opus-4")
    check("temperature preserved",          d["temperature"] == 0.2)
    check("reasoning_effort preserved",     d["reasoning_effort"] == "high")
    check("defaults preserved",             d["max_tokens"] == 4096)


def test_agent_full() -> None:
    section("Agent — full lifecycle")

    agent = Agent(
        name="CodeBot",
        role="developer",
        description="Writes code",
        system_prompt="You write clean Python.",
        config=AgentConfig(model="claude-sonnet-4-5", temperature=0.3),
    )

    # Tools
    agent.add_tool(ToolDefinition(
        name="run_code",
        description="Execute Python",
        parameters={"code": {"type": "string"}},
        required=["code"],
    ))
    check("tool registered",                len(agent.tools) == 1)

    # Messages
    agent.add_message(Message(role="system",    content=agent.system_prompt))
    agent.add_message(Message(role="user",      content="Write fibonacci"))
    agent.add_message(Message(role="assistant", content="Here it is"))
    check("messages in short-term memory",  len(agent.memory.short_term) == 3)

    # Tasks — priority ordering
    t_low  = agent.create_task("low priority task",  priority=1)
    t_high = agent.create_task("high priority task", priority=9)
    check("high-priority task sorted first",   agent.state.task_queue[0].priority == 9)
    check("low-priority task sorted second",   agent.state.task_queue[1].priority == 1)

    # State update
    agent.update_state(mode="thinking", current_task=t_high, iteration_count=1)
    check("mode updated via update_state",  agent.state.mode == "thinking")
    check("current_task set",               agent.state.current_task.description == "high priority task")

    # Serialisation round-trip
    d = agent.to_dict()
    check("id starts with agent_",          d["id"].startswith("agent_"))
    check("name preserved",                 d["name"] == "CodeBot")
    check("config.model preserved",         d["config"]["model"] == "claude-sonnet-4-5")
    check("tools serialised",               len(d["tools"]) == 1)
    check("memory.short_term serialised",   len(d["memory"]["short_term"]) == 3)
    check("state.mode serialised",          d["state"]["mode"] == "thinking")

    # to_json / valid JSON
    json_str = agent.to_json()
    parsed = json.loads(json_str)
    check("to_json produces valid JSON",    parsed["name"] == "CodeBot")


# ============================================================================
# Entry point
# ============================================================================

def run() -> bool:
    global _passed, _failed
    _passed = _failed = 0

    print("\n" + "=" * 60)
    print("  01 — Agent Data Models")
    print("=" * 60)

    test_tool_definition()
    test_tool_call()
    test_message()
    test_memory()
    test_task()
    test_agent_state()
    test_agent_config()
    test_agent_full()

    total = _passed + _failed
    print(f"\n{'=' * 60}")
    print(f"  Result: {_passed}/{total} passed", "✓" if _failed == 0 else "✗")
    print(f"{'=' * 60}")
    return _failed == 0


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
