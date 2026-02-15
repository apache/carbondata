from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Literal
from datetime import datetime
from uuid import uuid4
import json

# ============================================================================
# Core Data Models
# ============================================================================

@dataclass
class ToolDefinition:
    """Tool/Function definition that agent can use"""
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
                    "required": self.required
                }
            }
        }

@dataclass
class ToolCall:
    """Represents a tool/function call made by the agent"""
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
            "timestamp": self.timestamp
        }

@dataclass
class Message:
    """Message in agent conversation"""
    role: Literal["system", "user", "assistant", "tool"]
    content: str
    id: str = field(default_factory=lambda: f"msg_{uuid4().hex[:16]}")
    name: Optional[str] = None  # For tool or user identification
    tool_calls: List[ToolCall] = field(default_factory=list)
    tool_call_id: Optional[str] = None  # For tool response messages
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        data = {
            "id": self.id,
            "role": self.role,
            "content": self.content,
            "timestamp": self.timestamp
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
    """Agent's memory system"""
    short_term: List[Message] = field(default_factory=list)  # Recent conversation
    long_term: List[Dict[str, Any]] = field(default_factory=list)  # Persistent facts
    working_memory: Dict[str, Any] = field(default_factory=dict)  # Current context
    max_short_term: int = 20
    
    def add_to_short_term(self, message: Message):
        """Add message to short-term memory with sliding window"""
        self.short_term.append(message)
        if len(self.short_term) > self.max_short_term:
            self.short_term.pop(0)
    
    def add_to_long_term(self, key: str, value: Any, importance: float = 0.5):
        """Add to long-term memory with importance scoring"""
        self.long_term.append({
            "key": key,
            "value": value,
            "importance": importance,
            "timestamp": datetime.now().isoformat()
        })
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "short_term": [msg.to_dict() for msg in self.short_term],
            "long_term": self.long_term,
            "working_memory": self.working_memory
        }

@dataclass
class Task:
    """Task/Goal for the agent to complete"""
    id: str = field(default_factory=lambda: f"task_{uuid4().hex[:16]}")
    description: str = ""
    status: Literal["pending", "in_progress", "completed", "failed"] = "pending"
    priority: int = 0  # Higher number = higher priority
    subtasks: List['Task'] = field(default_factory=list)
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
            "completed_at": self.completed_at
        }

@dataclass
class AgentState:
    """Current state of the agent"""
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
            "task_queue": [task.to_dict() for task in self.task_queue],
            "context": self.context,
            "iteration_count": self.iteration_count,
            "max_iterations": self.max_iterations,
            "last_action": self.last_action,
            "last_updated": self.last_updated
        }

@dataclass
class AgentConfig:
    """Agent configuration"""
    model: str = "claude-sonnet-4-5-20250929"
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
            "streaming": self.streaming
        }

# ============================================================================
# Main Agent Class
# ============================================================================

@dataclass
class Agent:
    """Complete AI Agent structure"""
    id: str = field(default_factory=lambda: f"agent_{uuid4().hex[:12]}")
    name: str = "Assistant"
    role: str = "general_assistant"
    description: str = "A helpful AI assistant"
    system_prompt: str = "You are a helpful AI assistant."
    
    # Components
    config: AgentConfig = field(default_factory=AgentConfig)
    tools: List[ToolDefinition] = field(default_factory=list)
    memory: Memory = field(default_factory=Memory)
    state: AgentState = field(default_factory=AgentState)
    
    # Metadata
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    version: str = "1.0.0"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_tool(self, tool: ToolDefinition):
        """Register a new tool"""
        self.tools.append(tool)
    
    def add_message(self, message: Message):
        """Add message to memory"""
        self.memory.add_to_short_term(message)
    
    def create_task(self, description: str, priority: int = 0) -> Task:
        """Create and queue a new task"""
        task = Task(description=description, priority=priority)
        self.state.task_queue.append(task)
        self.state.task_queue.sort(key=lambda t: t.priority, reverse=True)
        return task
    
    def update_state(self, **kwargs):
        """Update agent state"""
        for key, value in kwargs.items():
            if hasattr(self.state, key):
                setattr(self.state, key, value)
        self.state.last_updated = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Export agent to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "role": self.role,
            "description": self.description,
            "system_prompt": self.system_prompt,
            "config": self.config.to_dict(),
            "tools": [tool.to_dict() for tool in self.tools],
            "memory": self.memory.to_dict(),
            "state": self.state.to_dict(),
            "created_at": self.created_at,
            "version": self.version,
            "metadata": self.metadata
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Export agent to JSON string"""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Agent':
        """Create agent from dictionary"""
        # This would need proper deserialization logic
        agent = cls()
        agent.id = data.get("id", agent.id)
        agent.name = data.get("name", agent.name)
        # ... more deserialization
        return agent

# ============================================================================
# Example Usage
# ============================================================================

def create_example_agent():
    """Create a complete example agent with all components"""
    
    # 1. Create agent with configuration
    agent = Agent(
        name="CodeAssistant",
        role="software_developer",
        description="An AI agent specialized in software development",
        system_prompt="You are an expert software developer who writes clean, efficient code.",
        config=AgentConfig(
            model="claude-sonnet-4-5-20250929",
            temperature=0.3,
            enable_reasoning=True,
            reasoning_effort="high"
        )
    )
    
    # 2. Define and add tools
    web_search_tool = ToolDefinition(
        name="web_search",
        description="Search the web for information",
        parameters={
            "query": {
                "type": "string",
                "description": "The search query"
            },
            "max_results": {
                "type": "integer",
                "description": "Maximum number of results to return"
            }
        },
        required=["query"]
    )
    
    code_executor_tool = ToolDefinition(
        name="execute_code",
        description="Execute Python code and return results",
        parameters={
            "code": {
                "type": "string",
                "description": "Python code to execute"
            },
            "timeout": {
                "type": "integer",
                "description": "Execution timeout in seconds"
            }
        },
        required=["code"]
    )
    
    agent.add_tool(web_search_tool)
    agent.add_tool(code_executor_tool)
    
    # 3. Add conversation history
    system_msg = Message(
        role="system",
        content=agent.system_prompt
    )
    agent.add_message(system_msg)
    
    user_msg = Message(
        role="user",
        content="Write a Python function to calculate fibonacci numbers",
        metadata={"session_id": "sess_123", "user_id": "user_456"}
    )
    agent.add_message(user_msg)
    
    # 4. Simulate agent response with tool call
    tool_call = ToolCall(
        name="execute_code",
        arguments={
            "code": "def fibonacci(n):\n    if n <= 1:\n        return n\n    return fibonacci(n-1) + fibonacci(n-2)\n\nprint(fibonacci(10))",
            "timeout": 5
        },
        result="55",
        status="success"
    )
    
    assistant_msg = Message(
        role="assistant",
        content="I'll write a fibonacci function and test it:",
        tool_calls=[tool_call]
    )
    agent.add_message(assistant_msg)
    
    # 5. Add tool result message
    tool_result_msg = Message(
        role="tool",
        content="55",
        name="execute_code",
        tool_call_id=tool_call.id
    )
    agent.add_message(tool_result_msg)
    
    # 6. Create tasks
    task1 = agent.create_task("Write fibonacci function", priority=10)
    task1.status = "completed"
    task1.result = "Function written and tested successfully"
    task1.completed_at = datetime.now().isoformat()
    
    task2 = agent.create_task("Optimize the function with memoization", priority=8)
    task2.status = "in_progress"
    
    # 7. Add to long-term memory
    agent.memory.add_to_long_term(
        "fibonacci_implementation",
        "Recursive implementation completed, memoization optimization pending",
        importance=0.8
    )
    
    # 8. Update agent state
    agent.update_state(
        mode="thinking",
        current_task=task2,
        iteration_count=3,
        last_action="executed_code"
    )
    
    # 9. Add metadata
    agent.metadata = {
        "owner": "user_456",
        "environment": "development",
        "tags": ["python", "algorithms", "learning"],
        "performance_metrics": {
            "avg_response_time": 2.3,
            "success_rate": 0.95,
            "total_interactions": 42
        }
    }
    
    return agent


# ============================================================================
# Run Example
# ============================================================================

if __name__ == "__main__":
    # Create example agent
    agent = create_example_agent()
    
    # Export to JSON
    print("=" * 80)
    print("AGENT DATA FORMAT EXAMPLE")
    print("=" * 80)
    print(agent.to_json())
    
    # Show specific components
    print("\n" + "=" * 80)
    print("AGENT STATE")
    print("=" * 80)
    print(json.dumps(agent.state.to_dict(), indent=2))
    
    print("\n" + "=" * 80)
    print("CONVERSATION HISTORY")
    print("=" * 80)
    for msg in agent.memory.short_term:
        print(f"\n[{msg.role.upper()}] {msg.content[:100]}...")
        if msg.tool_calls:
            print(f"  Tool calls: {[tc.name for tc in msg.tool_calls]}")
    
    print("\n" + "=" * 80)
    print("TASKS")
    print("=" * 80)
    for task in [agent.state.current_task] + agent.state.task_queue:
        if task:
            print(f"\n{task.id}: {task.description}")
            print(f"  Status: {task.status}, Priority: {task.priority}")