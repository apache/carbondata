import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import random
import json

class AgentStatus(Enum):
    IDLE = "idle"
    PROCESSING = "processing"
    ERROR = "error"
    STOPPED = "stopped"

@dataclass
class AgentMessage:
    content: Any
    sender: str
    timestamp: float
    message_type: str = "text"
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        self.timestamp = self.timestamp or time.time()

class BaseAgent(ABC):
    """Base class for all agents"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.status = AgentStatus.IDLE
        self.message_history: List[AgentMessage] = []
        self._callbacks: List[Callable] = []
        
    async def process(self, message: AgentMessage) -> AgentMessage:
        """Process a message and return response"""
        self.status = AgentStatus.PROCESSING
        self.message_history.append(message)
        
        try:
            result = await self._process_impl(message)
            self.status = AgentStatus.IDLE
            self._notify_callbacks(message, result)
            return result
        except Exception as e:
            self.status = AgentStatus.ERROR
            error_msg = AgentMessage(
                content=f"Error: {str(e)}",
                sender=self.name,
                timestamp=time.time(),
                message_type="error"
            )
            return error_msg
    
    @abstractmethod
    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        """Agent-specific processing logic"""
        pass
    
    def add_callback(self, callback: Callable):
        """Add callback for message processing events"""
        self._callbacks.append(callback)
    
    def _notify_callbacks(self, input_msg: AgentMessage, output_msg: AgentMessage):
        """Notify all callbacks"""
        for callback in self._callbacks:
            try:
                callback(input_msg, output_msg)
            except Exception:
                pass  # Don't break agent if callback fails
    
    def get_recent_messages(self, count: int = 10) -> List[AgentMessage]:
        """Get recent message history"""
        return self.message_history[-count:]