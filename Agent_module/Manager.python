class AgentManager:
    """Manages multiple agents and coordinates between them"""
    
    def __init__(self):
        self.agents: Dict[str, BaseAgent] = {}
        self.conversation_log: List[Dict] = []
    
    def register_agent(self, agent: BaseAgent):
        """Register a new agent"""
        self.agents[agent.name] = agent
        
        # Add logging callback to all agents
        agent.add_callback(self._log_interaction)
    
    async def send_message(self, agent_name: str, content: Any, 
                         metadata: Dict[str, Any] = None) -> AgentMessage:
        """Send message to specific agent"""
        if agent_name not in self.agents:
            raise ValueError(f"Agent '{agent_name}' not found")
        
        message = AgentMessage(
            content=content,
            sender="User",
            timestamp=time.time(),
            metadata=metadata or {}
        )
        
        agent = self.agents[agent_name]
        response = await agent.process(message)
        
        return response
    
    async def broadcast_message(self, content: Any, 
                              metadata: Dict[str, Any] = None) -> Dict[str, AgentMessage]:
        """Send message to all agents and collect responses"""
        message = AgentMessage(
            content=content,
            sender="Broadcast",
            timestamp=time.time(),
            metadata=metadata or {}
        )
        
        tasks = []
        for agent_name, agent in self.agents.items():
            tasks.append(agent.process(message))
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        results = {}
        for agent_name, response in zip(self.agents.keys(), responses):
            if isinstance(response, Exception):
                results[agent_name] = AgentMessage(
                    content=f"Error: {str(response)}",
                    sender=agent_name,
                    timestamp=time.time(),
                    message_type="error"
                )
            else:
                results[agent_name] = response
        
        return results
    
    def find_agent_by_capability(self, keyword: str) -> List[str]:
        """Find agents that might handle a specific capability"""
        matching_agents = []
        for agent_name, agent in self.agents.items():
            if (keyword.lower() in agent.name.lower() or 
                keyword.lower() in agent.description.lower()):
                matching_agents.append(agent_name)
        return matching_agents
    
    def _log_interaction(self, input_msg: AgentMessage, output_msg: AgentMessage):
        """Log agent interactions"""
        log_entry = {
            "timestamp": time.time(),
            "input": {
                "content": input_msg.content,
                "sender": input_msg.sender,
                "type": input_msg.message_type
            },
            "output": {
                "content": output_msg.content,
                "sender": output_msg.sender,
                "type": output_msg.message_type
            },
            "processing_time": output_msg.timestamp - input_msg.timestamp
        }
        self.conversation_log.append(log_entry)
    
    def get_agent_statuses(self) -> Dict[str, str]:
        """Get status of all agents"""
        return {name: agent.status.value for name, agent in self.agents.items()}
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get system statistics"""
        total_messages = sum(len(agent.message_history) for agent in self.agents.values())
        
        return {
            "total_agents": len(self.agents),
            "total_messages_processed": total_messages,
            "conversation_log_entries": len(self.conversation_log),
            "agent_statuses": self.get_agent_statuses()
        }