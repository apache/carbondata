class ChatAgent(BaseAgent):
    """Intelligent chat agent with context awareness"""
    
    def __init__(self, name: str = "ChatAgent"):
        super().__init__(name, "Intelligent conversational agent")
        self.context = {}
        self.personality_traits = [
            "friendly", "helpful", "knowledgeable", "enthusiastic"
        ]
    
    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        user_input = str(message.content).lower()
        user_id = message.metadata.get("user_id", "unknown")
        
        # Update context
        if user_id not in self.context:
            self.context[user_id] = {
                "conversation_count": 0,
                "last_interaction": time.time(),
                "topics": set()
            }
        
        self.context[user_id]["conversation_count"] += 1
        self.context[user_id]["last_interaction"] = time.time()
        
        # Simple intent recognition
        response = await self._generate_response(user_input, user_id)
        
        return AgentMessage(
            content=response,
            sender=self.name,
            timestamp=time.time(),
            message_type="chat_response",
            metadata={"user_context": self.context[user_id]}
        )
    
    async def _generate_response(self, user_input: str, user_id: str) -> str:
        """Generate context-aware responses"""
        
        # Greeting detection
        if any(word in user_input for word in ["hello", "hi", "hey", "greetings"]):
            return random.choice([
                "Hello! How can I assist you today?",
                "Hi there! What can I help you with?",
                "Greetings! I'm here to help.",
                "Hello! Nice to meet you."
            ])
        
        # Question detection
        elif "?" in user_input or any(word in user_input for word in ["what", "how", "when", "where", "why"]):
            return await self._answer_question(user_input)
        
        # Weather inquiry
        elif "weather" in user_input:
            return "I can help with weather information! Please use the WeatherAgent for accurate weather data."
        
        # Calculation request
        elif any(word in user_input for word in ["calculate", "math", "equation"]):
            return "I can help with calculations! Try using the CalculatorAgent for mathematical operations."
        
        # Translation request
        elif any(word in user_input for word in ["translate", "translation"]):
            return "I can assist with translations! The TranslatorAgent specializes in language translation."
        
        # Default response
        else:
            return random.choice([
                "That's interesting! Can you tell me more?",
                "I understand. How can I help you with that?",
                "Thanks for sharing! Is there anything specific you'd like to know?",
                "I see. What would you like to do next?"
            ])
    
    async def _answer_question(self, question: str) -> str:
        """Answer general knowledge questions"""
        question_lower = question.lower()
        
        if "time" in question_lower:
            return f"The current time is {time.strftime('%H:%M:%S')}"
        
        elif "date" in question_lower:
            return f"Today's date is {time.strftime('%Y-%m-%d')}"
        
        elif "name" in question_lower:
            return "I'm ChatAgent, your friendly AI assistant!"
        
        elif "purpose" in question_lower or "what can you do" in question_lower:
            return "I can chat with you, answer questions, and coordinate with other specialized agents for weather, calculations, translations, and more!"
        
        else:
            responses = [
                "That's a good question! Let me think about it...",
                "I'm not entirely sure about that, but I'd be happy to help you find out!",
                "Interesting question! Here's what I know about that topic...",
                "I understand your curiosity about that subject."
            ]
            return random.choice(responses)