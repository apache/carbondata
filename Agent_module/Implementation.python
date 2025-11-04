class EchoAgent(BaseAgent):
    """Simple echo agent that repeats messages"""
    
    def __init__(self, name: str = "EchoAgent"):
        super().__init__(name, "Echoes back received messages")
    
    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        await asyncio.sleep(0.1)  # Simulate processing time
        
        response_content = f"Echo: {message.content}"
        
        return AgentMessage(
            content=response_content,
            sender=self.name,
            timestamp=time.time(),
            message_type="echo_response"
        )

class CalculatorAgent(BaseAgent):
    """Agent that performs mathematical calculations"""
    
    def __init__(self, name: str = "CalculatorAgent"):
        super().__init__(name, "Performs mathematical calculations")
    
    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        try:
            expression = str(message.content).strip()
            # Security: Only allow basic math operations
            allowed_chars = set('0123456789+-*/.() ')
            if not all(c in allowed_chars for c in expression):
                raise ValueError("Invalid characters in expression")
            
            result = eval(expression)  # In real code, use a safe eval or parser
            
            return AgentMessage(
                content=f"{expression} = {result}",
                sender=self.name,
                timestamp=time.time(),
                message_type="calculation_result"
            )
        except Exception as e:
            return AgentMessage(
                content=f"Calculation error: {str(e)}",
                sender=self.name,
                timestamp=time.time(),
                message_type="error"
            )

class WeatherAgent(BaseAgent):
    """Simulated weather information agent"""
    
    def __init__(self, name: str = "WeatherAgent"):
        super().__init__(name, "Provides weather information")
        self.weather_data = {
            "New York": {"temp": 22, "condition": "Sunny", "humidity": 65},
            "London": {"temp": 15, "condition": "Cloudy", "humidity": 80},
            "Tokyo": {"temp": 18, "condition": "Rainy", "humidity": 75},
            "Sydney": {"temp": 25, "condition": "Clear", "humidity": 60}
        }
    
    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        location = str(message.content).strip().title()
        
        await asyncio.sleep(0.2)  # Simulate API call
        
        if location in self.weather_data:
            weather = self.weather_data[location]
            response = (
                f"Weather in {location}:\n"
                f"Temperature: {weather['temp']}°C\n"
                f"Condition: {weather['condition']}\n"
                f"Humidity: {weather['humidity']}%"
            )
        else:
            # Generate random weather for unknown locations
            temp = random.randint(-10, 35)
            conditions = ["Sunny", "Cloudy", "Rainy", "Snowy"]
            condition = random.choice(conditions)
            humidity = random.randint(30, 95)
            
            response = (
                f"Weather in {location} (simulated):\n"
                f"Temperature: {temp}°C\n"
                f"Condition: {condition}\n"
                f"Humidity: {humidity}%"
            )
        
        return AgentMessage(
            content=response,
            sender=self.name,
            timestamp=time.time(),
            message_type="weather_report"
        )

class TranslatorAgent(BaseAgent):
    """Simple translation agent (simulated)"""
    
    def __init__(self, name: str = "TranslatorAgent"):
        super().__init__(name, "Translates text between languages")
        self.supported_languages = ["english", "spanish", "french", "german", "italian"]
    
    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        text = str(message.content)
        target_language = message.metadata.get("target_language", "spanish").lower()
        
        await asyncio.sleep(0.3)  # Simulate translation processing
        
        if target_language not in self.supported_languages:
            response = f"Sorry, I don't support {target_language}. Supported languages: {', '.join(self.supported_languages)}"
        else:
            # Simulated translation - in real world, this would call a translation API
            response = f"[{target_language.upper()} TRANSLATION] {text} -> Translated text in {target_language}"
        
        return AgentMessage(
            content=response,
            sender=self.name,
            timestamp=time.time(),
            message_type="translation"
        )