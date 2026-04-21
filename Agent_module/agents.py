"""
agents.py
=========
Built-in agent implementations.

    EchoAgent        — echoes every input
    CalculatorAgent  — arithmetic via safe_eval (no eval/import access)
    WeatherAgent     — simulated weather lookup
    TranslatorAgent  — simulated translation
    ChatAgent        — rule-based conversational agent with per-user context
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any

from .framework import AgentMessage, BaseAgent
from .safe_eval import safe_eval


class EchoAgent(BaseAgent):
    """Repeats every received message."""

    def __init__(self, name: str = "EchoAgent") -> None:
        super().__init__(name, "Echoes back received messages")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        await asyncio.sleep(0.1)
        return AgentMessage(
            content=f"Echo: {message.content}",
            sender=self.name,
            message_type="echo_response",
        )


class CalculatorAgent(BaseAgent):
    """Evaluates arithmetic expressions using an AST allowlist."""

    def __init__(self, name: str = "CalculatorAgent") -> None:
        super().__init__(name, "Performs mathematical calculations")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        expression = str(message.content).strip()
        try:
            result = safe_eval(expression)
            return AgentMessage(
                content=f"{expression} = {result}",
                sender=self.name,
                message_type="calculation_result",
            )
        except (ValueError, ZeroDivisionError, OverflowError) as exc:
            return AgentMessage(
                content=f"Calculation error: {exc}",
                sender=self.name,
                message_type="error",
            )


class WeatherAgent(BaseAgent):
    """Simulated weather lookup: four preset cities, random data for others."""

    _PRESETS: dict[str, dict[str, Any]] = {
        "New York": {"temp": 22, "condition": "Sunny",  "humidity": 65},
        "London":   {"temp": 15, "condition": "Cloudy", "humidity": 80},
        "Tokyo":    {"temp": 18, "condition": "Rainy",  "humidity": 75},
        "Sydney":   {"temp": 25, "condition": "Clear",  "humidity": 60},
    }

    def __init__(self, name: str = "WeatherAgent") -> None:
        super().__init__(name, "Provides weather information")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        location = str(message.content).strip().title()
        await asyncio.sleep(0.2)

        if location in self._PRESETS:
            w = self._PRESETS[location]
            body = (
                f"Weather in {location}:\n"
                f"Temperature: {w['temp']}°C\n"
                f"Condition: {w['condition']}\n"
                f"Humidity: {w['humidity']}%"
            )
        else:
            body = (
                f"Weather in {location} (simulated):\n"
                f"Temperature: {random.randint(-10, 35)}°C\n"
                f"Condition: {random.choice(['Sunny', 'Cloudy', 'Rainy', 'Snowy'])}\n"
                f"Humidity: {random.randint(30, 95)}%"
            )

        return AgentMessage(
            content=body,
            sender=self.name,
            message_type="weather_report",
        )


class TranslatorAgent(BaseAgent):
    """Simulated translator. Target language set via metadata.target_language."""

    _SUPPORTED = ["english", "spanish", "french", "german", "italian"]

    def __init__(self, name: str = "TranslatorAgent") -> None:
        super().__init__(name, "Translates text between languages")

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        text = str(message.content)
        target = str(message.metadata.get("target_language", "spanish")).lower()
        await asyncio.sleep(0.3)

        if target not in self._SUPPORTED:
            body = (
                f"Sorry, I don't support {target}. "
                f"Supported languages: {', '.join(self._SUPPORTED)}"
            )
        else:
            body = f"[{target.upper()} TRANSLATION] {text} -> Translated text in {target}"

        return AgentMessage(
            content=body,
            sender=self.name,
            message_type="translation",
        )


class ChatAgent(BaseAgent):
    """Rule-based conversational agent with per-user context."""

    _GREETINGS = ("hello", "hi", "hey", "greetings")
    _QUESTION_WORDS = ("what", "how", "when", "where", "why")

    _GREETING_REPLIES = (
        "Hello! How can I assist you today?",
        "Hi there! What can I help you with?",
        "Greetings! I'm here to help.",
        "Hello! Nice to meet you.",
    )
    _DEFAULT_REPLIES = (
        "That's interesting! Can you tell me more?",
        "I understand. How can I help you with that?",
        "Thanks for sharing! Is there anything specific you'd like to know?",
        "I see. What would you like to do next?",
    )
    _FALLBACK_QA = (
        "That's a good question! Let me think about it...",
        "I'm not entirely sure about that, but I'd be happy to help you find out!",
        "Interesting question! Here's what I know about that topic...",
        "I understand your curiosity about that subject.",
    )

    def __init__(self, name: str = "ChatAgent") -> None:
        super().__init__(name, "Intelligent conversational agent")
        self._context: dict[str, dict[str, Any]] = {}

    async def _process_impl(self, message: AgentMessage) -> AgentMessage:
        user_input = str(message.content).lower()
        user_id = message.metadata.get("user_id", "unknown")

        ctx = self._context.setdefault(
            user_id,
            {"conversation_count": 0, "last_interaction": time.time(), "topics": set()},
        )
        ctx["conversation_count"] += 1
        ctx["last_interaction"] = time.time()

        reply = self._generate_response(user_input)

        return AgentMessage(
            content=reply,
            sender=self.name,
            message_type="chat_response",
            metadata={"user_context": ctx},
        )

    def _generate_response(self, user_input: str) -> str:
        if any(w in user_input for w in self._GREETINGS):
            return random.choice(self._GREETING_REPLIES)
        if "?" in user_input or any(w in user_input for w in self._QUESTION_WORDS):
            return self._answer_question(user_input)
        if "weather" in user_input:
            return "I can help with weather information! Please use the WeatherAgent for accurate weather data."
        if any(w in user_input for w in ("calculate", "math", "equation")):
            return "I can help with calculations! Try using the CalculatorAgent for mathematical operations."
        if any(w in user_input for w in ("translate", "translation")):
            return "I can assist with translations! The TranslatorAgent specializes in language translation."
        return random.choice(self._DEFAULT_REPLIES)

    def _answer_question(self, question: str) -> str:
        q = question.lower()
        if "time" in q:
            return f"The current time is {time.strftime('%H:%M:%S')}"
        if "date" in q:
            return f"Today's date is {time.strftime('%Y-%m-%d')}"
        if "name" in q:
            return "I'm ChatAgent, your friendly AI assistant!"
        if "purpose" in q or "what can you do" in q:
            return (
                "I can chat with you, answer questions, and coordinate with other "
                "specialized agents for weather, calculations, translations, and more!"
            )
        return random.choice(self._FALLBACK_QA)
