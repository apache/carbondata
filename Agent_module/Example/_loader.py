"""
_loader.py
==========
Utility for loading Agent_module source files that share a namespace.

Framework.py, Implementation.py, Manager.py, and Chat_Agent.py were written
as a single-namespace codebase (no per-file imports). This loader execs them
together into one shared dict so the cross-file dependencies resolve correctly.

Usage:
    from _loader import load
    m = load("Framework", "Implementation", "Manager", "Chat_Agent")
    BaseAgent  = m.BaseAgent
    EchoAgent  = m.EchoAgent
    AgentManager = m.AgentManager
"""

import os
import types
import asyncio
import time
import random
import json

_MODULE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def load(*names: str) -> types.SimpleNamespace:
    """
    Execute each named .py file in _MODULE_DIR into a shared namespace
    and return a SimpleNamespace with all defined names as attributes.

    Example:
        m = load("Framework", "Implementation")
        agent = m.EchoAgent()
    """
    ns: dict = {
        "__builtins__": __builtins__,
        "asyncio": asyncio,
        "time": time,
        "random": random,
        "json": json,
    }
    for name in names:
        path = os.path.join(_MODULE_DIR, f"{name}.py")
        with open(path) as fh:
            exec(compile(fh.read(), path, "exec"), ns)

    return types.SimpleNamespace(**{k: v for k, v in ns.items() if not k.startswith("_")})
