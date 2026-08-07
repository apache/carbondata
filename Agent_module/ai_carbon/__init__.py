"""The AI_carbon project archive and context reuse API."""

from .core import AI_carbon, Artifact, ContextError, InvalidArchive, OptimizationContext
from .web import serve


def create(filename, name=None):
    return AI_carbon.create(filename, name)


def open(filename):  # noqa: A001
    return AI_carbon.open(filename)

__all__ = [
    "AI_carbon",
    "Artifact",
    "ContextError",
    "InvalidArchive",
    "OptimizationContext",
    "serve",
    "create",
    "open",
]
