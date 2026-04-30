"""
Data model for Agent Data Infra.

See docs/DESIGN.md §4.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    import numpy as np


def _uuid() -> str:
    return str(uuid.uuid4())


@dataclass
class Entity:
    id: str
    kind: str
    namespace: str = "default"
    content: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    version: int = 1


@dataclass
class Chunk:
    id: str
    entity_id: str
    seq: int
    content: str
    kind: str = "text"
    metadata: dict[str, Any] = field(default_factory=dict)
    token_count: Optional[int] = None


@dataclass
class Embedding:
    chunk_id: str
    model: str
    dim: int
    vector: "np.ndarray"


@dataclass
class Relation:
    src: str
    dst: str
    kind: str
    weight: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MemoryItem(Entity):
    kind: str = "memory"
    session_id: Optional[str] = None
    actor: Optional[str] = None
    salience: float = 0.5
    expires_at: Optional[float] = None


@dataclass
class SearchHit:
    """Result row from `CarbonStore.search`."""

    chunk: Chunk
    entity: Entity
    score: float


@dataclass
class MemoryHit:
    """Result row from `CarbonStore.recall`."""

    memory: MemoryItem
    score: float


@dataclass
class Neighbor:
    """A single-hop graph neighbor with the edge that connects it."""

    entity: Entity
    relation_kind: str          # the edge's kind
    weight: float
    direction: str              # "out" | "in" relative to the query node
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TraversalHit:
    """A node visited during a multi-hop traversal."""

    entity: Entity
    hop: int                    # shortest distance from the start node


@dataclass
class Subgraph:
    """An induced subgraph: entities plus the relations among them."""

    entities: list[Entity] = field(default_factory=list)
    relations: list[Relation] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Outcome of :py:meth:`CarbonStore.validate`."""

    ok: bool
    issues: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)


__all__ = [
    "Entity",
    "Chunk",
    "Embedding",
    "Relation",
    "MemoryItem",
    "SearchHit",
    "MemoryHit",
    "Neighbor",
    "TraversalHit",
    "Subgraph",
    "ValidationReport",
]
