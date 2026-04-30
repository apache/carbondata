"""
Agent Data Infra — .carbondata file format and query API.

See docs/DESIGN.md for design rationale.

Quick start (M1 surface):

    from Agent_module.carbon_data import open, create

    store = create("kb.carbondata")
    store.stats()    # {'entities': 0, ...}
    store.close()

    store = open("kb.carbondata")
    store.close()
"""
from __future__ import annotations

from . import chunkers
from .chunkers import Chunker, by_paragraph, by_sentence, by_tokens
from .embedders import BaseEmbedder, Embedder, LambdaEmbedder, NullEmbedder
from .models import (
    Chunk,
    Embedding,
    Entity,
    MemoryHit,
    MemoryItem,
    Neighbor,
    Relation,
    SearchHit,
    Subgraph,
    TraversalHit,
    ValidationReport,
)
from .schema import (
    APPLICATION_ID,
    SCHEMA_VERSION,
    NotACarbondataFile,
    UnsupportedSchemaVersion,
)
from .store import CarbonStore, PathLike


def open(path: PathLike, *, mode: str = "rw") -> CarbonStore:  # noqa: A001
    """Open an existing .carbondata file (or create it if missing in rw mode)."""
    return CarbonStore.open(path, mode=mode)  # type: ignore[arg-type]


def create(path: PathLike, *, exist_ok: bool = False) -> CarbonStore:
    """Create a new .carbondata file. Raises FileExistsError unless exist_ok=True."""
    return CarbonStore.create(path, exist_ok=exist_ok)


__all__ = [
    "open",
    "create",
    "CarbonStore",
    "Entity",
    "Chunk",
    "Embedding",
    "Relation",
    "MemoryItem",
    "MemoryHit",
    "SearchHit",
    "Neighbor",
    "TraversalHit",
    "Subgraph",
    "ValidationReport",
    "chunkers",
    "Chunker",
    "by_tokens",
    "by_paragraph",
    "by_sentence",
    "Embedder",
    "BaseEmbedder",
    "NullEmbedder",
    "LambdaEmbedder",
    "SCHEMA_VERSION",
    "APPLICATION_ID",
    "NotACarbondataFile",
    "UnsupportedSchemaVersion",
]
