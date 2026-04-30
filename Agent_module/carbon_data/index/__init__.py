"""
Index layer.

- M3: brute-force vector search (always available; pure numpy).
- M5: HNSW via hnswlib as an optional dependency. Falls back to brute
       when hnswlib is not installed or when filters/metrics rule it out.
"""
from .vector_brute import (
    bytes_to_vector,
    search_brute,
    vector_to_bytes,
)
from .vector_hnsw import HnswIndex, hnsw_available

__all__ = [
    "bytes_to_vector",
    "vector_to_bytes",
    "search_brute",
    "HnswIndex",
    "hnsw_available",
]
