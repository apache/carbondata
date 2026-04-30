"""
Brute-force nearest-neighbour search over in-memory float32 matrices.

Fine for < ~10k chunks. HNSW (M5) swaps in for larger corpora behind the
same interface on ``CarbonStore.search``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Sequence

if TYPE_CHECKING:
    import numpy as np

Metric = Literal["cosine", "dot", "l2"]

_EPS = 1e-12  # guard against divide-by-zero on zero-vectors


def vector_to_bytes(vec) -> bytes:
    """Serialize a 1-D float32 vector to bytes for BLOB storage."""
    import numpy as np

    arr = np.ascontiguousarray(vec, dtype=np.float32)
    if arr.ndim != 1:
        raise ValueError(f"expected 1-D vector, got shape {arr.shape}")
    return arr.tobytes()


def bytes_to_vector(blob: bytes, dim: int) -> "np.ndarray":
    """Deserialize a BLOB back into a 1-D float32 numpy array."""
    import numpy as np

    arr = np.frombuffer(blob, dtype=np.float32)
    if arr.size != dim:
        raise ValueError(
            f"vector blob has {arr.size} floats, expected {dim}"
        )
    # frombuffer is read-only and zero-copy; caller may want to mutate
    return arr.copy()


def search_brute(
    query: "np.ndarray",
    vectors: "np.ndarray",
    ids: Sequence[str],
    *,
    top_k: int,
    metric: Metric = "cosine",
) -> list[tuple[str, float]]:
    """
    Brute-force top-k over an in-memory matrix.

    Args:
        query:    shape (dim,) float32
        vectors:  shape (N, dim) float32
        ids:      length-N iterable of chunk ids aligned with vectors
        top_k:    max hits to return
        metric:   'cosine' | 'dot' | 'l2'  (l2 is returned as negative distance
                  so that larger = better, same convention as cosine/dot)

    Returns:
        list of (chunk_id, score) sorted descending by score.
    """
    import numpy as np

    if vectors.size == 0:
        return []

    q = np.ascontiguousarray(query, dtype=np.float32).reshape(-1)
    if q.shape[0] != vectors.shape[1]:
        raise ValueError(
            f"query dim {q.shape[0]} != vector dim {vectors.shape[1]}"
        )

    if metric == "cosine":
        v_norms = np.linalg.norm(vectors, axis=1)
        q_norm = float(np.linalg.norm(q))
        scores = (vectors @ q) / (v_norms * q_norm + _EPS)
    elif metric == "dot":
        scores = vectors @ q
    elif metric == "l2":
        # negated distance so argsort-descending yields nearest first
        diffs = vectors - q
        scores = -np.linalg.norm(diffs, axis=1)
    else:
        raise ValueError(f"unknown metric: {metric!r}")

    if top_k >= scores.shape[0]:
        order = np.argsort(-scores)
    else:
        # argpartition is O(N); sort only the top-k slice
        part = np.argpartition(-scores, top_k)[:top_k]
        order = part[np.argsort(-scores[part])]

    return [(ids[int(i)], float(scores[int(i)])) for i in order]


__all__ = ["vector_to_bytes", "bytes_to_vector", "search_brute"]
