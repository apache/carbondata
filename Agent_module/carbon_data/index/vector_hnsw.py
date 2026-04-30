"""
HNSW (Hierarchical Navigable Small World) wrapper around hnswlib.

Optional dependency. Use ``hnsw_available()`` before constructing an index;
when hnswlib is missing the carbondata search path falls back to brute force.
"""
from __future__ import annotations

import os
import tempfile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np


_DEFAULT_M = 16
_DEFAULT_EF_CONSTRUCTION = 200
_DEFAULT_EF_SEARCH = 50


def hnsw_available() -> bool:
    """True iff hnswlib can be imported."""
    try:
        import hnswlib  # noqa: F401
    except ImportError:
        return False
    return True


class HnswIndex:
    """
    Cosine-space HNSW with integer labels and tempfile blob roundtripping.

    Labels are caller-assigned ints (typically chunk.rowid). hnswlib's
    save_index / load_index APIs only know how to talk to filesystem
    paths, so serialization stages through a tempfile.
    """

    def __init__(
        self,
        dim: int,
        max_elements: int,
        *,
        M: int = _DEFAULT_M,
        ef_construction: int = _DEFAULT_EF_CONSTRUCTION,
        ef_search: int = _DEFAULT_EF_SEARCH,
    ) -> None:
        import hnswlib

        self.dim = dim
        self.M = M
        self.ef_construction = ef_construction
        self.ef_search = ef_search
        self._idx = hnswlib.Index(space="cosine", dim=dim)
        self._idx.init_index(
            max_elements=max(max_elements, 1),
            ef_construction=ef_construction,
            M=M,
        )
        self._idx.set_ef(ef_search)

    # ------------------------------------------------------------------
    # construction
    # ------------------------------------------------------------------

    @classmethod
    def build(
        cls,
        vectors: "np.ndarray",
        labels: list[int],
        **kwargs,
    ) -> "HnswIndex":
        """Build an index from a contiguous (N, dim) array and N labels."""
        import numpy as np

        if vectors.ndim != 2:
            raise ValueError(
                f"expected 2-D vectors, got shape {vectors.shape}"
            )
        if len(labels) != vectors.shape[0]:
            raise ValueError(
                f"labels/vectors size mismatch: "
                f"{len(labels)} vs {vectors.shape[0]}"
            )
        idx = cls(
            dim=vectors.shape[1], max_elements=len(labels), **kwargs
        )
        idx._idx.add_items(
            np.ascontiguousarray(vectors, dtype=np.float32), labels
        )
        return idx

    # ------------------------------------------------------------------
    # query
    # ------------------------------------------------------------------

    def search(
        self, q: "np.ndarray", top_k: int
    ) -> list[tuple[int, float]]:
        """Return ``[(label, similarity), ...]`` ranked best-first."""
        import numpy as np

        cur = self._idx.get_current_count()
        if cur == 0 or top_k <= 0:
            return []
        k = min(top_k, cur)
        q2 = np.ascontiguousarray(q, dtype=np.float32).reshape(1, -1)
        labels, distances = self._idx.knn_query(q2, k=k)
        # cosine distance ∈ [0, 2]; similarity = 1 - distance.
        return [
            (int(lbl), float(1.0 - dist))
            for lbl, dist in zip(labels[0], distances[0])
        ]

    @property
    def count(self) -> int:
        return self._idx.get_current_count()

    # ------------------------------------------------------------------
    # serialization
    # ------------------------------------------------------------------

    def serialize(self) -> bytes:
        """Save the index to a tempfile and read it back as bytes."""
        fd, path = tempfile.mkstemp(suffix=".hnsw")
        os.close(fd)
        try:
            self._idx.save_index(path)
            with open(path, "rb") as f:
                return f.read()
        finally:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    @classmethod
    def deserialize(
        cls,
        blob: bytes,
        *,
        dim: int,
        ef_search: int = _DEFAULT_EF_SEARCH,
    ) -> "HnswIndex":
        import hnswlib

        fd, path = tempfile.mkstemp(suffix=".hnsw")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(blob)
            inst = cls.__new__(cls)
            inst.dim = dim
            inst.M = _DEFAULT_M
            inst.ef_construction = _DEFAULT_EF_CONSTRUCTION
            inst.ef_search = ef_search
            inst._idx = hnswlib.Index(space="cosine", dim=dim)
            inst._idx.load_index(path)
            inst._idx.set_ef(ef_search)
            return inst
        finally:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass


__all__ = ["HnswIndex", "hnsw_available"]
