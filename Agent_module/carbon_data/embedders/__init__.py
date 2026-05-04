"""
Embedders.

An `Embedder` is any object with ``model: str``, ``dim: int``, and an
``encode(texts) -> np.ndarray`` method returning shape ``(len(texts), dim)``
as float32.

We do NOT bundle embedding models. Users bring their own (OpenAI,
sentence-transformers, local llama, ...) and wrap them with
`LambdaEmbedder`, or they subclass `BaseEmbedder`.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Protocol, Sequence, runtime_checkable

if TYPE_CHECKING:
    import numpy as np


@runtime_checkable
class Embedder(Protocol):
    """
    Duck-typed embedder interface.

    Attributes:
        model: a stable identifier written alongside each vector; queries
               must use the same ``model`` to be comparable.
        dim:   vector dimensionality.

    Method:
        encode(texts): return float32 numpy array shape ``(len(texts), dim)``.
    """

    model: str
    dim: int

    def encode(self, texts: Sequence[str]) -> "np.ndarray":  # pragma: no cover
        ...


class BaseEmbedder:
    """Convenience base class that validates output shape."""

    model: str
    dim: int

    def __init__(self, model: str, dim: int) -> None:
        self.model = model
        self.dim = dim

    def encode(self, texts: Sequence[str]) -> "np.ndarray":  # pragma: no cover
        raise NotImplementedError

    def _validate(self, arr, n_texts: int) -> "np.ndarray":
        import numpy as np

        out = np.ascontiguousarray(arr, dtype=np.float32)
        if out.ndim != 2 or out.shape != (n_texts, self.dim):
            raise ValueError(
                f"embedder {self.model!r} returned shape {out.shape}, "
                f"expected ({n_texts}, {self.dim})"
            )
        return out


class NullEmbedder(BaseEmbedder):
    """All-zero embedder. Intended for tests and placeholders only."""

    def __init__(self, model: str = "null", dim: int = 8) -> None:
        super().__init__(model=model, dim=dim)

    def encode(self, texts: Sequence[str]) -> "np.ndarray":
        import numpy as np

        return np.zeros((len(texts), self.dim), dtype=np.float32)


class LambdaEmbedder(BaseEmbedder):
    """Wrap any callable ``fn(texts) -> array-like`` as an Embedder."""

    def __init__(
        self,
        fn: Callable[[Sequence[str]], "np.ndarray"],
        *,
        model: str,
        dim: int,
    ) -> None:
        super().__init__(model=model, dim=dim)
        self._fn = fn

    def encode(self, texts: Sequence[str]) -> "np.ndarray":
        return self._validate(self._fn(texts), len(texts))


__all__ = ["Embedder", "BaseEmbedder", "NullEmbedder", "LambdaEmbedder"]
