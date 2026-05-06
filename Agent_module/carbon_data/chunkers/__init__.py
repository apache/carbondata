"""
Pluggable chunker factories.

A ``Chunker`` is just a function ``str -> list[str]``. Factories return
configured chunkers so the call site stays declarative:

    chunker = by_tokens(max_tokens=512, overlap=64)
    store.ingest_text(text, chunker=chunker)
"""
from __future__ import annotations

import re
from typing import Callable

Chunker = Callable[[str], list[str]]


def by_tokens(
    *,
    max_tokens: int = 512,
    overlap: int = 0,
    sep: str = " ",
) -> Chunker:
    """
    Whitespace-token sliding window. Each chunk holds at most ``max_tokens``
    tokens; consecutive chunks share ``overlap`` tokens at the boundary.

    Tokens are produced by :py:meth:`str.split` — there is no model-specific
    tokenizer here. Use it as a coarse proxy for real token budgets, not
    as a substitute.
    """
    if max_tokens <= 0:
        raise ValueError("max_tokens must be > 0")
    if overlap < 0:
        raise ValueError("overlap must be >= 0")
    if overlap >= max_tokens:
        raise ValueError("overlap must be < max_tokens")

    step = max_tokens - overlap

    def _chunk(text: str) -> list[str]:
        words = text.split()
        if not words:
            return []
        out: list[str] = []
        n = len(words)
        start = 0
        while start < n:
            end = start + max_tokens
            out.append(sep.join(words[start:end]))
            if end >= n:
                break
            start += step
        return out

    return _chunk


def by_paragraph(*, min_chars: int = 0) -> Chunker:
    """
    Split on runs of blank lines. Drops any chunk shorter than
    ``min_chars`` after stripping; set to 0 to keep every paragraph.
    """
    splitter = re.compile(r"\n\s*\n")

    def _chunk(text: str) -> list[str]:
        return [
            p.strip()
            for p in splitter.split(text)
            if p.strip() and len(p.strip()) >= min_chars
        ]

    return _chunk


def by_sentence(*, min_chars: int = 0) -> Chunker:
    """
    Naive sentence splitter: end-of-sentence punctuation followed by
    whitespace and a capital letter. Good enough for prose; for legal/
    technical text use ``by_tokens`` or a real tokenizer.
    """
    splitter = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")

    def _chunk(text: str) -> list[str]:
        return [
            s.strip()
            for s in splitter.split(text)
            if s.strip() and len(s.strip()) >= min_chars
        ]

    return _chunk


__all__ = ["Chunker", "by_tokens", "by_paragraph", "by_sentence"]
