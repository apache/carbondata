"""M4: FTS5 keyword search + RRF hybrid fusion."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from Agent_module.carbon_data import (
    CarbonStore,
    LambdaEmbedder,
    SearchHit,
    create,
    open as cd_open,
)
from Agent_module.carbon_data.store import _fts_escape_query


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store(tmp_path: Path):
    p = tmp_path / "kb.carbondata"
    s = create(p)
    yield s
    if not s.closed:
        s.close()


def _fixed_embedder(mapping: dict[str, list[float]], *, model="test", dim=3):
    """Returns pre-defined vectors for known texts, zero vector otherwise."""
    arr = {k: np.asarray(v, dtype=np.float32).reshape(-1) for k, v in mapping.items()}

    def _fn(texts):
        out = np.zeros((len(texts), dim), dtype=np.float32)
        for i, t in enumerate(texts):
            if t in arr:
                out[i] = arr[t]
        return out

    return LambdaEmbedder(_fn, model=model, dim=dim)


def _fts_count(store: CarbonStore) -> int:
    conn = store._require_open()
    return conn.execute("SELECT COUNT(*) FROM chunk_fts").fetchone()[0]


# ---------------------------------------------------------------------------
# FTS escape helper
# ---------------------------------------------------------------------------

class TestFtsEscape:
    def test_simple_tokens(self) -> None:
        assert _fts_escape_query("hello world") == '"hello" "world"'

    def test_empty(self) -> None:
        assert _fts_escape_query("") == ""
        assert _fts_escape_query("   ") == ""

    def test_strips_quotes(self) -> None:
        # Internal double-quotes get stripped (replaced with whitespace),
        # so "exact phrase" never produces an unbalanced FTS5 string.
        assert _fts_escape_query('say "hi"') == '"say" "hi"'

    def test_special_chars_become_phrases(self) -> None:
        # FTS5 reserves AND/OR/NOT/NEAR/(/)/* etc — quoting neutralizes them.
        out = _fts_escape_query("foo AND bar* (baz)")
        assert out == '"foo" "AND" "bar*" "(baz)"'


# ---------------------------------------------------------------------------
# Trigger sync: chunk → chunk_fts
# ---------------------------------------------------------------------------

class TestFtsTriggers:
    def test_insert_propagates(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["the quick brown fox", "lazy dog"])
        assert _fts_count(store) == 2

    def test_delete_chunks_propagates(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["alpha", "beta"])
        assert _fts_count(store) == 2
        store.delete_chunks("e1")
        assert _fts_count(store) == 0

    def test_delete_entity_cascades(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["alpha", "beta", "gamma"])
        assert _fts_count(store) == 3
        store.delete_entity("e1")
        assert _fts_count(store) == 0

    def test_update_propagates(self, store: CarbonStore) -> None:
        # Only chunk.content updates need to flow into FTS. We don't have
        # a public update_chunk yet; exercise the trigger via direct UPDATE
        # so the sync invariant is locked in for future writers.
        store.put_entity(id="e1", kind="document")
        ids = store.add_chunks("e1", ["original text"])
        with store._write_ctx() as conn:
            conn.execute("UPDATE chunk SET content=? WHERE id=?", ("rewritten", ids[0]))
        # Old token gone, new token findable.
        hits_old = store.search("original", mode="keyword")
        hits_new = store.search("rewritten", mode="keyword")
        assert hits_old == []
        assert len(hits_new) == 1


# ---------------------------------------------------------------------------
# Keyword (FTS5 + BM25) search
# ---------------------------------------------------------------------------

class TestKeywordSearch:
    def test_basic_match(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", [
            "the quick brown fox jumps",
            "totally unrelated content",
            "fox hunting in the woods",
        ])
        hits = store.search("fox", mode="keyword", top_k=5)
        contents = {h.chunk.content for h in hits}
        assert any("quick brown fox" in c for c in contents)
        assert any("fox hunting" in c for c in contents)
        assert not any("unrelated" in c for c in contents)

    def test_bm25_ranking_prefers_rarer_terms(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        # "common" appears everywhere; "unique" appears in only one chunk.
        store.add_chunks("e1", [
            "common common common common",
            "common rare and unique words",
            "common words again here",
        ])
        hits = store.search("unique", mode="keyword", top_k=3)
        assert len(hits) == 1
        assert "unique" in hits[0].chunk.content

    def test_score_higher_is_better(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", [
            "fox cat dog bird fish",                # 1 hit on "fox"
            "fox fox fox terrier fox",              # repeated → higher BM25
        ])
        hits = store.search("fox", mode="keyword", top_k=2)
        assert len(hits) == 2
        # Our score = -bm25(): higher means better match.
        assert hits[0].score >= hits[1].score

    def test_namespace_isolation(self, store: CarbonStore) -> None:
        store.put_entity(id="a", kind="document", namespace="ns1")
        store.put_entity(id="b", kind="document", namespace="ns2")
        store.add_chunks("a", ["unique_term in namespace one"])
        store.add_chunks("b", ["unique_term in namespace two"])
        hits = store.search("unique_term", mode="keyword", namespace="ns1")
        assert len(hits) == 1
        assert hits[0].entity.id == "a"

    def test_filters_pushdown(self, store: CarbonStore) -> None:
        store.put_entity(id="a", kind="document", metadata={"lang": "en"})
        store.put_entity(id="b", kind="document", metadata={"lang": "fr"})
        store.add_chunks("a", ["the keyword appears here"])
        store.add_chunks("b", ["the keyword appears here too"])
        hits = store.search(
            "keyword", mode="keyword", filters={"metadata.lang": "fr"}
        )
        assert len(hits) == 1
        assert hits[0].entity.id == "b"

    def test_top_k_caps(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", [f"token chunk-{i}" for i in range(10)])
        hits = store.search("token", mode="keyword", top_k=3)
        assert len(hits) == 3

    def test_empty_query_short_circuits(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["something"])
        assert store.search("", mode="keyword") == []
        assert store.search("    ", mode="keyword") == []

    def test_special_chars_dont_crash(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["match this content"])
        # FTS5 reserved words/syntax must not raise via the escape path.
        for q in ["AND", "OR (", "match*", '"', "(foo) AND NOT bar"]:
            store.search(q, mode="keyword")  # any of these may legitimately
            # return zero hits — we only assert no exception.

    def test_raw_fts_passes_operators(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["fox jumps over the lazy dog"])
        # Without raw_fts, "fox AND jumps" would be quoted into nonsense.
        # With raw_fts, FTS5 sees the AND operator.
        hits = store.search("fox AND jumps", mode="keyword", raw_fts=True)
        assert len(hits) == 1

    def test_raw_fts_invalid_raises(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["whatever"])
        with pytest.raises(ValueError, match="FTS5 query failed"):
            # Unbalanced quote — only reachable via raw_fts=True.
            store.search('"unclosed', mode="keyword", raw_fts=True)

    def test_keyword_returns_search_hit(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["needle in haystack"])
        hits = store.search("needle", mode="keyword")
        assert len(hits) == 1
        assert isinstance(hits[0], SearchHit)
        assert hits[0].entity.id == "e1"
        assert hits[0].chunk.content == "needle in haystack"

    def test_keyword_rejects_vector_query(self, store: CarbonStore) -> None:
        with pytest.raises(TypeError, match="keyword mode"):
            store.search(np.array([1.0, 0.0, 0.0]), mode="keyword")


# ---------------------------------------------------------------------------
# Hybrid (RRF)
# ---------------------------------------------------------------------------

class TestHybridSearch:
    def test_combines_both_modalities(self, store: CarbonStore) -> None:
        # Build a corpus where vector and keyword disagree, and check
        # that hybrid surfaces both winners.
        emb = _fixed_embedder(
            {
                "vec_winner": [1.0, 0.0, 0.0],   # vec match for q
                "shared": [0.5, 0.5, 0.0],       # weak vec, no kw
                "kw_winner": [0.0, 0.0, 1.0],    # weak vec, strong kw
                "irrelevant": [0.0, 1.0, 0.0],
                "needle":    [0.9, 0.1, 0.0],    # query vector (~vec_winner)
            },
            dim=3,
        )
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["vec_winner", "shared", "kw_winner", "irrelevant"])
        store.embed_chunks(embedder=emb)

        hits = store.search(
            "kw_winner", mode="hybrid", top_k=2, embedder=emb,
        )
        ids = [h.chunk.content for h in hits]
        # kw_winner: in keyword top, also in vector tail → strong RRF
        # vec_winner: not matching keyword, but #1 in vector → also strong
        assert "kw_winner" in ids
        assert "vec_winner" in ids

    def test_rrf_dedupes_overlap(self, store: CarbonStore) -> None:
        # A chunk that wins both modalities should rank above either alone.
        emb = _fixed_embedder(
            {
                "needle": [1.0, 0.0, 0.0],
                "decoy_a": [0.0, 1.0, 0.0],
                "decoy_b": [0.0, 0.0, 1.0],
            },
            dim=3,
        )
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["needle", "decoy_a", "decoy_b"])
        store.embed_chunks(embedder=emb)

        hits = store.search("needle", mode="hybrid", top_k=3, embedder=emb)
        assert hits[0].chunk.content == "needle"
        # Should appear once, not twice.
        contents = [h.chunk.content for h in hits]
        assert contents.count("needle") == 1

    def test_rrf_score_formula(self, store: CarbonStore) -> None:
        # Single-source case: chunk only ranks in one modality, score should
        # equal 1/(rrf_k + 1).
        emb = _fixed_embedder({"only_vec": [1.0, 0.0, 0.0]}, dim=3)
        store.put_entity(id="e1", kind="document")
        # Use a content string that won't match the vector query token.
        store.add_chunks("e1", ["only_vec"])
        store.embed_chunks(embedder=emb)

        # Query with a vector-side term that does NOT appear as a keyword.
        # We construct a query string that the embedder maps to [1,0,0]
        # but whose tokens won't FTS-match the chunk content.
        # The embedder maps unknown strings to zero, so we map a synthetic.
        emb2 = _fixed_embedder(
            {"zzzqueryzzz": [1.0, 0.0, 0.0], "only_vec": [1.0, 0.0, 0.0]},
            dim=3,
        )
        hits = store.search(
            "zzzqueryzzz", mode="hybrid", top_k=1,
            embedder=emb2, rrf_k=60,
        )
        assert len(hits) == 1
        assert abs(hits[0].score - 1.0 / (60 + 1)) < 1e-9

    def test_hybrid_requires_string(self, store: CarbonStore) -> None:
        with pytest.raises(TypeError, match="hybrid mode"):
            store.search(np.array([1.0, 0.0, 0.0]), mode="hybrid")

    def test_hybrid_requires_embedder_or_model(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["something"])
        with pytest.raises(TypeError, match="model="):
            store.search("something", mode="hybrid")

    def test_hybrid_degrades_when_keyword_empty(self, store: CarbonStore) -> None:
        # Whitespace query → keyword side returns nothing; hybrid should
        # still return vector-side results without error.
        emb = _fixed_embedder({"a": [1.0, 0.0, 0.0]}, dim=3)
        store.put_entity(id="e1", kind="document")
        store.add_chunks("e1", ["a"])
        store.embed_chunks(embedder=emb)
        # Encoder returns zero-vec for whitespace; that's fine, just no
        # vector match either. Confirm it doesn't crash.
        result = store.search("   ", mode="hybrid", top_k=3, embedder=emb)
        assert isinstance(result, list)

    def test_oversample_validation(self, store: CarbonStore) -> None:
        emb = _fixed_embedder({"x": [1.0, 0.0, 0.0]}, dim=3)
        with pytest.raises(ValueError, match="fusion_oversample"):
            store.search("x", mode="hybrid", embedder=emb, fusion_oversample=0)


# ---------------------------------------------------------------------------
# Mode dispatch
# ---------------------------------------------------------------------------

class TestSearchModeDispatch:
    def test_default_is_vector(self, store: CarbonStore) -> None:
        # No mode= → vector path. Should require model/embedder.
        with pytest.raises(TypeError, match="model="):
            store.search("anything")

    def test_unknown_mode_raises(self, store: CarbonStore) -> None:
        with pytest.raises(ValueError, match="unknown search mode"):
            store.search("foo", mode="laser")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# ensure_triggers idempotency on reopen
# ---------------------------------------------------------------------------

class TestTriggerIdempotency:
    def test_reopen_keeps_fts_in_sync(self, tmp_path: Path) -> None:
        p = tmp_path / "kb.carbondata"
        s = create(p)
        s.put_entity(id="e1", kind="document")
        s.add_chunks("e1", ["alpha beta gamma"])
        s.close()

        s2 = cd_open(p)
        try:
            hits = s2.search("beta", mode="keyword")
            assert len(hits) == 1
            # Writes after reopen still flow into FTS.
            s2.put_entity(id="e2", kind="document")
            s2.add_chunks("e2", ["delta epsilon"])
            hits2 = s2.search("epsilon", mode="keyword")
            assert len(hits2) == 1
        finally:
            s2.close()
