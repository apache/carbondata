"""M5: HNSW (hnswlib) ANN index — wrapper, dispatch, cache, persistence."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from Agent_module.carbon_data import (
    CarbonStore,
    LambdaEmbedder,
    create,
    open as cd_open,
)
from Agent_module.carbon_data.index import (
    HnswIndex,
    hnsw_available,
)


pytestmark = pytest.mark.skipif(
    not hnsw_available(), reason="hnswlib not installed"
)


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


def _seed(store: CarbonStore, *, namespace="default", n=20, dim=8, seed=0):
    """Add n random embeddings under one entity. Returns the embedder."""
    rng = np.random.default_rng(seed)
    matrix = rng.standard_normal((n, dim)).astype(np.float32)
    # Map text -> row in the matrix so the embedder is deterministic.
    texts = [f"chunk-{i}" for i in range(n)]
    text_to_vec = {t: matrix[i] for i, t in enumerate(texts)}

    def _fn(ts):
        return np.stack([text_to_vec.get(t, np.zeros(dim, dtype=np.float32))
                         for t in ts])

    emb = LambdaEmbedder(_fn, model="m5-test", dim=dim)

    eid = store.put_entity(id=f"e-{namespace}", kind="document", namespace=namespace)
    store.add_chunks(eid, texts)
    store.embed_chunks(emb)
    return emb, texts, matrix


def _vector_index_count(store: CarbonStore) -> int:
    conn = store._require_open()
    return conn.execute("SELECT COUNT(*) FROM vector_index").fetchone()[0]


# ---------------------------------------------------------------------------
# bare wrapper — independent of CarbonStore
# ---------------------------------------------------------------------------

class TestHnswWrapper:
    def test_available(self) -> None:
        assert hnsw_available() is True

    def test_build_and_search_self_hit(self) -> None:
        rng = np.random.default_rng(42)
        v = rng.standard_normal((20, 6)).astype(np.float32)
        labels = list(range(1000, 1020))
        idx = HnswIndex.build(v, labels)
        # Query equals row 7 → should top-rank label 1007.
        hits = idx.search(v[7], top_k=3)
        assert hits[0][0] == 1007
        # Cosine sim with self ~= 1.0
        assert hits[0][1] > 0.99

    def test_search_caps_at_count(self) -> None:
        v = np.eye(3, dtype=np.float32)
        idx = HnswIndex.build(v, [10, 20, 30])
        hits = idx.search(v[0], top_k=100)
        assert len(hits) == 3

    def test_serialize_roundtrip_preserves_ranking(self) -> None:
        rng = np.random.default_rng(7)
        v = rng.standard_normal((30, 4)).astype(np.float32)
        labels = list(range(30))
        idx = HnswIndex.build(v, labels)

        before = idx.search(v[5], top_k=5)
        blob = idx.serialize()
        assert isinstance(blob, bytes) and len(blob) > 0

        idx2 = HnswIndex.deserialize(blob, dim=4)
        after = idx2.search(v[5], top_k=5)
        assert before == after

    def test_dimension_mismatch(self) -> None:
        v = np.zeros((5, 3), dtype=np.float32)
        with pytest.raises(ValueError, match="size mismatch"):
            HnswIndex.build(v, labels=[1, 2])

    def test_2d_required(self) -> None:
        with pytest.raises(ValueError, match="2-D"):
            HnswIndex.build(np.zeros(5, dtype=np.float32), labels=[1])


# ---------------------------------------------------------------------------
# dispatch — when does HNSW get used?
# ---------------------------------------------------------------------------

class TestDispatch:
    def test_auto_uses_hnsw_when_eligible(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store)
        # Default search → should populate the HNSW cache.
        store.search(np.array([1.0] + [0.0] * 7, dtype=np.float32),
                     model=emb.model, top_k=3)
        assert emb.model in store._hnsw_cache

    def test_off_skips_hnsw(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store)
        store.search(np.array([1.0] + [0.0] * 7, dtype=np.float32),
                     model=emb.model, top_k=3, use_hnsw="off")
        assert emb.model not in store._hnsw_cache

    def test_filters_force_brute(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store)
        # filters present → auto falls back to brute, cache untouched.
        store.search(
            np.array([1.0] + [0.0] * 7, dtype=np.float32),
            model=emb.model, top_k=3,
            filters={"kind": "document"},
        )
        assert emb.model not in store._hnsw_cache

    def test_non_cosine_forces_brute(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store)
        store.search(
            np.array([1.0] + [0.0] * 7, dtype=np.float32),
            model=emb.model, top_k=3, metric="dot",
        )
        assert emb.model not in store._hnsw_cache

    def test_on_with_filters_raises(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store)
        with pytest.raises(ValueError, match="use_hnsw='on' is incompatible"):
            store.search(
                np.array([1.0] + [0.0] * 7, dtype=np.float32),
                model=emb.model, use_hnsw="on",
                filters={"kind": "document"},
            )

    def test_on_with_non_cosine_raises(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store)
        with pytest.raises(ValueError, match="metric='cosine'"):
            store.search(
                np.array([1.0] + [0.0] * 7, dtype=np.float32),
                model=emb.model, use_hnsw="on", metric="l2",
            )

    def test_on_without_embeddings_returns_empty(self, store: CarbonStore) -> None:
        # Force HNSW with a model that has zero embeddings — should not
        # raise; just produce no hits.
        out = store.search(
            np.array([1.0, 0.0, 0.0], dtype=np.float32),
            model="ghost-model", use_hnsw="on", top_k=5,
        )
        assert out == []


# ---------------------------------------------------------------------------
# correctness — HNSW vs brute should agree at top-1 for clean data
# ---------------------------------------------------------------------------

class TestParity:
    def test_top1_matches_brute(self, store: CarbonStore) -> None:
        emb, texts, matrix = _seed(store, n=50, dim=16, seed=1)
        rng = np.random.default_rng(99)
        for _ in range(5):
            q = rng.standard_normal(16).astype(np.float32)
            hnsw_hits = store.search(q, model=emb.model, top_k=1, use_hnsw="on")
            brute_hits = store.search(q, model=emb.model, top_k=1, use_hnsw="off")
            assert hnsw_hits[0].chunk.id == brute_hits[0].chunk.id


# ---------------------------------------------------------------------------
# persistence — blob is written and reloaded across open()
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_blob_persisted_after_first_search(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store)
        assert _vector_index_count(store) == 0
        store.search(np.array([1.0] + [0.0] * 7, dtype=np.float32),
                     model=emb.model, top_k=3)
        assert _vector_index_count(store) == 1

    def test_reopen_loads_from_blob_not_rebuilt(self, tmp_path: Path) -> None:
        p = tmp_path / "kb.carbondata"
        s = create(p)
        emb, _, matrix = _seed(s, n=12, dim=4, seed=2)
        # Trigger initial build + persist.
        s.search(matrix[0], model=emb.model, top_k=1)
        s.close()

        s2 = cd_open(p)
        try:
            # Cache empty, but blob is current → load path, no rebuild.
            assert emb.model not in s2._hnsw_cache
            hits = s2.search(matrix[0], model=emb.model, top_k=1)
            assert hits[0].chunk.content == "chunk-0"
            # Cache populated via the load branch.
            assert emb.model in s2._hnsw_cache
        finally:
            s2.close()

    def test_blob_with_stale_count_triggers_rebuild(self, tmp_path: Path) -> None:
        p = tmp_path / "kb.carbondata"
        s = create(p)
        emb, _, matrix = _seed(s, n=8, dim=4, seed=3)
        s.search(matrix[0], model=emb.model, top_k=1)
        s.close()

        # Tamper with persisted meta count → reopen should treat blob as
        # stale and rebuild from the embedding table.
        import sqlite3
        with sqlite3.connect(str(p)) as raw:
            raw.execute(
                "UPDATE vector_index SET index_meta = ? WHERE model = ?",
                ('{"count": 0, "dim": 4}', emb.model),
            )

        s2 = cd_open(p)
        try:
            hits = s2.search(matrix[0], model=emb.model, top_k=1)
            assert hits[0].chunk.content == "chunk-0"
            # After rebuild, cache count == real embedding count.
            assert s2._hnsw_cache[emb.model].count == 8
        finally:
            s2.close()


# ---------------------------------------------------------------------------
# cache invalidation — adds/deletes change the embedding count
# ---------------------------------------------------------------------------

class TestCacheInvalidation:
    def test_add_invalidates_cache(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store, n=5, dim=4, seed=10)
        store.search(np.zeros(4, dtype=np.float32),
                     model=emb.model, top_k=1)
        cached_before = store._hnsw_cache[emb.model]
        assert cached_before.count == 5

        # Add another chunk + embedding under the same entity.
        eid = store.get_entity("e-default").id
        new_ids = store.add_chunks(eid, ["chunk-extra"])

        def _fn(texts):
            arr = np.zeros((len(texts), 4), dtype=np.float32)
            for i, t in enumerate(texts):
                if t == "chunk-extra":
                    arr[i] = np.array([0.5, 0.5, 0.5, 0.5], dtype=np.float32)
            return arr
        ext_emb = LambdaEmbedder(_fn, model=emb.model, dim=4)
        store.embed_chunks(ext_emb, entity_id=eid, missing_only=True)

        store.search(np.zeros(4, dtype=np.float32),
                     model=emb.model, top_k=1)
        cached_after = store._hnsw_cache[emb.model]
        # Either a new instance (rebuild) or the same instance with bumped
        # count — both are acceptable. The invariant is: count is current.
        assert cached_after.count == 6

    def test_delete_invalidates_cache(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store, n=6, dim=4, seed=11)
        store.search(np.zeros(4, dtype=np.float32),
                     model=emb.model, top_k=1)
        assert store._hnsw_cache[emb.model].count == 6

        # Delete a chunk → cascade also drops its embedding row.
        first_chunk = store.list_chunks("e-default")[0]
        with store._write_ctx() as conn:
            conn.execute("DELETE FROM chunk WHERE id=?", (first_chunk.id,))

        store.search(np.zeros(4, dtype=np.float32),
                     model=emb.model, top_k=1)
        assert store._hnsw_cache[emb.model].count == 5

    def test_full_cleanup_drops_cache_entry(self, store: CarbonStore) -> None:
        emb, _, _ = _seed(store, n=3, dim=4, seed=12)
        store.search(np.zeros(4, dtype=np.float32),
                     model=emb.model, top_k=1)
        assert emb.model in store._hnsw_cache

        store.delete_entity("e-default")
        # Next search → no embeddings → cache entry should be removed.
        out = store.search(np.zeros(4, dtype=np.float32),
                           model=emb.model, top_k=1)
        assert out == []
        assert emb.model not in store._hnsw_cache


# ---------------------------------------------------------------------------
# namespace post-filter
# ---------------------------------------------------------------------------

class TestNamespacePostFilter:
    def test_namespace_filter_applied(self, tmp_path: Path) -> None:
        p = tmp_path / "kb.carbondata"
        s = create(p)
        try:
            # Two namespaces, same model; HNSW index spans both.
            _seed(s, namespace="ns-a", n=4, dim=4, seed=20)
            _seed(s, namespace="ns-b", n=4, dim=4, seed=21)
            # Sanity: 8 embeddings now across one model.
            assert s.stats()["embeddings"] == 8

            # Searching with namespace=ns-a must only return ns-a hits.
            hits = s.search(
                np.zeros(4, dtype=np.float32),
                model="m5-test", top_k=4, namespace="ns-a",
            )
            assert all(h.entity.namespace == "ns-a" for h in hits)
        finally:
            s.close()
