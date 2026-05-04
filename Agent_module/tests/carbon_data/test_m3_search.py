"""M3: embeddings + semantic search."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from Agent_module.carbon_data import (
    CarbonStore,
    LambdaEmbedder,
    NullEmbedder,
    SearchHit,
    create,
)
from Agent_module.carbon_data.index import (
    bytes_to_vector,
    search_brute,
    vector_to_bytes,
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


def _fixed_embedder(mapping: dict[str, list[float]], *, model="test", dim=3):
    """Embedder that returns pre-defined vectors for each text, else zeros."""
    arr_cache = {
        k: np.asarray(v, dtype=np.float32).reshape(-1) for k, v in mapping.items()
    }
    for k, v in arr_cache.items():
        if v.shape[0] != dim:
            raise ValueError(f"mapping[{k!r}] has dim {v.shape[0]} != {dim}")

    def _fn(texts):
        out = np.zeros((len(texts), dim), dtype=np.float32)
        for i, t in enumerate(texts):
            if t in arr_cache:
                out[i] = arr_cache[t]
        return out

    return LambdaEmbedder(_fn, model=model, dim=dim)


# ---------------------------------------------------------------------------
# pure vector utils
# ---------------------------------------------------------------------------

class TestVectorPacking:
    def test_roundtrip(self) -> None:
        v = np.array([1.0, 2.0, 3.0, 4.5], dtype=np.float32)
        blob = vector_to_bytes(v)
        back = bytes_to_vector(blob, 4)
        assert np.array_equal(back, v)

    def test_rejects_2d(self) -> None:
        with pytest.raises(ValueError):
            vector_to_bytes(np.zeros((2, 3), dtype=np.float32))

    def test_dim_mismatch_raises(self) -> None:
        blob = vector_to_bytes(np.array([1, 2, 3], dtype=np.float32))
        with pytest.raises(ValueError):
            bytes_to_vector(blob, 4)


class TestSearchBrute:
    def test_cosine_ranking(self) -> None:
        q = np.array([1.0, 0.0, 0.0], dtype=np.float32)
        vectors = np.array(
            [[1.0, 0.0, 0.0], [0.9, 0.1, 0.0], [0.0, 1.0, 0.0]],
            dtype=np.float32,
        )
        ids = ["match", "close", "far"]
        hits = search_brute(q, vectors, ids, top_k=3)
        assert [h[0] for h in hits] == ["match", "close", "far"]
        assert hits[0][1] > hits[1][1] > hits[2][1]

    def test_top_k_smaller_than_corpus(self) -> None:
        q = np.array([1.0, 0.0], dtype=np.float32)
        vectors = np.stack(
            [np.array([i, 0.0], dtype=np.float32) for i in range(1, 6)]
        )
        ids = [str(i) for i in range(1, 6)]
        hits = search_brute(q, vectors, ids, top_k=2)
        assert len(hits) == 2
        assert hits[0][0] in ids  # all cosines are 1.0 — order just stable top-k

    def test_empty_corpus(self) -> None:
        assert (
            search_brute(
                np.array([1.0], dtype=np.float32),
                np.zeros((0, 1), dtype=np.float32),
                [],
                top_k=5,
            )
            == []
        )

    def test_dim_mismatch(self) -> None:
        q = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        vectors = np.array([[1.0, 2.0]], dtype=np.float32)
        with pytest.raises(ValueError):
            search_brute(q, vectors, ["a"], top_k=1)


# ---------------------------------------------------------------------------
# Embedders
# ---------------------------------------------------------------------------

class TestEmbedders:
    def test_null_embedder_shape(self) -> None:
        emb = NullEmbedder(dim=16)
        out = emb.encode(["a", "b", "c"])
        assert out.shape == (3, 16)
        assert out.dtype == np.float32
        assert np.all(out == 0.0)

    def test_lambda_validates_shape(self) -> None:
        bad = LambdaEmbedder(
            lambda texts: np.zeros((len(texts), 5), dtype=np.float32),
            model="bad", dim=3,
        )
        with pytest.raises(ValueError, match="expected"):
            bad.encode(["x"])

    def test_lambda_casts_to_float32(self) -> None:
        emb = LambdaEmbedder(
            lambda texts: [[1, 2, 3] for _ in texts],
            model="ints", dim=3,
        )
        out = emb.encode(["x", "y"])
        assert out.dtype == np.float32
        assert out.shape == (2, 3)


# ---------------------------------------------------------------------------
# set_embedding / get_embedding
# ---------------------------------------------------------------------------

class TestEmbeddingCrud:
    def test_set_then_get(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.add_chunks("e1", ["hello"])
        c = store.list_chunks("e1")[0]
        v = np.array([0.1, 0.2, 0.3], dtype=np.float32)
        store.set_embedding(c.id, v, model="m1")
        got = store.get_embedding(c.id, model="m1")
        assert got is not None
        np.testing.assert_array_equal(got, v)

    def test_set_replaces_existing(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.add_chunks("e1", ["hello"])
        c = store.list_chunks("e1")[0]
        store.set_embedding(c.id, [0.1, 0.2], model="m1")
        store.set_embedding(c.id, [0.9, 0.8], model="m1")
        got = store.get_embedding(c.id, model="m1")
        np.testing.assert_array_almost_equal(got, [0.9, 0.8])

    def test_multiple_models_per_chunk(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.add_chunks("e1", ["hello"])
        c = store.list_chunks("e1")[0]
        store.set_embedding(c.id, [1.0, 0.0], model="small")
        store.set_embedding(c.id, [0.0, 0.0, 1.0], model="big")
        np.testing.assert_array_equal(
            store.get_embedding(c.id, model="small"), [1.0, 0.0]
        )
        np.testing.assert_array_equal(
            store.get_embedding(c.id, model="big"), [0.0, 0.0, 1.0]
        )

    def test_set_on_missing_chunk(self, store: CarbonStore) -> None:
        with pytest.raises(KeyError):
            store.set_embedding("ghost", [1.0, 2.0], model="m1")

    def test_get_missing_returns_none(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.add_chunks("e1", ["hello"])
        c = store.list_chunks("e1")[0]
        assert store.get_embedding(c.id, model="nothere") is None


# ---------------------------------------------------------------------------
# embed_chunks
# ---------------------------------------------------------------------------

class TestEmbedChunks:
    def test_embeds_all_pending(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.add_chunks("e1", ["a", "b", "c"])
        emb = NullEmbedder(dim=4, model="m1")
        count = store.embed_chunks(emb)
        assert count == 3
        for c in store.list_chunks("e1"):
            v = store.get_embedding(c.id, model="m1")
            assert v is not None and v.shape == (4,)

    def test_missing_only_skips_existing(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.add_chunks("e1", ["a", "b"])
        emb = NullEmbedder(dim=2, model="m1")
        assert store.embed_chunks(emb) == 2
        # second call should skip all
        assert store.embed_chunks(emb) == 0

    def test_missing_only_false_reencodes(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.add_chunks("e1", ["a"])
        emb = NullEmbedder(dim=2, model="m1")
        assert store.embed_chunks(emb) == 1
        assert store.embed_chunks(emb, missing_only=False) == 1

    def test_scoped_to_entity(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.put_entity(id="e2", kind="doc")
        store.add_chunks("e1", ["a", "b"])
        store.add_chunks("e2", ["c", "d"])
        emb = NullEmbedder(dim=2, model="m1")
        assert store.embed_chunks(emb, entity_id="e1") == 2
        # e2 chunks still pending
        assert store.embed_chunks(emb) == 2


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------

class TestSearch:
    def _seed_docs(self, store: CarbonStore):
        store.put_entity(id="d1", kind="doc", metadata={"topic": "cats"})
        store.put_entity(id="d2", kind="doc", metadata={"topic": "dogs"})
        store.add_chunks("d1", ["a cat sat"])
        store.add_chunks("d2", ["a dog ran"])
        emb = _fixed_embedder({
            "a cat sat": [1.0, 0.0, 0.0],
            "a dog ran": [0.0, 1.0, 0.0],
        })
        store.embed_chunks(emb)
        return emb

    def test_vector_query(self, store: CarbonStore) -> None:
        emb = self._seed_docs(store)
        hits = store.search(
            np.array([1.0, 0.0, 0.0], dtype=np.float32),
            model=emb.model,
            top_k=2,
        )
        assert len(hits) == 2
        assert isinstance(hits[0], SearchHit)
        assert hits[0].entity.id == "d1"
        assert hits[0].score > hits[1].score

    def test_string_query_uses_embedder(self, store: CarbonStore) -> None:
        emb = self._seed_docs(store)
        # embedder also maps the query string:
        emb_with_query = _fixed_embedder({
            "a cat sat": [1.0, 0.0, 0.0],
            "a dog ran": [0.0, 1.0, 0.0],
            "kitten": [1.0, 0.0, 0.0],
        })
        store.embed_chunks(emb_with_query, missing_only=False)
        hits = store.search("kitten", embedder=emb_with_query, top_k=1)
        assert len(hits) == 1
        assert hits[0].entity.id == "d1"

    def test_string_query_without_embedder_errors(
        self, store: CarbonStore
    ) -> None:
        self._seed_docs(store)
        with pytest.raises(TypeError, match="embedder"):
            store.search("hi", model="test", top_k=1)

    def test_search_requires_model_or_embedder(
        self, store: CarbonStore
    ) -> None:
        self._seed_docs(store)
        with pytest.raises(TypeError, match="model"):
            store.search(
                np.array([1.0, 0.0, 0.0], dtype=np.float32), top_k=1
            )

    def test_filters_restrict_results(self, store: CarbonStore) -> None:
        emb = self._seed_docs(store)
        hits = store.search(
            np.array([1.0, 0.0, 0.0], dtype=np.float32),
            model=emb.model,
            filters={"metadata.topic": "dogs"},
            top_k=5,
        )
        # Only d2 survives filter
        assert {h.entity.id for h in hits} == {"d2"}

    def test_namespace_isolation(self, store: CarbonStore) -> None:
        store.put_entity(id="a", kind="doc", namespace="ns_a")
        store.put_entity(id="b", kind="doc", namespace="ns_b")
        store.add_chunks("a", ["same text"])
        store.add_chunks("b", ["same text"])
        emb = _fixed_embedder({"same text": [1.0, 0.0, 0.0]})
        store.embed_chunks(emb)

        hits_a = store.search(
            np.array([1.0, 0.0, 0.0], dtype=np.float32),
            model=emb.model,
            namespace="ns_a",
            top_k=10,
        )
        assert {h.entity.id for h in hits_a} == {"a"}

        hits_b = store.search(
            np.array([1.0, 0.0, 0.0], dtype=np.float32),
            model=emb.model,
            namespace="ns_b",
            top_k=10,
        )
        assert {h.entity.id for h in hits_b} == {"b"}

    def test_model_isolation(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.add_chunks("e1", ["x"])
        c = store.list_chunks("e1")[0]
        store.set_embedding(c.id, [1.0, 0.0], model="m1")
        store.set_embedding(c.id, [0.0, 1.0], model="m2")
        # Query under m1: found
        hits_m1 = store.search(
            np.array([1.0, 0.0], dtype=np.float32), model="m1", top_k=1
        )
        assert len(hits_m1) == 1
        # Query under m2: different vector, still finds the chunk (only 1)
        hits_m2 = store.search(
            np.array([0.0, 1.0], dtype=np.float32), model="m2", top_k=1
        )
        assert len(hits_m2) == 1

    def test_search_empty_store(self, store: CarbonStore) -> None:
        assert (
            store.search(
                np.array([1.0, 0.0, 0.0], dtype=np.float32),
                model="nothing",
                top_k=5,
            )
            == []
        )

    def test_search_query_dim_mismatch(self, store: CarbonStore) -> None:
        emb = self._seed_docs(store)
        with pytest.raises(ValueError, match="query dim"):
            store.search(
                np.array([1.0, 0.0], dtype=np.float32),  # 2-D query vs 3-D corpus
                model=emb.model,
                top_k=1,
            )

    def test_top_k_caps_results(self, store: CarbonStore) -> None:
        store.put_entity(id="e1", kind="doc")
        store.add_chunks("e1", [f"chunk {i}" for i in range(5)])
        emb = NullEmbedder(dim=3, model="null")
        # With zero vectors the ranks will all tie at 0 cosine after eps —
        # still need to verify top_k truncates.
        store.embed_chunks(emb)
        hits = store.search(
            np.array([1.0, 0.0, 0.0], dtype=np.float32),
            model="null", top_k=3,
        )
        assert len(hits) == 3
