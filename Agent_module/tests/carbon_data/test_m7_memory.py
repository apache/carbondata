"""M7: long-term memory — remember / recall / forget."""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pytest

from Agent_module.carbon_data import (
    CarbonStore,
    LambdaEmbedder,
    MemoryHit,
    MemoryItem,
    create,
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
    arr = {k: np.asarray(v, dtype=np.float32).reshape(-1) for k, v in mapping.items()}

    def _fn(texts):
        out = np.zeros((len(texts), dim), dtype=np.float32)
        for i, t in enumerate(texts):
            if t in arr:
                out[i] = arr[t]
        return out

    return LambdaEmbedder(_fn, model=model, dim=dim)


# ---------------------------------------------------------------------------
# remember
# ---------------------------------------------------------------------------

class TestRemember:
    def test_creates_entity_chunk_and_memory_ext(self, store: CarbonStore) -> None:
        eid = store.remember("user prefers dark mode", session_id="s1", actor="ui-bot")
        ent = store.get_entity(eid)
        assert ent is not None
        assert ent.kind == "memory"
        assert ent.content == "user prefers dark mode"

        chunks = store.list_chunks(eid)
        assert len(chunks) == 1
        assert chunks[0].content == "user prefers dark mode"

        mem = store._load_memory(eid)
        assert isinstance(mem, MemoryItem)
        assert mem.session_id == "s1"
        assert mem.actor == "ui-bot"
        assert mem.salience == 0.5  # default

    def test_returns_assigned_id(self, store: CarbonStore) -> None:
        eid = store.remember("foo", id="mem-001")
        assert eid == "mem-001"
        assert store.get_entity("mem-001") is not None

    def test_auto_uuid_when_no_id(self, store: CarbonStore) -> None:
        eid = store.remember("foo")
        assert isinstance(eid, str) and len(eid) >= 16

    def test_ttl_sets_expires_at(self, store: CarbonStore) -> None:
        before = time.time()
        eid = store.remember("expires soon", ttl=60)
        mem = store._load_memory(eid)
        assert mem.expires_at is not None
        assert before + 59 <= mem.expires_at <= time.time() + 61

    def test_explicit_expires_at(self, store: CarbonStore) -> None:
        eat = time.time() + 3600
        eid = store.remember("X", expires_at=eat)
        mem = store._load_memory(eid)
        assert mem.expires_at == eat

    def test_ttl_and_expires_at_mutually_exclusive(self, store: CarbonStore) -> None:
        with pytest.raises(TypeError, match="ttl OR expires_at"):
            store.remember("x", ttl=60, expires_at=time.time() + 60)

    def test_salience_bounds_validated(self, store: CarbonStore) -> None:
        with pytest.raises(ValueError, match="salience"):
            store.remember("x", salience=1.5)
        with pytest.raises(ValueError, match="salience"):
            store.remember("x", salience=-0.1)

    def test_eager_embedding_via_embedder(self, store: CarbonStore) -> None:
        emb = _fixed_embedder({"likes vim": [1.0, 0.0, 0.0]})
        eid = store.remember("likes vim", embedder=emb)
        # Should have an embedding under the embedder's model.
        chunk = store.list_chunks(eid)[0]
        vec = store.get_embedding(chunk.id, model=emb.model)
        assert vec is not None
        assert vec.shape == (3,)

    def test_atomic_failure_rolls_back(self, store: CarbonStore) -> None:
        # If embedding fails, no entity/chunk should remain.
        class BoomEmbedder:
            model = "boom"
            dim = 3

            def encode(self, texts):
                raise RuntimeError("boom")

        before = store.stats()
        with pytest.raises(RuntimeError, match="boom"):
            store.remember("doomed", embedder=BoomEmbedder())
        after = store.stats()
        assert after["entities"] == before["entities"]
        assert after["chunks"] == before["chunks"]
        assert after["memories"] == before["memories"]

    def test_namespace_isolation(self, store: CarbonStore) -> None:
        eid = store.remember("private", namespace="tenant-a")
        ent = store.get_entity(eid)
        assert ent.namespace == "tenant-a"

    def test_metadata_passes_through(self, store: CarbonStore) -> None:
        eid = store.remember("note", metadata={"topic": "auth"})
        ent = store.get_entity(eid)
        assert ent.metadata == {"topic": "auth"}


# ---------------------------------------------------------------------------
# recall
# ---------------------------------------------------------------------------

class TestRecall:
    def test_semantic_ranking(self, store: CarbonStore) -> None:
        emb = _fixed_embedder(
            {
                "loves vim":     [1.0, 0.0, 0.0],
                "prefers emacs": [0.0, 1.0, 0.0],
                "likes nano":    [0.0, 0.0, 1.0],
                "vim editor":    [0.95, 0.05, 0.0],  # query
            },
            dim=3,
        )
        store.remember("loves vim", embedder=emb)
        store.remember("prefers emacs", embedder=emb)
        store.remember("likes nano", embedder=emb)

        hits = store.recall("vim editor", top_k=3, embedder=emb)
        assert len(hits) == 3
        assert all(isinstance(h, MemoryHit) for h in hits)
        assert hits[0].memory.content == "loves vim"

    def test_session_filter(self, store: CarbonStore) -> None:
        emb = _fixed_embedder(
            {"a": [1.0, 0.0, 0.0], "b": [1.0, 0.0, 0.0]}, dim=3
        )
        store.remember("a", session_id="s1", embedder=emb)
        store.remember("b", session_id="s2", embedder=emb)

        hits = store.recall("a", session_id="s1", top_k=5, embedder=emb)
        assert len(hits) == 1
        assert hits[0].memory.session_id == "s1"

    def test_actor_filter(self, store: CarbonStore) -> None:
        emb = _fixed_embedder(
            {"a": [1.0, 0.0, 0.0], "b": [1.0, 0.0, 0.0]}, dim=3
        )
        store.remember("a", actor="bot1", embedder=emb)
        store.remember("b", actor="bot2", embedder=emb)

        hits = store.recall("a", actor="bot1", embedder=emb)
        assert {h.memory.actor for h in hits} == {"bot1"}

    def test_min_salience_filter(self, store: CarbonStore) -> None:
        emb = _fixed_embedder(
            {"low": [1.0, 0.0, 0.0], "high": [1.0, 0.0, 0.0]}, dim=3
        )
        store.remember("low", salience=0.2, embedder=emb)
        store.remember("high", salience=0.9, embedder=emb)

        hits = store.recall("low", min_salience=0.5, embedder=emb)
        assert len(hits) == 1
        assert hits[0].memory.salience == 0.9

    def test_excludes_expired_by_default(self, store: CarbonStore) -> None:
        emb = _fixed_embedder(
            {"old": [1.0, 0.0, 0.0], "fresh": [1.0, 0.0, 0.0]}, dim=3
        )
        store.remember("old", expires_at=time.time() - 1, embedder=emb)
        store.remember("fresh", embedder=emb)

        hits = store.recall("old", embedder=emb)
        assert {h.memory.content for h in hits} == {"fresh"}

    def test_include_expired_opts_in(self, store: CarbonStore) -> None:
        emb = _fixed_embedder(
            {"old": [1.0, 0.0, 0.0], "fresh": [1.0, 0.0, 0.0]}, dim=3
        )
        store.remember("old", expires_at=time.time() - 1, embedder=emb)
        store.remember("fresh", embedder=emb)

        hits = store.recall(
            "old", embedder=emb, include_expired=True, top_k=5
        )
        assert {h.memory.content for h in hits} == {"old", "fresh"}

    def test_namespace_isolation(self, store: CarbonStore) -> None:
        emb = _fixed_embedder(
            {"a": [1.0, 0.0, 0.0], "b": [1.0, 0.0, 0.0]}, dim=3
        )
        store.remember("a", namespace="ns1", embedder=emb)
        store.remember("b", namespace="ns2", embedder=emb)

        hits = store.recall("a", namespace="ns1", embedder=emb)
        assert len(hits) == 1
        assert hits[0].memory.namespace == "ns1"

    def test_does_not_match_non_memory_entities(self, store: CarbonStore) -> None:
        # Even if a regular document-kind entity is in the store, recall
        # restricts to kind='memory'.
        emb = _fixed_embedder(
            {"doc-text": [1.0, 0.0, 0.0], "mem-text": [1.0, 0.0, 0.0]}, dim=3
        )
        store.put_entity(id="doc1", kind="document", content="doc-text")
        store.add_chunks("doc1", ["doc-text"])
        store.embed_chunks(emb, entity_id="doc1")
        store.remember("mem-text", embedder=emb)

        hits = store.recall("doc-text", embedder=emb, top_k=5)
        assert all(h.memory.kind == "memory" for h in hits)
        assert len(hits) == 1

    def test_empty_store_returns_empty(self, store: CarbonStore) -> None:
        emb = _fixed_embedder({"q": [1.0, 0.0, 0.0]}, dim=3)
        assert store.recall("q", embedder=emb) == []

    def test_requires_model_or_embedder(self, store: CarbonStore) -> None:
        with pytest.raises(TypeError, match="model="):
            store.recall("q")

    def test_string_query_requires_embedder(self, store: CarbonStore) -> None:
        with pytest.raises(TypeError, match="embedder="):
            store.recall("q", model="m")

    def test_vector_query_works_without_embedder(self, store: CarbonStore) -> None:
        emb = _fixed_embedder({"a": [1.0, 0.0, 0.0]}, dim=3)
        store.remember("a", embedder=emb)
        hits = store.recall(
            np.array([1.0, 0.0, 0.0]), model=emb.model, top_k=3
        )
        assert len(hits) == 1


# ---------------------------------------------------------------------------
# forget
# ---------------------------------------------------------------------------

class TestForget:
    def test_no_criteria_raises(self, store: CarbonStore) -> None:
        with pytest.raises(TypeError, match="at least one criterion"):
            store.forget()

    def test_by_entity_id(self, store: CarbonStore) -> None:
        eid = store.remember("x", session_id="s1")
        n = store.forget(entity_id=eid)
        assert n == 1
        assert store.get_entity(eid) is None

    def test_by_session(self, store: CarbonStore) -> None:
        store.remember("a", session_id="s1")
        store.remember("b", session_id="s1")
        store.remember("c", session_id="s2")
        n = store.forget(session_id="s1")
        assert n == 2
        # s2 untouched
        remaining_sessions = [
            store._load_memory(eid).session_id
            for eid, in store._require_open().execute(
                "SELECT id FROM entity WHERE kind='memory'"
            )
        ]
        assert remaining_sessions == ["s2"]

    def test_by_actor(self, store: CarbonStore) -> None:
        store.remember("a", actor="bot")
        store.remember("b", actor="user")
        n = store.forget(actor="bot")
        assert n == 1

    def test_by_namespace(self, store: CarbonStore) -> None:
        store.remember("a", namespace="t1")
        store.remember("b", namespace="t2")
        n = store.forget(namespace="t1")
        assert n == 1

    def test_by_before(self, store: CarbonStore) -> None:
        eid_old = store.remember("old")
        # Force created_at to a known past timestamp.
        store._require_rw().execute(
            "UPDATE entity SET created_at=? WHERE id=?",
            (1000.0, eid_old),
        )
        store._require_rw().commit()
        store.remember("new")  # created_at = now
        n = store.forget(before=2000.0)
        assert n == 1

    def test_only_targets_memories(self, store: CarbonStore) -> None:
        # A document-kind entity in the same namespace should not be
        # touched by forget(namespace=...).
        store.put_entity(id="doc", kind="document", namespace="ns1")
        store.remember("m", namespace="ns1")
        n = store.forget(namespace="ns1")
        assert n == 1
        assert store.get_entity("doc") is not None

    def test_combined_criteria_are_anded(self, store: CarbonStore) -> None:
        store.remember("a", session_id="s1", actor="bot")
        store.remember("b", session_id="s1", actor="user")
        store.remember("c", session_id="s2", actor="bot")
        n = store.forget(session_id="s1", actor="bot")
        assert n == 1

    def test_no_match_returns_zero(self, store: CarbonStore) -> None:
        store.remember("x", session_id="s1")
        assert store.forget(session_id="nonexistent") == 0

    def test_cascade_removes_chunks_and_embeddings(self, store: CarbonStore) -> None:
        emb = _fixed_embedder({"x": [1.0, 0.0, 0.0]})
        eid = store.remember("x", session_id="s1", embedder=emb)
        before = store.stats()
        assert before["chunks"] >= 1
        assert before["embeddings"] >= 1

        store.forget(session_id="s1")
        after = store.stats()
        assert after["entities"] == before["entities"] - 1
        assert after["chunks"] == before["chunks"] - 1
        assert after["embeddings"] == before["embeddings"] - 1
        assert after["memories"] == before["memories"] - 1


# ---------------------------------------------------------------------------
# forget_expired
# ---------------------------------------------------------------------------

class TestForgetExpired:
    def test_clears_past_expirations(self, store: CarbonStore) -> None:
        store.remember("a", expires_at=time.time() - 10)
        store.remember("b", expires_at=time.time() - 1)
        store.remember("c", expires_at=time.time() + 3600)
        store.remember("d")  # no expiry
        n = store.forget_expired()
        assert n == 2

    def test_explicit_now_boundary(self, store: CarbonStore) -> None:
        # expires_at == now → expired (uses <=).
        eid = store.remember("boundary", expires_at=1500.0)
        n = store.forget_expired(now=1500.0)
        assert n == 1
        assert store.get_entity(eid) is None

    def test_explicit_now_strictly_before(self, store: CarbonStore) -> None:
        store.remember("not_yet", expires_at=2000.0)
        n = store.forget_expired(now=1999.999)
        assert n == 0

    def test_no_expirations_returns_zero(self, store: CarbonStore) -> None:
        store.remember("permanent")
        assert store.forget_expired() == 0
