"""M9: management tools — validate, compact, export."""
from __future__ import annotations

import base64
import json
import sqlite3
from pathlib import Path

import numpy as np
import pytest

from Agent_module.carbon_data import (
    APPLICATION_ID,
    CarbonStore,
    LambdaEmbedder,
    ValidationReport,
    create,
    open as cd_open,
)


@pytest.fixture
def store(tmp_path: Path):
    p = tmp_path / "kb.carbondata"
    s = create(p)
    yield s
    if not s.closed:
        s.close()


def _populate(s: CarbonStore) -> None:
    """Drop a small but multi-feature corpus into the store."""
    s.put_entity(id="d1", kind="document", content="hello world")
    s.add_chunks("d1", ["hello world"])
    s.put_entity(id="d2", kind="document", content="another")
    s.add_chunks("d2", ["another"])
    s.add_relation("d1", "d2", "references")

    def _fn(texts):
        return np.ones((len(texts), 4), dtype=np.float32)
    emb = LambdaEmbedder(_fn, model="m9", dim=4)
    s.embed_chunks(emb)
    s.remember("user likes vim", session_id="s1")


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

class TestValidateClean:
    def test_clean_db_passes(self, store: CarbonStore) -> None:
        _populate(store)
        r = store.validate()
        assert isinstance(r, ValidationReport)
        assert r.ok is True
        assert r.issues == []
        assert r.counts["entity"] == 3  # d1, d2, memory entity
        assert r.counts["chunk"] == 3
        assert r.counts["embedding"] == 2
        assert r.counts["relation"] == 1
        assert r.counts["memory_ext"] == 1

    def test_empty_db_passes(self, store: CarbonStore) -> None:
        r = store.validate()
        assert r.ok is True
        assert r.counts["entity"] == 0


class TestValidateDetectsIssues:
    def test_detects_stale_hnsw_meta(self, tmp_path: Path) -> None:
        p = tmp_path / "kb.carbondata"
        s = create(p)
        try:
            _populate(s)
            # Trigger HNSW build + persist.
            s.search(np.ones(4, dtype=np.float32), model="m9", top_k=1)
            # Tamper with persisted count.
            s._require_rw().execute(
                "UPDATE vector_index SET index_meta=? WHERE model='m9'",
                ('{"count": 999, "dim": 4}',),
            )
            s._require_rw().commit()
            r = s.validate()
            assert r.ok is False
            assert any("vector_index[m9]" in msg and "stale" in msg
                       for msg in r.issues)
        finally:
            s.close()

    def test_detects_corrupt_index_meta(self, tmp_path: Path) -> None:
        p = tmp_path / "kb.carbondata"
        s = create(p)
        try:
            _populate(s)
            s.search(np.ones(4, dtype=np.float32), model="m9", top_k=1)
            s._require_rw().execute(
                "UPDATE vector_index SET index_meta=? WHERE model='m9'",
                ("not a json",),
            )
            s._require_rw().commit()
            r = s.validate()
            assert r.ok is False
            assert any("corrupt index_meta" in msg for msg in r.issues)
        finally:
            s.close()


# ---------------------------------------------------------------------------
# compact
# ---------------------------------------------------------------------------

class TestCompact:
    def test_returns_size_pair(self, store: CarbonStore) -> None:
        _populate(store)
        result = store.compact()
        assert "size_before" in result and "size_after" in result
        assert result["size_before"] >= 0
        assert result["size_after"] >= 0

    def test_clears_persisted_hnsw_blob(self, store: CarbonStore) -> None:
        _populate(store)
        # Force persistence.
        store.search(np.ones(4, dtype=np.float32), model="m9", top_k=1)
        assert store._require_open().execute(
            "SELECT COUNT(*) FROM vector_index"
        ).fetchone()[0] == 1
        store.compact()
        assert store._require_open().execute(
            "SELECT COUNT(*) FROM vector_index"
        ).fetchone()[0] == 0
        assert "m9" not in store._hnsw_cache

    def test_rebuild_recovers_search(self, store: CarbonStore) -> None:
        _populate(store)
        store.compact()
        # After compact, search should still work — HNSW rebuilt lazily.
        hits = store.search(np.ones(4, dtype=np.float32), model="m9", top_k=2)
        assert len(hits) == 2

    def test_fts_still_works_after_compact(self, store: CarbonStore) -> None:
        _populate(store)
        store.compact()
        hits = store.search("hello", mode="keyword")
        assert len(hits) >= 1
        assert any("hello" in h.chunk.content for h in hits)

    def test_rejected_inside_transaction(self, store: CarbonStore) -> None:
        with pytest.raises(RuntimeError, match="transaction"):
            with store.transaction():
                store.compact()


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

class TestExport:
    def test_writes_json_file_with_expected_shape(
        self, store: CarbonStore, tmp_path: Path
    ) -> None:
        _populate(store)
        out = tmp_path / "dump.json"
        report = store.export(out)
        assert out.exists()
        assert report["bytes_written"] == out.stat().st_size

        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["schema_version"] == 1
        assert {"entities", "chunks", "embeddings", "relations",
                "memory_ext"} <= set(data.keys())
        assert len(data["entities"]) == report["entities"]
        assert len(data["chunks"]) == report["chunks"]
        assert len(data["relations"]) == report["relations"]

    def test_embeddings_round_trip_via_base64(
        self, store: CarbonStore, tmp_path: Path
    ) -> None:
        _populate(store)
        out = tmp_path / "dump.json"
        store.export(out)
        data = json.loads(out.read_text(encoding="utf-8"))

        for emb_record in data["embeddings"]:
            raw = base64.b64decode(emb_record["vector_b64"])
            arr = np.frombuffer(raw, dtype=np.float32)
            assert arr.shape == (emb_record["dim"],)
            # _populate seeds all-ones vectors.
            assert np.allclose(arr, 1.0)

    def test_include_embeddings_false_omits_them(
        self, store: CarbonStore, tmp_path: Path
    ) -> None:
        _populate(store)
        out = tmp_path / "dump.json"
        store.export(out, include_embeddings=False)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["embeddings"] == []

    def test_export_then_reload_matches_counts(
        self, store: CarbonStore, tmp_path: Path
    ) -> None:
        _populate(store)
        out = tmp_path / "dump.json"
        store.export(out)
        data = json.loads(out.read_text(encoding="utf-8"))

        # Re-open the original; counts should match what we exported.
        path = store.path
        store.close()
        s2 = cd_open(path)
        try:
            stats = s2.stats()
            assert len(data["entities"]) == stats["entities"]
            assert len(data["chunks"]) == stats["chunks"]
            assert len(data["embeddings"]) == stats["embeddings"]
            assert len(data["relations"]) == stats["relations"]
            assert len(data["memory_ext"]) == stats["memories"]
        finally:
            s2.close()

    def test_metadata_preserved(
        self, store: CarbonStore, tmp_path: Path
    ) -> None:
        store.put_entity(
            id="d", kind="document",
            metadata={"author": "Alice", "tags": ["x", "y"]},
        )
        out = tmp_path / "dump.json"
        store.export(out)
        data = json.loads(out.read_text(encoding="utf-8"))
        ent = next(e for e in data["entities"] if e["id"] == "d")
        assert ent["metadata"] == {"author": "Alice", "tags": ["x", "y"]}

    def test_application_id_constant_exported(self) -> None:
        # Sanity: confirm the carbondata signature still matches what the
        # spec promises (used as a doc/regression anchor).
        assert APPLICATION_ID == 0x4342_4E44
