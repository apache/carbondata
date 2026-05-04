"""M2: Entity/Chunk CRUD + structured query + transactions."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from Agent_module.carbon_data import CarbonStore, Chunk, Entity, create
from Agent_module.carbon_data import open as cd_open


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


# ---------------------------------------------------------------------------
# Entity CRUD
# ---------------------------------------------------------------------------

class TestEntityCrud:
    def test_put_then_get_kwargs(self, store: CarbonStore) -> None:
        eid = store.put_entity(
            id="doc1", kind="document", content="hello",
            metadata={"author": "X", "tags": ["a", "b"]},
        )
        assert eid == "doc1"
        got = store.get_entity("doc1")
        assert got is not None
        assert got.id == "doc1"
        assert got.kind == "document"
        assert got.namespace == "default"
        assert got.content == "hello"
        assert got.metadata == {"author": "X", "tags": ["a", "b"]}
        assert got.version == 1

    def test_put_with_entity_object(self, store: CarbonStore) -> None:
        e = Entity(id="doc2", kind="document", namespace="ns1", content="x")
        store.put_entity(e)
        got = store.get_entity("doc2")
        assert got is not None
        assert got.namespace == "ns1"

    def test_put_requires_id_and_kind(self, store: CarbonStore) -> None:
        with pytest.raises(TypeError):
            store.put_entity()
        with pytest.raises(TypeError):
            store.put_entity(id="x")
        with pytest.raises(TypeError):
            store.put_entity(kind="doc")

    def test_put_mixing_entity_and_kwargs_rejected(self, store: CarbonStore) -> None:
        e = Entity(id="x", kind="doc")
        with pytest.raises(TypeError):
            store.put_entity(e, content="hi")

    def test_get_missing_returns_none(self, store: CarbonStore) -> None:
        assert store.get_entity("nope") is None

    def test_upsert_bumps_version_preserves_created_at(
        self, store: CarbonStore
    ) -> None:
        store.put_entity(id="x", kind="doc", content="v1")
        first = store.get_entity("x")
        assert first is not None
        store.put_entity(id="x", kind="doc", content="v2", metadata={"y": 1})
        second = store.get_entity("x")
        assert second is not None
        assert second.content == "v2"
        assert second.metadata == {"y": 1}
        assert second.version == first.version + 1
        assert second.created_at == first.created_at
        assert second.updated_at >= first.updated_at

    def test_delete_entity_returns_true(self, store: CarbonStore) -> None:
        store.put_entity(id="x", kind="doc")
        assert store.delete_entity("x") is True
        assert store.get_entity("x") is None

    def test_delete_missing_returns_false(self, store: CarbonStore) -> None:
        assert store.delete_entity("ghost") is False

    def test_delete_cascades_to_chunks(self, store: CarbonStore) -> None:
        store.put_entity(id="x", kind="doc")
        store.add_chunks("x", ["a", "b", "c"])
        assert len(store.list_chunks("x")) == 3
        store.delete_entity("x")
        assert store.list_chunks("x") == []

    def test_metadata_roundtrip_nested(self, store: CarbonStore) -> None:
        meta = {
            "nested": {"a": [1, 2, {"b": True}]},
            "unicode": "你好",
            "none": None,
        }
        store.put_entity(id="x", kind="doc", metadata=meta)
        got = store.get_entity("x")
        assert got is not None
        assert got.metadata == meta


# ---------------------------------------------------------------------------
# Chunk CRUD
# ---------------------------------------------------------------------------

class TestChunkCrud:
    def test_add_chunks_from_strings(self, store: CarbonStore) -> None:
        store.put_entity(id="x", kind="doc")
        ids = store.add_chunks("x", ["first", "second", "third"])
        assert len(ids) == 3
        chunks = store.list_chunks("x")
        assert [c.content for c in chunks] == ["first", "second", "third"]
        assert [c.seq for c in chunks] == [0, 1, 2]

    def test_add_chunks_mixed_types(self, store: CarbonStore) -> None:
        store.put_entity(id="x", kind="doc")
        ids = store.add_chunks("x", [
            "plain string",
            Chunk(id="c2", entity_id="x", seq=0, content="from dataclass", kind="code"),
            {"content": "from dict", "kind": "row", "metadata": {"col": "name"}},
        ])
        assert len(ids) == 3
        chunks = store.list_chunks("x")
        assert chunks[0].kind == "text"
        assert chunks[1].id == "c2"
        assert chunks[1].kind == "code"
        assert chunks[2].kind == "row"
        assert chunks[2].metadata == {"col": "name"}

    def test_add_chunks_seq_continues(self, store: CarbonStore) -> None:
        store.put_entity(id="x", kind="doc")
        store.add_chunks("x", ["a", "b"])
        store.add_chunks("x", ["c", "d"])
        chunks = store.list_chunks("x")
        assert [c.seq for c in chunks] == [0, 1, 2, 3]
        assert [c.content for c in chunks] == ["a", "b", "c", "d"]

    def test_add_chunks_base_seq_override(self, store: CarbonStore) -> None:
        store.put_entity(id="x", kind="doc")
        store.add_chunks("x", ["a"])
        store.add_chunks("x", ["b", "c"], base_seq=100)
        chunks = store.list_chunks("x")
        assert [c.seq for c in chunks] == [0, 100, 101]

    def test_add_chunks_entity_id_overridden(self, store: CarbonStore) -> None:
        store.put_entity(id="x", kind="doc")
        # Chunk constructed with a different entity_id gets corrected.
        store.add_chunks("x", [
            Chunk(id="c1", entity_id="WRONG", seq=0, content="hi"),
        ])
        assert store.get_chunk("c1").entity_id == "x"  # type: ignore[union-attr]

    def test_add_chunks_missing_entity_raises(self, store: CarbonStore) -> None:
        with pytest.raises(KeyError):
            store.add_chunks("nope", ["hi"])

    def test_add_chunks_bad_type_raises(self, store: CarbonStore) -> None:
        store.put_entity(id="x", kind="doc")
        with pytest.raises(TypeError):
            store.add_chunks("x", [123])  # type: ignore[list-item]

    def test_get_chunk_missing(self, store: CarbonStore) -> None:
        assert store.get_chunk("ghost") is None

    def test_delete_chunks(self, store: CarbonStore) -> None:
        store.put_entity(id="x", kind="doc")
        store.add_chunks("x", ["a", "b"])
        assert store.delete_chunks("x") == 2
        assert store.list_chunks("x") == []


# ---------------------------------------------------------------------------
# Structured query
# ---------------------------------------------------------------------------

class TestQueryEntities:
    @pytest.fixture
    def populated(self, store: CarbonStore) -> CarbonStore:
        store.put_entity(
            id="d1", kind="document",
            metadata={"author": "Alice", "year": 2020},
        )
        store.put_entity(
            id="d2", kind="document",
            metadata={"author": "Bob", "year": 2023},
        )
        store.put_entity(
            id="c1", kind="code",
            metadata={"author": "Alice", "lang": "python"},
        )
        store.put_entity(
            id="n1", kind="note", namespace="archive",
            metadata={"author": "Charlie"},
        )
        return store

    def test_filter_by_kind(self, populated: CarbonStore) -> None:
        results = populated.query_entities(kind="document")
        assert {e.id for e in results} == {"d1", "d2"}

    def test_filter_by_namespace(self, populated: CarbonStore) -> None:
        results = populated.query_entities(namespace="archive")
        assert {e.id for e in results} == {"n1"}

    def test_filter_by_metadata_equality(self, populated: CarbonStore) -> None:
        results = populated.query_entities(where={"metadata.author": "Alice"})
        assert {e.id for e in results} == {"d1", "c1"}

    def test_filter_by_operator_tuple(self, populated: CarbonStore) -> None:
        results = populated.query_entities(
            where={"metadata.year": (">=", 2023)},
        )
        assert {e.id for e in results} == {"d2"}

    def test_filter_by_in(self, populated: CarbonStore) -> None:
        results = populated.query_entities(
            where={"kind": ("IN", ["document", "code"])},
        )
        assert {e.id for e in results} == {"d1", "d2", "c1"}

    def test_combined_filters(self, populated: CarbonStore) -> None:
        results = populated.query_entities(
            kind="document",
            where={"metadata.author": "Alice"},
        )
        assert {e.id for e in results} == {"d1"}

    def test_limit_and_offset(self, populated: CarbonStore) -> None:
        all_ids = [e.id for e in populated.query_entities()]
        assert len(all_ids) == 4
        page1 = populated.query_entities(limit=2, offset=0)
        page2 = populated.query_entities(limit=2, offset=2)
        combined = [e.id for e in page1] + [e.id for e in page2]
        assert combined == all_ids

    def test_order_by_custom(self, populated: CarbonStore) -> None:
        results = populated.query_entities(order_by="id ASC")
        assert [e.id for e in results] == ["c1", "d1", "d2", "n1"]

    def test_reject_unknown_column_in_where(self, populated: CarbonStore) -> None:
        with pytest.raises(ValueError, match="unknown entity column"):
            populated.query_entities(where={"evil; DROP TABLE entity": "x"})

    def test_reject_unknown_order_by_column(self, populated: CarbonStore) -> None:
        with pytest.raises(ValueError, match="unknown order_by column"):
            populated.query_entities(order_by="password ASC")

    def test_reject_bad_order_by_direction(self, populated: CarbonStore) -> None:
        with pytest.raises(ValueError, match="invalid direction"):
            populated.query_entities(order_by="id SIDEWAYS")

    def test_reject_bad_operator(self, populated: CarbonStore) -> None:
        with pytest.raises(ValueError, match="unsupported operator"):
            populated.query_entities(where={"kind": ("DROP", "document")})

    def test_in_requires_list(self, populated: CarbonStore) -> None:
        with pytest.raises(ValueError, match="IN operator"):
            populated.query_entities(where={"kind": ("IN", "document")})


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

class TestTransactions:
    def test_commit_on_success(self, store: CarbonStore) -> None:
        with store.transaction():
            store.put_entity(id="a", kind="doc")
            store.put_entity(id="b", kind="doc")
        assert store.get_entity("a") is not None
        assert store.get_entity("b") is not None

    def test_rollback_on_exception(self, store: CarbonStore) -> None:
        with pytest.raises(RuntimeError):
            with store.transaction():
                store.put_entity(id="a", kind="doc")
                store.put_entity(id="b", kind="doc")
                raise RuntimeError("boom")
        assert store.get_entity("a") is None
        assert store.get_entity("b") is None

    def test_nested_transactions_commit_once(self, store: CarbonStore) -> None:
        with store.transaction():
            store.put_entity(id="a", kind="doc")
            with store.transaction():
                store.put_entity(id="b", kind="doc")
            store.put_entity(id="c", kind="doc")
        assert store.get_entity("a") is not None
        assert store.get_entity("b") is not None
        assert store.get_entity("c") is not None

    def test_nested_inner_exception_rolls_back_all(
        self, store: CarbonStore
    ) -> None:
        with pytest.raises(RuntimeError):
            with store.transaction():
                store.put_entity(id="a", kind="doc")
                with store.transaction():
                    store.put_entity(id="b", kind="doc")
                    raise RuntimeError("boom")
        assert store.get_entity("a") is None
        assert store.get_entity("b") is None

    def test_chunks_added_in_transaction_all_or_nothing(
        self, store: CarbonStore
    ) -> None:
        store.put_entity(id="x", kind="doc")
        with pytest.raises(RuntimeError):
            with store.transaction():
                store.add_chunks("x", ["one"])
                store.add_chunks("x", ["two"])
                raise RuntimeError("boom")
        assert store.list_chunks("x") == []


# ---------------------------------------------------------------------------
# Read-only mode
# ---------------------------------------------------------------------------

class TestReadOnly:
    def test_readonly_rejects_put(self, tmp_path: Path) -> None:
        p = tmp_path / "kb.carbondata"
        create(p).close()
        ro = cd_open(p, mode="r")
        try:
            with pytest.raises(PermissionError):
                ro.put_entity(id="x", kind="doc")
        finally:
            ro.close()

    def test_readonly_rejects_delete(self, tmp_path: Path) -> None:
        p = tmp_path / "kb.carbondata"
        s = create(p)
        s.put_entity(id="x", kind="doc")
        s.close()
        ro = cd_open(p, mode="r")
        try:
            with pytest.raises(PermissionError):
                ro.delete_entity("x")
        finally:
            ro.close()

    def test_readonly_get_and_query_work(self, tmp_path: Path) -> None:
        p = tmp_path / "kb.carbondata"
        with create(p) as s:
            s.put_entity(id="x", kind="doc", content="hi")
        with cd_open(p, mode="r") as ro:
            got = ro.get_entity("x")
            assert got is not None
            assert got.content == "hi"
            assert len(ro.query_entities(kind="doc")) == 1
