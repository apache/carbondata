"""M1: schema + open/create/close tests."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from Agent_module.carbon_data import (
    APPLICATION_ID,
    SCHEMA_VERSION,
    CarbonStore,
    NotACarbondataFile,
    UnsupportedSchemaVersion,
    create,
)
from Agent_module.carbon_data import open as cd_open


EXPECTED_TABLES = {
    "_meta",
    "entity",
    "chunk",
    "embedding",
    "relation",
    "memory_ext",
    "vector_index",
    "chunk_fts",
}


def _tables(path: Path) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','virtual')"
        ).fetchall()
    finally:
        conn.close()
    return {r[0] for r in rows}


def test_create_and_reopen_empty(tmp_path: Path) -> None:
    p = tmp_path / "kb.carbondata"
    store = create(p)
    assert p.exists()
    stats = store.stats()
    assert stats["entities"] == 0
    assert stats["chunks"] == 0
    assert stats["embeddings"] == 0
    assert stats["relations"] == 0
    assert stats["memories"] == 0
    assert stats["schema_version"] == SCHEMA_VERSION
    assert stats["application_id"] == APPLICATION_ID
    store.close()

    reopened = cd_open(p)
    assert reopened.stats()["entities"] == 0
    reopened.close()


def test_application_id_and_user_version_on_disk(tmp_path: Path) -> None:
    p = tmp_path / "kb.carbondata"
    create(p).close()
    conn = sqlite3.connect(str(p))
    try:
        (app_id,) = conn.execute("PRAGMA application_id").fetchone()
        (ver,) = conn.execute("PRAGMA user_version").fetchone()
    finally:
        conn.close()
    assert app_id == APPLICATION_ID
    assert ver == SCHEMA_VERSION


def test_schema_has_all_expected_tables(tmp_path: Path) -> None:
    p = tmp_path / "kb.carbondata"
    create(p).close()
    tables = _tables(p)
    missing = EXPECTED_TABLES - tables
    assert not missing, f"missing tables: {missing}"


def test_reject_non_carbondata_sqlite(tmp_path: Path) -> None:
    p = tmp_path / "random.db"
    conn = sqlite3.connect(str(p))
    conn.execute("CREATE TABLE foo (x INTEGER)")
    conn.commit()
    conn.close()
    with pytest.raises(NotACarbondataFile):
        cd_open(p)


def test_create_refuses_existing_by_default(tmp_path: Path) -> None:
    p = tmp_path / "kb.carbondata"
    create(p).close()
    with pytest.raises(FileExistsError):
        create(p)


def test_create_exist_ok_reopens(tmp_path: Path) -> None:
    p = tmp_path / "kb.carbondata"
    create(p).close()
    store = create(p, exist_ok=True)
    assert store.stats()["schema_version"] == SCHEMA_VERSION
    store.close()


def test_open_missing_readonly_fails(tmp_path: Path) -> None:
    p = tmp_path / "nope.carbondata"
    with pytest.raises(FileNotFoundError):
        cd_open(p, mode="r")


def test_open_missing_rw_creates(tmp_path: Path) -> None:
    p = tmp_path / "new.carbondata"
    store = cd_open(p)
    assert p.exists()
    assert store.stats()["entities"] == 0
    store.close()


def test_context_manager_closes(tmp_path: Path) -> None:
    p = tmp_path / "kb.carbondata"
    with create(p) as store:
        assert not store.closed
        assert store.stats()["entities"] == 0
    assert store.closed


def test_stats_after_close_errors(tmp_path: Path) -> None:
    p = tmp_path / "kb.carbondata"
    store = create(p)
    store.close()
    with pytest.raises(RuntimeError):
        store.stats()


def test_future_schema_version_rejected(tmp_path: Path) -> None:
    """Simulate a file written by a newer library version."""
    p = tmp_path / "future.carbondata"
    create(p).close()
    # Bump user_version past SCHEMA_VERSION
    conn = sqlite3.connect(str(p))
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    conn.close()
    with pytest.raises(UnsupportedSchemaVersion):
        cd_open(p)


def test_readonly_open_rejects_writes(tmp_path: Path) -> None:
    p = tmp_path / "kb.carbondata"
    create(p).close()
    ro = cd_open(p, mode="r")
    assert ro.mode == "r"
    # Attempting a write through the underlying connection must fail.
    with pytest.raises(sqlite3.OperationalError):
        ro._require_open().execute(
            "INSERT INTO _meta (key, value) VALUES ('x', 'y')"
        )
    ro.close()


def test_dataclasses_constructable() -> None:
    """Sanity: data model dataclasses can be instantiated without numpy."""
    from Agent_module.carbon_data.models import (
        Chunk,
        Entity,
        MemoryItem,
        Relation,
    )

    e = Entity(id="e1", kind="document", content="hi")
    assert e.namespace == "default"
    assert e.version == 1

    c = Chunk(id="c1", entity_id="e1", seq=0, content="hello")
    assert c.kind == "text"

    r = Relation(src="e1", dst="e2", kind="references")
    assert r.weight == 1.0

    m = MemoryItem(id="m1", session_id="s1", actor="A", salience=0.9)
    assert m.kind == "memory"
    assert m.session_id == "s1"
