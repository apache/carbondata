"""
SQLite schema for .carbondata files.

See docs/DESIGN.md §5.
"""
from __future__ import annotations

import sqlite3
import time

SCHEMA_VERSION = 1

# "CBND" = 0x43 42 4E 44. Stored via PRAGMA application_id so `file` recognizes
# the carbondata signature and other carbondata libraries can distinguish it
# from arbitrary SQLite files.
APPLICATION_ID = 0x4342_4E44


_DDL = [
    """CREATE TABLE IF NOT EXISTS _meta (
        key TEXT PRIMARY KEY,
        value TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS entity (
        id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        namespace TEXT NOT NULL DEFAULT 'default',
        content TEXT,
        metadata TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        version INTEGER NOT NULL DEFAULT 1
    )""",
    "CREATE INDEX IF NOT EXISTS idx_entity_kind_ns ON entity(kind, namespace)",
    "CREATE INDEX IF NOT EXISTS idx_entity_updated ON entity(updated_at)",
    """CREATE TABLE IF NOT EXISTS chunk (
        id TEXT PRIMARY KEY,
        entity_id TEXT NOT NULL REFERENCES entity(id) ON DELETE CASCADE,
        seq INTEGER NOT NULL,
        content TEXT NOT NULL,
        kind TEXT NOT NULL DEFAULT 'text',
        metadata TEXT,
        token_count INTEGER
    )""",
    "CREATE INDEX IF NOT EXISTS idx_chunk_entity ON chunk(entity_id, seq)",
    """CREATE VIRTUAL TABLE IF NOT EXISTS chunk_fts USING fts5(
        content,
        content='chunk',
        content_rowid='rowid'
    )""",
    """CREATE TABLE IF NOT EXISTS embedding (
        chunk_id TEXT NOT NULL REFERENCES chunk(id) ON DELETE CASCADE,
        model TEXT NOT NULL,
        dim INTEGER NOT NULL,
        vector BLOB NOT NULL,
        PRIMARY KEY (chunk_id, model)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_embedding_model ON embedding(model)",
    """CREATE TABLE IF NOT EXISTS relation (
        src TEXT NOT NULL REFERENCES entity(id) ON DELETE CASCADE,
        dst TEXT NOT NULL REFERENCES entity(id) ON DELETE CASCADE,
        kind TEXT NOT NULL,
        weight REAL NOT NULL DEFAULT 1.0,
        metadata TEXT,
        PRIMARY KEY (src, dst, kind)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_relation_dst ON relation(dst, kind)",
    "CREATE INDEX IF NOT EXISTS idx_relation_kind ON relation(kind)",
    """CREATE TABLE IF NOT EXISTS memory_ext (
        entity_id TEXT PRIMARY KEY REFERENCES entity(id) ON DELETE CASCADE,
        session_id TEXT,
        actor TEXT,
        salience REAL NOT NULL DEFAULT 0.5,
        expires_at REAL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_memory_session ON memory_ext(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_memory_expires ON memory_ext(expires_at)",
    """CREATE TABLE IF NOT EXISTS vector_index (
        model TEXT NOT NULL,
        namespace TEXT NOT NULL DEFAULT 'default',
        index_blob BLOB NOT NULL,
        index_meta TEXT,
        updated_at REAL NOT NULL,
        PRIMARY KEY (model, namespace)
    )""",
]


# FTS5 external-content tables don't auto-sync from their source table; we
# install row triggers so chunk writes propagate into chunk_fts. Defined
# separately so `open()` can re-apply them idempotently on legacy files.
_TRIGGERS = [
    """CREATE TRIGGER IF NOT EXISTS chunk_ai AFTER INSERT ON chunk BEGIN
        INSERT INTO chunk_fts(rowid, content) VALUES (new.rowid, new.content);
    END""",
    """CREATE TRIGGER IF NOT EXISTS chunk_ad AFTER DELETE ON chunk BEGIN
        INSERT INTO chunk_fts(chunk_fts, rowid, content)
        VALUES ('delete', old.rowid, old.content);
    END""",
    """CREATE TRIGGER IF NOT EXISTS chunk_au AFTER UPDATE ON chunk BEGIN
        INSERT INTO chunk_fts(chunk_fts, rowid, content)
        VALUES ('delete', old.rowid, old.content);
        INSERT INTO chunk_fts(rowid, content) VALUES (new.rowid, new.content);
    END""",
]


def init_schema(conn: sqlite3.Connection) -> None:
    """Apply carbondata schema to a fresh SQLite connection."""
    conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    with conn:
        for stmt in _DDL:
            conn.execute(stmt)
        for stmt in _TRIGGERS:
            conn.execute(stmt)
        conn.execute(
            "INSERT OR IGNORE INTO _meta (key, value) VALUES (?, ?)",
            ("schema_version", str(SCHEMA_VERSION)),
        )
        conn.execute(
            "INSERT OR IGNORE INTO _meta (key, value) VALUES (?, ?)",
            ("created_at", str(time.time())),
        )


def ensure_triggers(conn: sqlite3.Connection) -> None:
    """Idempotently install FTS sync triggers. Safe to call on every open."""
    with conn:
        for stmt in _TRIGGERS:
            conn.execute(stmt)


class NotACarbondataFile(ValueError):
    pass


class UnsupportedSchemaVersion(ValueError):
    pass


def verify_schema(conn: sqlite3.Connection) -> int:
    """
    Validate that `conn` points to a carbondata file.

    Returns the on-disk schema version. Raises if the file is not carbondata
    or is written by a future version this library cannot read.
    """
    (app_id,) = conn.execute("PRAGMA application_id").fetchone()
    if app_id != APPLICATION_ID:
        raise NotACarbondataFile(
            f"Not a carbondata file: application_id={app_id:#010x} "
            f"(expected {APPLICATION_ID:#010x})"
        )
    (ver,) = conn.execute("PRAGMA user_version").fetchone()
    if ver > SCHEMA_VERSION:
        raise UnsupportedSchemaVersion(
            f"carbondata file schema version {ver} is newer than "
            f"library version {SCHEMA_VERSION}"
        )
    return ver


__all__ = [
    "SCHEMA_VERSION",
    "APPLICATION_ID",
    "init_schema",
    "ensure_triggers",
    "verify_schema",
    "NotACarbondataFile",
    "UnsupportedSchemaVersion",
]
