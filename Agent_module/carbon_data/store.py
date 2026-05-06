"""
CarbonStore: primary handle to a .carbondata file.

Milestones implemented here:
- M1: open/create/close + schema verification + stats
- M2: Entity/Chunk CRUD + structured query + transactions
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Literal, Optional, Union

from . import schema as _schema
from .chunkers import Chunker, by_paragraph
from .embedders import Embedder
from .index import (
    HnswIndex,
    bytes_to_vector,
    hnsw_available,
    search_brute,
    vector_to_bytes,
)
from .models import (
    Chunk,
    Entity,
    MemoryHit,
    MemoryItem,
    Neighbor,
    Relation,
    SearchHit,
    Subgraph,
    TraversalHit,
    ValidationReport,
)

PathLike = Union[str, Path]
Mode = Literal["r", "rw"]

# Whitelist of entity columns users may reference directly in `where`
# and `order_by`. Guards against SQL injection: anything outside this
# set gets rejected before it hits the query string.
_ENTITY_COLUMNS = frozenset(
    {"id", "kind", "namespace", "content", "created_at", "updated_at", "version"}
)

_OPERATORS = frozenset({"=", "!=", "<", "<=", ">", ">=", "LIKE", "IN"})


def _uuid() -> str:
    return str(uuid.uuid4())


def _dumps(obj: Any) -> str:
    return json.dumps(obj or {}, sort_keys=True, ensure_ascii=False)


def _loads(s: Optional[str]) -> dict[str, Any]:
    if s is None or s == "":
        return {}
    return json.loads(s)


def _render_row(row: dict[str, Any]) -> str:
    """Render a table row as ``key: value`` lines for indexing."""
    return "\n".join(f"{k}: {v}" for k, v in row.items())


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    """Read a CSV via DictReader; non-string values stay as strings here."""
    import csv

    with path.open(newline="", encoding="utf-8") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _fts_escape_query(q: str) -> str:
    """
    Convert an arbitrary user string into a safe FTS5 query.

    Strips embedded double-quotes, splits on whitespace, wraps each token
    as an FTS5 phrase. Tokens are AND-combined (FTS5 default). Returns
    "" (caller must short-circuit) when the input has no usable tokens.
    """
    tokens = q.replace('"', " ").split()
    if not tokens:
        return ""
    return " ".join(f'"{t}"' for t in tokens)


class CarbonStore:
    """
    A single .carbondata file.

    Use `CarbonStore.open(path)` or the package-level `carbon_data.open(path)`.
    """

    def __init__(
        self,
        path: Path,
        conn: sqlite3.Connection,
        mode: Mode,
    ) -> None:
        self.path = path
        self._conn = conn
        self._mode: Mode = mode
        self._tx_depth = 0  # 0 = autocommit-per-write; >0 = user transaction
        # Per-model HNSW index cache. Populated lazily on first vector
        # search; invalidated whenever embedding count diverges.
        self._hnsw_cache: dict[str, HnswIndex] = {}

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    @classmethod
    def create(cls, path: PathLike, *, exist_ok: bool = False) -> "CarbonStore":
        p = Path(path)
        if p.exists():
            if not exist_ok:
                raise FileExistsError(str(p))
            return cls.open(p, mode="rw")
        conn = sqlite3.connect(str(p))
        _schema.init_schema(conn)
        return cls(p, conn, mode="rw")

    @classmethod
    def open(cls, path: PathLike, *, mode: Mode = "rw") -> "CarbonStore":
        p = Path(path)
        if not p.exists():
            if mode == "r":
                raise FileNotFoundError(str(p))
            return cls.create(p)

        if mode == "r":
            conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        else:
            conn = sqlite3.connect(str(p))
            conn.execute("PRAGMA foreign_keys = ON")

        try:
            _schema.verify_schema(conn)
            if mode != "r":
                _schema.ensure_triggers(conn)
        except Exception:
            conn.close()
            raise
        return cls(p, conn, mode)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None  # type: ignore[assignment]

    def __enter__(self) -> "CarbonStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------

    @property
    def mode(self) -> Mode:
        return self._mode

    @property
    def closed(self) -> bool:
        return self._conn is None

    def _require_open(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("CarbonStore is closed")
        return self._conn

    def _require_rw(self) -> sqlite3.Connection:
        conn = self._require_open()
        if self._mode == "r":
            raise PermissionError("CarbonStore opened read-only")
        return conn

    def stats(self) -> dict[str, object]:
        c = self._require_open()

        def count(table: str) -> int:
            return c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        (ver,) = c.execute("PRAGMA user_version").fetchone()
        (app_id,) = c.execute("PRAGMA application_id").fetchone()
        return {
            "path": str(self.path),
            "mode": self._mode,
            "schema_version": ver,
            "application_id": app_id,
            "entities": count("entity"),
            "chunks": count("chunk"),
            "embeddings": count("embedding"),
            "relations": count("relation"),
            "memories": count("memory_ext"),
        }

    # ------------------------------------------------------------------
    # transactions
    # ------------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """
        Group writes atomically.

        Nested calls are a no-op at the inner level: only the outermost
        context commits/rolls back. On any exception, the outer frame
        rolls back and re-raises.
        """
        conn = self._require_rw()
        outer = self._tx_depth == 0
        self._tx_depth += 1
        try:
            yield
            if outer:
                conn.commit()
        except Exception:
            if outer:
                conn.rollback()
            raise
        finally:
            self._tx_depth -= 1

    @contextmanager
    def _write_ctx(self) -> Iterator[sqlite3.Connection]:
        """Internal per-write wrapper: commits iff no user transaction is active."""
        conn = self._require_rw()
        if self._tx_depth > 0:
            yield conn
            return
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # ------------------------------------------------------------------
    # Entity CRUD
    # ------------------------------------------------------------------

    def put_entity(
        self,
        entity: Optional[Entity] = None,
        *,
        id: Optional[str] = None,  # noqa: A002
        kind: Optional[str] = None,
        namespace: str = "default",
        content: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Insert or update an entity. On upsert, `created_at` is preserved and
        `version` is bumped by one. Returns the entity id.

        Call either with an Entity:
            store.put_entity(Entity(id="x", kind="doc"))
        or with kwargs:
            store.put_entity(id="x", kind="doc", content="...")
        """
        if entity is not None:
            mixed = any(
                v is not None for v in (id, kind, content, metadata)
            ) or namespace != "default"
            if mixed:
                raise TypeError("pass either an Entity or kwargs, not both")
            e = entity
        else:
            if id is None or kind is None:
                raise TypeError(
                    "put_entity requires either an Entity positional "
                    "or (id=, kind=, ...) kwargs"
                )
            e = Entity(
                id=id,
                kind=kind,
                namespace=namespace,
                content=content,
                metadata=metadata or {},
            )

        now = time.time()
        meta_json = _dumps(e.metadata)

        with self._write_ctx() as conn:
            existing = conn.execute(
                "SELECT version FROM entity WHERE id=?", (e.id,)
            ).fetchone()
            if existing is None:
                conn.execute(
                    """INSERT INTO entity
                       (id, kind, namespace, content, metadata,
                        created_at, updated_at, version)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        e.id, e.kind, e.namespace, e.content, meta_json,
                        e.created_at, e.updated_at, e.version,
                    ),
                )
            else:
                (old_version,) = existing
                conn.execute(
                    """UPDATE entity
                       SET kind=?, namespace=?, content=?, metadata=?,
                           updated_at=?, version=?
                       WHERE id=?""",
                    (
                        e.kind, e.namespace, e.content, meta_json,
                        now, old_version + 1, e.id,
                    ),
                )
        return e.id

    def get_entity(self, entity_id: str) -> Optional[Entity]:
        conn = self._require_open()
        row = conn.execute(
            """SELECT id, kind, namespace, content, metadata,
                      created_at, updated_at, version
               FROM entity WHERE id=?""",
            (entity_id,),
        ).fetchone()
        return _row_to_entity(row) if row else None

    def delete_entity(self, entity_id: str) -> bool:
        """Remove an entity and (via ON DELETE CASCADE) its chunks,
        embeddings, relations, and memory_ext row. Returns True iff
        something was deleted."""
        with self._write_ctx() as conn:
            cur = conn.execute("DELETE FROM entity WHERE id=?", (entity_id,))
            return cur.rowcount > 0

    # ------------------------------------------------------------------
    # Chunk CRUD
    # ------------------------------------------------------------------

    def add_chunks(
        self,
        entity_id: str,
        chunks: Iterable[Union[Chunk, str, dict[str, Any]]],
        *,
        base_seq: Optional[int] = None,
    ) -> list[str]:
        """
        Append chunks to an entity.

        Each item may be:
          - a ``Chunk`` dataclass
          - a ``str`` (treated as content; id/seq auto-assigned)
          - a ``dict`` with keys matching Chunk fields

        ``seq`` auto-assigns continuing from the entity's current max seq
        unless ``base_seq`` is provided explicitly.

        Returns the list of chunk ids.
        """
        conn = self._require_rw()
        if not conn.execute(
            "SELECT 1 FROM entity WHERE id=?", (entity_id,)
        ).fetchone():
            raise KeyError(f"entity not found: {entity_id!r}")

        if base_seq is None:
            row = conn.execute(
                "SELECT COALESCE(MAX(seq), -1) FROM chunk WHERE entity_id=?",
                (entity_id,),
            ).fetchone()
            base_seq = (row[0] if row else -1) + 1

        materialized: list[Chunk] = []
        for offset, raw in enumerate(chunks):
            seq = base_seq + offset
            materialized.append(_coerce_chunk(raw, entity_id=entity_id, default_seq=seq))

        rows = [
            (
                c.id, c.entity_id, c.seq, c.content, c.kind,
                _dumps(c.metadata), c.token_count,
            )
            for c in materialized
        ]
        with self._write_ctx() as conn:
            conn.executemany(
                """INSERT INTO chunk
                   (id, entity_id, seq, content, kind, metadata, token_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
        return [c.id for c in materialized]

    def get_chunk(self, chunk_id: str) -> Optional[Chunk]:
        conn = self._require_open()
        row = conn.execute(
            """SELECT id, entity_id, seq, content, kind, metadata, token_count
               FROM chunk WHERE id=?""",
            (chunk_id,),
        ).fetchone()
        return _row_to_chunk(row) if row else None

    def list_chunks(self, entity_id: str) -> list[Chunk]:
        conn = self._require_open()
        rows = conn.execute(
            """SELECT id, entity_id, seq, content, kind, metadata, token_count
               FROM chunk WHERE entity_id=? ORDER BY seq""",
            (entity_id,),
        ).fetchall()
        return [_row_to_chunk(r) for r in rows]

    def delete_chunks(self, entity_id: str) -> int:
        """Delete all chunks for an entity. Returns the count removed."""
        with self._write_ctx() as conn:
            cur = conn.execute("DELETE FROM chunk WHERE entity_id=?", (entity_id,))
            return cur.rowcount

    # ------------------------------------------------------------------
    # Structured query
    # ------------------------------------------------------------------

    def query_entities(
        self,
        *,
        kind: Optional[str] = None,
        namespace: Optional[str] = None,
        where: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        order_by: str = "updated_at DESC",
    ) -> list[Entity]:
        """
        Structured query over the entity table.

        `where` keys:
          - direct column names from _ENTITY_COLUMNS (exact match or tuple (op, val))
          - ``metadata.<path>`` → json_extract(metadata, '$.<path>')

        Operator tuples: ``{"created_at": (">=", 1700000000)}``,
                         ``{"kind": ("IN", ["document", "code"])}``.
        """
        conn = self._require_open()
        clauses: list[str] = []
        params: list[Any] = []

        if kind is not None:
            clauses.append("kind = ?")
            params.append(kind)
        if namespace is not None:
            clauses.append("namespace = ?")
            params.append(namespace)

        if where:
            extra_c, extra_p = _build_entity_where(where, table_prefix="")
            clauses.extend(extra_c)
            params.extend(extra_p)

        sql = (
            "SELECT id, kind, namespace, content, metadata, "
            "created_at, updated_at, version FROM entity"
        )
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += f" ORDER BY {_sanitize_order_by(order_by)}"
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.append(int(limit))
            params.append(int(offset))

        rows = conn.execute(sql, params).fetchall()
        return [_row_to_entity(r) for r in rows]

    # ------------------------------------------------------------------
    # Embeddings (M3)
    # ------------------------------------------------------------------

    def set_embedding(
        self,
        chunk_id: str,
        vector: Any,
        *,
        model: str,
    ) -> None:
        """Store (or replace) the embedding of a chunk under ``model``."""
        import numpy as np

        arr = np.ascontiguousarray(vector, dtype=np.float32).reshape(-1)
        dim = int(arr.shape[0])
        blob = vector_to_bytes(arr)
        with self._write_ctx() as conn:
            exists = conn.execute(
                "SELECT 1 FROM chunk WHERE id=?", (chunk_id,)
            ).fetchone()
            if not exists:
                raise KeyError(f"chunk not found: {chunk_id!r}")
            conn.execute(
                """INSERT OR REPLACE INTO embedding
                   (chunk_id, model, dim, vector) VALUES (?, ?, ?, ?)""",
                (chunk_id, model, dim, blob),
            )

    def get_embedding(self, chunk_id: str, *, model: str) -> Optional[Any]:
        """Return the stored vector (numpy array) or None."""
        conn = self._require_open()
        row = conn.execute(
            "SELECT vector, dim FROM embedding WHERE chunk_id=? AND model=?",
            (chunk_id, model),
        ).fetchone()
        if row is None:
            return None
        blob, dim = row
        return bytes_to_vector(blob, dim)

    def embed_chunks(
        self,
        embedder: Embedder,
        *,
        entity_id: Optional[str] = None,
        missing_only: bool = True,
        batch_size: int = 64,
    ) -> int:
        """
        Encode chunks with ``embedder`` and store the resulting vectors.

        Args:
            embedder:     anything with ``.model``, ``.dim``, ``.encode``.
            entity_id:    restrict to chunks of this entity. None = all.
            missing_only: skip chunks that already have an embedding under
                          ``embedder.model``. Set False to re-encode.
            batch_size:   chunks per ``encode`` call.

        Returns:
            number of chunks embedded.
        """
        import numpy as np

        conn = self._require_rw()

        clauses = []
        params: list[Any] = []
        if entity_id is not None:
            clauses.append("chunk.entity_id = ?")
            params.append(entity_id)
        if missing_only:
            clauses.append(
                "NOT EXISTS (SELECT 1 FROM embedding e "
                "WHERE e.chunk_id = chunk.id AND e.model = ?)"
            )
            params.append(embedder.model)

        sql = "SELECT chunk.id, chunk.content FROM chunk"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY chunk.rowid"
        rows = conn.execute(sql, params).fetchall()
        if not rows:
            return 0

        total = 0
        with self._write_ctx() as write_conn:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                texts = [r[1] for r in batch]
                vecs = embedder.encode(texts)
                arr = np.ascontiguousarray(vecs, dtype=np.float32)
                if arr.shape != (len(batch), embedder.dim):
                    raise ValueError(
                        f"embedder {embedder.model!r} returned shape "
                        f"{arr.shape}, expected ({len(batch)}, {embedder.dim})"
                    )
                payload = [
                    (batch[j][0], embedder.model, embedder.dim, arr[j].tobytes())
                    for j in range(len(batch))
                ]
                write_conn.executemany(
                    """INSERT OR REPLACE INTO embedding
                       (chunk_id, model, dim, vector) VALUES (?, ?, ?, ?)""",
                    payload,
                )
                total += len(batch)
        return total

    # ------------------------------------------------------------------
    # Semantic / keyword / hybrid search
    # ------------------------------------------------------------------

    def search(
        self,
        query: Union[str, Any],
        *,
        mode: Literal["vector", "keyword", "hybrid"] = "vector",
        top_k: int = 5,
        model: Optional[str] = None,
        embedder: Optional[Embedder] = None,
        namespace: Optional[str] = None,
        filters: Optional[dict[str, Any]] = None,
        metric: Literal["cosine", "dot", "l2"] = "cosine",
        rrf_k: int = 60,
        fusion_oversample: int = 2,
        raw_fts: bool = False,
        use_hnsw: Literal["auto", "on", "off"] = "auto",
    ) -> list[SearchHit]:
        """
        Search chunks. Three modes:

        - ``mode="vector"`` (default): embedding cosine/dot/l2 ranking.
        - ``mode="keyword"``: SQLite FTS5 BM25.
        - ``mode="hybrid"``: vector + keyword fused by Reciprocal Rank Fusion.

        Args:
            query:     str (any mode) OR 1-D vector (vector mode only).
            mode:      'vector' | 'keyword' | 'hybrid'.
            top_k:     max hits.
            model:     embedding model name (vector / hybrid).
            embedder:  encodes string queries; provides ``model`` if missing.
            namespace: restrict to entities in this namespace.
            filters:   same shape as ``query_entities`` where=, applied
                       before ranking.
            metric:    vector metric ('cosine' | 'dot' | 'l2').
            rrf_k:     Reciprocal Rank Fusion constant (hybrid only).
            fusion_oversample: each modality fetches top_k*N before fusion.
            raw_fts:   pass keyword query through to FTS5 unescaped (advanced).
            use_hnsw:  'auto' (default) picks HNSW when applicable;
                       'on' forces HNSW (errors if filters/non-cosine/no
                       hnswlib); 'off' always uses brute force.
        """
        if mode == "vector":
            return self._search_vector(
                query, top_k=top_k, model=model, embedder=embedder,
                namespace=namespace, filters=filters, metric=metric,
                use_hnsw=use_hnsw,
            )
        if mode == "keyword":
            if not isinstance(query, str):
                raise TypeError("keyword mode requires a string query")
            return self._search_keyword(
                query, top_k=top_k, namespace=namespace,
                filters=filters, raw_fts=raw_fts,
            )
        if mode == "hybrid":
            if not isinstance(query, str):
                raise TypeError("hybrid mode requires a string query")
            return self._search_hybrid(
                query, top_k=top_k, model=model, embedder=embedder,
                namespace=namespace, filters=filters, metric=metric,
                rrf_k=rrf_k, fusion_oversample=fusion_oversample,
                raw_fts=raw_fts, use_hnsw=use_hnsw,
            )
        raise ValueError(
            f"unknown search mode {mode!r}: expected 'vector', "
            f"'keyword', or 'hybrid'"
        )

    # ---- vector ------------------------------------------------------

    def _search_vector(
        self,
        query: Union[str, Any],
        *,
        top_k: int,
        model: Optional[str],
        embedder: Optional[Embedder],
        namespace: Optional[str],
        filters: Optional[dict[str, Any]],
        metric: Literal["cosine", "dot", "l2"],
        use_hnsw: Literal["auto", "on", "off"] = "auto",
    ) -> list[SearchHit]:
        ranked = self._run_vector(
            query, top_k=top_k, model=model, embedder=embedder,
            namespace=namespace, filters=filters, metric=metric,
            use_hnsw=use_hnsw,
        )
        return self._materialize_hits(ranked)

    def _run_vector(
        self,
        query: Union[str, Any],
        *,
        top_k: int,
        model: Optional[str],
        embedder: Optional[Embedder],
        namespace: Optional[str],
        filters: Optional[dict[str, Any]],
        metric: Literal["cosine", "dot", "l2"],
        use_hnsw: Literal["auto", "on", "off"] = "auto",
    ) -> list[tuple[str, float]]:
        import numpy as np

        if embedder is not None and model is None:
            model = embedder.model
        if model is None:
            raise TypeError(
                "vector search requires either `model=` or an `embedder=` "
                "(to infer the model)"
            )

        if isinstance(query, str):
            if embedder is None:
                raise TypeError("string query requires an `embedder=`")
            q_vec = embedder.encode([query])[0]
        else:
            q_vec = np.ascontiguousarray(query, dtype=np.float32).reshape(-1)

        # Pick HNSW vs brute. HNSW only handles cosine + no JSON filters;
        # namespace is fine because we post-filter on the result set.
        hnsw_eligible = (
            metric == "cosine"
            and not filters
            and hnsw_available()
        )
        if use_hnsw == "on":
            if metric != "cosine":
                raise ValueError(
                    f"use_hnsw='on' requires metric='cosine', got {metric!r}"
                )
            if filters:
                raise ValueError(
                    "use_hnsw='on' is incompatible with filters= "
                    "(HNSW cannot pre-filter; use 'auto' or 'off')"
                )
            if not hnsw_available():
                raise RuntimeError(
                    "use_hnsw='on' but hnswlib is not installed"
                )
            return self._run_vector_hnsw(
                q_vec, model=model, namespace=namespace, top_k=top_k
            )
        if use_hnsw == "auto" and hnsw_eligible:
            return self._run_vector_hnsw(
                q_vec, model=model, namespace=namespace, top_k=top_k
            )
        return self._run_vector_brute(
            q_vec, model=model, namespace=namespace,
            filters=filters, top_k=top_k, metric=metric,
        )

    def _run_vector_brute(
        self,
        q_vec: "Any",
        *,
        model: str,
        namespace: Optional[str],
        filters: Optional[dict[str, Any]],
        top_k: int,
        metric: Literal["cosine", "dot", "l2"],
    ) -> list[tuple[str, float]]:
        import numpy as np

        conn = self._require_open()
        clauses = ["embedding.model = ?"]
        params: list[Any] = [model]
        if namespace is not None:
            clauses.append("entity.namespace = ?")
            params.append(namespace)
        if filters:
            fc, fp = _build_entity_where(filters, table_prefix="entity.")
            clauses.extend(fc)
            params.extend(fp)

        sql = (
            "SELECT chunk.id, embedding.vector, embedding.dim "
            "FROM embedding "
            "JOIN chunk ON chunk.id = embedding.chunk_id "
            "JOIN entity ON entity.id = chunk.entity_id "
            f"WHERE {' AND '.join(clauses)}"
        )
        rows = conn.execute(sql, params).fetchall()
        if not rows:
            return []

        ids = [r[0] for r in rows]
        dims = {r[2] for r in rows}
        if len(dims) != 1:
            raise RuntimeError(
                f"inconsistent dims for model {model!r}: {sorted(dims)}"
            )
        dim = dims.pop()
        if q_vec.shape[0] != dim:
            raise ValueError(
                f"query dim {q_vec.shape[0]} != embedding dim {dim} "
                f"for model {model!r}"
            )

        vectors = np.empty((len(rows), dim), dtype=np.float32)
        for i, r in enumerate(rows):
            vectors[i] = bytes_to_vector(r[1], dim)

        return search_brute(q_vec, vectors, ids, top_k=top_k, metric=metric)

    def _run_vector_hnsw(
        self,
        q_vec: "Any",
        *,
        model: str,
        namespace: Optional[str],
        top_k: int,
    ) -> list[tuple[str, float]]:
        idx = self._get_hnsw(model)
        if idx is None:
            return []
        if q_vec.shape[0] != idx.dim:
            raise ValueError(
                f"query dim {q_vec.shape[0]} != index dim {idx.dim} "
                f"for model {model!r}"
            )

        # Over-fetch when post-filtering by namespace so we can survive
        # candidates that get dropped. 3x is plenty for typical workloads.
        oversample = 3 if namespace is not None else 1
        raw = idx.search(q_vec, top_k=top_k * oversample)
        if not raw:
            return []

        rowids = [r[0] for r in raw]
        placeholders = ",".join("?" * len(rowids))
        params: list[Any] = list(rowids)
        sql = (
            f"SELECT chunk.rowid, chunk.id "
            f"FROM chunk JOIN entity ON entity.id = chunk.entity_id "
            f"WHERE chunk.rowid IN ({placeholders})"
        )
        if namespace is not None:
            sql += " AND entity.namespace = ?"
            params.append(namespace)

        conn = self._require_open()
        rid_to_cid = {r[0]: r[1] for r in conn.execute(sql, params).fetchall()}

        out: list[tuple[str, float]] = []
        for rid, score in raw:
            cid = rid_to_cid.get(rid)
            if cid is None:
                continue
            out.append((cid, score))
            if len(out) >= top_k:
                break
        return out

    # ---- HNSW cache + persistence ------------------------------------

    def _embedding_count(self, model: str) -> int:
        conn = self._require_open()
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM embedding WHERE model=?", (model,)
        ).fetchone()
        return int(n)

    def _get_hnsw(self, model: str) -> Optional[HnswIndex]:
        """
        Return a usable HNSW for ``model``, building or reloading as needed.

        Cache invariant: ``HnswIndex.count`` equals the live embedding
        count. Any divergence — adds, deletes, or first use after a
        reopen — triggers a rebuild from the embedding table.
        """
        cur_count = self._embedding_count(model)
        if cur_count == 0:
            self._hnsw_cache.pop(model, None)
            return None

        cached = self._hnsw_cache.get(model)
        if cached is not None and cached.count == cur_count:
            return cached

        # Try the persisted blob — only useful on first access after open.
        if cached is None:
            loaded = self._load_hnsw_blob(model, expected_count=cur_count)
            if loaded is not None:
                self._hnsw_cache[model] = loaded
                return loaded

        return self._rebuild_hnsw(model)

    def _load_hnsw_blob(
        self, model: str, *, expected_count: int
    ) -> Optional[HnswIndex]:
        conn = self._require_open()
        row = conn.execute(
            "SELECT index_blob, index_meta FROM vector_index "
            "WHERE model=? AND namespace='*'",
            (model,),
        ).fetchone()
        if row is None:
            return None
        blob, meta_json = row
        try:
            meta = json.loads(meta_json) if meta_json else {}
        except json.JSONDecodeError:
            return None
        if meta.get("count") != expected_count:
            return None
        dim = meta.get("dim")
        if not isinstance(dim, int):
            return None
        try:
            return HnswIndex.deserialize(blob, dim=dim)
        except Exception:
            # Corrupt blob shouldn't block search — fall through to rebuild.
            return None

    def _rebuild_hnsw(self, model: str) -> Optional[HnswIndex]:
        import numpy as np

        conn = self._require_open()
        rows = conn.execute(
            "SELECT chunk.rowid, embedding.vector, embedding.dim "
            "FROM embedding JOIN chunk ON chunk.id = embedding.chunk_id "
            "WHERE embedding.model = ?",
            (model,),
        ).fetchall()
        if not rows:
            return None

        dims = {r[2] for r in rows}
        if len(dims) != 1:
            raise RuntimeError(
                f"inconsistent dims for model {model!r}: {sorted(dims)}"
            )
        dim = dims.pop()

        vectors = np.empty((len(rows), dim), dtype=np.float32)
        labels = [int(r[0]) for r in rows]
        for i, r in enumerate(rows):
            vectors[i] = bytes_to_vector(r[1], dim)

        idx = HnswIndex.build(vectors, labels)
        self._hnsw_cache[model] = idx
        if self._mode != "r":
            self._persist_hnsw(model, idx, dim=dim)
        return idx

    def _persist_hnsw(self, model: str, idx: HnswIndex, *, dim: int) -> None:
        if self._mode == "r":
            return
        blob = idx.serialize()
        meta = json.dumps(
            {"count": idx.count, "dim": dim, "M": idx.M},
            sort_keys=True,
        )
        with self._write_ctx() as conn:
            conn.execute(
                """INSERT INTO vector_index
                   (model, namespace, index_blob, index_meta, updated_at)
                   VALUES (?, '*', ?, ?, ?)
                   ON CONFLICT(model, namespace) DO UPDATE SET
                     index_blob = excluded.index_blob,
                     index_meta = excluded.index_meta,
                     updated_at = excluded.updated_at""",
                (model, blob, meta, time.time()),
            )

    # ---- keyword -----------------------------------------------------

    def _search_keyword(
        self,
        query: str,
        *,
        top_k: int,
        namespace: Optional[str],
        filters: Optional[dict[str, Any]],
        raw_fts: bool,
    ) -> list[SearchHit]:
        ranked = self._run_keyword(
            query, top_k=top_k, namespace=namespace,
            filters=filters, raw_fts=raw_fts,
        )
        return self._materialize_hits(ranked)

    def _run_keyword(
        self,
        query: str,
        *,
        top_k: int,
        namespace: Optional[str],
        filters: Optional[dict[str, Any]],
        raw_fts: bool,
    ) -> list[tuple[str, float]]:
        fts_q = query if raw_fts else _fts_escape_query(query)
        if not fts_q:
            return []

        conn = self._require_open()
        clauses = ["chunk_fts MATCH ?"]
        params: list[Any] = [fts_q]
        if namespace is not None:
            clauses.append("entity.namespace = ?")
            params.append(namespace)
        if filters:
            fc, fp = _build_entity_where(filters, table_prefix="entity.")
            clauses.extend(fc)
            params.extend(fp)
        params.append(top_k)

        # bm25() returns negative; smaller (more negative) = better.
        # Negate to expose a "higher is better" score.
        sql = (
            "SELECT chunk.id, -bm25(chunk_fts) AS score "
            "FROM chunk_fts "
            "JOIN chunk ON chunk.rowid = chunk_fts.rowid "
            "JOIN entity ON entity.id = chunk.entity_id "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY bm25(chunk_fts) ASC LIMIT ?"
        )
        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError as exc:
            # FTS5 syntax errors when raw_fts=True or escape misses an edge case.
            raise ValueError(f"FTS5 query failed: {exc} (query={fts_q!r})") from exc
        return [(r[0], float(r[1])) for r in rows]

    # ---- hybrid (RRF) ------------------------------------------------

    def _search_hybrid(
        self,
        query: str,
        *,
        top_k: int,
        model: Optional[str],
        embedder: Optional[Embedder],
        namespace: Optional[str],
        filters: Optional[dict[str, Any]],
        metric: Literal["cosine", "dot", "l2"],
        rrf_k: int,
        fusion_oversample: int,
        raw_fts: bool,
        use_hnsw: Literal["auto", "on", "off"] = "auto",
    ) -> list[SearchHit]:
        if fusion_oversample < 1:
            raise ValueError("fusion_oversample must be >= 1")
        per_modality_k = max(top_k * fusion_oversample, top_k)

        vec_ranked = self._run_vector(
            query, top_k=per_modality_k, model=model, embedder=embedder,
            namespace=namespace, filters=filters, metric=metric,
            use_hnsw=use_hnsw,
        )
        kw_ranked = self._run_keyword(
            query, top_k=per_modality_k, namespace=namespace,
            filters=filters, raw_fts=raw_fts,
        )

        scores: dict[str, float] = {}
        for rank, (cid, _s) in enumerate(vec_ranked, start=1):
            scores[cid] = scores.get(cid, 0.0) + 1.0 / (rrf_k + rank)
        for rank, (cid, _s) in enumerate(kw_ranked, start=1):
            scores[cid] = scores.get(cid, 0.0) + 1.0 / (rrf_k + rank)

        fused = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:top_k]
        return self._materialize_hits(fused)

    # ---- shared materialization -------------------------------------

    def _materialize_hits(
        self, ranked: list[tuple[str, float]]
    ) -> list[SearchHit]:
        hits: list[SearchHit] = []
        for chunk_id, score in ranked:
            chunk = self.get_chunk(chunk_id)
            if chunk is None:
                continue  # raced against delete
            entity = self.get_entity(chunk.entity_id)
            if entity is None:
                continue
            hits.append(SearchHit(chunk=chunk, entity=entity, score=score))
        return hits

    # ------------------------------------------------------------------
    # Memory: remember / recall / forget
    # ------------------------------------------------------------------

    def remember(
        self,
        content: str,
        *,
        session_id: Optional[str] = None,
        actor: Optional[str] = None,
        salience: float = 0.5,
        ttl: Optional[float] = None,
        expires_at: Optional[float] = None,
        namespace: str = "default",
        metadata: Optional[dict[str, Any]] = None,
        id: Optional[str] = None,  # noqa: A002
        embedder: Optional[Embedder] = None,
    ) -> str:
        """
        Persist a memory item. Atomic across entity + chunk + memory_ext
        (and embedding when ``embedder`` is provided).

        Args:
            content:    the memory text.
            session_id: which conversation/task this belongs to.
            actor:      which agent recorded it.
            salience:   importance weight in [0, 1].
            ttl:        seconds-from-now until expiry. Mutually exclusive
                        with ``expires_at``.
            expires_at: absolute unix timestamp of expiry.
            namespace:  tenant/dataset bucket.
            metadata:   arbitrary JSON metadata on the underlying entity.
            id:         pre-assigned entity id; auto-uuid otherwise.
            embedder:   if given, embed the chunk eagerly so ``recall``
                        can do semantic retrieval immediately.

        Returns the memory's entity id.
        """
        if ttl is not None and expires_at is not None:
            raise TypeError("provide ttl OR expires_at, not both")
        if not 0.0 <= salience <= 1.0:
            raise ValueError(f"salience must be in [0, 1], got {salience}")

        eid = id if id is not None else _uuid()
        now = time.time()
        eat: Optional[float]
        if expires_at is not None:
            eat = expires_at
        elif ttl is not None:
            eat = now + ttl
        else:
            eat = None

        with self.transaction():
            self.put_entity(
                id=eid,
                kind="memory",
                namespace=namespace,
                content=content,
                metadata=metadata or {},
            )
            self.add_chunks(eid, [content])
            with self._write_ctx() as conn:
                conn.execute(
                    """INSERT INTO memory_ext
                       (entity_id, session_id, actor, salience, expires_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (eid, session_id, actor, salience, eat),
                )
            if embedder is not None:
                self.embed_chunks(embedder, entity_id=eid)
        return eid

    def recall(
        self,
        query: Union[str, Any],
        *,
        top_k: int = 5,
        session_id: Optional[str] = None,
        actor: Optional[str] = None,
        namespace: Optional[str] = None,
        min_salience: Optional[float] = None,
        include_expired: bool = False,
        model: Optional[str] = None,
        embedder: Optional[Embedder] = None,
        metric: Literal["cosine", "dot", "l2"] = "cosine",
    ) -> list[MemoryHit]:
        """
        Semantic recall over stored memories.

        Filters compose with AND. Expired memories are excluded by default.
        Returns ranked ``MemoryHit`` (highest score first).
        """
        import numpy as np

        if embedder is not None and model is None:
            model = embedder.model
        if model is None:
            raise TypeError(
                "recall requires either `model=` or an `embedder=` "
                "(to infer the model)"
            )

        if isinstance(query, str):
            if embedder is None:
                raise TypeError("string query requires an `embedder=`")
            q_vec = embedder.encode([query])[0]
        else:
            q_vec = np.ascontiguousarray(query, dtype=np.float32).reshape(-1)

        conn = self._require_open()
        clauses = ["embedding.model = ?", "entity.kind = 'memory'"]
        params: list[Any] = [model]
        if namespace is not None:
            clauses.append("entity.namespace = ?")
            params.append(namespace)
        if session_id is not None:
            clauses.append("memory_ext.session_id = ?")
            params.append(session_id)
        if actor is not None:
            clauses.append("memory_ext.actor = ?")
            params.append(actor)
        if min_salience is not None:
            clauses.append("memory_ext.salience >= ?")
            params.append(min_salience)
        if not include_expired:
            clauses.append(
                "(memory_ext.expires_at IS NULL OR memory_ext.expires_at > ?)"
            )
            params.append(time.time())

        sql = (
            "SELECT entity.id, embedding.vector, embedding.dim "
            "FROM embedding "
            "JOIN chunk ON chunk.id = embedding.chunk_id "
            "JOIN entity ON entity.id = chunk.entity_id "
            "JOIN memory_ext ON memory_ext.entity_id = entity.id "
            f"WHERE {' AND '.join(clauses)}"
        )
        rows = conn.execute(sql, params).fetchall()
        if not rows:
            return []

        ids = [r[0] for r in rows]
        dims = {r[2] for r in rows}
        if len(dims) != 1:
            raise RuntimeError(
                f"inconsistent dims for model {model!r}: {sorted(dims)}"
            )
        dim = dims.pop()
        if q_vec.shape[0] != dim:
            raise ValueError(
                f"query dim {q_vec.shape[0]} != embedding dim {dim} "
                f"for model {model!r}"
            )

        vectors = np.empty((len(rows), dim), dtype=np.float32)
        for i, r in enumerate(rows):
            vectors[i] = bytes_to_vector(r[1], dim)

        ranked = search_brute(q_vec, vectors, ids, top_k=top_k, metric=metric)
        return [
            MemoryHit(memory=m, score=score)
            for entity_id, score in ranked
            for m in [self._load_memory(entity_id)]
            if m is not None
        ]

    def forget(
        self,
        *,
        entity_id: Optional[str] = None,
        session_id: Optional[str] = None,
        actor: Optional[str] = None,
        namespace: Optional[str] = None,
        before: Optional[float] = None,
    ) -> int:
        """
        Delete memories matching ALL provided criteria. At least one
        criterion is required (no-arg ``forget()`` raises). Returns the
        number of memories removed.
        """
        if all(
            x is None
            for x in (entity_id, session_id, actor, namespace, before)
        ):
            raise TypeError("forget() requires at least one criterion")

        conn = self._require_rw()
        clauses = ["entity.kind = 'memory'"]
        params: list[Any] = []
        if entity_id is not None:
            clauses.append("entity.id = ?")
            params.append(entity_id)
        if namespace is not None:
            clauses.append("entity.namespace = ?")
            params.append(namespace)
        if before is not None:
            clauses.append("entity.created_at < ?")
            params.append(before)
        if session_id is not None:
            clauses.append("memory_ext.session_id = ?")
            params.append(session_id)
        if actor is not None:
            clauses.append("memory_ext.actor = ?")
            params.append(actor)

        sql = (
            "SELECT entity.id FROM entity "
            "JOIN memory_ext ON memory_ext.entity_id = entity.id "
            f"WHERE {' AND '.join(clauses)}"
        )
        ids = [r[0] for r in conn.execute(sql, params).fetchall()]
        if not ids:
            return 0
        with self._write_ctx() as wconn:
            wconn.executemany(
                "DELETE FROM entity WHERE id=?", [(i,) for i in ids]
            )
        return len(ids)

    def forget_expired(self, *, now: Optional[float] = None) -> int:
        """Delete every memory whose ``expires_at`` is in the past."""
        t = now if now is not None else time.time()
        conn = self._require_rw()
        ids = [
            r[0]
            for r in conn.execute(
                """SELECT entity_id FROM memory_ext
                   WHERE expires_at IS NOT NULL AND expires_at <= ?""",
                (t,),
            ).fetchall()
        ]
        if not ids:
            return 0
        with self._write_ctx() as wconn:
            wconn.executemany(
                "DELETE FROM entity WHERE id=?", [(i,) for i in ids]
            )
        return len(ids)

    def _load_memory(self, entity_id: str) -> Optional[MemoryItem]:
        conn = self._require_open()
        row = conn.execute(
            """SELECT entity.id, entity.kind, entity.namespace, entity.content,
                      entity.metadata, entity.created_at, entity.updated_at,
                      entity.version,
                      memory_ext.session_id, memory_ext.actor,
                      memory_ext.salience, memory_ext.expires_at
               FROM entity
               JOIN memory_ext ON memory_ext.entity_id = entity.id
               WHERE entity.id = ?""",
            (entity_id,),
        ).fetchone()
        if row is None:
            return None
        return MemoryItem(
            id=row[0],
            kind=row[1],
            namespace=row[2],
            content=row[3],
            metadata=_loads(row[4]),
            created_at=row[5],
            updated_at=row[6],
            version=row[7],
            session_id=row[8],
            actor=row[9],
            salience=row[10],
            expires_at=row[11],
        )

    # ------------------------------------------------------------------
    # Graph: relations + neighbors + traverse + subgraph
    # ------------------------------------------------------------------

    def add_relation(
        self,
        src: str,
        dst: str,
        kind: str,
        *,
        weight: float = 1.0,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Insert or replace a directed edge (src -> dst, labeled ``kind``).
        Both endpoints must already exist as entities; missing endpoints
        raise ``KeyError`` rather than relying on the FK error.
        """
        conn = self._require_rw()
        for name, eid in (("src", src), ("dst", dst)):
            if not conn.execute(
                "SELECT 1 FROM entity WHERE id=?", (eid,)
            ).fetchone():
                raise KeyError(f"{name} entity not found: {eid!r}")

        meta_json = _dumps(metadata or {})
        with self._write_ctx() as wconn:
            wconn.execute(
                """INSERT INTO relation (src, dst, kind, weight, metadata)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(src, dst, kind) DO UPDATE SET
                     weight=excluded.weight,
                     metadata=excluded.metadata""",
                (src, dst, kind, weight, meta_json),
            )

    def get_relation(
        self, src: str, dst: str, kind: str
    ) -> Optional[Relation]:
        conn = self._require_open()
        row = conn.execute(
            """SELECT src, dst, kind, weight, metadata
               FROM relation WHERE src=? AND dst=? AND kind=?""",
            (src, dst, kind),
        ).fetchone()
        return _row_to_relation(row) if row else None

    def delete_relation(self, src: str, dst: str, kind: str) -> bool:
        """Returns True iff a row was deleted."""
        with self._write_ctx() as conn:
            cur = conn.execute(
                "DELETE FROM relation WHERE src=? AND dst=? AND kind=?",
                (src, dst, kind),
            )
            return cur.rowcount > 0

    def list_relations(
        self,
        *,
        src: Optional[str] = None,
        dst: Optional[str] = None,
        kind: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[Relation]:
        clauses: list[str] = []
        params: list[Any] = []
        if src is not None:
            clauses.append("src=?")
            params.append(src)
        if dst is not None:
            clauses.append("dst=?")
            params.append(dst)
        if kind is not None:
            clauses.append("kind=?")
            params.append(kind)
        sql = "SELECT src, dst, kind, weight, metadata FROM relation"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY src, dst, kind"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        conn = self._require_open()
        return [_row_to_relation(r) for r in conn.execute(sql, params).fetchall()]

    def neighbors(
        self,
        entity_id: str,
        *,
        kind: Optional[str] = None,
        direction: Literal["out", "in", "both"] = "out",
        limit: Optional[int] = None,
    ) -> list[Neighbor]:
        """
        Single-hop neighbors of ``entity_id``.

        ``direction``:
          - "out":  edges where this node is the src; returns dst nodes.
          - "in":   edges where this node is the dst; returns src nodes.
          - "both": both, with each result tagged accordingly.
        """
        if direction not in ("out", "in", "both"):
            raise ValueError(
                f"direction must be 'out' | 'in' | 'both', got {direction!r}"
            )
        conn = self._require_open()
        out: list[Neighbor] = []

        def _query_one(
            this_col: str, other_col: str, label: str
        ) -> list[Neighbor]:
            clauses = [f"relation.{this_col} = ?"]
            params: list[Any] = [entity_id]
            if kind is not None:
                clauses.append("relation.kind = ?")
                params.append(kind)
            sql = (
                "SELECT entity.id, entity.kind, entity.namespace, "
                "entity.content, entity.metadata, entity.created_at, "
                "entity.updated_at, entity.version, "
                "relation.kind, relation.weight, relation.metadata "
                f"FROM relation JOIN entity ON entity.id = relation.{other_col} "
                f"WHERE {' AND '.join(clauses)}"
            )
            rows = conn.execute(sql, params).fetchall()
            return [
                Neighbor(
                    entity=_row_to_entity(r[0:8]),
                    relation_kind=r[8],
                    weight=r[9],
                    direction=label,
                    metadata=_loads(r[10]),
                )
                for r in rows
            ]

        if direction in ("out", "both"):
            out.extend(_query_one("src", "dst", "out"))
        if direction in ("in", "both"):
            out.extend(_query_one("dst", "src", "in"))

        if limit is not None:
            out = out[:limit]
        return out

    def traverse(
        self,
        start: str,
        *,
        kind: Optional[str] = None,
        direction: Literal["out", "in", "both"] = "out",
        max_hops: int = 3,
        namespace: Optional[str] = None,
    ) -> list[TraversalHit]:
        """
        Multi-hop BFS from ``start``. Returns one row per reachable node
        (excluding ``start``) with its shortest hop count.

        Cycle-safe: bounded by ``max_hops``, hops dedup via ``UNION``,
        final selection takes ``MIN(hop)``.
        """
        if max_hops < 1:
            raise ValueError("max_hops must be >= 1")
        if direction not in ("out", "in", "both"):
            raise ValueError(
                f"direction must be 'out' | 'in' | 'both', got {direction!r}"
            )

        # Recursive step varies by direction.
        if direction == "out":
            join_cond = "r.src = walk.id"
            next_node = "r.dst"
        elif direction == "in":
            join_cond = "r.dst = walk.id"
            next_node = "r.src"
        else:
            join_cond = "(r.src = walk.id OR r.dst = walk.id)"
            next_node = (
                "CASE WHEN r.src = walk.id THEN r.dst ELSE r.src END"
            )

        kind_clause = "AND r.kind = ?" if kind is not None else ""

        cte_sql = (
            f"WITH RECURSIVE walk(id, hop) AS ("
            f"  SELECT ?, 0"
            f"  UNION"
            f"  SELECT {next_node}, walk.hop + 1 "
            f"  FROM walk JOIN relation r ON {join_cond} "
            f"  WHERE walk.hop < ? {kind_clause}"
            f") "
            f"SELECT entity.id, entity.kind, entity.namespace, entity.content, "
            f"       entity.metadata, entity.created_at, entity.updated_at, "
            f"       entity.version, MIN(walk.hop) as min_hop "
            f"FROM walk JOIN entity ON entity.id = walk.id "
            f"WHERE walk.id != ? "
        )
        params: list[Any] = [start, max_hops]
        if kind is not None:
            params.append(kind)
        params.append(start)
        if namespace is not None:
            cte_sql += "AND entity.namespace = ? "
            params.append(namespace)
        cte_sql += "GROUP BY entity.id ORDER BY min_hop, entity.id"

        conn = self._require_open()
        rows = conn.execute(cte_sql, params).fetchall()
        return [
            TraversalHit(entity=_row_to_entity(r[0:8]), hop=int(r[8]))
            for r in rows
        ]

    def subgraph(
        self,
        seeds: Iterable[str],
        *,
        kind: Optional[str] = None,
        direction: Literal["out", "in", "both"] = "both",
        max_hops: int = 2,
    ) -> Subgraph:
        """
        Induced subgraph reachable from ``seeds``. Includes the seeds
        themselves plus every node within ``max_hops``, and every relation
        where both endpoints land in the visited set.
        """
        seed_list = list(seeds)
        if not seed_list:
            return Subgraph()
        if max_hops < 0:
            raise ValueError("max_hops must be >= 0")
        if direction not in ("out", "in", "both"):
            raise ValueError(
                f"direction must be 'out' | 'in' | 'both', got {direction!r}"
            )

        # Multi-source seed set is fed to the CTE via json_each — works
        # for any seed count without dynamic placeholders.
        if direction == "out":
            join_cond = "r.src = walk.id"
            next_node = "r.dst"
        elif direction == "in":
            join_cond = "r.dst = walk.id"
            next_node = "r.src"
        else:
            join_cond = "(r.src = walk.id OR r.dst = walk.id)"
            next_node = (
                "CASE WHEN r.src = walk.id THEN r.dst ELSE r.src END"
            )
        kind_clause = "AND r.kind = ?" if kind is not None else ""

        seeds_json = json.dumps(seed_list)
        params: list[Any] = [seeds_json]
        if max_hops >= 1:
            params.append(max_hops)
            if kind is not None:
                params.append(kind)

        # When max_hops == 0, just collect the seeds themselves.
        if max_hops == 0:
            cte_sql = (
                "SELECT DISTINCT value AS id FROM json_each(?)"
            )
            params = [seeds_json]
        else:
            cte_sql = (
                f"WITH RECURSIVE walk(id, hop) AS ("
                f"  SELECT value, 0 FROM json_each(?)"
                f"  UNION"
                f"  SELECT {next_node}, walk.hop + 1 "
                f"  FROM walk JOIN relation r ON {join_cond} "
                f"  WHERE walk.hop < ? {kind_clause}"
                f") "
                f"SELECT DISTINCT id FROM walk"
            )

        conn = self._require_open()
        ids = [r[0] for r in conn.execute(cte_sql, params).fetchall()]
        if not ids:
            return Subgraph()

        placeholders = ",".join("?" * len(ids))
        ent_rows = conn.execute(
            f"""SELECT id, kind, namespace, content, metadata,
                       created_at, updated_at, version
                FROM entity WHERE id IN ({placeholders})
                ORDER BY id""",
            ids,
        ).fetchall()
        entities = [_row_to_entity(r) for r in ent_rows]

        rel_clauses = [f"src IN ({placeholders})", f"dst IN ({placeholders})"]
        rel_params = list(ids) + list(ids)
        if kind is not None:
            rel_clauses.append("kind = ?")
            rel_params.append(kind)
        rel_rows = conn.execute(
            "SELECT src, dst, kind, weight, metadata FROM relation "
            f"WHERE {' AND '.join(rel_clauses)} ORDER BY src, dst, kind",
            rel_params,
        ).fetchall()
        relations = [_row_to_relation(r) for r in rel_rows]
        return Subgraph(entities=entities, relations=relations)

    # ------------------------------------------------------------------
    # Ingestion helpers
    # ------------------------------------------------------------------

    def ingest_text(
        self,
        text: str,
        *,
        id: Optional[str] = None,  # noqa: A002
        kind: str = "document",
        namespace: str = "default",
        chunker: Optional[Chunker] = None,
        metadata: Optional[dict[str, Any]] = None,
        embedder: Optional[Embedder] = None,
    ) -> str:
        """
        Atomic text ingestion: entity + chunks (+ embeddings).

        ``chunker`` defaults to :py:func:`chunkers.by_paragraph`. If the
        chosen chunker yields no chunks (e.g. the text has no blank-line
        boundaries), the whole text is stored as one chunk so the entity
        is never silently empty.
        """
        eid = id if id is not None else _uuid()
        active_chunker = chunker if chunker is not None else by_paragraph()
        pieces = active_chunker(text)
        if not pieces:
            stripped = text.strip()
            if stripped:
                pieces = [stripped]

        with self.transaction():
            self.put_entity(
                id=eid, kind=kind, namespace=namespace,
                content=text, metadata=metadata or {},
            )
            if pieces:
                self.add_chunks(eid, pieces)
            if embedder is not None and pieces:
                self.embed_chunks(embedder, entity_id=eid)
        return eid

    def ingest_table(
        self,
        rows: Union[Iterable[dict[str, Any]], str, Path],
        *,
        table_name: str,
        id_column: Optional[str] = None,
        namespace: str = "default",
        embedder: Optional[Embedder] = None,
    ) -> list[str]:
        """
        Atomic table ingestion. Each row becomes one entity (kind=
        ``"table_row"``) holding the row in ``metadata.columns``, plus
        one chunk rendered as ``"k: v\\n..."`` for full-text + embedding.

        ``rows`` may be:
          - an iterable of dicts (in-memory rows), or
          - a path / string path to a CSV file (read via ``csv.DictReader``).

        ``id_column`` picks the entity id from a row column when present;
        otherwise an auto-uuid is assigned.
        """
        if isinstance(rows, (str, Path)):
            rows = _read_csv_rows(Path(rows))
        materialized = list(rows)
        if not materialized:
            return []

        ids: list[str] = []
        with self.transaction():
            for row in materialized:
                if id_column is not None and id_column in row:
                    eid = str(row[id_column])
                else:
                    eid = _uuid()
                ids.append(eid)
                content = _render_row(row)
                self.put_entity(
                    id=eid, kind="table_row", namespace=namespace,
                    content=content,
                    metadata={"table": table_name, "columns": dict(row)},
                )
                self.add_chunks(eid, [content])
            if embedder is not None:
                for eid in ids:
                    self.embed_chunks(embedder, entity_id=eid)
        return ids

    # ------------------------------------------------------------------
    # Management: validate / compact / export
    # ------------------------------------------------------------------

    def validate(self) -> ValidationReport:
        """
        Run consistency checks. Read-only; safe on a r-mode handle.

        Catches:
          - SQLite page-level corruption (PRAGMA integrity_check)
          - Foreign-key violations (PRAGMA foreign_key_check)
          - Bad application_id / unsupported schema_version
          - chunk vs chunk_fts row-count divergence
          - Stale per-model HNSW blob (count != live embedding count)
        """
        conn = self._require_open()
        issues: list[str] = []
        counts: dict[str, int] = {}

        rows = conn.execute("PRAGMA integrity_check").fetchall()
        if not (len(rows) == 1 and rows[0][0] == "ok"):
            issues.append(
                "integrity_check: " + "; ".join(r[0] for r in rows)
            )

        fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_rows:
            issues.append(
                f"foreign_key_check: {len(fk_rows)} violation(s) "
                f"(first: {fk_rows[0]})"
            )

        (app_id,) = conn.execute("PRAGMA application_id").fetchone()
        if app_id != _schema.APPLICATION_ID:
            issues.append(
                f"application_id={app_id:#010x} "
                f"(expected {_schema.APPLICATION_ID:#010x})"
            )
        (sv,) = conn.execute("PRAGMA user_version").fetchone()
        if sv > _schema.SCHEMA_VERSION:
            issues.append(
                f"schema_version={sv} > library {_schema.SCHEMA_VERSION}"
            )

        for table in ("entity", "chunk", "embedding", "relation", "memory_ext"):
            counts[table] = conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]

        # FTS5 'integrity-check' compares index entries against the
        # external content table (chunk). It surfaces missing index rows,
        # stale tokens, and corruption — anything a row-count check
        # cannot, since external-content COUNT(*) is served from chunk.
        try:
            conn.execute(
                "INSERT INTO chunk_fts(chunk_fts) VALUES('integrity-check')"
            )
        except sqlite3.DatabaseError as exc:
            issues.append(f"chunk_fts integrity-check: {exc}")

        # Per-model HNSW staleness — only complain when a blob exists.
        idx_rows = conn.execute(
            "SELECT model, index_meta FROM vector_index"
        ).fetchall()
        for model, meta_json in idx_rows:
            try:
                meta = json.loads(meta_json) if meta_json else {}
            except json.JSONDecodeError:
                issues.append(f"vector_index[{model}]: corrupt index_meta JSON")
                continue
            (live,) = conn.execute(
                "SELECT COUNT(*) FROM embedding WHERE model=?", (model,)
            ).fetchone()
            if meta.get("count") != live:
                issues.append(
                    f"vector_index[{model}] stale: "
                    f"meta.count={meta.get('count')} != live={live}"
                )

        return ValidationReport(ok=not issues, issues=issues, counts=counts)

    def compact(self) -> dict[str, int]:
        """
        Reclaim space and rebuild auxiliary indexes.

        Steps:
          1. Rebuild the FTS5 index from ``chunk``.
          2. Drop persisted HNSW blobs (next search rebuilds them).
          3. ``VACUUM`` the database.

        Returns ``{"size_before": N, "size_after": M}`` in bytes.
        """
        conn = self._require_rw()
        if self._tx_depth > 0:
            raise RuntimeError("compact() cannot run inside a transaction()")

        size_before = self.path.stat().st_size if self.path.exists() else 0

        with self._write_ctx() as wconn:
            wconn.execute(
                "INSERT INTO chunk_fts(chunk_fts) VALUES('rebuild')"
            )
            wconn.execute("DELETE FROM vector_index")
        self._hnsw_cache.clear()

        # VACUUM cannot run inside a transaction; sqlite3 implicitly opens
        # one for any DML, so issue a COMMIT first to be safe, then VACUUM.
        conn.commit()
        conn.execute("VACUUM")

        size_after = self.path.stat().st_size if self.path.exists() else 0
        return {"size_before": size_before, "size_after": size_after}

    def export(
        self,
        path: PathLike,
        *,
        include_embeddings: bool = True,
    ) -> dict[str, int]:
        """
        Dump the store to a single JSON file.

        Embeddings are emitted as base64-encoded float32 bytes so the
        output stays text-only and round-trippable. Set
        ``include_embeddings=False`` to omit them — useful when you only
        want a content backup and plan to re-embed at import time.
        """
        import base64

        conn = self._require_open()
        out_path = Path(path)

        def _rows(table: str, cols: str) -> list[dict]:
            return [
                dict(zip(cols.split(", "), r))
                for r in conn.execute(f"SELECT {cols} FROM {table}")
            ]

        entities = []
        for r in conn.execute(
            "SELECT id, kind, namespace, content, metadata, "
            "created_at, updated_at, version FROM entity"
        ):
            entities.append({
                "id": r[0], "kind": r[1], "namespace": r[2],
                "content": r[3], "metadata": _loads(r[4]),
                "created_at": r[5], "updated_at": r[6], "version": r[7],
            })

        chunks = []
        for r in conn.execute(
            "SELECT id, entity_id, seq, content, kind, metadata, "
            "token_count FROM chunk"
        ):
            chunks.append({
                "id": r[0], "entity_id": r[1], "seq": r[2],
                "content": r[3], "kind": r[4],
                "metadata": _loads(r[5]), "token_count": r[6],
            })

        relations = []
        for r in conn.execute(
            "SELECT src, dst, kind, weight, metadata FROM relation"
        ):
            relations.append({
                "src": r[0], "dst": r[1], "kind": r[2],
                "weight": r[3], "metadata": _loads(r[4]),
            })

        memories = []
        for r in conn.execute(
            "SELECT entity_id, session_id, actor, salience, expires_at "
            "FROM memory_ext"
        ):
            memories.append({
                "entity_id": r[0], "session_id": r[1], "actor": r[2],
                "salience": r[3], "expires_at": r[4],
            })

        embeddings: list[dict] = []
        if include_embeddings:
            for r in conn.execute(
                "SELECT chunk_id, model, dim, vector FROM embedding"
            ):
                embeddings.append({
                    "chunk_id": r[0],
                    "model": r[1],
                    "dim": r[2],
                    "vector_b64": base64.b64encode(r[3]).decode("ascii"),
                })

        payload = {
            "schema_version": _schema.SCHEMA_VERSION,
            "exported_at": time.time(),
            "entities": entities,
            "chunks": chunks,
            "embeddings": embeddings,
            "relations": relations,
            "memory_ext": memories,
        }
        out_path.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        return {
            "entities": len(entities),
            "chunks": len(chunks),
            "embeddings": len(embeddings),
            "relations": len(relations),
            "memories": len(memories),
            "bytes_written": out_path.stat().st_size,
        }


# ----------------------------------------------------------------------
# row <-> dataclass helpers
# ----------------------------------------------------------------------

def _row_to_entity(row: tuple) -> Entity:
    return Entity(
        id=row[0],
        kind=row[1],
        namespace=row[2],
        content=row[3],
        metadata=_loads(row[4]),
        created_at=row[5],
        updated_at=row[6],
        version=row[7],
    )


def _row_to_chunk(row: tuple) -> Chunk:
    return Chunk(
        id=row[0],
        entity_id=row[1],
        seq=row[2],
        content=row[3],
        kind=row[4],
        metadata=_loads(row[5]),
        token_count=row[6],
    )


def _row_to_relation(row: tuple) -> Relation:
    return Relation(
        src=row[0],
        dst=row[1],
        kind=row[2],
        weight=row[3],
        metadata=_loads(row[4]),
    )


def _coerce_chunk(
    raw: Union[Chunk, str, dict[str, Any]],
    *,
    entity_id: str,
    default_seq: int,
) -> Chunk:
    if isinstance(raw, Chunk):
        # Force entity_id to the one we're inserting under; seq defaults
        # only if the caller passed seq=-1 as a sentinel (unusual).
        if raw.entity_id != entity_id:
            raw.entity_id = entity_id
        if not raw.id:
            raw.id = _uuid()
        return raw
    if isinstance(raw, str):
        return Chunk(
            id=_uuid(),
            entity_id=entity_id,
            seq=default_seq,
            content=raw,
        )
    if isinstance(raw, dict):
        d = dict(raw)
        return Chunk(
            id=d.get("id") or _uuid(),
            entity_id=entity_id,
            seq=d.get("seq", default_seq),
            content=d["content"],
            kind=d.get("kind", "text"),
            metadata=d.get("metadata", {}),
            token_count=d.get("token_count"),
        )
    raise TypeError(f"unsupported chunk type: {type(raw).__name__}")


def _build_entity_where(
    where: dict[str, Any],
    *,
    table_prefix: str = "",
) -> tuple[list[str], list[Any]]:
    """
    Shared builder for entity `where=` clauses used by both
    `query_entities` and `search`.

    ``table_prefix`` is prepended to column names (e.g. ``"entity."``) so the
    same builder works inside multi-table joins.
    """
    clauses: list[str] = []
    params: list[Any] = []
    for key, raw_val in where.items():
        op, val = _unpack_op(raw_val)
        if key.startswith("metadata."):
            path = "$." + key[len("metadata."):]
            col_expr = f"json_extract({table_prefix}metadata, ?)"
            if op == "IN":
                placeholders = ",".join("?" * len(val))
                clauses.append(f"{col_expr} IN ({placeholders})")
                params.append(path)
                params.extend(val)
            else:
                clauses.append(f"{col_expr} {op} ?")
                params.append(path)
                params.append(val)
        else:
            if key not in _ENTITY_COLUMNS:
                raise ValueError(
                    f"unknown entity column: {key!r}. "
                    f"Use 'metadata.<path>' for JSON fields."
                )
            col = f"{table_prefix}{key}"
            if op == "IN":
                placeholders = ",".join("?" * len(val))
                clauses.append(f"{col} IN ({placeholders})")
                params.extend(val)
            else:
                clauses.append(f"{col} {op} ?")
                params.append(val)
    return clauses, params


def _unpack_op(raw_val: Any) -> tuple[str, Any]:
    """
    Accept either a bare value (=) or a (op, value) tuple.
    Validate op against the allow-list to block SQL injection
    via the operator slot.
    """
    if isinstance(raw_val, tuple) and len(raw_val) == 2 and isinstance(raw_val[0], str):
        op, val = raw_val
        op = op.upper()
        if op not in _OPERATORS:
            raise ValueError(f"unsupported operator: {raw_val[0]!r}")
        if op == "IN" and not isinstance(val, (list, tuple)):
            raise ValueError("IN operator requires a list/tuple value")
        return op, val
    return "=", raw_val


def _sanitize_order_by(clause: str) -> str:
    """Accept only `<col> [ASC|DESC]` pairs, comma-separated."""
    out = []
    for segment in clause.split(","):
        tokens = segment.strip().split()
        if len(tokens) == 1:
            col, direction = tokens[0], "ASC"
        elif len(tokens) == 2:
            col, direction = tokens[0], tokens[1].upper()
        else:
            raise ValueError(f"invalid order_by clause: {segment!r}")
        if col not in _ENTITY_COLUMNS:
            raise ValueError(f"unknown order_by column: {col!r}")
        if direction not in {"ASC", "DESC"}:
            raise ValueError(f"invalid direction: {direction!r}")
        out.append(f"{col} {direction}")
    return ", ".join(out)


__all__ = ["CarbonStore"]
