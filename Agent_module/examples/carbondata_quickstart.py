"""
05_carbondata_quickstart.py
===========================
End-to-end demo of carbon_data — Agent Data Infra.

Walks through all four core scenarios on a tiny in-memory corpus:

  1. RAG semantic / keyword / hybrid search
  2. Long-term memory (remember / recall / forget)
  3. Structured queries over an ingested table
  4. Knowledge-graph traversal (relations / neighbors / subgraph)

Plus admin: stats, validate, compact, export, persistence across reopen.

The "embedder" used here is a hand-rolled vocabulary bag (no external
model). It's deterministic and gives semantically meaningful clustering
on the demo corpus, so you can see real top-k ranking — but it's only
useful for didactic data of this size. Swap in any
``embedder.encode(texts) -> np.ndarray`` for real workloads.

Run:
    cd Agent_module
    .venv/bin/python examples/05_carbondata_quickstart.py
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from Agent_module.carbon_data import (
    LambdaEmbedder,
    by_paragraph,
    by_tokens,
    create,
    open as cd_open,
)


# ---------------------------------------------------------------------------
# Tiny vocabulary embedder — gives meaningful cosine clustering for the demo
# without needing a real embedding model.
# ---------------------------------------------------------------------------

VOCAB = [
    # web
    "django", "flask", "fastapi", "web", "framework", "http", "route",
    # ml
    "tensorflow", "pytorch", "neural", "network", "model", "training",
    "learning", "embedding", "transformer",
    # cooking
    "recipe", "cooking", "ingredient", "kitchen", "flavor", "dish",
    # generic
    "python", "code", "data", "system",
]
DIM = len(VOCAB)


def vocab_embed(texts):
    """Bag-of-vocabulary one-hot, L2-normalized — cosine acts like Jaccard."""
    out = np.zeros((len(texts), DIM), dtype=np.float32)
    for i, text in enumerate(texts):
        words = set(text.lower().split())
        for j, term in enumerate(VOCAB):
            if term in words:
                out[i, j] = 1.0
        norm = np.linalg.norm(out[i])
        if norm > 0:
            out[i] /= norm
    return out


EMBEDDER = LambdaEmbedder(vocab_embed, model="vocab-demo", dim=DIM)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _h(title: str) -> None:
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


def _row(label: str, value) -> None:
    print(f"  {label:<24} {value}")


# ---------------------------------------------------------------------------
# the demo
# ---------------------------------------------------------------------------

def run() -> bool:
    workdir = Path(tempfile.mkdtemp(prefix="carbondata_demo_"))
    db_path = workdir / "kb.carbondata"
    print(f"Working in {workdir}")

    store = create(db_path)

    # -----------------------------------------------------------------
    # 1) Ingest text documents
    # -----------------------------------------------------------------
    _h("1. ingest_text — three short documents")

    docs = {
        "doc:django": (
            "Django is a high-level Python web framework that encourages "
            "rapid development.\n\n"
            "It ships with an ORM, an admin panel, and a templating system "
            "for building HTTP routes.\n\n"
            "Django is opinionated and convention-heavy."
        ),
        "doc:flask": (
            "Flask is a lightweight Python web framework with a minimal core.\n\n"
            "Routes, request parsing, and templating are unbundled, letting "
            "you choose the components yourself.\n\n"
            "Flask is unopinionated and small."
        ),
        "doc:transformer": (
            "A transformer is a neural network architecture built around "
            "self-attention.\n\n"
            "Modern embedding models and large language models use the "
            "transformer as the core training substrate.\n\n"
            "It largely replaced earlier recurrent neural network designs."
        ),
        "doc:recipe": (
            "Pasta carbonara is an Italian dish from Rome.\n\n"
            "The recipe uses egg, cheese, pepper, and cured pork — "
            "simple ingredients combined in the kitchen for rich flavor."
        ),
    }

    for eid, text in docs.items():
        store.ingest_text(
            text,
            id=eid,
            chunker=by_paragraph(),
            embedder=EMBEDDER,
            metadata={"source": "demo"},
        )

    stats = store.stats()
    _row("entities", stats["entities"])
    _row("chunks",   stats["chunks"])
    _row("embeddings", stats["embeddings"])

    # -----------------------------------------------------------------
    # 2) Vector search — semantic ranking
    # -----------------------------------------------------------------
    _h("2. semantic search — query: 'python web framework'")

    hits = store.search(
        "python web framework", embedder=EMBEDDER, top_k=3
    )
    for h in hits:
        _row(f"{h.entity.id} (score={h.score:.3f})", h.chunk.content[:60] + "...")
    assert hits[0].entity.id in {"doc:django", "doc:flask"}, hits

    # -----------------------------------------------------------------
    # 3) Keyword search — exact-token retrieval via FTS5/BM25
    # -----------------------------------------------------------------
    _h("3. keyword search — query: 'transformer'")

    hits = store.search("transformer", mode="keyword", top_k=3)
    for h in hits:
        _row(f"{h.entity.id} (BM25={h.score:.3f})", h.chunk.content[:60] + "...")
    assert any(h.entity.id == "doc:transformer" for h in hits)

    # -----------------------------------------------------------------
    # 4) Hybrid search — vector ⊕ keyword via Reciprocal Rank Fusion
    # -----------------------------------------------------------------
    _h("4. hybrid search — query: 'neural training'")

    hits = store.search(
        "neural training", mode="hybrid", embedder=EMBEDDER, top_k=3
    )
    for h in hits:
        _row(f"{h.entity.id} (RRF={h.score:.4f})", h.chunk.content[:60] + "...")

    # -----------------------------------------------------------------
    # 5) Ingest a table — each row becomes a queryable entity
    # -----------------------------------------------------------------
    _h("5. ingest_table — users")

    users = [
        {"id": "u1", "name": "Alice", "team": "infra",   "lang": "Go"},
        {"id": "u2", "name": "Bob",   "team": "ml",      "lang": "Python"},
        {"id": "u3", "name": "Carol", "team": "ml",      "lang": "Python"},
        {"id": "u4", "name": "Dan",   "team": "product", "lang": "Rust"},
    ]
    store.ingest_table(users, table_name="users", id_column="id",
                       embedder=EMBEDDER)

    # Table column filter — note the JSON path goes through `columns.`
    # because ingest_table stores the row as metadata = {table, columns}.
    rows = store.query_entities(
        kind="table_row", where={"metadata.columns.team": "ml"}
    )
    print(f"  team=ml rows: {len(rows)}")
    for e in rows:
        _row(e.id, e.metadata["columns"])

    # -----------------------------------------------------------------
    # 6) Long-term memory
    # -----------------------------------------------------------------
    _h("6. memory — remember + recall")

    store.remember(
        "user prefers django over flask",
        session_id="sess-1", actor="CodingAgent",
        salience=0.9, embedder=EMBEDDER,
    )
    store.remember(
        "user works mainly with python web framework code",
        session_id="sess-1", actor="CodingAgent",
        salience=0.7, embedder=EMBEDDER,
    )
    store.remember(
        "user once asked about a neural transformer model",
        session_id="sess-1", actor="CodingAgent",
        salience=0.4, embedder=EMBEDDER,
    )

    memories = store.recall(
        "django framework",
        session_id="sess-1",
        embedder=EMBEDDER, top_k=3,
    )
    for m in memories:
        _row(f"score={m.score:.3f} sal={m.memory.salience}", m.memory.content)

    # min_salience filters out the third memory
    important = store.recall(
        "framework", session_id="sess-1", min_salience=0.6,
        embedder=EMBEDDER, top_k=5,
    )
    _row("with min_salience=0.6", f"{len(important)} memories")

    # -----------------------------------------------------------------
    # 7) Knowledge graph
    # -----------------------------------------------------------------
    _h("7. graph — relations + traversal")

    store.add_relation("doc:django", "doc:flask",       "compares_with",
                       weight=0.8)
    store.add_relation("doc:django", "doc:transformer", "tangentially_about",
                       weight=0.2)
    store.add_relation("doc:flask",  "doc:transformer", "tangentially_about",
                       weight=0.2)
    store.add_relation("doc:transformer", "doc:recipe", "unrelated",
                       weight=0.0)

    print("  neighbors of doc:django (out):")
    for n in store.neighbors("doc:django", direction="out"):
        _row(f"  -> {n.entity.id}", f"{n.relation_kind} (w={n.weight})")

    print("  traverse from doc:django, max_hops=2:")
    for t in store.traverse("doc:django", direction="both", max_hops=2):
        _row(f"  hop={t.hop}", t.entity.id)

    print("  subgraph from {doc:django, doc:flask}, max_hops=1:")
    sg = store.subgraph(["doc:django", "doc:flask"], max_hops=1)
    _row("  entities", [e.id for e in sg.entities])
    _row("  relations", [(r.src, r.dst, r.kind) for r in sg.relations])

    # -----------------------------------------------------------------
    # 8) Filter pushdown — restrict to a single entity kind
    # -----------------------------------------------------------------
    _h("8. filter pushdown — kind=document only")

    # Without the filter, table rows and memories also rank in.
    # With filters set, the search routes through brute force (HNSW
    # cannot pre-filter on JSON metadata).
    hits = store.search(
        "python web framework", embedder=EMBEDDER,
        filters={"kind": "document"}, top_k=3,
    )
    for h in hits:
        _row(f"{h.entity.id} ({h.entity.kind})",
             h.chunk.content[:60] + "...")
    assert all(h.entity.kind == "document" for h in hits)

    # -----------------------------------------------------------------
    # 9) Admin: validate, compact, export
    # -----------------------------------------------------------------
    _h("9. admin")

    # First validate may flag a stale HNSW blob: section 2 persisted the
    # index at the original 11 embeddings, but later sections added
    # table rows + memories. Filter-bearing searches in section 8 used
    # brute force, so the persisted blob was never refreshed. validate()
    # catches the divergence; compact() resolves it (next search lazily
    # rebuilds from the live embedding table).
    report = store.validate()
    _row("validate.ok (before compact)", report.ok)
    for i in report.issues:
        print(f"    ! {i}")

    sizes = store.compact()
    _row("compact size_before", sizes["size_before"])
    _row("compact size_after",  sizes["size_after"])

    report2 = store.validate()
    _row("validate.ok (after compact)", report2.ok)
    assert report2.ok, report2.issues

    export_path = workdir / "backup.json"
    summary = store.export(export_path)
    _row("export bytes",     summary["bytes_written"])
    _row("export entities",  summary["entities"])
    _row("export embeddings", summary["embeddings"])

    # -----------------------------------------------------------------
    # 10) Persistence — close, reopen, search still works
    # -----------------------------------------------------------------
    _h("10. persistence — reopen the file")

    store.close()
    store2 = cd_open(db_path)
    try:
        hits = store2.search(
            "python web framework", embedder=EMBEDDER, top_k=2
        )
        _row("post-reopen top-1", hits[0].entity.id)
        # FTS persisted via triggers, recall persisted via memory_ext, etc.
        ms = store2.recall("django", session_id="sess-1",
                           embedder=EMBEDDER, top_k=1)
        _row("post-reopen recall", ms[0].memory.content if ms else "(none)")
    finally:
        store2.close()

    print("\nDone. Demo data lives at:", db_path)
    return True


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
