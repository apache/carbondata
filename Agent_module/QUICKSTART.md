# carbon_data Quickstart

`Agent_module.carbon_data` is the Agent Data Infra: one `.carbondata` file =
one self-contained knowledge base, simultaneously serving **RAG semantic
search / long-term memory / structured queries / knowledge-graph traversal**.

> Companion runnable demo: [`examples/carbondata_quickstart.py`](../examples/carbondata_quickstart.py)

---

## 1. Install & dependencies

The library itself only needs the Python standard library plus `numpy`.
Optional dependencies are loaded on demand:

| Dependency | Purpose | Required? |
|---|---|---|
| Python ≥ 3.10 | | required |
| `numpy` | vector ops | required |
| `hnswlib` | fast ANN for 10k+ vectors | optional (falls back to brute force when missing) |

```bash
pip install numpy
pip install hnswlib   # optional
```

A `.carbondata` file **is just a SQLite database**, so you can crack it open
with any SQLite tool (`sqlite3 kb.carbondata`, DBeaver, DataGrip) for ad-hoc
debugging.

---

## 2. 30-second tour

```python
import numpy as np
from Agent_module.carbon_data import create, LambdaEmbedder

# Your embedding function — the library does not bundle a model.
def embed(texts):
    return np.random.randn(len(texts), 384).astype(np.float32)

embedder = LambdaEmbedder(embed, model="my-model", dim=384)

# 1) Create or open a knowledge base
store = create("kb.carbondata")

# 2) Feed it data
store.ingest_text(
    "This is paragraph one.\n\nParagraph two.\n\nAnd paragraph three.",
    id="doc-1",
    embedder=embedder,
)

# 3) Semantic search
hits = store.search("paragraph one", embedder=embedder, top_k=3)
for h in hits:
    print(h.score, h.chunk.content)

store.close()
```

---

## 3. The four core scenarios

### 3.1 RAG semantic search

```python
# Vector retrieval (default)
hits = store.search("user login flow", embedder=embedder, top_k=5)

# BM25 keyword
hits = store.search("login", mode="keyword", top_k=5)

# Vector + keyword fused via Reciprocal Rank Fusion
hits = store.search("user login", mode="hybrid", embedder=embedder, top_k=5)
```

Every `SearchHit` carries `chunk` (the matched fragment), `entity` (its
parent), and `score`.

### 3.2 Long-term memory

```python
# Write
store.remember(
    "user prefers Vim for editing code",
    session_id="sess-001",
    actor="CodingAgent",
    salience=0.8,           # importance, 0..1
    ttl=86400,              # expires after 24h
    embedder=embedder,
)

# Semantic recall (expired memories are excluded by default)
memories = store.recall(
    "editor preferences",
    session_id="sess-001",
    min_salience=0.5,
    embedder=embedder,
    top_k=3,
)
for m in memories:
    print(m.score, m.memory.content, m.memory.actor)

# Cleanup
store.forget(session_id="sess-001")    # by session
store.forget_expired()                  # GC expired memories
```

### 3.3 Structured queries

```python
# Ingest a table (list of dicts or a CSV path)
store.ingest_table(
    [{"id": "u1", "name": "Alice", "team": "infra"},
     {"id": "u2", "name": "Bob",   "team": "ml"}],
    table_name="users",
    id_column="id",
    embedder=embedder,
)

# Structured filter — note the JSON path goes through `columns.`
# because ingest_table stores rows as metadata = {table, columns}.
rows = store.query_entities(
    kind="table_row",
    where={"metadata.columns.team": "ml"},
)

# Metadata filters with operators
recent = store.query_entities(
    kind="document",
    where={"created_at": (">=", 1700000000.0)},
    order_by="updated_at DESC",
    limit=20,
)
```

### 3.4 Knowledge graph

```python
# Add edges between existing entities
store.add_relation("doc:paper", "doc:guide", "references", weight=0.9)
store.add_relation("doc:paper", "doc:guide", "supersedes")  # multiple kinds per pair

# 1-hop neighbors
ns = store.neighbors("doc:paper", direction="out", kind="references")

# Multi-hop traversal (recursive CTE, cycle-safe, bounded)
hits = store.traverse(
    "doc:paper",
    kind="references",
    direction="out",
    max_hops=3,
)
for h in hits:
    print(h.entity.id, "at hop", h.hop)

# Subgraph extraction
sg = store.subgraph(seeds=["doc:paper"], max_hops=2, direction="both")
print(len(sg.entities), len(sg.relations))
```

---

## 4. Going further

### 4.1 Custom chunkers

```python
from Agent_module.carbon_data import by_tokens, by_paragraph, by_sentence

# Sliding token window (with overlap)
chunker = by_tokens(max_tokens=256, overlap=32)

# By paragraph (default)
chunker = by_paragraph(min_chars=20)

# By sentence
chunker = by_sentence()

store.ingest_text(text, chunker=chunker, embedder=embedder)
```

You can roll your own: `Chunker = Callable[[str], list[str]]`.

### 4.2 HNSW dispatch

When `hnswlib` is installed, the library **automatically** uses HNSW
acceleration whenever:

- `mode="vector"` (including the vector leg of `mode="hybrid"`)
- `metric="cosine"`
- no JSON `filters=` are present
- `namespace=` is applied as a post-filter (with 3× over-sampling)

Explicit override:
```python
store.search(q, embedder=emb, use_hnsw="auto")  # default
store.search(q, embedder=emb, use_hnsw="off")   # force brute force
store.search(q, embedder=emb, use_hnsw="on")    # force HNSW (raises if not eligible)
```

On the first search, HNSW is built from the entire `embedding` table and
persisted to a `vector_index` blob; later add/delete calls are detected via
a row-count comparison and trigger a lazy rebuild.

### 4.3 Namespace isolation

```python
store.put_entity(id="x", kind="document", namespace="tenant-a")
hits = store.search(q, embedder=emb, namespace="tenant-a")
```

Useful when multiple tenants or datasets share one file.

### 4.4 Transactions

```python
with store.transaction():
    store.put_entity(id="a", kind="document")
    store.add_chunks("a", [...])
    store.add_relation("a", "b", "ref")
    # any exception inside the block → full rollback
```

`ingest_text` / `ingest_table` / `remember` are already wrapped in their own
transactions internally.

### 4.5 Admin & ops

```python
# Consistency check (read-only)
report = store.validate()
if not report.ok:
    for issue in report.issues:
        print(issue)

# VACUUM + FTS rebuild + clear vector_index cache
sizes = store.compact()
print("freed:", sizes["size_before"] - sizes["size_after"], "bytes")

# One-file JSON backup (embeddings encoded as base64)
store.export("backup.json")
store.export("backup_no_vec.json", include_embeddings=False)

# Snapshot of current state
print(store.stats())
```

---

## 5. Debugging tips

- **It really is SQLite.** `sqlite3 kb.carbondata` drops you straight into
  the REPL where you can run raw SQL against any table.
- **Schema** lives entirely in `carbon_data/schema.py`, ~150 lines.
- **Test fixtures are good textbook material**: `tests/carbon_data/test_m{1..9}_*.py`
  — each file covers one milestone end-to-end.
- **Run the suite:**
  ```bash
  cd Agent_module
  .venv/bin/pytest tests/carbon_data/ -q
  ```

---


## 6. Next steps

Run the demo to see it all in action:

```bash
cd Agent_module
.venv/bin/python examples/carbondata_quickstart.py
```

For the full API surface, read the `class CarbonStore` docstring in
`carbon_data/store.py`.
