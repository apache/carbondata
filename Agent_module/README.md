# Agent Module — Agent Data Infra

`Agent_module` is a Python library that gives an LLM agent **a single
knowledge file** to read from. That file — with the `.carbondata` extension
— is a self-contained SQLite database that simultaneously serves the four
data-access patterns agents actually need:

| Pattern | What an agent calls it for |
|---|---|
| **RAG semantic search** | "Find chunks relevant to this question." |
| **Long-term memory** | "What have I learned about this user / session before?" |
| **Structured query** | "Give me all rows where `team = 'ml'`." |
| **Knowledge-graph traversal** | "Which documents does this one reference, two hops out?" |

One file. One Python handle (`CarbonStore`). One schema. No separate vector
DB, KV store, graph DB, or relational DB to wire together.

```
                   ┌─────────────────────────────┐
   agent code ───► │  CarbonStore (kb.carbondata)│
                   │                             │
                   │  search()    recall()       │
                   │  query()     traverse()     │
                   └─────────────────────────────┘
                                 │
                  one SQLite file: entities, chunks,
                  embeddings, relations, memories
```

---

## Layout

```
Agent_module/
├── carbon_data/                   # the library
│   ├── store.py                   # CarbonStore — open/create + all APIs
│   ├── models.py                  # Entity / Chunk / Memory / Relation / ...
│   ├── schema.py                  # SQLite DDL + version pragmas
│   ├── chunkers/                  # by_tokens / by_paragraph / by_sentence
│   ├── embedders/                 # Embedder protocol + helpers
│   └── index/                     # vector_brute + vector_hnsw
├── QUICKSTART.md                  # full hands-on guide (read this next)
├── examples/
│   └── carbondata_quickstart.py   # end-to-end runnable demo
└── tests/carbon_data/             # 249 tests across M1–M9
```

---

## Requirements

- Python 3.10+
- `numpy` (required)
- `hnswlib` (optional — enables ANN acceleration when corpus > ~10k chunks;
  falls back to brute force when missing)
- `pytest` (optional — for running the test suite)

```bash
pip install numpy
pip install hnswlib   # optional but recommended
```

---

## Your first demo in 5 minutes

The fastest path is to run the bundled end-to-end example, then read
[QUICKSTART.md](QUICKSTART.md) to understand each step.

### Step 1 — run the bundled demo

From the repo root:

```bash
python3 Agent_module/examples/carbondata_quickstart.py
```

This script walks through all four scenarios on a tiny in-memory corpus
about Python web frameworks, ML libraries, and recipes. It uses a
hand-rolled vocabulary embedder so you don't need any external model to see
real top-k ranking. Expected output is a series of labelled sections
showing search hits, recalled memories, structured-query rows, and a graph
traversal.

### Step 2 — write your own minimal demo

Once the bundled example runs, paste this into `my_first_demo.py` and run
it from the repo root with `python3 my_first_demo.py`:

```python
import numpy as np
from Agent_module.carbon_data import LambdaEmbedder, create

# 1) An embedder. In a real app this wraps OpenAI / sentence-transformers /
#    a local model. Here we use random vectors just to make the API run.
def encode(texts):
    return np.random.randn(len(texts), 384).astype(np.float32)

embedder = LambdaEmbedder(encode, model="demo-model", dim=384)

# 2) Create a knowledge file.
store = create("kb.carbondata", exist_ok=True)

# 3) Ingest some text — chunked by paragraph, embedded, indexed for FTS.
store.ingest_text(
    "Django is a Python web framework.\n\n"
    "PyTorch is a deep-learning library.\n\n"
    "Sourdough needs a long fermentation.",
    id="doc-1",
    embedder=embedder,
)

# 4) Query in three modes.
print("vector :", store.search("python web", embedder=embedder, top_k=2))
print("keyword:", store.search("python", mode="keyword", top_k=2))
print("hybrid :", store.search("python", mode="hybrid", embedder=embedder, top_k=2))

# 5) Write and read a memory tied to a session.
store.remember("user prefers Vim", session_id="s1", embedder=embedder)
print("recall :", store.recall("editor", session_id="s1", embedder=embedder))

# 6) Add a relation and walk the graph.
store.put_entity(id="doc-2", kind="document", content="Flask")
store.add_relation("doc-1", "doc-2", "references")
print("subgraph:", store.subgraph(["doc-1"], max_hops=2))

store.close()
```

You now have a working `kb.carbondata` file on disk. Open it again in
another script with `carbon_data.open("kb.carbondata")` — all your
entities, embeddings, relations, and memories will still be there.

### Step 3 — go deeper

[**QUICKSTART.md**](QUICKSTART.md) covers each scenario in detail:
custom chunkers, namespace isolation, transactions, HNSW dispatch policy,
admin operations (`validate`, `compact`, `export`, `stats`), and what is
deliberately **out of scope** for v1.

For the full API surface, read the `class CarbonStore` docstring in
[`carbon_data/store.py`](carbon_data/store.py).

---

## Running the tests

```bash
pytest Agent_module/tests/ -v
```

Tests are split by milestone (M1 schema → M9 admin); each file is a
self-contained walkthrough of one feature area and doubles as readable
documentation.

---

## Debugging tip

A `.carbondata` file is just SQLite. You can inspect it with any SQLite
tool at any time:

```bash
sqlite3 kb.carbondata
sqlite> .tables
sqlite> SELECT id, kind, namespace FROM entity LIMIT 10;
```
