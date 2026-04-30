# Agent Module

Two cooperating modules in one package:

- **`carbon_data/`** — **Agent Data Infra**. A self-contained `.carbondata`
  file format serving four query modes (RAG / long-term memory / structured
  query / graph traversal) over one shared data model. **This is the primary
  product.** See [QUICKSTART.md](QUICKSTART.md) and [docs/DESIGN.md](docs/DESIGN.md).
- **`framework.py` / `manager.py` / `agents.py` / `context_store.py`** — a
  multi-agent runtime with async context store. Retained for in-memory
  agent orchestration; complementary to (not replaced by) carbon_data.

**Requirements:** Python 3.10+ • `numpy` • optional: `hnswlib` (ANN search),
`pytest` (tests).

---

## Layout

```
Agent_module/
├── carbon_data/                   # Agent Data Infra (primary product)
│   ├── store.py                   # CarbonStore: open/create + all APIs
│   ├── models.py                  # Entity / Chunk / Memory / Relation / ...
│   ├── schema.py                  # SQLite DDL + version pragmas
│   ├── chunkers/                  # by_tokens / by_paragraph / by_sentence
│   ├── embedders/                 # Embedder protocol + helpers
│   └── index/                     # vector_brute + vector_hnsw
├── framework.py                   # BaseAgent, AgentMessage, AgentStatus
├── manager.py                     # AgentManager — registry + routing
├── agents.py                      # Built-in demo agents
├── safe_eval.py                   # AST-based arithmetic evaluator
├── context_store.py               # Async in-memory context + pub/sub
├── docs/DESIGN.md                 # carbon_data design doc
├── QUICKSTART.md                  # carbon_data 上手指南
├── examples/
│   └── carbondata_quickstart.py   # End-to-end carbon_data demo
└── tests/carbon_data/             # 249 tests across M1–M9
```

---

## carbon_data — Agent Data Infra

A `.carbondata` file is a SQLite database with carbondata schema markers; one
file holds entities, chunks, embeddings, relations, and memories — and serves
all of them through a single Python handle.

**30-second tour:**

```python
import numpy as np
from Agent_module.carbon_data import LambdaEmbedder, create

def encode(texts):
    return np.random.randn(len(texts), 384).astype(np.float32)

embedder = LambdaEmbedder(encode, model="my-model", dim=384)

store = create("kb.carbondata")
store.ingest_text("para 1.\n\npara 2.\n\npara 3.", id="doc-1", embedder=embedder)

hits = store.search("para 2", embedder=embedder, top_k=2)        # vector RAG
hits = store.search("para",   mode="keyword")                    # FTS5 BM25
hits = store.search("para",   mode="hybrid", embedder=embedder)  # RRF fusion

store.remember("user prefers vim", session_id="s1", embedder=embedder)
mems = store.recall("editor preferences", session_id="s1", embedder=embedder)

store.add_relation("doc-1", "doc-2", "references")
sg = store.subgraph(["doc-1"], max_hops=2)

store.close()
```

Full API walkthrough: [QUICKSTART.md](QUICKSTART.md). Design rationale and
nine-milestone breakdown: [docs/DESIGN.md](docs/DESIGN.md). Runnable end-to-end
demo: `examples/carbondata_quickstart.py`.

---

## Multi-agent runtime

Pre-carbon_data scaffolding; still works for agent orchestration, kept as a
companion to carbon_data (long-term knowledge) for the in-memory side
(transient agent state, P2P channels, pub/sub).

```python
import asyncio
from Agent_module import AgentManager, CalculatorAgent, ChatAgent

async def main():
    manager = AgentManager()
    manager.register_agent(CalculatorAgent())
    manager.register_agent(ChatAgent())

    print((await manager.send_message("CalculatorAgent", "(15+25)*2")).content)
    responses = await manager.broadcast_message("Hello!")
    for name, r in responses.items():
        print(name, "→", r.content[:60])

asyncio.run(main())
```

Custom agents subclass `BaseAgent` and implement `async _process_impl`.
Shared mutable state goes through `ContextStore` / `ContextNamespace`
(async-first, write-history, TTL, P2P channels, pub/sub).

| Module | Public surface |
|---|---|
| `framework.py` | `AgentStatus`, `AgentMessage`, `BaseAgent` |
| `manager.py` | `AgentManager.{register_agent, send_message, broadcast_message, find_agent_by_capability, get_system_stats}` |
| `agents.py` | `EchoAgent`, `CalculatorAgent`, `WeatherAgent`, `TranslatorAgent`, `ChatAgent` |
| `context_store.py` | `ContextStore`, `ContextNamespace`, `ContextRecord` |

---

## Running

From the repo root:

```bash
# carbon_data end-to-end demo (heavily commented)
python3 Agent_module/examples/carbondata_quickstart.py

# Test suite — 249 tests across M1–M9
pytest Agent_module/tests/ -v
```

---

## Notes on security

`CalculatorAgent` uses `safe_eval.py`, which parses input with `ast.parse`
and only accepts arithmetic nodes — attribute access, function calls,
names, comprehensions, and comparisons are rejected *before* any code
runs. It is safe to feed untrusted input.
