"""M8: chunkers + ingest_text + ingest_table."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from Agent_module.carbon_data import (
    CarbonStore,
    LambdaEmbedder,
    by_paragraph,
    by_sentence,
    by_tokens,
    create,
)


@pytest.fixture
def store(tmp_path: Path):
    p = tmp_path / "kb.carbondata"
    s = create(p)
    yield s
    if not s.closed:
        s.close()


# ---------------------------------------------------------------------------
# chunker primitives
# ---------------------------------------------------------------------------

class TestByTokens:
    def test_basic_window(self) -> None:
        chunk = by_tokens(max_tokens=3, overlap=0)
        out = chunk("a b c d e f g")
        assert out == ["a b c", "d e f", "g"]

    def test_overlap(self) -> None:
        chunk = by_tokens(max_tokens=3, overlap=1)
        out = chunk("a b c d e")
        # step = 2; windows: [a b c], [c d e]
        assert out == ["a b c", "c d e"]

    def test_empty_input(self) -> None:
        assert by_tokens(max_tokens=10)("") == []
        assert by_tokens(max_tokens=10)("   \n  ") == []

    def test_single_token(self) -> None:
        assert by_tokens(max_tokens=10)("solo") == ["solo"]

    def test_validation(self) -> None:
        with pytest.raises(ValueError, match="max_tokens"):
            by_tokens(max_tokens=0)
        with pytest.raises(ValueError, match="overlap must be >= 0"):
            by_tokens(max_tokens=10, overlap=-1)
        with pytest.raises(ValueError, match="overlap must be < max_tokens"):
            by_tokens(max_tokens=10, overlap=10)

    def test_no_double_terminal_chunk(self) -> None:
        # Boundary case: when N is a multiple of step, must not emit empty tail.
        chunk = by_tokens(max_tokens=2, overlap=0)
        out = chunk("a b c d")
        assert out == ["a b", "c d"]


class TestByParagraph:
    def test_blank_line_split(self) -> None:
        text = "Para one.\n\nPara two.\n\n\nPara three."
        out = by_paragraph()(text)
        assert out == ["Para one.", "Para two.", "Para three."]

    def test_min_chars(self) -> None:
        text = "short\n\nlonger paragraph"
        out = by_paragraph(min_chars=10)(text)
        assert out == ["longer paragraph"]

    def test_no_paragraphs(self) -> None:
        assert by_paragraph()("") == []
        assert by_paragraph()("just one line") == ["just one line"]


class TestBySentence:
    def test_split_on_punctuation(self) -> None:
        out = by_sentence()(
            "Hello world. This is a test! Is it working? Yes."
        )
        assert out == [
            "Hello world.",
            "This is a test!",
            "Is it working?",
            "Yes.",
        ]

    def test_lowercase_continuation_kept_together(self) -> None:
        # The naive splitter requires whitespace+capital; "1.5 inches" stays
        # in one sentence.
        out = by_sentence()("It is 1.5 inches long.")
        assert out == ["It is 1.5 inches long."]


# ---------------------------------------------------------------------------
# ingest_text
# ---------------------------------------------------------------------------

class TestIngestText:
    def test_default_chunker_paragraphs(self, store: CarbonStore) -> None:
        eid = store.ingest_text("Para A.\n\nPara B.\n\nPara C.")
        chunks = store.list_chunks(eid)
        assert [c.content for c in chunks] == ["Para A.", "Para B.", "Para C."]

    def test_returns_id(self, store: CarbonStore) -> None:
        eid = store.ingest_text("hi", id="doc-1")
        assert eid == "doc-1"
        assert store.get_entity("doc-1") is not None

    def test_auto_id(self, store: CarbonStore) -> None:
        eid = store.ingest_text("hi")
        assert isinstance(eid, str) and len(eid) >= 16

    def test_entity_carries_full_text_and_metadata(self, store: CarbonStore) -> None:
        eid = store.ingest_text(
            "Para A.\n\nPara B.",
            id="d", metadata={"author": "x"}, namespace="ns1",
        )
        ent = store.get_entity(eid)
        assert ent.content == "Para A.\n\nPara B."
        assert ent.metadata == {"author": "x"}
        assert ent.namespace == "ns1"
        assert ent.kind == "document"

    def test_custom_chunker(self, store: CarbonStore) -> None:
        eid = store.ingest_text(
            "a b c d e f", chunker=by_tokens(max_tokens=2, overlap=0)
        )
        assert [c.content for c in store.list_chunks(eid)] == ["a b", "c d", "e f"]

    def test_falls_back_to_whole_text_when_chunker_empty(
        self, store: CarbonStore
    ) -> None:
        # by_paragraph yields nothing on a single line; fallback should
        # store the whole text as one chunk so the entity isn't empty.
        eid = store.ingest_text("just one sentence with no breaks")
        assert [c.content for c in store.list_chunks(eid)] == [
            "just one sentence with no breaks"
        ]

    def test_pure_whitespace_yields_no_chunks(self, store: CarbonStore) -> None:
        eid = store.ingest_text("   \n   ")
        # Entity exists; chunks is empty (no usable content to index).
        assert store.get_entity(eid) is not None
        assert store.list_chunks(eid) == []

    def test_with_embedder(self, store: CarbonStore) -> None:
        def _fn(texts):
            return np.ones((len(texts), 4), dtype=np.float32)
        emb = LambdaEmbedder(_fn, model="m8", dim=4)

        eid = store.ingest_text(
            "Hello.\n\nWorld.", embedder=emb,
        )
        chunks = store.list_chunks(eid)
        assert len(chunks) == 2
        for c in chunks:
            assert store.get_embedding(c.id, model="m8") is not None

    def test_atomic_rollback(self, store: CarbonStore) -> None:
        class BoomEmbedder:
            model = "boom"
            dim = 3

            def encode(self, texts):
                raise RuntimeError("boom")

        before = store.stats()
        with pytest.raises(RuntimeError, match="boom"):
            store.ingest_text("a\n\nb", embedder=BoomEmbedder())
        after = store.stats()
        assert after["entities"] == before["entities"]
        assert after["chunks"] == before["chunks"]


# ---------------------------------------------------------------------------
# ingest_table
# ---------------------------------------------------------------------------

class TestIngestTable:
    def test_list_of_dicts(self, store: CarbonStore) -> None:
        rows = [
            {"id": "u1", "name": "Alice", "team": "infra"},
            {"id": "u2", "name": "Bob", "team": "ml"},
        ]
        ids = store.ingest_table(rows, table_name="users", id_column="id")
        assert ids == ["u1", "u2"]

        e = store.get_entity("u1")
        assert e.kind == "table_row"
        assert e.metadata["table"] == "users"
        assert e.metadata["columns"] == rows[0]
        # Rendered chunk should embed the row content for FTS / vector retrieval.
        chunks = store.list_chunks("u1")
        assert len(chunks) == 1
        assert "Alice" in chunks[0].content
        assert "infra" in chunks[0].content

    def test_auto_uuid_when_no_id_column(self, store: CarbonStore) -> None:
        rows = [{"name": "Alice"}, {"name": "Bob"}]
        ids = store.ingest_table(rows, table_name="users")
        assert len(ids) == 2
        assert all(len(i) >= 16 for i in ids)

    def test_id_column_missing_falls_back_to_uuid(
        self, store: CarbonStore
    ) -> None:
        # First row has id, second row doesn't — second should auto-uuid
        # rather than error or skip.
        rows = [{"id": "u1", "x": 1}, {"x": 2}]
        ids = store.ingest_table(rows, table_name="t", id_column="id")
        assert ids[0] == "u1"
        assert ids[1] != "u1" and len(ids[1]) >= 16

    def test_csv_file(self, tmp_path: Path, store: CarbonStore) -> None:
        csv_path = tmp_path / "users.csv"
        csv_path.write_text(
            "id,name,team\nu1,Alice,infra\nu2,Bob,ml\n",
            encoding="utf-8",
        )
        ids = store.ingest_table(csv_path, table_name="users", id_column="id")
        assert ids == ["u1", "u2"]
        e = store.get_entity("u1")
        assert e.metadata["columns"] == {"id": "u1", "name": "Alice", "team": "infra"}

    def test_csv_path_as_string(self, tmp_path: Path, store: CarbonStore) -> None:
        csv_path = tmp_path / "users.csv"
        csv_path.write_text("id,name\nu1,Alice\n", encoding="utf-8")
        ids = store.ingest_table(str(csv_path), table_name="users", id_column="id")
        assert ids == ["u1"]

    def test_empty_rows(self, store: CarbonStore) -> None:
        assert store.ingest_table([], table_name="t") == []

    def test_with_embedder(self, store: CarbonStore) -> None:
        rows = [{"id": "u1", "x": "alpha"}, {"id": "u2", "x": "beta"}]

        def _fn(texts):
            return np.ones((len(texts), 4), dtype=np.float32)
        emb = LambdaEmbedder(_fn, model="m8t", dim=4)

        ids = store.ingest_table(
            rows, table_name="t", id_column="id", embedder=emb
        )
        for eid in ids:
            chunks = store.list_chunks(eid)
            assert store.get_embedding(chunks[0].id, model="m8t") is not None

    def test_namespace_passes_through(self, store: CarbonStore) -> None:
        store.ingest_table(
            [{"id": "u1", "x": 1}], table_name="t", id_column="id",
            namespace="tenant-a",
        )
        assert store.get_entity("u1").namespace == "tenant-a"

    def test_atomic_rollback(self, store: CarbonStore) -> None:
        class BoomEmbedder:
            model = "boom"
            dim = 3

            def encode(self, texts):
                raise RuntimeError("boom")

        before = store.stats()
        with pytest.raises(RuntimeError, match="boom"):
            store.ingest_table(
                [{"id": "u1", "x": 1}], table_name="t", id_column="id",
                embedder=BoomEmbedder(),
            )
        after = store.stats()
        assert after["entities"] == before["entities"]
        assert after["chunks"] == before["chunks"]
