"""M6: knowledge graph — relations, neighbors, traverse, subgraph."""
from __future__ import annotations

from pathlib import Path

import pytest

from Agent_module.carbon_data import (
    CarbonStore,
    Neighbor,
    Relation,
    Subgraph,
    TraversalHit,
    create,
)


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


def _node(s: CarbonStore, id: str, *, kind="document", namespace="default") -> str:
    return s.put_entity(id=id, kind=kind, namespace=namespace)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

class TestRelationCRUD:
    def test_add_and_get(self, store: CarbonStore) -> None:
        _node(store, "a")
        _node(store, "b")
        store.add_relation("a", "b", "references", weight=0.7,
                           metadata={"reason": "cite"})
        r = store.get_relation("a", "b", "references")
        assert isinstance(r, Relation)
        assert r.weight == 0.7
        assert r.metadata == {"reason": "cite"}

    def test_missing_endpoints_raise(self, store: CarbonStore) -> None:
        _node(store, "a")
        with pytest.raises(KeyError, match="dst entity not found"):
            store.add_relation("a", "ghost", "references")
        with pytest.raises(KeyError, match="src entity not found"):
            store.add_relation("ghost", "a", "references")

    def test_upsert_replaces_weight_and_metadata(self, store: CarbonStore) -> None:
        _node(store, "a")
        _node(store, "b")
        store.add_relation("a", "b", "references", weight=0.5)
        store.add_relation("a", "b", "references", weight=0.9,
                           metadata={"updated": True})
        r = store.get_relation("a", "b", "references")
        assert r.weight == 0.9
        assert r.metadata == {"updated": True}

    def test_kind_is_part_of_pk(self, store: CarbonStore) -> None:
        _node(store, "a")
        _node(store, "b")
        store.add_relation("a", "b", "references")
        store.add_relation("a", "b", "supersedes")
        # Two distinct edges between the same endpoints with different kinds.
        assert len(store.list_relations(src="a", dst="b")) == 2

    def test_delete(self, store: CarbonStore) -> None:
        _node(store, "a")
        _node(store, "b")
        store.add_relation("a", "b", "references")
        assert store.delete_relation("a", "b", "references") is True
        assert store.delete_relation("a", "b", "references") is False
        assert store.get_relation("a", "b", "references") is None

    def test_list_filters(self, store: CarbonStore) -> None:
        for n in ("a", "b", "c"):
            _node(store, n)
        store.add_relation("a", "b", "references")
        store.add_relation("a", "c", "references")
        store.add_relation("b", "c", "supersedes")

        assert len(store.list_relations(src="a")) == 2
        assert len(store.list_relations(kind="supersedes")) == 1
        assert len(store.list_relations(dst="c")) == 2
        assert len(store.list_relations(src="a", kind="references")) == 2

    def test_list_limit(self, store: CarbonStore) -> None:
        for n in ("a", "b", "c", "d"):
            _node(store, n)
        for d in ("b", "c", "d"):
            store.add_relation("a", d, "ref")
        rels = store.list_relations(src="a", limit=2)
        assert len(rels) == 2

    def test_entity_delete_cascades_relations(self, store: CarbonStore) -> None:
        _node(store, "a")
        _node(store, "b")
        store.add_relation("a", "b", "references")
        store.delete_entity("b")
        assert store.list_relations() == []


# ---------------------------------------------------------------------------
# neighbors (1-hop)
# ---------------------------------------------------------------------------

class TestNeighbors:
    def _build_star(self, s: CarbonStore) -> None:
        # a -> b, a -> c (out of a)
        # d -> a            (in to a)
        for n in ("a", "b", "c", "d"):
            _node(s, n)
        s.add_relation("a", "b", "references")
        s.add_relation("a", "c", "references", weight=0.5)
        s.add_relation("d", "a", "supersedes")

    def test_out(self, store: CarbonStore) -> None:
        self._build_star(store)
        ns = store.neighbors("a", direction="out")
        assert all(isinstance(n, Neighbor) for n in ns)
        assert {n.entity.id for n in ns} == {"b", "c"}
        assert all(n.direction == "out" for n in ns)

    def test_in(self, store: CarbonStore) -> None:
        self._build_star(store)
        ns = store.neighbors("a", direction="in")
        assert {n.entity.id for n in ns} == {"d"}
        assert all(n.direction == "in" for n in ns)

    def test_both(self, store: CarbonStore) -> None:
        self._build_star(store)
        ns = store.neighbors("a", direction="both")
        assert {n.entity.id for n in ns} == {"b", "c", "d"}
        per_dir = {n.entity.id: n.direction for n in ns}
        assert per_dir["b"] == "out"
        assert per_dir["d"] == "in"

    def test_kind_filter(self, store: CarbonStore) -> None:
        self._build_star(store)
        ns = store.neighbors("a", direction="both", kind="supersedes")
        assert {n.entity.id for n in ns} == {"d"}

    def test_weight_and_metadata(self, store: CarbonStore) -> None:
        _node(store, "a")
        _node(store, "b")
        store.add_relation("a", "b", "references", weight=0.42,
                           metadata={"src_page": 7})
        ns = store.neighbors("a")
        assert ns[0].weight == 0.42
        assert ns[0].metadata == {"src_page": 7}
        assert ns[0].relation_kind == "references"

    def test_isolated_node(self, store: CarbonStore) -> None:
        _node(store, "lonely")
        assert store.neighbors("lonely") == []

    def test_limit(self, store: CarbonStore) -> None:
        _node(store, "a")
        for d in ("b", "c", "d"):
            _node(store, d)
            store.add_relation("a", d, "ref")
        ns = store.neighbors("a", limit=2)
        assert len(ns) == 2

    def test_invalid_direction(self, store: CarbonStore) -> None:
        _node(store, "a")
        with pytest.raises(ValueError, match="direction"):
            store.neighbors("a", direction="sideways")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# traverse (multi-hop)
# ---------------------------------------------------------------------------

class TestTraverse:
    def _build_chain(self, s: CarbonStore) -> None:
        # a -> b -> c -> d -> e
        for n in ("a", "b", "c", "d", "e"):
            _node(s, n)
        for u, v in [("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")]:
            s.add_relation(u, v, "next")

    def test_max_hops_bounds_walk(self, store: CarbonStore) -> None:
        self._build_chain(store)
        hits = store.traverse("a", max_hops=2, direction="out")
        ids_to_hop = {h.entity.id: h.hop for h in hits}
        assert ids_to_hop == {"b": 1, "c": 2}

    def test_max_hops_three(self, store: CarbonStore) -> None:
        self._build_chain(store)
        hits = store.traverse("a", max_hops=3, direction="out")
        assert {h.entity.id: h.hop for h in hits} == {"b": 1, "c": 2, "d": 3}

    def test_excludes_start(self, store: CarbonStore) -> None:
        self._build_chain(store)
        hits = store.traverse("a", max_hops=5)
        assert "a" not in {h.entity.id for h in hits}

    def test_returns_min_hop(self, store: CarbonStore) -> None:
        # a -> b -> c, a -> c (direct). c reachable at hop 1 and hop 2.
        for n in ("a", "b", "c"):
            _node(store, n)
        store.add_relation("a", "b", "ref")
        store.add_relation("b", "c", "ref")
        store.add_relation("a", "c", "ref")
        hits = store.traverse("a", max_hops=3)
        c_hop = next(h.hop for h in hits if h.entity.id == "c")
        assert c_hop == 1  # picks shortest path

    def test_cycle_safety(self, store: CarbonStore) -> None:
        # a -> b -> a (cycle). Without bounding this would loop forever.
        _node(store, "a")
        _node(store, "b")
        store.add_relation("a", "b", "x")
        store.add_relation("b", "a", "x")
        hits = store.traverse("a", max_hops=5, direction="out")
        # b reachable; a is the start, excluded.
        assert {h.entity.id for h in hits} == {"b"}

    def test_direction_in(self, store: CarbonStore) -> None:
        self._build_chain(store)
        hits = store.traverse("e", max_hops=2, direction="in")
        assert {h.entity.id: h.hop for h in hits} == {"d": 1, "c": 2}

    def test_direction_both(self, store: CarbonStore) -> None:
        # a -> b, c -> b (b has both incoming and outgoing options if both used)
        _node(store, "a")
        _node(store, "b")
        _node(store, "c")
        store.add_relation("a", "b", "x")
        store.add_relation("c", "b", "x")
        hits = store.traverse("b", max_hops=1, direction="both")
        assert {h.entity.id for h in hits} == {"a", "c"}

    def test_kind_filter(self, store: CarbonStore) -> None:
        # a -ref-> b -ref-> c, a -other-> d
        for n in ("a", "b", "c", "d"):
            _node(store, n)
        store.add_relation("a", "b", "ref")
        store.add_relation("b", "c", "ref")
        store.add_relation("a", "d", "other")
        hits = store.traverse("a", kind="ref", max_hops=3)
        assert {h.entity.id for h in hits} == {"b", "c"}

    def test_namespace_filter(self, store: CarbonStore) -> None:
        _node(store, "a", namespace="ns1")
        _node(store, "b", namespace="ns1")
        _node(store, "c", namespace="ns2")
        store.add_relation("a", "b", "x")
        store.add_relation("b", "c", "x")
        hits = store.traverse("a", max_hops=2, namespace="ns1")
        assert {h.entity.id for h in hits} == {"b"}  # c filtered out

    def test_invalid_max_hops(self, store: CarbonStore) -> None:
        _node(store, "a")
        with pytest.raises(ValueError, match="max_hops"):
            store.traverse("a", max_hops=0)

    def test_invalid_direction(self, store: CarbonStore) -> None:
        _node(store, "a")
        with pytest.raises(ValueError, match="direction"):
            store.traverse("a", direction="weird")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# subgraph
# ---------------------------------------------------------------------------

class TestSubgraph:
    def test_includes_seeds_at_hop_zero(self, store: CarbonStore) -> None:
        _node(store, "a")
        _node(store, "b")
        sg = store.subgraph(["a"], max_hops=0)
        assert {e.id for e in sg.entities} == {"a"}
        assert sg.relations == []

    def test_one_hop_collects_neighbors(self, store: CarbonStore) -> None:
        for n in ("a", "b", "c", "d"):
            _node(store, n)
        store.add_relation("a", "b", "x")
        store.add_relation("a", "c", "x")
        store.add_relation("c", "d", "x")
        sg = store.subgraph(["a"], max_hops=1, direction="out")
        assert {e.id for e in sg.entities} == {"a", "b", "c"}
        assert {(r.src, r.dst) for r in sg.relations} == {("a", "b"), ("a", "c")}

    def test_two_hops(self, store: CarbonStore) -> None:
        for n in ("a", "b", "c", "d"):
            _node(store, n)
        store.add_relation("a", "b", "x")
        store.add_relation("b", "c", "x")
        store.add_relation("c", "d", "x")
        sg = store.subgraph(["a"], max_hops=2, direction="out")
        assert {e.id for e in sg.entities} == {"a", "b", "c"}
        assert len(sg.relations) == 2

    def test_relations_only_internal(self, store: CarbonStore) -> None:
        # Subgraph picks up a, b, c (max_hops=1 from a). The edge c -> d
        # has only c inside the visited set; it must NOT appear in the
        # subgraph's relation list.
        for n in ("a", "b", "c", "d"):
            _node(store, n)
        store.add_relation("a", "b", "x")
        store.add_relation("a", "c", "x")
        store.add_relation("c", "d", "x")  # leaks outside
        sg = store.subgraph(["a"], max_hops=1, direction="out")
        assert all(
            r.src in {"a", "b", "c"} and r.dst in {"a", "b", "c"}
            for r in sg.relations
        )

    def test_multi_seed(self, store: CarbonStore) -> None:
        # a -> b ; c -> d. Two disjoint seeds, two disjoint subgraphs.
        for n in ("a", "b", "c", "d"):
            _node(store, n)
        store.add_relation("a", "b", "x")
        store.add_relation("c", "d", "x")
        sg = store.subgraph(["a", "c"], max_hops=1, direction="out")
        assert {e.id for e in sg.entities} == {"a", "b", "c", "d"}
        assert {(r.src, r.dst) for r in sg.relations} == {
            ("a", "b"), ("c", "d"),
        }

    def test_empty_seeds(self, store: CarbonStore) -> None:
        sg = store.subgraph([])
        assert isinstance(sg, Subgraph)
        assert sg.entities == []
        assert sg.relations == []

    def test_kind_filter(self, store: CarbonStore) -> None:
        for n in ("a", "b", "c"):
            _node(store, n)
        store.add_relation("a", "b", "ref")
        store.add_relation("a", "c", "other")
        sg = store.subgraph(["a"], kind="ref", max_hops=1, direction="out")
        # Only "ref" walked → b reachable; c not.
        assert {e.id for e in sg.entities} == {"a", "b"}
        assert all(r.kind == "ref" for r in sg.relations)

    def test_direction_in_only(self, store: CarbonStore) -> None:
        for n in ("a", "b", "c"):
            _node(store, n)
        store.add_relation("a", "b", "x")
        store.add_relation("c", "b", "x")
        sg = store.subgraph(["b"], direction="in", max_hops=1)
        assert {e.id for e in sg.entities} == {"a", "b", "c"}
