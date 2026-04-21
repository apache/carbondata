"""
04_context_store.py
===================
Tests every major feature of ContextStore and ContextNamespace.

Run:
    python3 Agent_module/examples/04_context_store.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from Agent_module import ContextNamespace, ContextRecord, ContextStore


_passed = 0
_failed = 0


def check(label: str, condition: bool) -> None:
    global _passed, _failed
    if condition:
        _passed += 1
        print(f"  PASS  {label}")
    else:
        _failed += 1
        print(f"  FAIL  {label}")


def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


async def test_basic_put_get() -> None:
    section("Basic put / get / delete")

    store = ContextStore()

    rec = await store.put("color", "red", writer="alice")
    check("put returns a ContextRecord", isinstance(rec, ContextRecord))
    check("record key is correct", rec.key == "color")
    check("record value is correct", rec.value == "red")
    check("record writer is correct", rec.writer == "alice")
    check("first write gets version 1", rec.version == 1)
    check("record_id is a non-empty string", len(rec.record_id) > 0)

    check("get returns current value", await store.get("color") == "red")

    rec2 = await store.put("color", "blue", writer="bob")
    check("second write gets version 2", rec2.version == 2)
    check("get returns updated value", await store.get("color") == "blue")

    check("get missing key returns None", await store.get("__missing__") is None)

    deleted = await store.delete("color", writer="alice")
    check("delete returns True for existing key", deleted)
    check("get after delete returns None", await store.get("color") is None)
    check("delete returns False for missing key", not await store.delete("__missing__", writer="alice"))


async def test_history() -> None:
    section("History")

    store = ContextStore()
    await store.put("status", "pending", writer="coordinator")
    await store.put("status", "in_progress", writer="worker")
    await store.put("status", "completed", writer="worker")

    history = await store.history("status")
    check("history returns 3 records", len(history) == 3)
    check("history is newest-first", history[0].value == "completed")
    check("oldest record is pending", history[-1].value == "pending")

    limited = await store.history("status", limit=2)
    check("limit=2 returns 2 records", len(limited) == 2)
    check("limit respects newest-first order", limited[0].value == "completed")

    check("history for missing key is []", await store.history("__missing__") == [])


async def test_keys_and_query() -> None:
    section("keys() and query()")

    store = ContextStore()
    ns = "test_ns"

    await store.put("a", 1, writer="alice", namespace=ns, tags=["x"])
    await store.put("b", 2, writer="bob", namespace=ns, tags=["x", "y"])
    await store.put("c", 3, writer="alice", namespace=ns, tags=["y"])

    check("keys() returns all 3 keys", set(await store.keys(namespace=ns)) == {"a", "b", "c"})

    alice_entries = await store.query(namespace=ns, writer="alice")
    check("query(writer='alice') returns 2 entries", len(alice_entries) == 2)
    check("all alice entries have writer='alice'",
          all(e.current.writer == "alice" for e in alice_entries))

    tag_x = await store.query(namespace=ns, tags=["x"])
    check("query(tags=['x']) returns 2 entries", len(tag_x) == 2)

    tag_xy = await store.query(namespace=ns, tags=["x", "y"])
    check("query(tags=['x','y']) returns 1 entry", len(tag_xy) == 1)
    check("that entry is 'b'", tag_xy[0].current.key == "b")

    check("query with no match returns []", await store.query(namespace=ns, writer="nobody") == [])


async def test_namespace_isolation() -> None:
    section("Namespace isolation")

    store = ContextStore()
    await store.put("key", "ns1-value", writer="agent", namespace="ns1")
    await store.put("key", "ns2-value", writer="agent", namespace="ns2")

    check("ns1 reads its own value", await store.get("key", namespace="ns1") == "ns1-value")
    check("ns2 reads its own value", await store.get("key", namespace="ns2") == "ns2-value")
    check("default ns is empty", await store.get("key") is None)
    check("ns1 has exactly 1 key", await store.keys(namespace="ns1") == ["key"])
    check("ns2 has exactly 1 key", await store.keys(namespace="ns2") == ["key"])


async def test_context_namespace_proxy() -> None:
    section("ContextNamespace proxy")

    store = ContextStore()
    ns = ContextNamespace(store, "proxy_ns")

    check("namespace property correct", ns.namespace == "proxy_ns")

    await ns.put("x", 42, writer="agent")
    check("ns.put writes to correct namespace", await ns.get("x") == 42)
    check("direct store read matches", await store.get("x", "proxy_ns") == 42)

    await ns.delete("x", writer="agent")
    check("ns.delete removes the key", await ns.get("x") is None)

    await ns.put("h", "v1", writer="a")
    await ns.put("h", "v2", writer="b")
    check("ns.history returns 2 records", len(await ns.history("h")) == 2)
    check("ns.keys() returns ['h']", await ns.keys() == ["h"])


async def test_channels() -> None:
    section("P2P Channels — send / receive / peek / inbox")

    store = ContextStore()
    m1 = await store.send("coordinator", "worker", {"cmd": "run"})
    m2 = await store.send("coordinator", "worker", {"cmd": "stop"})
    check("send returns a ContextMessage", m1.sender == "coordinator")
    check("message_id is unique", m1.message_id != m2.message_id)

    ch = store.channel("coordinator", "worker")
    check("channel depth is 2", len(ch) == 2)

    peeked = await ch.peek()
    check("peek returns first message", peeked.payload == {"cmd": "run"})
    check("peek does not consume (depth still 2)", len(ch) == 2)

    r = await store.receive("coordinator", "worker")
    check("receive returns oldest message (FIFO)", r.payload == {"cmd": "run"})
    check("channel depth is now 1", len(ch) == 1)

    await store.send("auditor", "worker", {"cmd": "check"})
    inbox = await store.inbox("worker")
    check("inbox collects from all senders", len(inbox) == 2)
    check("inbox sorted oldest-first by timestamp", inbox[0].payload == {"cmd": "stop"})
    check("inbox is destructive (channel empty)", len(store.channel("coordinator", "worker")) == 0)

    check("receive on empty channel returns None", await store.receive("coordinator", "worker") is None)

    tagged = await store.send("a", "b", "payload", tags=["urgent"])
    check("message tags preserved", "urgent" in tagged.tags)


async def test_pubsub() -> None:
    section("Pub/Sub")

    store = ContextStore()
    ns = ContextNamespace(store, "pubsub_ns")
    log: list = []

    async def async_cb(key: str, record: ContextRecord) -> None:
        log.append(("async", key, record.value, record.writer))

    def sync_cb(key: str, record: ContextRecord) -> None:
        log.append(("sync", key, record.value, record.writer))

    ns.subscribe("monitor", async_cb)
    ns.subscribe("logger", sync_cb)

    await ns.put("status", "started", writer="coordinator")
    check("async callback fired", any(e[0] == "async" for e in log))
    check("sync callback fired", any(e[0] == "sync" for e in log))
    check("both callbacks fired (2 entries)", len(log) == 2)
    check("correct key delivered", all(e[1] == "status" for e in log))
    check("correct value delivered", all(e[2] == "started" for e in log))

    log.clear()
    ns.subscribe("coordinator", lambda k, r: log.append("self-notified"))
    await ns.put("status", "done", writer="coordinator")
    check("writer not notified of own write", "self-notified" not in log)
    check("other subscribers still notified", len(log) == 2)

    log.clear()
    ns.unsubscribe("monitor")
    ns.unsubscribe("logger")
    ns.unsubscribe("coordinator")

    ns.subscribe("watcher", lambda k, r: log.append(k), keys=["important"])
    await ns.put("important", "yes", writer="a")
    await ns.put("unimportant", "skip", writer="a")
    check("key filter: watched key triggers callback", "important" in log)
    check("key filter: unwatched key does not trigger", "unimportant" not in log)

    log.clear()
    ns.unsubscribe("watcher")
    await ns.put("important", "again", writer="a")
    check("after unsubscribe, no callback fires", log == [])


async def test_ttl() -> None:
    section("TTL expiry")

    store = ContextStore()
    ns = ContextNamespace(store, "ttl_ns")

    await ns.put("short", "alive", writer="agent", ttl=1)
    check("immediate read returns value", await ns.get("short") == "alive")

    await asyncio.sleep(1.2)
    check("read after TTL returns None", await ns.get("short") is None)

    await ns.put("a", 1, writer="x", ttl=1)
    await ns.put("b", 2, writer="x", ttl=1)
    await ns.put("c", 3, writer="x")

    await asyncio.sleep(1.2)
    removed = await ns.cleanup_expired()
    check("cleanup_expired removed 2 keys", removed == 2)
    check("non-expiring key 'c' still readable", await ns.get("c") == 3)


async def test_merge() -> None:
    section("Merge — last-write-wins by version")

    store_a = ContextStore()
    store_b = ContextStore()
    ns = "merge_ns"

    await store_a.put("status", "v1", writer="a", namespace=ns)
    await store_a.put("status", "v2", writer="a", namespace=ns)

    await store_b.put("status", "b-v1", writer="b", namespace=ns)
    await store_b.put("extra", "only-in-b", writer="b", namespace=ns)

    merged = await store_a.merge(store_b, namespace=ns)
    check("merge returns count of accepted entries", merged >= 1)
    check("higher-version local value retained", await store_a.get("status", ns) == "v2")
    check("new key from b accepted", await store_a.get("extra", ns) == "only-in-b")

    await store_b.merge(store_a, namespace=ns)
    check("higher-version remote value wins on merge", await store_b.get("status", ns) == "v2")


async def test_snapshot_and_serialisation() -> None:
    section("Snapshot and serialisation")

    store = ContextStore()
    ns = ContextNamespace(store, "serial_ns")

    await ns.put("k1", "v1", writer="a", tags=["t1"])
    await ns.put("k2", {"nested": True}, writer="b")
    await store.send("a", "b", {"cmd": "go"})

    snap = await ns.snapshot()
    check("snapshot has k1 and k2", set(snap.keys()) == {"k1", "k2"})
    check("snapshot is a deep copy", snap["k1"]["current"]["value"] == "v1")

    json_str = await store.to_json()
    parsed = json.loads(json_str)

    check("to_json produces valid JSON",
          "entries" in parsed and "channels" in parsed and "subscriptions" in parsed)
    check("namespace present in JSON", "serial_ns" in parsed["entries"])
    check("k1 present in JSON entries", "k1" in parsed["entries"]["serial_ns"])
    check("channel captured in JSON", "a->b" in parsed["channels"])

    stats = await store.stats()
    check("stats has namespaces key", "namespaces" in stats)
    check("stats has total_channels key", "total_channels" in stats)
    check("serial_ns appears in stats", "serial_ns" in stats["namespaces"])
    check("total_channels >= 1", stats["total_channels"] >= 1)


async def _run_all() -> bool:
    global _passed, _failed
    _passed = _failed = 0

    await test_basic_put_get()
    await test_history()
    await test_keys_and_query()
    await test_namespace_isolation()
    await test_context_namespace_proxy()
    await test_channels()
    await test_pubsub()
    await test_ttl()
    await test_merge()
    await test_snapshot_and_serialisation()

    total = _passed + _failed
    print(f"\n{'=' * 60}")
    print(f"  Result: {_passed}/{total} passed", "✓" if _failed == 0 else "✗")
    print(f"{'=' * 60}")
    return _failed == 0


def run() -> bool:
    print("\n" + "=" * 60)
    print("  04 — ContextStore")
    print("=" * 60)
    return asyncio.run(_run_all())


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
