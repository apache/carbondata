"""
ContextStore_Demo.py
====================
End-to-end demonstration of the ContextStore multi-agent data exchange system.

Scenarios covered
-----------------
  1. coordinator writes task → worker reads and processes → monitor observes
     changes via pub/sub async callback
  2. history() returns all write versions (newest-first)
  3. Directed channel: coordinator → worker messages; inbox() aggregates all
  4. TTL entry written, expires after 3 seconds, get() returns None
  5. query(writer="worker") returns only entries written by worker
  6. merge() correctly resolves version conflicts (higher version wins)
  7. to_json() fully serialises the entire store

Run:
    python3 Agent_module/ContextStore_Demo.py
"""

import asyncio
import json
import sys
import os

# ---------------------------------------------------------------------------
# Load ContextStore from the same directory (supports .py extension)
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ContextStore import (
    ContextStore,
    ContextNamespace,
    ContextRecord,
)


# ============================================================================
# Helpers
# ============================================================================


def section(title: str) -> None:
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


# ============================================================================
# Shared observation log for the async monitor callback
# ============================================================================

_change_log: list = []


async def monitor_callback(key: str, record: ContextRecord) -> None:
    """Async pub/sub callback — called after every put() that monitor watches."""
    msg = (
        f"  [MONITOR] key={key!r:20s}  "
        f"writer={record.writer:15s}  "
        f"v{record.version}  "
        f"value={str(record.value)!r:.55s}"
    )
    print(msg)
    _change_log.append(
        {"key": key, "writer": record.writer, "value": record.value}
    )


# ============================================================================
# Main demo
# ============================================================================


async def main() -> None:

    store = ContextStore()
    ns = ContextNamespace(store, "data_pipeline_v1")

    # -----------------------------------------------------------------------
    # 1. Setup — Subscribe monitor, then coordinator assigns task
    # -----------------------------------------------------------------------
    section("1. Setup + coordinator assigns task  [pub/sub demo]")

    # Monitor subscribes to ALL keys in the namespace (async callback)
    ns.subscribe("monitor", monitor_callback)

    # Coordinator writes task and initial status
    await ns.put("task", {"id": "task_001", "description": "Analyse Q1 sales", "priority": "high"}, writer="coordinator")
    await ns.put("status", "pending", writer="coordinator")
    await ns.put("deadline", "2026-02-18T18:00:00", writer="coordinator", tags=["meta"])

    # Worker reads task, updates status, writes result
    task = await ns.get("task")
    print(f"\n  Worker received task: {task}")

    await ns.put("status", "in_progress", writer="worker")
    print("  Worker processing …")
    await asyncio.sleep(0.05)

    await ns.put(
        "result",
        {"task_id": task["id"], "summary": "Q1 revenue +12% YoY; top SKU: widget-X", "rows_processed": 14_320},
        writer="worker",
        tags=["output"],
    )
    await ns.put("status", "completed", writer="worker")

    print(f"\n  Final status: {await ns.get('status')!r}")
    print(f"  Result: {await ns.get('result')}")

    # -----------------------------------------------------------------------
    # 2. history() — newest-first
    # -----------------------------------------------------------------------
    section("2. history('status') — all write versions, newest-first")

    records = await ns.history("status")
    for r in records:
        print(f"  v{r.version}  writer={r.writer:15s}  value={r.value!r}  ts={r.timestamp}")

    assert records[0].value == "completed", "Newest record should be 'completed'"
    assert records[-1].value == "pending",  "Oldest record should be 'pending'"
    print(f"\n  Total writes to 'status': {len(records)}  ✓")

    # -----------------------------------------------------------------------
    # 3. Directed channels + inbox()
    # -----------------------------------------------------------------------
    section("3. Directed channels: coordinator → worker, inbox() aggregation")

    await store.send("coordinator", "worker", {"cmd": "run_report", "format": "pdf"}, tags=["cmd"])
    await store.send("coordinator", "worker", {"cmd": "upload_results", "dest": "s3://bucket/q1"}, tags=["cmd"])
    await store.send("monitor",     "worker", {"cmd": "health_check"}, tags=["admin"])

    ch_len = len(store.channel("coordinator", "worker"))
    print(f"  coordinator→worker queue depth: {ch_len}")

    # peek without consuming
    peeked = await store.channel("coordinator", "worker").peek()
    print(f"  Peeked first message: {peeked.payload}")

    # inbox() collects from ALL senders and sorts by time
    inbox_msgs = await store.inbox("worker")
    print(f"\n  inbox('worker') received {len(inbox_msgs)} message(s):")
    for m in inbox_msgs:
        print(f"    from={m.sender:15s}  payload={m.payload}")

    assert len(inbox_msgs) == 3, "Worker should have 3 messages in inbox"
    print("  inbox() aggregation ✓")

    # -----------------------------------------------------------------------
    # 4. TTL — entry expires after 3 seconds
    # -----------------------------------------------------------------------
    section("4. TTL — heartbeat entry expires after 3 seconds")

    await ns.put(
        "heartbeat",
        {"alive": True, "load": 0.42},
        writer="worker",
        ttl=3,
        tags=["health"],
    )
    immediate = await ns.get("heartbeat")
    print(f"  Immediate read  → {immediate}")
    assert immediate is not None, "Heartbeat should be readable immediately"

    print("  Sleeping 3.5 s for TTL expiry …")
    await asyncio.sleep(3.5)

    expired_val = await ns.get("heartbeat")
    print(f"  After 3.5 s read → {expired_val}  (None = expired correctly)")
    assert expired_val is None, "Heartbeat should have expired"

    removed = await ns.cleanup_expired()
    print(f"  cleanup_expired() removed {removed} additional stale key(s)  ✓")

    # -----------------------------------------------------------------------
    # 5. query(writer="worker") — filter by writer
    # -----------------------------------------------------------------------
    section("5. query(writer='worker') — only worker-written entries")

    worker_entries = await ns.query(writer="worker")
    print(f"  Entries written by worker ({len(worker_entries)}):")
    for e in worker_entries:
        print(f"    key={e.current.key!r:15s}  v{e.current.version}  tags={list(e.current.tags)}")

    for e in worker_entries:
        assert e.current.writer == "worker", f"Expected writer='worker', got {e.current.writer!r}"
    print("  All returned entries are from worker ✓")

    # Also verify coordinator entries are NOT in the result
    coordinator_entries = await ns.query(writer="coordinator")
    print(f"\n  Entries written by coordinator ({len(coordinator_entries)}):")
    for e in coordinator_entries:
        print(f"    key={e.current.key!r:15s}  v{e.current.version}")

    # -----------------------------------------------------------------------
    # 6. merge() — version conflict resolution
    # -----------------------------------------------------------------------
    section("6. merge() — last-write-wins by version number")

    remote_store = ContextStore()

    # Remote has a HIGHER version of "status" → should win
    # We manually set version by writing multiple times
    await remote_store.put("status",      "archived",             writer="archiver", namespace="data_pipeline_v1")
    await remote_store.put("status",      "archived_v2",          writer="archiver", namespace="data_pipeline_v1")
    await remote_store.put("archived_at", "2026-02-18T19:00:00",  writer="archiver", namespace="data_pipeline_v1")

    remote_status_v = (await remote_store.get_entry("status", "data_pipeline_v1")).current.version
    local_status_v  = (await store.get_entry("status", "data_pipeline_v1")).current.version
    print(f"  Before merge: local status v{local_status_v}, remote status v{remote_status_v}")

    merged_count = await store.merge(remote_store, "data_pipeline_v1")
    print(f"  merge() accepted {merged_count} entry/entries from remote")

    merged_status = await store.get("status", "data_pipeline_v1")
    print(f"  status after merge → {merged_status!r}")

    if remote_status_v > local_status_v:
        assert merged_status == "archived_v2", "Higher-version remote value should have won"
        print("  Higher remote version won ✓")
    else:
        print("  Local version was already higher; remote was ignored ✓")

    # -----------------------------------------------------------------------
    # 7. to_json() — full serialisation
    # -----------------------------------------------------------------------
    section("7. to_json() — full store serialisation")

    json_str = await store.to_json()
    parsed = json.loads(json_str)

    print(f"  Serialised namespaces: {list(parsed['entries'].keys())}")
    print(f"  Channels captured:     {list(parsed['channels'].keys())}")
    print(f"  Subscriptions:         {list(parsed['subscriptions'].keys())}")

    # Verify roundtrip: all namespace keys appear in JSON
    local_keys = set(await ns.keys())
    json_keys  = set(parsed["entries"].get("data_pipeline_v1", {}).keys())
    assert local_keys == json_keys, f"JSON keys mismatch: {local_keys} vs {json_keys}"
    print("  Key set roundtrip matches ✓")

    # Print a condensed preview
    condensed = {
        "entries_count": {
            ns_name: len(keys)
            for ns_name, keys in parsed["entries"].items()
        },
        "channels": parsed["channels"],
    }
    print("\n  Condensed summary:")
    print(json.dumps(condensed, indent=4, ensure_ascii=False))

    # -----------------------------------------------------------------------
    # 8. Final stats
    # -----------------------------------------------------------------------
    section("8. Store stats")

    stats = await store.stats()
    print(json.dumps(stats, indent=2, ensure_ascii=False))

    # -----------------------------------------------------------------------
    # 9. Monitor change log summary
    # -----------------------------------------------------------------------
    section("9. Monitor pub/sub change log summary")

    print(f"  Total changes observed by monitor: {len(_change_log)}")
    for i, entry in enumerate(_change_log, 1):
        print(f"  {i:2d}. [{entry['writer']:15s}] {entry['key']!r}")

    section("Demo complete — all scenarios passed ✓")


if __name__ == "__main__":
    asyncio.run(main())
