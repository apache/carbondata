"""
SharedContext_Demo.python
=========================
End-to-end demonstration of multi-agent shared memory.

Scenario
--------
  coordinator  — assigns a task, sets status to "pending"
  worker       — picks up the task, processes it, writes result back
  monitor      — passive observer; callback fires on every change

Run:
    python3 Agent_module/SharedContext_Demo.python
"""

import sys, os, json, time, importlib.util as _ilu

def _load(name: str):
    from importlib.machinery import SourceFileLoader
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, f"{name}.python")
    loader = SourceFileLoader(name, path)
    spec   = _ilu.spec_from_loader(name, loader)
    mod    = _ilu.module_from_spec(spec)
    sys.modules[name] = mod
    loader.exec_module(mod)
    return mod

_sc  = _load("SharedContext")
_cb  = _load("ContextBridge")

SharedContext  = _sc.SharedContext
ContextEntry   = _sc.ContextEntry
ContextBridge  = _cb.ContextBridge

# ============================================================================
# Helper: pretty section headers
# ============================================================================

def section(title: str) -> None:
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


# ============================================================================
# Monitor callback — fires whenever a subscribed key changes
# ============================================================================

_change_log: list = []   # record all changes for summary at the end

def monitor_callback(key: str, entry: ContextEntry) -> None:
    msg = (
        f"[MONITOR] key={key!r:20s}  "
        f"agent={entry.agent_id:15s}  "
        f"value={str(entry.value)!r:.60s}"
    )
    print(msg)
    _change_log.append({"key": key, "agent_id": entry.agent_id, "value": entry.value})


# ============================================================================
# Main demo
# ============================================================================

def main() -> None:

    # ------------------------------------------------------------------
    # 1. Create SharedContext and wrap in ContextBridge
    # ------------------------------------------------------------------
    section("1. Setup — SharedContext + ContextBridge")

    ctx = SharedContext(namespace="data_pipeline_v1")
    bridge = ContextBridge(ctx)

    # ------------------------------------------------------------------
    # 2. Register agents
    # ------------------------------------------------------------------
    section("2. Agent registration")

    # Monitor subscribes to ALL keys (empty keys list)
    bridge.register_agent("monitor", monitor_callback, keys=[])

    # Coordinator and worker subscribe to keys they care about,
    # but we skip giving them callbacks in this demo (they act, not react).
    bridge.register_agent("coordinator", lambda k, e: None, keys=["result"])
    bridge.register_agent("worker",      lambda k, e: None, keys=["task", "status"])

    print("  Registered: coordinator, worker, monitor")

    # ------------------------------------------------------------------
    # 3. Coordinator writes task + initial status
    # ------------------------------------------------------------------
    section("3. Coordinator assigns task")

    bridge.write(
        agent_id="coordinator",
        key="task",
        value={
            "id": "task_001",
            "description": "Analyse sales data for Q1",
            "priority": "high",
        },
    )
    bridge.write(agent_id="coordinator", key="status", value="pending")
    bridge.write(
        agent_id="coordinator",
        key="deadline",
        value="2026-02-18T18:00:00",
        tags=["meta"],
    )

    # ------------------------------------------------------------------
    # 4. Worker reads task, updates status, writes result
    # ------------------------------------------------------------------
    section("4. Worker processes task")

    task = bridge.read("worker", "task")
    print(f"  Worker received task: {task}")

    bridge.write(agent_id="worker", key="status", value="in_progress")

    # Simulate processing
    print("  Worker processing …")
    time.sleep(0.05)

    bridge.write(
        agent_id="worker",
        key="result",
        value={
            "task_id": task["id"],
            "summary": "Q1 revenue up 12 % YoY; top SKU: widget-X",
            "rows_processed": 14_320,
        },
    )
    bridge.write(agent_id="worker", key="status", value="completed")

    # ------------------------------------------------------------------
    # 5. Short-lived TTL entry (expires in 2 seconds)
    # ------------------------------------------------------------------
    section("5. TTL demo — heartbeat entry (TTL = 2 s)")

    bridge.write(
        agent_id="worker",
        key="heartbeat",
        value={"alive": True, "load": 0.42},
        ttl=2,
        tags=["health"],
    )
    print("  heartbeat written (TTL=2s)")
    print(f"  Immediate read  → {bridge.read('coordinator', 'heartbeat')}")

    print("  Sleeping 3 s …")
    time.sleep(3)

    expired_val = bridge.read("coordinator", "heartbeat")
    print(f"  After 3 s read  → {expired_val}  (None = expired correctly)")

    removed = ctx.cleanup_expired()
    print(f"  cleanup_expired() removed: {removed}")

    # ------------------------------------------------------------------
    # 6. Atomic broadcast
    # ------------------------------------------------------------------
    section("6. Atomic broadcast by coordinator")

    bridge.broadcast(
        agent_id="coordinator",
        data={
            "audit_approved": True,
            "reviewer": "agent_audit_bot",
        },
        tags=["audit"],
    )

    # ------------------------------------------------------------------
    # 7. Full context snapshot
    # ------------------------------------------------------------------
    section("7. Full context snapshot (read_all)")

    snapshot = bridge.read_all("monitor")
    print(json.dumps(snapshot, indent=2, ensure_ascii=False))

    # ------------------------------------------------------------------
    # 8. Merge demo
    # ------------------------------------------------------------------
    section("8. Merge — remote snapshot into local context")

    remote_ctx = SharedContext(namespace="data_pipeline_v1")
    remote_ctx.set("status",   "archived",  "archiver")
    remote_ctx.set("archived_at", "2026-02-18T19:00:00", "archiver")

    ctx.merge(remote_ctx)
    print("  Merged remote context (status overridden by higher version).")
    print(f"  status after merge → {ctx.get('status')!r}")

    # ------------------------------------------------------------------
    # 9. Export final context to JSON
    # ------------------------------------------------------------------
    section("9. Final context — to_json() export")

    print(ctx.to_json())

    # ------------------------------------------------------------------
    # 10. Monitor change summary
    # ------------------------------------------------------------------
    section("10. Monitor change log summary")

    print(f"  Total changes observed by monitor: {len(_change_log)}")
    for i, entry in enumerate(_change_log, 1):
        print(f"  {i:2d}. [{entry['agent_id']:15s}] {entry['key']}")

    section("Demo complete")


if __name__ == "__main__":
    main()
