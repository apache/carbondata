from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime

# Load SharedContext from the same directory.
# .python extension requires importlib to load by file path.
import sys, os, importlib.util as _ilu

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

_sc = _load("SharedContext")
SharedContext  = _sc.SharedContext
ContextEntry   = _sc.ContextEntry

# ============================================================================
# Pub/Sub Bridge
# ============================================================================

@dataclass
class ContextSubscription:
    """Subscription record: which agent watches which keys."""
    agent_id: str
    keys: List[str]          # empty list = subscribe to ALL keys
    callback: Callable       # called as callback(key, entry) on change


class ContextBridge:
    """
    Connects agents to a SharedContext and fires callbacks on writes.

    Usage pattern:
        bridge = ContextBridge(context)
        bridge.register_agent("monitor", on_change)   # subscribe to all
        bridge.write("coordinator", "task", {"id": 1})
    """

    def __init__(self, context: SharedContext):
        self._context = context
        self._subscriptions: Dict[str, ContextSubscription] = {}
        self._access_log: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Agent registration
    # ------------------------------------------------------------------

    def register_agent(
        self,
        agent_id: str,
        callback: Callable,
        keys: Optional[List[str]] = None,
    ) -> None:
        """Subscribe *agent_id* to context changes on *keys* (all if empty)."""
        self._subscriptions[agent_id] = ContextSubscription(
            agent_id=agent_id,
            keys=keys or [],
            callback=callback,
        )

    def unregister_agent(self, agent_id: str) -> None:
        """Remove the subscription for *agent_id*."""
        self._subscriptions.pop(agent_id, None)

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def write(
        self,
        agent_id: str,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None,
    ) -> ContextEntry:
        """Write *key* → *value* on behalf of *agent_id*, then notify."""
        entry = self._context.set(key, value, agent_id, ttl=ttl, tags=tags)
        self._notify(key, entry)
        return entry

    def broadcast(
        self,
        agent_id: str,
        data: Dict[str, Any],
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None,
    ) -> List[ContextEntry]:
        """Write multiple keys atomically (no interleaved notifications)."""
        written: List[ContextEntry] = []
        for key, value in data.items():
            entry = self._context.set(key, value, agent_id, ttl=ttl, tags=tags)
            written.append(entry)
        # Notify after all entries are committed
        for entry in written:
            self._notify(entry.key, entry)
        return written

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def read(self, agent_id: str, key: str) -> Optional[Any]:
        """Read *key* on behalf of *agent_id*; access is logged."""
        value = self._context.get(key)
        self._log_access(agent_id, "read", key)
        return value

    def read_all(self, agent_id: str) -> Dict[str, Any]:
        """Return a snapshot of all non-expired entries visible to *agent_id*."""
        self._log_access(agent_id, "read_all", "*")
        return self._context.snapshot()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _notify(self, changed_key: str, entry: ContextEntry) -> None:
        """Fire callbacks for every subscriber that watches *changed_key*."""
        for sub in self._subscriptions.values():
            # Skip the writer itself (no self-notification)
            if sub.agent_id == entry.agent_id:
                continue
            if sub.keys and changed_key not in sub.keys:
                continue
            try:
                sub.callback(changed_key, entry)
            except Exception:
                pass  # Don't let a bad callback break the bridge

    def _log_access(self, agent_id: str, operation: str, key: str) -> None:
        self._access_log.append({
            "agent_id": agent_id,
            "operation": operation,
            "key": key,
            "timestamp": datetime.now().isoformat(),
        })

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def access_log(self) -> List[Dict[str, Any]]:
        """Return a copy of the access log."""
        return list(self._access_log)
