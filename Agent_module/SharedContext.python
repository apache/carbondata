from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime
import copy
import json

# ============================================================================
# Core Data Models — Shared Context
# NOTE: SharedContext is NOT thread-safe by default. For concurrent access,
# callers must provide external locking (e.g., threading.Lock).
# ============================================================================

@dataclass
class ContextEntry:
    """A single key/value entry written by an agent into the shared context."""
    key: str
    value: Any
    agent_id: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    version: int = 1
    ttl: Optional[int] = None        # seconds until expiry; None = forever
    tags: List[str] = field(default_factory=list)

    def is_expired(self) -> bool:
        """Return True if this entry has passed its TTL."""
        if self.ttl is None:
            return False
        written_at = datetime.fromisoformat(self.timestamp)
        elapsed = (datetime.now() - written_at).total_seconds()
        return elapsed > self.ttl

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "agent_id": self.agent_id,
            "timestamp": self.timestamp,
            "version": self.version,
            "ttl": self.ttl,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ContextEntry":
        return cls(
            key=data["key"],
            value=data["value"],
            agent_id=data["agent_id"],
            timestamp=data.get("timestamp", datetime.now().isoformat()),
            version=data.get("version", 1),
            ttl=data.get("ttl"),
            tags=data.get("tags", []),
        )


@dataclass
class SharedContext:
    """
    A namespace-scoped shared memory store for multiple agents.

    Agents read and write ContextEntry objects keyed by string.
    Version numbers enable last-write-wins conflict resolution during merge.

    NOTE: Not thread-safe. Use external locking for concurrent access.
    """
    namespace: str
    entries: Dict[str, ContextEntry] = field(default_factory=dict)
    version: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def set(
        self,
        key: str,
        value: Any,
        agent_id: str,
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None,
    ) -> ContextEntry:
        """Write a value.  Bumps the store-level version counter."""
        existing = self.entries.get(key)
        entry_version = (existing.version + 1) if existing else 1

        entry = ContextEntry(
            key=key,
            value=value,
            agent_id=agent_id,
            version=entry_version,
            ttl=ttl,
            tags=tags or [],
        )
        self.entries[key] = entry
        self.version += 1
        self.updated_at = datetime.now().isoformat()
        return entry

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[Any]:
        """Return the value for *key*, or None if missing or TTL-expired."""
        entry = self.entries.get(key)
        if entry is None:
            return None
        if entry.is_expired():
            del self.entries[key]
            return None
        return entry.value

    def get_entry(self, key: str) -> Optional[ContextEntry]:
        """Return the full ContextEntry for *key*, or None if missing/expired."""
        entry = self.entries.get(key)
        if entry is None:
            return None
        if entry.is_expired():
            del self.entries[key]
            return None
        return entry

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete(self, key: str, agent_id: str) -> bool:
        """Remove *key* from the store.  Returns True if it existed."""
        if key in self.entries:
            del self.entries[key]
            self.version += 1
            self.updated_at = datetime.now().isoformat()
            return True
        return False

    # ------------------------------------------------------------------
    # Iteration helpers
    # ------------------------------------------------------------------

    def keys(self) -> List[str]:
        """Return all non-expired keys."""
        self.cleanup_expired()
        return list(self.entries.keys())

    def items(self) -> List[tuple]:
        """Return all non-expired (key, value) pairs."""
        self.cleanup_expired()
        return [(k, e.value) for k, e in self.entries.items()]

    # ------------------------------------------------------------------
    # Snapshot / merge
    # ------------------------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """Return a deep copy of the current (non-expired) entries as a dict."""
        self.cleanup_expired()
        return copy.deepcopy({k: e.to_dict() for k, e in self.entries.items()})

    def merge(self, other: "SharedContext") -> None:
        """
        Merge *other* into this context using last-write-wins by entry version.
        Only entries with a higher version number replace existing ones.
        """
        for key, other_entry in other.entries.items():
            existing = self.entries.get(key)
            if existing is None or other_entry.version > existing.version:
                self.entries[key] = copy.deepcopy(other_entry)
        self.version = max(self.version, other.version)
        self.updated_at = datetime.now().isoformat()

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def cleanup_expired(self) -> List[str]:
        """Remove all TTL-expired entries.  Returns the list of removed keys."""
        expired = [k for k, e in self.entries.items() if e.is_expired()]
        for k in expired:
            del self.entries[k]
        if expired:
            self.updated_at = datetime.now().isoformat()
        return expired

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "namespace": self.namespace,
            "entries": {k: e.to_dict() for k, e in self.entries.items()},
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SharedContext":
        ctx = cls(
            namespace=data["namespace"],
            version=data.get("version", 0),
            created_at=data.get("created_at", datetime.now().isoformat()),
            updated_at=data.get("updated_at", datetime.now().isoformat()),
        )
        for key, entry_data in data.get("entries", {}).items():
            ctx.entries[key] = ContextEntry.from_dict(entry_data)
        return ctx

    @classmethod
    def from_json(cls, json_str: str) -> "SharedContext":
        return cls.from_dict(json.loads(json_str))
