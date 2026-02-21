"""
ContextStore.py
===============
Unified, thread-safe, async-first context exchange for multi-agent systems.

Replaces SharedContext + ContextBridge with a single module that provides:
  - Keyed state store with full write history (last 50 writes per key)
  - Point-to-point FIFO message channels between agents
  - Pub/sub notifications supporting both sync and async callbacks
  - TTL, tagging, querying, merge, snapshot, and serialisation

Usage:
    store = ContextStore()

    # State store
    rec = await store.put("task", {"id": 1}, writer="coordinator")
    val = await store.get("task")

    # Channels
    await store.send("coordinator", "worker", {"cmd": "run"})
    msg = await store.receive("coordinator", "worker")

    # Pub/sub
    store.subscribe("monitor", my_callback)

    # Namespace-scoped view
    ns = ContextNamespace(store, "pipeline_v1")
    await ns.put("status", "running", writer="coordinator")
"""

from __future__ import annotations

import asyncio
import copy
import json
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


# ============================================================================
# Data Models
# ============================================================================


@dataclass(frozen=True)
class ContextRecord:
    """
    Immutable record of a single write.

    Every put() call appends a new ContextRecord to the key's history.
    Frozen so the record is safe to share across tasks without copying.
    """

    key: str
    value: Any
    writer: str
    version: int
    timestamp: str
    record_id: str
    ttl: Optional[int]      # seconds until expiry; None = never expires
    tags: Tuple[str, ...]

    def is_expired(self) -> bool:
        """Return True if this record has passed its TTL."""
        if self.ttl is None:
            return False
        written_at = datetime.fromisoformat(self.timestamp)
        return (datetime.now() - written_at).total_seconds() > self.ttl

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "writer": self.writer,
            "version": self.version,
            "timestamp": self.timestamp,
            "record_id": self.record_id,
            "ttl": self.ttl,
            "tags": list(self.tags),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ContextRecord":
        return cls(
            key=data["key"],
            value=data["value"],
            writer=data["writer"],
            version=data["version"],
            timestamp=data["timestamp"],
            record_id=data["record_id"],
            ttl=data.get("ttl"),
            tags=tuple(data.get("tags", [])),
        )


@dataclass
class ContextEntry:
    """
    Active state for one key: current record + full write history.

    history is a deque with maxlen=50; oldest records are dropped
    automatically when the limit is reached.
    """

    current: ContextRecord
    history: deque = field(default_factory=lambda: deque(maxlen=50))

    def is_expired(self) -> bool:
        return self.current.is_expired()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "current": self.current.to_dict(),
            "history": [r.to_dict() for r in self.history],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ContextEntry":
        current = ContextRecord.from_dict(data["current"])
        history: deque = deque(
            [ContextRecord.from_dict(r) for r in data.get("history", [])],
            maxlen=50,
        )
        return cls(current=current, history=history)


@dataclass
class ContextMessage:
    """Directed message from one agent to another."""

    sender: str
    receiver: str
    payload: Any
    timestamp: str
    message_id: str
    tags: Tuple[str, ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "message_id": self.message_id,
            "tags": list(self.tags),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ContextMessage":
        return cls(
            sender=data["sender"],
            receiver=data["receiver"],
            payload=data["payload"],
            timestamp=data["timestamp"],
            message_id=data["message_id"],
            tags=tuple(data.get("tags", [])),
        )


# ============================================================================
# ContextChannel — point-to-point FIFO queue
# ============================================================================


class ContextChannel:
    """FIFO message queue from one specific agent to another."""

    def __init__(self) -> None:
        self._queue: deque[ContextMessage] = deque()
        self._lock = asyncio.Lock()

    async def send(self, message: ContextMessage) -> None:
        async with self._lock:
            self._queue.append(message)

    async def receive(self) -> Optional[ContextMessage]:
        """Pop and return the oldest message, or None if empty."""
        async with self._lock:
            return self._queue.popleft() if self._queue else None

    async def receive_all(self) -> List[ContextMessage]:
        """Pop and return all pending messages in FIFO order."""
        async with self._lock:
            msgs = list(self._queue)
            self._queue.clear()
            return msgs

    async def peek(self) -> Optional[ContextMessage]:
        """Return the oldest message without removing it."""
        async with self._lock:
            return self._queue[0] if self._queue else None

    def __len__(self) -> int:
        return len(self._queue)


# ============================================================================
# Subscription
# ============================================================================


@dataclass
class Subscription:
    """Internal pub/sub record."""

    agent_id: str
    namespace: str
    keys: Set[str]      # empty set = subscribe to ALL keys in namespace
    callback: Callable  # sync or async; called as callback(key, record)


# ============================================================================
# ContextStore — core
# ============================================================================


class ContextStore:
    """
    Unified, thread-safe, async-first context exchange for multi-agent systems.

    Internal layout
    ---------------
    _entries:  Dict[namespace, Dict[key, ContextEntry]]  — asyncio.Lock protected
    _channels: Dict[(sender, receiver), ContextChannel]  — each has its own lock
    _subs:     Dict[namespace, Dict[agent_id, Subscription]]
    _lock:     asyncio.Lock  (single global lock protecting _entries)

    Notification contract
    ---------------------
    - Callbacks fire AFTER the lock is released to avoid re-entrant deadlocks.
    - Writers never receive their own notifications.
    - Both sync and async callbacks are supported.

    TTL eviction
    ------------
    - Lazy eviction on get_entry() reads.
    - Batch eviction via cleanup_expired().
    """

    def __init__(self) -> None:
        self._entries: Dict[str, Dict[str, ContextEntry]] = {}
        self._channels: Dict[Tuple[str, str], ContextChannel] = {}
        self._subs: Dict[str, Dict[str, Subscription]] = {}
        self._lock = asyncio.Lock()

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _ns_entries(self, namespace: str) -> Dict[str, ContextEntry]:
        """Return (creating if needed) the entries dict for *namespace*."""
        if namespace not in self._entries:
            self._entries[namespace] = {}
        return self._entries[namespace]

    def _ns_subs(self, namespace: str) -> Dict[str, Subscription]:
        if namespace not in self._subs:
            self._subs[namespace] = {}
        return self._subs[namespace]

    # -----------------------------------------------------------------------
    # State API — writes
    # -----------------------------------------------------------------------

    async def put(
        self,
        key: str,
        value: Any,
        writer: str,
        namespace: str = "default",
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None,
    ) -> ContextRecord:
        """
        Write *key* → *value* on behalf of *writer*.

        Creates a new ContextRecord and appends it to the key's history.
        Notifies subscribers after the lock is released.
        Returns the new ContextRecord.
        """
        to_notify: List[Subscription] = []
        record: ContextRecord

        async with self._lock:
            entries = self._ns_entries(namespace)
            existing = entries.get(key)
            version = (existing.current.version + 1) if existing else 1

            record = ContextRecord(
                key=key,
                value=value,
                writer=writer,
                version=version,
                timestamp=datetime.now().isoformat(),
                record_id=str(uuid.uuid4()),
                ttl=ttl,
                tags=tuple(tags or []),
            )

            if existing is None:
                entry = ContextEntry(current=record)
                entry.history.append(record)
                entries[key] = entry
            else:
                existing.history.append(record)
                existing.current = record

            # Collect subscribers before releasing the lock
            for sub in self._ns_subs(namespace).values():
                if sub.agent_id == writer:
                    continue  # no self-notification
                if sub.keys and key not in sub.keys:
                    continue
                to_notify.append(sub)

        # Fire callbacks AFTER the lock is released
        for sub in to_notify:
            try:
                result = sub.callback(key, record)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass  # bad callbacks must not crash the store

        return record

    # -----------------------------------------------------------------------
    # State API — reads
    # -----------------------------------------------------------------------

    async def get(self, key: str, namespace: str = "default") -> Optional[Any]:
        """Return current value for *key*, or None if missing or TTL-expired."""
        entry = await self.get_entry(key, namespace)
        return entry.current.value if entry else None

    async def get_entry(
        self, key: str, namespace: str = "default"
    ) -> Optional[ContextEntry]:
        """
        Return the full ContextEntry (current + history) for *key*.

        Lazily evicts the entry if its TTL has expired.
        Returns None if missing or expired.
        """
        async with self._lock:
            entries = self._ns_entries(namespace)
            entry = entries.get(key)
            if entry is None:
                return None
            if entry.is_expired():
                del entries[key]
                return None
            return entry

    async def delete(
        self, key: str, writer: str, namespace: str = "default"
    ) -> bool:
        """Remove *key* from the store. Returns True if it existed."""
        async with self._lock:
            entries = self._ns_entries(namespace)
            if key in entries:
                del entries[key]
                return True
            return False

    async def history(
        self,
        key: str,
        namespace: str = "default",
        limit: Optional[int] = None,
    ) -> List[ContextRecord]:
        """
        Return write history for *key*, newest-first.

        Returns at most *limit* records if specified.
        Returns [] if the key does not exist or is expired.
        """
        entry = await self.get_entry(key, namespace)
        if entry is None:
            return []
        records = list(reversed(entry.history))
        return records[:limit] if limit is not None else records

    async def keys(self, namespace: str = "default") -> List[str]:
        """Return all non-expired keys in *namespace*."""
        await self.cleanup_expired(namespace)
        async with self._lock:
            return list(self._ns_entries(namespace).keys())

    async def query(
        self,
        namespace: str = "default",
        *,
        writer: Optional[str] = None,
        tags: Optional[List[str]] = None,
        since: Optional[str] = None,    # ISO-8601 datetime string (inclusive)
        until: Optional[str] = None,    # ISO-8601 datetime string (inclusive)
    ) -> List[ContextEntry]:
        """
        Return ContextEntry objects whose current record matches all filters.

        writer — only entries last written by this agent
        tags   — entry must have ALL of these tags
        since  — entry timestamp >= since
        until  — entry timestamp <= until
        """
        await self.cleanup_expired(namespace)
        tag_set = set(tags) if tags else None

        async with self._lock:
            entries = self._ns_entries(namespace)
            results: List[ContextEntry] = []
            for entry in entries.values():
                rec = entry.current
                if writer and rec.writer != writer:
                    continue
                if tag_set and not tag_set.issubset(set(rec.tags)):
                    continue
                if since and rec.timestamp < since:
                    continue
                if until and rec.timestamp > until:
                    continue
                results.append(entry)
            return results

    # -----------------------------------------------------------------------
    # Channel API — point-to-point messaging
    # -----------------------------------------------------------------------

    def channel(self, sender: str, receiver: str) -> ContextChannel:
        """Return (creating if needed) the channel from *sender* to *receiver*."""
        key = (sender, receiver)
        if key not in self._channels:
            self._channels[key] = ContextChannel()
        return self._channels[key]

    async def send(
        self,
        sender: str,
        receiver: str,
        payload: Any,
        tags: Optional[List[str]] = None,
    ) -> ContextMessage:
        """Send a directed message from *sender* to *receiver*."""
        msg = ContextMessage(
            sender=sender,
            receiver=receiver,
            payload=payload,
            timestamp=datetime.now().isoformat(),
            message_id=str(uuid.uuid4()),
            tags=tuple(tags or []),
        )
        await self.channel(sender, receiver).send(msg)
        return msg

    async def receive(
        self, sender: str, receiver: str
    ) -> Optional[ContextMessage]:
        """Pop the next message from the *sender* → *receiver* channel."""
        return await self.channel(sender, receiver).receive()

    async def inbox(self, receiver: str) -> List[ContextMessage]:
        """
        Collect and return all pending messages for *receiver* across every
        sender channel, sorted by timestamp (oldest first).

        This is a destructive read — messages are removed from the channel.
        """
        msgs: List[ContextMessage] = []
        for (s, r), ch in self._channels.items():
            if r == receiver:
                msgs.extend(await ch.receive_all())
        msgs.sort(key=lambda m: m.timestamp)
        return msgs

    # -----------------------------------------------------------------------
    # Pub/Sub API
    # -----------------------------------------------------------------------

    def subscribe(
        self,
        agent_id: str,
        callback: Callable,
        namespace: str = "default",
        keys: Optional[List[str]] = None,
    ) -> None:
        """
        Register *agent_id* to be notified on writes to *keys*.

        keys=None (or []) means subscribe to ALL keys in the namespace.
        callback(key, record) is called after each put(); both sync and
        async callbacks are supported.
        """
        subs = self._ns_subs(namespace)
        subs[agent_id] = Subscription(
            agent_id=agent_id,
            namespace=namespace,
            keys=set(keys) if keys else set(),
            callback=callback,
        )

    def unsubscribe(self, agent_id: str, namespace: str = "default") -> None:
        """Remove *agent_id*'s subscription in *namespace*."""
        self._ns_subs(namespace).pop(agent_id, None)

    # -----------------------------------------------------------------------
    # Maintenance
    # -----------------------------------------------------------------------

    async def cleanup_expired(self, namespace: str = "default") -> int:
        """
        Remove all TTL-expired entries from *namespace*.
        Returns the number of keys removed.
        """
        async with self._lock:
            entries = self._ns_entries(namespace)
            expired = [k for k, e in entries.items() if e.is_expired()]
            for k in expired:
                del entries[k]
            return len(expired)

    async def snapshot(self, namespace: str = "default") -> Dict[str, Any]:
        """
        Return a deep copy of current non-expired entries in *namespace*
        as a plain dictionary (key → entry dict).
        """
        await self.cleanup_expired(namespace)
        async with self._lock:
            entries = self._ns_entries(namespace)
            return copy.deepcopy({k: e.to_dict() for k, e in entries.items()})

    async def merge(
        self, other: "ContextStore", namespace: str = "default"
    ) -> int:
        """
        Merge entries from *other* into this store for *namespace*.

        Uses last-write-wins by version number: an entry from *other* is
        accepted only if its current version is higher than the local one.
        Returns the number of entries merged (updated or inserted).
        """
        # Copy other's entries while holding *its* lock (avoids concurrent mutation)
        async with other._lock:
            other_snapshot = copy.deepcopy(
                dict(other._entries.get(namespace, {}))
            )

        merged = 0
        async with self._lock:
            entries = self._ns_entries(namespace)
            for key, other_entry in other_snapshot.items():
                existing = entries.get(key)
                if (
                    existing is None
                    or other_entry.current.version > existing.current.version
                ):
                    entries[key] = other_entry
                    merged += 1
        return merged

    async def stats(self) -> Dict[str, Any]:
        """Return high-level statistics about the store."""
        async with self._lock:
            ns_stats = {}
            for ns, entries in self._entries.items():
                subs = self._subs.get(ns, {})
                ns_stats[ns] = {
                    "total_keys": len(entries),
                    "expired_keys": sum(
                        1 for e in entries.values() if e.is_expired()
                    ),
                    "subscribers": list(subs.keys()),
                }

        channel_stats = {
            f"{s}->{r}": len(ch)
            for (s, r), ch in self._channels.items()
        }

        return {
            "namespaces": ns_stats,
            "channels": channel_stats,
            "total_channels": len(self._channels),
        }

    async def to_dict(self) -> Dict[str, Any]:
        """Full serialisation of the store to a plain Python dict."""
        # Snapshot all namespaces, filtering out expired entries in one lock pass
        async with self._lock:
            entries_dict: Dict[str, Any] = {}
            for ns, entries in self._entries.items():
                entries_dict[ns] = {
                    k: e.to_dict()
                    for k, e in entries.items()
                    if not e.is_expired()
                }

        # Snapshot channels (each under its own lock)
        channels_dict: Dict[str, Any] = {}
        for (s, r), ch in self._channels.items():
            async with ch._lock:
                channels_dict[f"{s}->{r}"] = [m.to_dict() for m in ch._queue]

        subs_dict: Dict[str, Any] = {
            ns: {
                aid: {
                    "agent_id": sub.agent_id,
                    "namespace": sub.namespace,
                    "keys": list(sub.keys),
                }
                for aid, sub in subs.items()
            }
            for ns, subs in self._subs.items()
        }

        return {
            "entries": entries_dict,
            "channels": channels_dict,
            "subscriptions": subs_dict,
        }

    async def to_json(self, indent: int = 2) -> str:
        """Return a JSON string of the full store."""
        return json.dumps(await self.to_dict(), indent=indent, ensure_ascii=False)


# ============================================================================
# ContextNamespace — namespace-scoped proxy view
# ============================================================================


class ContextNamespace:
    """
    A proxy view of ContextStore that fixes the *namespace* parameter.

    All methods have the same signatures as ContextStore but without needing
    to specify *namespace* on every call.

    Example:
        store = ContextStore()
        ns = ContextNamespace(store, "pipeline_v1")
        await ns.put("status", "running", writer="coordinator")
        val = await ns.get("status")
    """

    def __init__(self, store: ContextStore, namespace: str) -> None:
        self._store = store
        self._namespace = namespace

    @property
    def namespace(self) -> str:
        return self._namespace

    # State API

    async def put(
        self,
        key: str,
        value: Any,
        writer: str,
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None,
    ) -> ContextRecord:
        return await self._store.put(
            key, value, writer, self._namespace, ttl, tags
        )

    async def get(self, key: str) -> Optional[Any]:
        return await self._store.get(key, self._namespace)

    async def get_entry(self, key: str) -> Optional[ContextEntry]:
        return await self._store.get_entry(key, self._namespace)

    async def delete(self, key: str, writer: str) -> bool:
        return await self._store.delete(key, writer, self._namespace)

    async def history(
        self, key: str, limit: Optional[int] = None
    ) -> List[ContextRecord]:
        return await self._store.history(key, self._namespace, limit)

    async def keys(self) -> List[str]:
        return await self._store.keys(self._namespace)

    async def query(
        self,
        *,
        writer: Optional[str] = None,
        tags: Optional[List[str]] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[ContextEntry]:
        return await self._store.query(
            self._namespace, writer=writer, tags=tags, since=since, until=until
        )

    # Pub/Sub API

    def subscribe(
        self,
        agent_id: str,
        callback: Callable,
        keys: Optional[List[str]] = None,
    ) -> None:
        self._store.subscribe(agent_id, callback, self._namespace, keys)

    def unsubscribe(self, agent_id: str) -> None:
        self._store.unsubscribe(agent_id, self._namespace)

    # Maintenance / serialisation

    async def cleanup_expired(self) -> int:
        return await self._store.cleanup_expired(self._namespace)

    async def snapshot(self) -> Dict[str, Any]:
        return await self._store.snapshot(self._namespace)

    async def merge(self, other: ContextStore) -> int:
        return await self._store.merge(other, self._namespace)
