"""
context_store.py
================
Unified, thread-safe, async-first context exchange for multi-agent systems.

Provides:
  - Keyed state store with full write history (last 50 writes per key)
  - Point-to-point FIFO message channels between agents
  - Pub/sub notifications supporting both sync and async callbacks
  - TTL, tagging, querying, merge, snapshot, and serialisation
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable


# ============================================================================
# Data Models
# ============================================================================


@dataclass(frozen=True)
class ContextRecord:
    """Immutable record of a single write."""

    key: str
    value: Any
    writer: str
    version: int
    timestamp: str
    record_id: str
    ttl: int | None              # seconds until expiry; None = never expires
    tags: tuple[str, ...]

    def is_expired(self) -> bool:
        if self.ttl is None:
            return False
        written_at = datetime.fromisoformat(self.timestamp)
        return (datetime.now() - written_at).total_seconds() > self.ttl

    def to_dict(self) -> dict[str, Any]:
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
    def from_dict(cls, data: dict[str, Any]) -> ContextRecord:
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
    """Active state for one key: current record plus bounded write history."""

    current: ContextRecord
    history: deque = field(default_factory=lambda: deque(maxlen=50))

    def is_expired(self) -> bool:
        return self.current.is_expired()

    def to_dict(self) -> dict[str, Any]:
        return {
            "current": self.current.to_dict(),
            "history": [r.to_dict() for r in self.history],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContextEntry:
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
    tags: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "message_id": self.message_id,
            "tags": list(self.tags),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContextMessage:
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

    async def receive(self) -> ContextMessage | None:
        async with self._lock:
            return self._queue.popleft() if self._queue else None

    async def receive_all(self) -> list[ContextMessage]:
        async with self._lock:
            msgs = list(self._queue)
            self._queue.clear()
            return msgs

    async def peek(self) -> ContextMessage | None:
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
    keys: set[str]      # empty set = subscribe to ALL keys in namespace
    callback: Callable


# ============================================================================
# ContextStore — core
# ============================================================================


class ContextStore:
    """
    Unified, thread-safe, async-first context exchange for multi-agent systems.

    Internal layout
    ---------------
    _entries:  dict[namespace, dict[key, ContextEntry]]  — asyncio.Lock protected
    _channels: dict[(sender, receiver), ContextChannel]  — each has its own lock
    _subs:     dict[namespace, dict[agent_id, Subscription]]
    _lock:     asyncio.Lock (single global lock protecting _entries)

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
        self._entries: dict[str, dict[str, ContextEntry]] = {}
        self._channels: dict[tuple[str, str], ContextChannel] = {}
        self._subs: dict[str, dict[str, Subscription]] = {}
        self._lock = asyncio.Lock()

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _ns_entries(self, namespace: str) -> dict[str, ContextEntry]:
        if namespace not in self._entries:
            self._entries[namespace] = {}
        return self._entries[namespace]

    def _ns_subs(self, namespace: str) -> dict[str, Subscription]:
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
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> ContextRecord:
        """Write *key* → *value* on behalf of *writer*."""
        to_notify: list[Subscription] = []

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

            for sub in self._ns_subs(namespace).values():
                if sub.agent_id == writer:
                    continue
                if sub.keys and key not in sub.keys:
                    continue
                to_notify.append(sub)

        for sub in to_notify:
            try:
                result = sub.callback(key, record)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass

        return record

    # -----------------------------------------------------------------------
    # State API — reads
    # -----------------------------------------------------------------------

    async def get(self, key: str, namespace: str = "default") -> Any | None:
        entry = await self.get_entry(key, namespace)
        return entry.current.value if entry else None

    async def get_entry(
        self, key: str, namespace: str = "default"
    ) -> ContextEntry | None:
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
        limit: int | None = None,
    ) -> list[ContextRecord]:
        entry = await self.get_entry(key, namespace)
        if entry is None:
            return []
        records = list(reversed(entry.history))
        return records[:limit] if limit is not None else records

    async def keys(self, namespace: str = "default") -> list[str]:
        await self.cleanup_expired(namespace)
        async with self._lock:
            return list(self._ns_entries(namespace).keys())

    async def query(
        self,
        namespace: str = "default",
        *,
        writer: str | None = None,
        tags: list[str] | None = None,
        since: str | None = None,
        until: str | None = None,
    ) -> list[ContextEntry]:
        """Filter entries by writer / tags / since / until."""
        await self.cleanup_expired(namespace)
        tag_set = set(tags) if tags else None

        async with self._lock:
            entries = self._ns_entries(namespace)
            results: list[ContextEntry] = []
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
        key = (sender, receiver)
        if key not in self._channels:
            self._channels[key] = ContextChannel()
        return self._channels[key]

    async def send(
        self,
        sender: str,
        receiver: str,
        payload: Any,
        tags: list[str] | None = None,
    ) -> ContextMessage:
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
    ) -> ContextMessage | None:
        return await self.channel(sender, receiver).receive()

    async def inbox(self, receiver: str) -> list[ContextMessage]:
        """Destructively collect every pending message addressed to *receiver*."""
        msgs: list[ContextMessage] = []
        for (_, r), ch in self._channels.items():
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
        keys: list[str] | None = None,
    ) -> None:
        """
        Register *agent_id* to be notified on writes. keys=None subscribes
        to every key in *namespace*.
        """
        subs = self._ns_subs(namespace)
        subs[agent_id] = Subscription(
            agent_id=agent_id,
            namespace=namespace,
            keys=set(keys) if keys else set(),
            callback=callback,
        )

    def unsubscribe(self, agent_id: str, namespace: str = "default") -> None:
        self._ns_subs(namespace).pop(agent_id, None)

    # -----------------------------------------------------------------------
    # Maintenance
    # -----------------------------------------------------------------------

    async def cleanup_expired(self, namespace: str = "default") -> int:
        async with self._lock:
            entries = self._ns_entries(namespace)
            expired = [k for k, e in entries.items() if e.is_expired()]
            for k in expired:
                del entries[k]
            return len(expired)

    async def snapshot(self, namespace: str = "default") -> dict[str, Any]:
        await self.cleanup_expired(namespace)
        async with self._lock:
            entries = self._ns_entries(namespace)
            return copy.deepcopy({k: e.to_dict() for k, e in entries.items()})

    async def merge(
        self, other: ContextStore, namespace: str = "default"
    ) -> int:
        """Merge *other*'s entries into this store (last-write-wins by version)."""
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

    async def stats(self) -> dict[str, Any]:
        async with self._lock:
            ns_stats: dict[str, Any] = {}
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

    async def to_dict(self) -> dict[str, Any]:
        async with self._lock:
            entries_dict: dict[str, Any] = {}
            for ns, entries in self._entries.items():
                entries_dict[ns] = {
                    k: e.to_dict()
                    for k, e in entries.items()
                    if not e.is_expired()
                }

        channels_dict: dict[str, Any] = {}
        for (s, r), ch in self._channels.items():
            async with ch._lock:
                channels_dict[f"{s}->{r}"] = [m.to_dict() for m in ch._queue]

        subs_dict: dict[str, Any] = {
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
        return json.dumps(await self.to_dict(), indent=indent, ensure_ascii=False)


# ============================================================================
# ContextNamespace — namespace-scoped proxy view
# ============================================================================


class ContextNamespace:
    """Proxy view of ContextStore that fixes the *namespace* parameter."""

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
        ttl: int | None = None,
        tags: list[str] | None = None,
    ) -> ContextRecord:
        return await self._store.put(
            key, value, writer, self._namespace, ttl, tags
        )

    async def get(self, key: str) -> Any | None:
        return await self._store.get(key, self._namespace)

    async def get_entry(self, key: str) -> ContextEntry | None:
        return await self._store.get_entry(key, self._namespace)

    async def delete(self, key: str, writer: str) -> bool:
        return await self._store.delete(key, writer, self._namespace)

    async def history(
        self, key: str, limit: int | None = None
    ) -> list[ContextRecord]:
        return await self._store.history(key, self._namespace, limit)

    async def keys(self) -> list[str]:
        return await self._store.keys(self._namespace)

    async def query(
        self,
        *,
        writer: str | None = None,
        tags: list[str] | None = None,
        since: str | None = None,
        until: str | None = None,
    ) -> list[ContextEntry]:
        return await self._store.query(
            self._namespace, writer=writer, tags=tags, since=since, until=until
        )

    # Pub/Sub API

    def subscribe(
        self,
        agent_id: str,
        callback: Callable,
        keys: list[str] | None = None,
    ) -> None:
        self._store.subscribe(agent_id, callback, self._namespace, keys)

    def unsubscribe(self, agent_id: str) -> None:
        self._store.unsubscribe(agent_id, self._namespace)

    # Maintenance / serialisation

    async def cleanup_expired(self) -> int:
        return await self._store.cleanup_expired(self._namespace)

    async def snapshot(self) -> dict[str, Any]:
        return await self._store.snapshot(self._namespace)

    async def merge(self, other: ContextStore) -> int:
        return await self._store.merge(other, self._namespace)
