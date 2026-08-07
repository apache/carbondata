from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import posixpath
import tempfile
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from uuid import uuid4


FORMAT = "AI_carbon"
SCHEMA_VERSION = 1


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")


def _json_line(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _safe_path(value: str) -> str:
    value = value.replace("\\", "/").strip("/")
    if not value or value.startswith("/") or any(part in ("", ".", "..") for part in value.split("/")):
        raise ValueError("artifact path must be a non-empty relative path")
    return value


class InvalidArchive(ValueError):
    pass


class ContextError(ValueError):
    pass


@dataclass(frozen=True)
class Artifact:
    id: str
    path: str
    content_path: str
    context_path: str
    revision: int
    size: int
    sha256: str
    created_at: str
    updated_at: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class OptimizationContext:
    artifact: Artifact
    content: bytes
    context: dict[str, Any]
    conversation: list[dict[str, Any]]
    revisions: list[dict[str, Any]]


class AI_carbon:
    """A self-contained, atomic `.AI_carbon` project archive.

    The archive is a ZIP container containing JSON metadata and generated files:
    ``manifest.json``, ``generated/``, ``contexts/`` and ``conversations/``.
    Mutations are written to a temporary archive and atomically replaced.
    """

    def __init__(self, filename: str | os.PathLike[str], state: dict[str, Any], files: dict[str, bytes]):
        self.filename = Path(filename)
        self._state = state
        self._files = files

    @classmethod
    def create(cls, filename: str | os.PathLike[str], name: str | None = None) -> "AI_carbon":
        path = Path(filename)
        if path.exists():
            raise FileExistsError(path)
        state = {
            "format": FORMAT,
            "schema_version": SCHEMA_VERSION,
            "project": {"id": str(uuid4()), "name": name or path.stem, "created_at": _now(), "updated_at": _now()},
            "artifacts": [],
        }
        archive = cls(path, state, {})
        archive._commit()
        return archive

    @classmethod
    def open(cls, filename: str | os.PathLike[str]) -> "AI_carbon":
        path = Path(filename)
        if not path.exists():
            raise InvalidArchive(
                f"AI_carbon archive not found: {path}. "
                f"Create it first with: python -m Agent_module.ai_carbon create {path}"
            )
        try:
            with zipfile.ZipFile(path, "r") as zf:
                state = json.loads(zf.read("manifest.json"))
                files = {n: zf.read(n) for n in zf.namelist() if n != "manifest.json" and not n.endswith("/")}
        except (OSError, KeyError, zipfile.BadZipFile, json.JSONDecodeError) as exc:
            raise InvalidArchive(f"not a valid {FORMAT} archive: {path}") from exc
        if state.get("format") != FORMAT or state.get("schema_version") != SCHEMA_VERSION:
            raise InvalidArchive("unsupported AI_carbon manifest")
        return cls(path, state, files)

    def _commit(self) -> None:
        self._state["project"]["updated_at"] = _now()
        self.filename.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=f".{self.filename.name}.", suffix=".tmp", dir=self.filename.parent)
        os.close(fd)
        try:
            with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("manifest.json", _json(self._state))
                for name, data in sorted(self._files.items()):
                    zf.writestr(name, data)
            os.replace(tmp, self.filename)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def manifest(self) -> dict[str, Any]:
        return json.loads(json.dumps(self._state))

    def list_files(self) -> list[Artifact]:
        return [Artifact(**item) for item in self._state["artifacts"]]

    def _find(self, artifact: str | Artifact) -> dict[str, Any]:
        key = artifact.id if isinstance(artifact, Artifact) else artifact
        for item in self._state["artifacts"]:
            if item["id"] == key or item["path"] == key:
                return item
        raise KeyError(f"artifact not found: {key}")

    def add_file(self, source: str | os.PathLike[str] | bytes, path: str | None = None, *, context: Mapping[str, Any] | None = None,
                 conversation: Iterable[Mapping[str, Any]] | None = None, metadata: Mapping[str, Any] | None = None,
                 artifact_id: str | None = None) -> Artifact:
        if isinstance(source, (str, os.PathLike)):
            source_path = Path(source)
            data = source_path.read_bytes()
            path = path or source_path.name
        else:
            data = bytes(source)
            if not path:
                raise ValueError("path is required when source is bytes")
        path = _safe_path(path)
        if any(item["path"] == path for item in self._state["artifacts"]):
            raise FileExistsError(f"artifact path already exists: {path}")
        artifact_id = artifact_id or str(uuid4())
        content_path = f"generated/{path}"
        context_path = f"contexts/{artifact_id}/v001.json"
        conversation_path = f"conversations/{artifact_id}.jsonl"
        now = _now()
        context_data = dict(context or {})
        context_data.setdefault("goal", "")
        context_data.setdefault("inputs", [])
        context_data.setdefault("decisions", [])
        context_data.setdefault("constraints", [])
        context_data.setdefault("acceptance_criteria", [])
        messages = [dict(message) for message in (conversation or [])]
        item = {"id": artifact_id, "path": path, "content_path": content_path, "context_path": context_path,
                "revision": 1, "size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                "created_at": now, "updated_at": now, "metadata": dict(metadata or {})}
        self._state["artifacts"].append(item)
        self._files[content_path] = data
        self._files[context_path] = _json({"artifact_id": artifact_id, "revision": 1, "updated_at": now, "context": context_data})
        self._files[conversation_path] = b"".join(_json_line(message) + b"\n" for message in messages)
        self._commit()
        return Artifact(**item)

    def add_text(self, text: str, path: str, **kwargs: Any) -> Artifact:
        return self.add_file(text.encode("utf-8"), path, **kwargs)

    def read_file(self, artifact: str | Artifact) -> bytes:
        item = self._find(artifact)
        return self._files[item["content_path"]]

    def get_context(self, artifact: str | Artifact) -> dict[str, Any]:
        item = self._find(artifact)
        return json.loads(self._files[item["context_path"]].decode("utf-8"))["context"]

    def get_conversation(self, artifact: str | Artifact) -> list[dict[str, Any]]:
        item = self._find(artifact)
        raw = self._files.get(f"conversations/{item['id']}.jsonl", b"")
        return [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]

    def record_message(self, artifact: str | Artifact, role: str, content: str, **metadata: Any) -> None:
        if role not in {"system", "user", "assistant", "tool"}:
            raise ContextError("role must be system, user, assistant or tool")
        item = self._find(artifact)
        message = {"role": role, "content": content, "created_at": _now(), **metadata}
        key = f"conversations/{item['id']}.jsonl"
        self._files[key] = self._files.get(key, b"") + _json_line(message) + b"\n"
        self._commit()

    def optimization_context(self, artifact: str | Artifact) -> OptimizationContext:
        item = self._find(artifact)
        revisions = []
        prefix = f"contexts/{item['id']}/v"
        for name in sorted(n for n in self._files if n.startswith(prefix) and n.endswith(".json")):
            revisions.append(json.loads(self._files[name].decode("utf-8")))
        return OptimizationContext(Artifact(**item), self.read_file(item["id"]), self.get_context(item["id"]), self.get_conversation(item["id"]), revisions)

    def revise_file(self, artifact: str | Artifact, content: str | bytes, *, context: Mapping[str, Any] | None = None,
                    instruction: str = "", conversation: Iterable[Mapping[str, Any]] | None = None) -> Artifact:
        item = self._find(artifact)
        data = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        revision = item["revision"] + 1
        now = _now()
        merged = self.get_context(item["id"])
        if context:
            merged.update(dict(context))
        if instruction:
            merged.setdefault("optimization_history", []).append({"instruction": instruction, "at": now, "from_revision": item["revision"]})
        content_path = f"generated/{item['path']}"
        context_path = f"contexts/{item['id']}/v{revision:03d}.json"
        item.update({"content_path": content_path, "context_path": context_path, "revision": revision, "size": len(data),
                     "sha256": hashlib.sha256(data).hexdigest(), "updated_at": now})
        self._files[content_path] = data
        self._files[context_path] = _json({"artifact_id": item["id"], "revision": revision, "updated_at": now, "context": merged})
        if conversation:
            key = f"conversations/{item['id']}.jsonl"
            self._files[key] = self._files.get(key, b"") + b"".join(_json_line(dict(m)) + b"\n" for m in conversation)
        self._commit()
        return Artifact(**item)

    def optimize(self, artifact: str | Artifact, instruction: str, generator: Callable[[OptimizationContext, str], str | bytes], *, context: Mapping[str, Any] | None = None) -> Artifact:
        """Generate a revision with the complete previous context supplied to the Agent callback."""
        previous = self.optimization_context(artifact)
        result = generator(previous, instruction)
        return self.revise_file(artifact, result, context=context, instruction=instruction,
                                conversation=[{"role": "user", "content": instruction, "created_at": _now()}])

    def close(self) -> None:
        self._commit()


def create(filename: str | os.PathLike[str], name: str | None = None) -> AI_carbon:
    return AI_carbon.create(filename, name)


def open(filename: str | os.PathLike[str]) -> AI_carbon:  # noqa: A001
    return AI_carbon.open(filename)
