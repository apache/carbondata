from __future__ import annotations

import argparse
from pathlib import Path

from .core import AI_carbon, InvalidArchive
from .web import serve


def _resolve_archive(value: str) -> str:
    """Accept both `project.AI_carbon` and `.AI_carbon/project.AI_carbon`."""
    path = Path(value)
    if path.exists() or path.parent != Path("."):
        return str(path)
    default_path = Path(".AI_carbon") / path.name
    return str(default_path) if default_path.exists() else str(path)


def _ensure_web_archive(value: str) -> str:
    path = Path(_resolve_archive(value))
    if not path.exists():
        AI_carbon.create(path, path.stem)
        print(f"Created new AI_carbon archive: {path}", flush=True)
    return str(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage .AI_carbon project archives")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create"); create.add_argument("archive"); create.add_argument("--name")
    show = sub.add_parser("show"); show.add_argument("archive")
    web = sub.add_parser("web"); web.add_argument("archive", nargs="?"); web.add_argument("--host", default="127.0.0.1"); web.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        if args.command == "create":
            AI_carbon.create(args.archive, args.name); print(args.archive)
        elif args.command == "show":
            archive = AI_carbon.open(_resolve_archive(args.archive))
            for artifact in archive.list_files():
                print(f"{artifact.path}\t revision={artifact.revision}\t context={artifact.context_path}")
        else:
            serve(_ensure_web_archive(args.archive), args.host, args.port) if args.archive else serve(None, args.host, args.port)
    except (InvalidArchive, FileExistsError, OSError) as exc:
        parser.exit(2, f"AI_carbon error: {exc}\n")


if __name__ == "__main__":
    main()
