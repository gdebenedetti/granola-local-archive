from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from .config import ArchiveConfig
from .index import ArchiveDatabase
from .mcp_server import StdioMCPServer, ToolRouter
from .storage import load_manifest
from .syncer import SyncService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="granola-local-archive")
    parser.add_argument("--workspace", type=Path, default=Path.cwd(), help="Project root for archive state and outputs")
    parser.add_argument("--granola-dir", type=Path, default=None, help="Granola application support directory")

    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync", help="Run an hourly or daily sync")
    sync_parser.add_argument("--mode", choices=("hourly", "daily"), default="hourly")
    sync_parser.add_argument("--force", action="store_true")

    queue_parser = subparsers.add_parser("hydrate-queue", help="Print the current transcript hydration queue")
    queue_parser.add_argument("--limit", type=int, default=25)

    import_parser = subparsers.add_parser(
        "import-transcript",
        help="Import a manually copied transcript so it stays archived locally",
    )
    reference_group = import_parser.add_mutually_exclusive_group(required=True)
    reference_group.add_argument("--meeting-id", help="Exact meeting id")
    reference_group.add_argument("--meeting", help="Meeting title or partial title")
    import_parser.add_argument("--created-at", help="Optional YYYY-MM-DD filter to disambiguate title matches")
    import_parser.add_argument("--file", type=Path, help="Read transcript text from a file")
    import_parser.add_argument("--from-clipboard", action="store_true", help="Read transcript text from the macOS clipboard")
    import_parser.add_argument("--source", default="manual_copy", help="Short label stored as transcript source")

    subparsers.add_parser("stats", help="Print archive counts and last snapshot metadata")
    subparsers.add_parser("serve-mcp", help="Run the local MCP server over stdio")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = ArchiveConfig.from_project_root(args.workspace, args.granola_dir)

    if args.command == "sync":
        result = SyncService(config).sync(mode=args.mode, force=args.force)
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        return 0

    if args.command == "hydrate-queue":
        database = ArchiveDatabase(config)
        try:
            payload = {
                "generated_at": load_manifest(config).get("last_checked_at"),
                "items": database.build_hydrate_queue(
                    limit=args.limit,
                    priority_titles=config.priority_folder_titles,
                ),
            }
            print(json.dumps(payload, indent=2, ensure_ascii=False))
        finally:
            database.close()
        return 0

    if args.command == "import-transcript":
        raw_text = _read_transcript_input(args)
        database = ArchiveDatabase(config)
        try:
            if args.meeting_id:
                meeting = database.resolve_meeting(args.meeting_id)
            else:
                meeting = database.resolve_meeting(args.meeting, created_at=args.created_at)
        finally:
            database.close()
        result = SyncService(config).import_manual_transcript(
            meeting_id=meeting["id"],
            raw_text=raw_text,
            source=args.source,
        )
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        return 0

    if args.command == "stats":
        database = ArchiveDatabase(config)
        try:
            print(json.dumps(database.stats(manifest=load_manifest(config)), indent=2, ensure_ascii=False))
        finally:
            database.close()
        return 0

    if args.command == "serve-mcp":
        database = ArchiveDatabase(config)
        try:
            StdioMCPServer(ToolRouter(config=config, database=database)).run()
        finally:
            database.close()
        return 0

    parser.print_help(sys.stderr)
    return 2


def _read_transcript_input(args: argparse.Namespace) -> str:
    if args.from_clipboard:
        result = subprocess.run(["pbpaste"], check=True, capture_output=True, text=True)
        return result.stdout
    if args.file:
        return args.file.read_text(encoding="utf-8")
    return sys.stdin.read()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
