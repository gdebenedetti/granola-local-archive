#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
from urllib.parse import quote


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a Cursor MCP install deeplink for this local checkout."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Project root containing ops/run-mcp.sh",
    )
    parser.add_argument(
        "--name",
        default="granola-local",
        help="Cursor MCP server name",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print only the cursor:// deeplink",
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Print a Markdown badge link instead of the plain URL",
    )
    return parser


def build_deeplink(root: Path, name: str) -> str:
    run_mcp = root.resolve() / "ops" / "run-mcp.sh"
    if not run_mcp.exists():
        raise SystemExit(f"Missing MCP launcher: {run_mcp}")

    config = {
        "command": "/bin/zsh",
        "args": [str(run_mcp)],
    }
    encoded = base64.b64encode(
        json.dumps(config, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).decode("ascii")
    return (
        "cursor://anysphere.cursor-deeplink/mcp/install"
        f"?name={quote(name)}&config={quote(encoded)}"
    )


def build_markdown(link: str, name: str) -> str:
    alt = f"Add {name} to Cursor"
    badge = (
        "https://img.shields.io/badge/Add%20to-Cursor-111111"
        "?style=for-the-badge"
    )
    return f"[![{alt}]({badge})]({link})"


def main() -> int:
    args = build_parser().parse_args()
    deeplink = build_deeplink(args.root, args.name)

    if args.raw:
        print(deeplink)
        return 0

    if args.markdown:
        print(build_markdown(deeplink, args.name))
        return 0

    print("Cursor install link:")
    print(deeplink)
    print()
    print("Markdown button:")
    print(build_markdown(deeplink, args.name))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
