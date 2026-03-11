from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path

from granola_local_archive.config import ArchiveConfig
from granola_local_archive.index import ArchiveDatabase
from granola_local_archive.mcp_server import StdioMCPServer
from granola_local_archive.mcp_server import ToolRouter
from granola_local_archive.syncer import SyncService


class DummyRouter:
    def call_tool(self, name, arguments):
        if name == "list_folders":
            return [{"id": "folder-1", "title": "Folder 1"}]
        return {"name": name, "arguments": arguments or {}}


def _write_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _build_tool_router(project: Path, granola: Path) -> tuple[ToolRouter, ArchiveDatabase]:
    """Build a minimal archive and return a real ToolRouter backed by it."""
    cache_path = granola / "cache-v4.json"
    _write_cache(
        cache_path,
        {
            "cache": {
                "state": {
                    "documents": {
                        "meeting-a": {
                            "id": "meeting-a",
                            "title": "Roadmap Review",
                            "created_at": "2026-03-09T10:00:00Z",
                            "updated_at": "2026-03-09T10:00:00Z",
                            "valid_meeting": True,
                            "transcribe": False,
                            "notes_markdown": "Roadmap.",
                        }
                    },
                    "transcripts": {},
                    "documentLists": {},
                    "documentListsMetadata": {},
                    "documentListsAttachments": {},
                    "meetingsMetadata": {},
                }
            }
        },
    )
    config = ArchiveConfig.from_project_root(project, granola)
    SyncService(config).sync(mode="hourly")
    database = ArchiveDatabase(config)
    return ToolRouter(config=config, database=database), database


class MCPServerTransportTests(unittest.TestCase):
    def test_ndjson_stdio_protocol(self) -> None:
        input_stream = io.BytesIO(
            (
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {"protocolVersion": "2025-03-26"},
                    }
                )
                + "\n"
                + json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
                + "\n"
            ).encode("utf-8")
        )
        output_stream = io.BytesIO()

        StdioMCPServer(DummyRouter(), input_stream=input_stream, output_stream=output_stream).run()

        responses = [
            json.loads(line)
            for line in output_stream.getvalue().decode("utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(responses[0]["result"]["protocolVersion"], "2025-03-26")
        self.assertIn("tools", responses[1]["result"])
        self.assertGreater(len(responses[1]["result"]["tools"]), 0)

    def test_content_length_protocol_remains_supported(self) -> None:
        request = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2024-11-05"},
            },
            separators=(",", ":"),
        ).encode("utf-8")
        input_stream = io.BytesIO(
            f"Content-Length: {len(request)}\r\n\r\n".encode("utf-8") + request
        )
        output_stream = io.BytesIO()

        StdioMCPServer(DummyRouter(), input_stream=input_stream, output_stream=output_stream).run()

        raw = output_stream.getvalue()
        header, payload = raw.split(b"\r\n\r\n", 1)
        self.assertIn(b"Content-Length:", header)
        response = json.loads(payload.decode("utf-8"))
        self.assertEqual(response["result"]["protocolVersion"], "2024-11-05")

    def test_tool_results_wrap_list_payloads_for_cursor_compatibility(self) -> None:
        input_stream = io.BytesIO(
            (
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "list_folders", "arguments": {}},
                    }
                )
                + "\n"
            ).encode("utf-8")
        )
        output_stream = io.BytesIO()

        StdioMCPServer(DummyRouter(), input_stream=input_stream, output_stream=output_stream).run()

        response = json.loads(output_stream.getvalue().decode("utf-8").strip())
        structured = response["result"]["structuredContent"]
        self.assertIsInstance(structured, dict)
        self.assertEqual(structured["items"][0]["title"], "Folder 1")


    def test_tools_call_without_name_returns_error(self) -> None:
        """tools/call that omits 'name' must return isError=true with a readable message."""
        input_stream = io.BytesIO(
            (
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"arguments": {}},
                    }
                )
                + "\n"
            ).encode("utf-8")
        )
        output_stream = io.BytesIO()

        StdioMCPServer(DummyRouter(), input_stream=input_stream, output_stream=output_stream).run()

        response = json.loads(output_stream.getvalue().decode("utf-8").strip())
        self.assertTrue(response["result"]["isError"])
        error_text = response["result"]["content"][0]["text"]
        self.assertIn("name", error_text.lower())

    def test_invalid_calendar_dates_rejected_via_mcp(self) -> None:
        """Impossible calendar dates must be rejected through the real ToolRouter dispatch path."""
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            router, database = _build_tool_router(Path(project_root), Path(granola_root))
            try:
                for bad_date in ("2026-02-31", "2026-13-01", "2025-02-29"):
                    with self.subTest(date=bad_date):
                        input_stream = io.BytesIO(
                            (
                                json.dumps(
                                    {
                                        "jsonrpc": "2.0",
                                        "id": 1,
                                        "method": "tools/call",
                                        "params": {
                                            "name": "list_meetings",
                                            "arguments": {"date_from": bad_date},
                                        },
                                    }
                                )
                                + "\n"
                            ).encode("utf-8")
                        )
                        output_stream = io.BytesIO()

                        StdioMCPServer(
                            router,
                            input_stream=input_stream,
                            output_stream=output_stream,
                        ).run()

                        response = json.loads(output_stream.getvalue().decode("utf-8").strip())
                        self.assertTrue(response["result"]["isError"])
            finally:
                database.close()


if __name__ == "__main__":
    unittest.main()
