from __future__ import annotations

import io
import json
import unittest

from granola_local_archive.mcp_server import StdioMCPServer
from granola_local_archive.mcp_server import _validate_date


class DummyRouter:
    def call_tool(self, name, arguments):
        if name == "list_folders":
            return [{"id": "folder-1", "title": "Folder 1"}]
        return {"name": name, "arguments": arguments or {}}


class DateValidatingRouter:
    """Minimal router that applies the same date validation as ToolRouter."""

    def call_tool(self, name, arguments):
        arguments = arguments or {}
        _validate_date(arguments.get("date_from"), "date_from")
        _validate_date(arguments.get("date_to"), "date_to")
        return {}


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
        """Impossible calendar dates (2026-02-31, 2026-13-01) must be rejected with isError=true."""
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
                    DateValidatingRouter(),
                    input_stream=input_stream,
                    output_stream=output_stream,
                ).run()

                response = json.loads(output_stream.getvalue().decode("utf-8").strip())
                self.assertTrue(response["result"]["isError"])


if __name__ == "__main__":
    unittest.main()
