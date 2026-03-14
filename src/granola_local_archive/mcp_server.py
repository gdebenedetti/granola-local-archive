from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import date as _date
from typing import BinaryIO
from typing import Any

from . import __version__
from .config import ArchiveConfig
from .index import ArchiveDatabase
from .storage import load_manifest


def _validate_date(value: str | None, param_name: str) -> None:
    """Raise ValueError when *value* is provided but not a real ISO 8601 calendar date."""
    if value is None:
        return
    try:
        _date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{param_name} must be a real ISO 8601 date (YYYY-MM-DD), got {value!r}")


TOOLS = [
    {
        "name": "search_meetings",
        "description": "Search meetings by note content, transcript content, title, attendees, or folder",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "folder": {"type": "string"},
                "date_from": {"type": "string", "format": "date", "description": "ISO 8601 date (YYYY-MM-DD)"},
                "date_to": {"type": "string", "format": "date", "description": "ISO 8601 date (YYYY-MM-DD)"},
                "has_transcript": {"type": "boolean"},
                "limit": {"type": "integer", "default": 10, "maximum": 50},
            },
            "required": ["query"],
        },
    },
    {
        "name": "list_meetings",
        "description": "List meetings with strict folder/date filters. Use this first before summarizing a folder or time window.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "folder": {"type": "string"},
                "date_from": {"type": "string", "format": "date", "description": "ISO 8601 date (YYYY-MM-DD)"},
                "date_to": {"type": "string", "format": "date", "description": "ISO 8601 date (YYYY-MM-DD)"},
                "has_transcript": {"type": "boolean"},
                "limit": {"type": "integer", "default": 25, "maximum": 100},
            },
        },
    },
    {
        "name": "get_meeting",
        "description": "Return normalized metadata and notes for a meeting. Use the `meeting_id` value returned by `list_meetings` or `search_meetings`.",
        "inputSchema": {
            "type": "object",
            "properties": {"meeting_id": {"type": "string"}},
            "required": ["meeting_id"],
        },
    },
    {
        "name": "get_meeting_transcript",
        "description": "Return transcript metadata or the full transcript for a meeting. When full=false (the default), the response includes an `is_truncated` flag and `full_length` so you can decide whether to re-fetch with full=true.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "meeting_id": {"type": "string"},
                "full": {"type": "boolean", "default": False},
            },
            "required": ["meeting_id"],
        },
    },
    {
        "name": "list_folders",
        "description": "List all Granola folders plus the synthetic Unlisted bucket (id='__unlisted__'). Meetings that have not been assigned to any folder appear under '__unlisted__'. Use `search_unlisted` to search that bucket.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_folder",
        "description": "Return folder metadata and recent meetings",
        "inputSchema": {
            "type": "object",
            "properties": {"folder_id_or_title": {"type": "string"}},
            "required": ["folder_id_or_title"],
        },
    },
    {
        "name": "search_folder",
        "description": "Run a search constrained to a folder, optionally bounded by date range or transcript availability",
        "inputSchema": {
            "type": "object",
            "properties": {
                "folder_id_or_title": {"type": "string"},
                "query": {"type": "string"},
                "date_from": {"type": "string", "format": "date", "description": "ISO 8601 date (YYYY-MM-DD)"},
                "date_to": {"type": "string", "format": "date", "description": "ISO 8601 date (YYYY-MM-DD)"},
                "has_transcript": {"type": "boolean"},
                "limit": {"type": "integer", "default": 10, "maximum": 50},
            },
            "required": ["folder_id_or_title", "query"],
        },
    },
    {
        "name": "search_evidence",
        "description": "Return exact snippets from meeting notes and transcript segments. Use this before claiming decisions, people, dates, or next steps.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "meeting_id": {"type": "string"},
                "folder": {"type": "string"},
                "date_from": {"type": "string", "format": "date", "description": "ISO 8601 date (YYYY-MM-DD)"},
                "date_to": {"type": "string", "format": "date", "description": "ISO 8601 date (YYYY-MM-DD)"},
                "limit": {"type": "integer", "default": 10, "maximum": 50},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_unlisted",
        "description": "Search documents that are not assigned to any Granola folder",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 10, "maximum": 50},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_folder_attachments",
        "description": "Return attachment metadata for a folder",
        "inputSchema": {
            "type": "object",
            "properties": {"folder_id_or_title": {"type": "string"}},
            "required": ["folder_id_or_title"],
        },
    },
    {
        "name": "stats",
        "description": "Return counts and last sync metadata for the local Granola MCP index",
        "inputSchema": {"type": "object", "properties": {}},
    },
]

SUPPORTED_PROTOCOL_VERSIONS = (
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
)


class MessageParseError(Exception):
    """Raised when an incoming JSON-RPC frame cannot be parsed."""

    def __init__(self, code: int = -32700, message: str = "Parse error"):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(slots=True)
class ToolRouter:
    config: ArchiveConfig
    database: ArchiveDatabase

    def call_tool(self, name: str, arguments: dict[str, Any] | None) -> Any:
        arguments = arguments or {}
        if name == "search_meetings":
            _validate_date(arguments.get("date_from"), "date_from")
            _validate_date(arguments.get("date_to"), "date_to")
            return self.database.search_meetings(
                query=arguments.get("query", ""),
                folder=arguments.get("folder"),
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
                has_transcript=arguments.get("has_transcript"),
                limit=int(arguments.get("limit", 10)),
            )
        if name == "list_meetings":
            _validate_date(arguments.get("date_from"), "date_from")
            _validate_date(arguments.get("date_to"), "date_to")
            return self.database.list_meetings(
                folder=arguments.get("folder"),
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
                has_transcript=arguments.get("has_transcript"),
                limit=int(arguments.get("limit", 25)),
            )
        if name == "get_meeting":
            return self.database.get_meeting(arguments["meeting_id"])
        if name == "get_meeting_transcript":
            return self.database.get_meeting_transcript(
                arguments["meeting_id"],
                full=bool(arguments.get("full", False)),
            )
        if name == "list_folders":
            return self.database.list_folders()
        if name == "get_folder":
            return self.database.get_folder(arguments["folder_id_or_title"])
        if name == "search_folder":
            _validate_date(arguments.get("date_from"), "date_from")
            _validate_date(arguments.get("date_to"), "date_to")
            return self.database.search_folder_with_filters(
                arguments["folder_id_or_title"],
                arguments["query"],
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
                has_transcript=arguments.get("has_transcript"),
                limit=int(arguments.get("limit", 10)),
            )
        if name == "search_evidence":
            _validate_date(arguments.get("date_from"), "date_from")
            _validate_date(arguments.get("date_to"), "date_to")
            return self.database.search_evidence(
                query=arguments["query"],
                meeting_id=arguments.get("meeting_id"),
                folder=arguments.get("folder"),
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
                limit=int(arguments.get("limit", 10)),
            )
        if name == "search_unlisted":
            return self.database.search_unlisted(
                arguments["query"],
                limit=int(arguments.get("limit", 10)),
            )
        if name == "get_folder_attachments":
            return self.database.get_folder_attachments(arguments["folder_id_or_title"])
        if name == "stats":
            return self.database.stats(manifest=load_manifest(self.config))
        raise KeyError(f"unknown tool {name}")


class StdioMCPServer:
    def __init__(
        self,
        router: ToolRouter,
        input_stream: BinaryIO | None = None,
        output_stream: BinaryIO | None = None,
    ):
        self.router = router
        self.input_stream = input_stream or sys.stdin.buffer
        self.output_stream = output_stream or sys.stdout.buffer
        self._shutdown_requested = False
        self._transport = "ndjson"
        self._protocol_version: str | None = None
        self._initialized = False

    def run(self) -> None:
        while True:
            try:
                message = self._read_message()
            except MessageParseError as exc:
                self._write_message(_error(None, exc.code, exc.message))
                continue
            if message is None:
                return
            responses = self._dispatch_message(message)
            for response in responses:
                if response is not None:
                    self._write_message(response)
            if self._shutdown_requested:
                return

    def _dispatch_message(self, message: dict[str, Any] | list[Any]) -> list[dict[str, Any] | None]:
        if isinstance(message, list):
            if not self._batch_requests_supported():
                return [_error(None, -32600, "Invalid Request")]
            if not message:
                return [_error(None, -32600, "Invalid Request")]
            responses: list[dict[str, Any] | None] = []
            for item in message:
                if not isinstance(item, dict):
                    responses.append(_error(None, -32600, "Invalid Request"))
                    continue
                responses.append(self._handle_message(item))
            return responses
        return [self._handle_message(message)]

    def _handle_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        method = message.get("method")
        message_id = message.get("id")
        params = message.get("params") or {}

        if method == "initialize":
            if self._protocol_version is not None:
                return _error(message_id, -32600, "Invalid Request")
            requested_version = params.get("protocolVersion")
            protocol_version = _negotiate_protocol_version(requested_version)
            self._protocol_version = protocol_version
            return _result(
                message_id,
                {
                    "protocolVersion": protocol_version,
                    "capabilities": {
                        "tools": {"listChanged": False},
                    },
                    "serverInfo": {
                        "name": "granola-local-mcp",
                        "title": "Granola Local MCP",
                        "version": __version__,
                    },
                },
            )
        if method == "notifications/initialized":
            if self._protocol_version is not None:
                self._initialized = True
            return None
        if method == "notifications/cancelled":
            return None
        if self._protocol_version is None:
            if message_id is None:
                return None
            return _error(message_id, -32600, "Server not initialized")
        if not self._initialized:
            if method == "ping":
                return _result(message_id, {})
            if message_id is None:
                return None
            return _error(message_id, -32600, "Server initialization incomplete")
        if method == "ping":
            return _result(message_id, {})
        if method == "shutdown":
            self._shutdown_requested = True
            return _result(message_id, {})
        if method == "tools/list":
            return _result(message_id, {"tools": TOOLS})
        if method == "prompts/list":
            return _result(message_id, {"prompts": []})
        if method == "resources/list":
            return _result(message_id, {"resources": []})
        if method == "resources/templates/list":
            return _result(message_id, {"resourceTemplates": []})
        if method == "tools/call":
            tool_name = params.get("name")
            if not tool_name:
                return _result(
                    message_id,
                    _tool_payload({"error": "tools/call requires a 'name' parameter"}, is_error=True),
                )
            try:
                payload = self.router.call_tool(tool_name, params.get("arguments"))
                return _result(message_id, _tool_payload(payload))
            except Exception as exc:
                return _result(message_id, _tool_payload({"error": str(exc)}, is_error=True))
        if message_id is None:
            return None
        return _error(message_id, -32601, f"method {method!r} not found")

    def _read_message(self) -> dict[str, Any] | None:
        while True:
            line = self.input_stream.readline()
            if not line:
                return None
            if line in {b"\r\n", b"\n"}:
                continue
            stripped = line.strip()
            if stripped.startswith((b"{", b"[")):
                self._transport = "ndjson"
                try:
                    return json.loads(stripped.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise MessageParseError() from exc
            try:
                headers = self._read_headers(line)
                content_length = int(headers["content-length"])
                payload = self.input_stream.read(content_length)
            except (KeyError, ValueError) as exc:
                raise MessageParseError() from exc
            self._transport = "content-length"
            try:
                return json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MessageParseError() from exc

    def _read_headers(self, first_line: bytes) -> dict[str, str]:
        headers: dict[str, str] = {}
        line = first_line
        while True:
            if line in {b"\r\n", b"\n"}:
                break
            name, value = line.decode("utf-8").split(":", 1)
            headers[name.lower()] = value.strip()
            line = self.input_stream.readline()
            if not line:
                break
        return headers

    def _batch_requests_supported(self) -> bool:
        return self._initialized and self._protocol_version in {"2025-03-26", "2024-11-05"}

    def _write_message(self, message: dict[str, Any]) -> None:
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if self._transport == "content-length":
            self.output_stream.write(f"Content-Length: {len(payload)}\r\n\r\n".encode("utf-8"))
            self.output_stream.write(payload)
        else:
            self.output_stream.write(payload + b"\n")
        self.output_stream.flush()


def _tool_payload(result: Any, is_error: bool = False) -> dict[str, Any]:
    structured = _normalize_tool_result(result)
    return {
        "content": [{"type": "text", "text": json.dumps(structured, indent=2, ensure_ascii=False)}],
        "structuredContent": structured,
        "isError": is_error,
    }


def _normalize_tool_result(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return result
    if isinstance(result, list):
        return {"items": result}
    return {"value": result}


def _result(message_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def _error(message_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "error": {"code": code, "message": message}}


def _negotiate_protocol_version(requested_version: str | None) -> str:
    if requested_version in SUPPORTED_PROTOCOL_VERSIONS:
        return str(requested_version)
    return SUPPORTED_PROTOCOL_VERSIONS[0]
