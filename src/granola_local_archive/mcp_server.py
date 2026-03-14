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


def _object_schema(
    properties: dict[str, Any],
    *,
    required: tuple[str, ...] = (),
    additional_properties: bool = False,
) -> dict[str, Any]:
    schema = {
        "type": "object",
        "properties": properties,
        "additionalProperties": additional_properties,
    }
    if required:
        schema["required"] = list(required)
    return schema


def _array_schema(items: dict[str, Any]) -> dict[str, Any]:
    return {"type": "array", "items": items}


def _nullable_schema(schema: dict[str, Any]) -> dict[str, Any]:
    if isinstance(schema.get("type"), str):
        updated = dict(schema)
        updated["type"] = [schema["type"], "null"]
        return updated
    return {"anyOf": [schema, {"type": "null"}]}


def _tool_definition(
    *,
    name: str,
    title: str,
    description: str,
    input_schema: dict[str, Any],
    output_schema: dict[str, Any],
) -> dict[str, Any]:
    return {
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": input_schema,
        "outputSchema": output_schema,
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    }


DATE_PARAM_SCHEMA = {
    "type": "string",
    "format": "date",
    "description": "ISO 8601 date (YYYY-MM-DD)",
}
STRING_PARAM_SCHEMA = {"type": "string"}
BOOLEAN_PARAM_SCHEMA = {"type": "boolean"}
QUERY_PARAM_SCHEMA = {"type": "string", "description": "Query text with one or more searchable terms."}
MEETING_ID_PARAM_SCHEMA = {"type": "string", "description": "Meeting id returned by search or list tools."}
FOLDER_REF_PARAM_SCHEMA = {"type": "string", "description": "Folder id or exact folder title."}
LIMIT_10_SCHEMA = {"type": "integer", "default": 10, "maximum": 50}
LIMIT_25_SCHEMA = {"type": "integer", "default": 25, "maximum": 100}
NULLABLE_STRING_SCHEMA = {"type": ["string", "null"]}
NULLABLE_BOOLEAN_SCHEMA = {"type": ["boolean", "null"]}
STRING_ARRAY_SCHEMA = _array_schema({"type": "string"})
ANY_OBJECT_SCHEMA = {"type": "object", "additionalProperties": True}

SEARCH_ROW_PROPERTIES = {
    "meeting_id": {"type": "string"},
    "title": {"type": "string"},
    "created_at": NULLABLE_STRING_SCHEMA,
    "updated_at": NULLABLE_STRING_SCHEMA,
    "transcript_segment_count": {"type": "integer"},
    "folder_titles": STRING_ARRAY_SCHEMA,
    "attendees": STRING_ARRAY_SCHEMA,
    "notes_snippet": {"type": "string"},
    "transcript_snippet": {"type": "string"},
    "score": {"type": "number"},
}
SEARCH_ROW_SCHEMA = _object_schema(
    SEARCH_ROW_PROPERTIES,
    required=(
        "meeting_id",
        "title",
        "created_at",
        "updated_at",
        "transcript_segment_count",
        "folder_titles",
        "attendees",
        "notes_snippet",
        "transcript_snippet",
        "score",
    ),
)
SEARCH_RESULTS_SCHEMA = _object_schema({"items": _array_schema(SEARCH_ROW_SCHEMA)}, required=("items",))

FILTERS_PROPERTIES = {
    "meeting_id": NULLABLE_STRING_SCHEMA,
    "folder_id": NULLABLE_STRING_SCHEMA,
    "folder_title": NULLABLE_STRING_SCHEMA,
    "date_from": NULLABLE_STRING_SCHEMA,
    "date_to": NULLABLE_STRING_SCHEMA,
    "has_transcript": NULLABLE_BOOLEAN_SCHEMA,
}
FILTERS_SCHEMA = _object_schema(
    FILTERS_PROPERTIES,
    required=("meeting_id", "folder_id", "folder_title", "date_from", "date_to", "has_transcript"),
)

RECENT_MEETING_SCHEMA = _object_schema(
    {
        "id": {"type": "string"},
        "title": {"type": "string"},
        "created_at": NULLABLE_STRING_SCHEMA,
        "transcript_segment_count": {"type": "integer"},
    },
    required=("id", "title", "created_at", "transcript_segment_count"),
)

FOLDER_SUMMARY_PROPERTIES = {
    "id": {"type": "string"},
    "title": {"type": "string"},
    "description": NULLABLE_STRING_SCHEMA,
    "document_count": {"type": "integer"},
    "updated_at": NULLABLE_STRING_SCHEMA,
    "is_space": {"type": "integer"},
}
FOLDER_SUMMARY_SCHEMA = _object_schema(
    FOLDER_SUMMARY_PROPERTIES,
    required=("id", "title", "description", "document_count", "updated_at", "is_space"),
)
FOLDER_DETAIL_PROPERTIES = {
    "id": {"type": "string"},
    "title": {"type": "string"},
    "description": NULLABLE_STRING_SCHEMA,
    "workspace_id": NULLABLE_STRING_SCHEMA,
    "workspace_display_name": NULLABLE_STRING_SCHEMA,
    "company_domain": NULLABLE_STRING_SCHEMA,
    "created_at": NULLABLE_STRING_SCHEMA,
    "updated_at": NULLABLE_STRING_SCHEMA,
    "is_space": {"type": "integer"},
    "is_default_folder": {"type": "integer"},
    "parent_folder_id": NULLABLE_STRING_SCHEMA,
    "document_count": {"type": "integer"},
    "folder_hash": {"type": "string"},
    "folder_sidecar_path": {"type": "string"},
}
FOLDER_DETAIL_SCHEMA = _object_schema(
    FOLDER_DETAIL_PROPERTIES,
    required=tuple(FOLDER_DETAIL_PROPERTIES),
)
FOLDER_WITH_RECENT_MEETINGS_SCHEMA = _object_schema(
    {
        **FOLDER_DETAIL_PROPERTIES,
        "recent_meetings": _array_schema(RECENT_MEETING_SCHEMA),
    },
    required=tuple(FOLDER_DETAIL_PROPERTIES) + ("recent_meetings",),
)

ATTACHMENT_SCHEMA = _object_schema(
    {
        "id": {"type": "string"},
        "name": NULLABLE_STRING_SCHEMA,
        "type": NULLABLE_STRING_SCHEMA,
        "mime_type": NULLABLE_STRING_SCHEMA,
        "content_summary": NULLABLE_STRING_SCHEMA,
        "content_markdown": NULLABLE_STRING_SCHEMA,
        "size_in_bytes": {"type": ["integer", "null"]},
        "created_at": NULLABLE_STRING_SCHEMA,
        "updated_at": NULLABLE_STRING_SCHEMA,
    },
    required=(
        "id",
        "name",
        "type",
        "mime_type",
        "content_summary",
        "content_markdown",
        "size_in_bytes",
        "created_at",
        "updated_at",
    ),
)

EVIDENCE_ITEM_SCHEMA = _object_schema(
    {
        "meeting_id": {"type": "string"},
        "title": {"type": "string"},
        "created_at": NULLABLE_STRING_SCHEMA,
        "updated_at": NULLABLE_STRING_SCHEMA,
        "source_kind": {"type": "string"},
        "snippet": {"type": "string"},
        "matched_terms": STRING_ARRAY_SCHEMA,
        "match_count": {"type": "integer"},
        "start_timestamp": NULLABLE_STRING_SCHEMA,
        "end_timestamp": NULLABLE_STRING_SCHEMA,
        "speaker": NULLABLE_STRING_SCHEMA,
    },
    required=(
        "meeting_id",
        "title",
        "created_at",
        "updated_at",
        "source_kind",
        "snippet",
        "matched_terms",
        "match_count",
    ),
)

TOOLS = [
    _tool_definition(
        name="search_meetings",
        title="Search Meetings",
        description="Search meetings by note content, transcript content, title, attendees, or folder.",
        input_schema=_object_schema(
            {
                "query": QUERY_PARAM_SCHEMA,
                "folder": STRING_PARAM_SCHEMA,
                "date_from": DATE_PARAM_SCHEMA,
                "date_to": DATE_PARAM_SCHEMA,
                "has_transcript": BOOLEAN_PARAM_SCHEMA,
                "limit": LIMIT_10_SCHEMA,
            },
            required=("query",),
        ),
        output_schema=SEARCH_RESULTS_SCHEMA,
    ),
    _tool_definition(
        name="list_meetings",
        title="List Meetings",
        description="List meetings with strict folder and date filters before summarizing a time window.",
        input_schema=_object_schema(
            {
                "folder": STRING_PARAM_SCHEMA,
                "date_from": DATE_PARAM_SCHEMA,
                "date_to": DATE_PARAM_SCHEMA,
                "has_transcript": BOOLEAN_PARAM_SCHEMA,
                "limit": LIMIT_25_SCHEMA,
            }
        ),
        output_schema=_object_schema(
            {
                "filters": FILTERS_SCHEMA,
                "items": _array_schema(SEARCH_ROW_SCHEMA),
            },
            required=("filters", "items"),
        ),
    ),
    _tool_definition(
        name="get_meeting",
        title="Get Meeting",
        description="Return normalized metadata and notes for a meeting id returned by list or search tools.",
        input_schema=_object_schema(
            {"meeting_id": MEETING_ID_PARAM_SCHEMA},
            required=("meeting_id",),
        ),
        output_schema=_object_schema(
            {
                "id": {"type": "string"},
                "title": {"type": "string"},
                "created_at": NULLABLE_STRING_SCHEMA,
                "updated_at": NULLABLE_STRING_SCHEMA,
                "valid_meeting": {"type": "integer"},
                "transcribe": {"type": "integer"},
                "notes_text": {"type": "string"},
                "notes_markdown": NULLABLE_STRING_SCHEMA,
                "notes_plain": NULLABLE_STRING_SCHEMA,
                "attendees": STRING_ARRAY_SCHEMA,
                "folder_ids": STRING_ARRAY_SCHEMA,
                "folder_titles": STRING_ARRAY_SCHEMA,
                "transcript_segment_count": {"type": "integer"},
                "metadata": ANY_OBJECT_SCHEMA,
            },
            required=(
                "id",
                "title",
                "created_at",
                "updated_at",
                "valid_meeting",
                "transcribe",
                "notes_text",
                "notes_markdown",
                "notes_plain",
                "attendees",
                "folder_ids",
                "folder_titles",
                "transcript_segment_count",
                "metadata",
            ),
        ),
    ),
    _tool_definition(
        name="get_meeting_transcript",
        title="Get Meeting Transcript",
        description="Return transcript metadata or the full transcript for a meeting.",
        input_schema=_object_schema(
            {
                "meeting_id": MEETING_ID_PARAM_SCHEMA,
                "full": {"type": "boolean", "default": False},
            },
            required=("meeting_id",),
        ),
        output_schema=_object_schema(
            {
                "meeting_id": {"type": "string"},
                "title": NULLABLE_STRING_SCHEMA,
                "created_at": NULLABLE_STRING_SCHEMA,
                "segment_count": {"type": ["integer", "null"]},
                "available": {"type": "boolean"},
                "source": NULLABLE_STRING_SCHEMA,
                "preview": {"type": "string"},
                "is_truncated": {"type": "boolean"},
                "full_length": {"type": "integer"},
                "text": {"type": "string"},
                "segments": _array_schema(ANY_OBJECT_SCHEMA),
            },
            required=("meeting_id", "title", "created_at", "available"),
            additional_properties=True,
        ),
    ),
    _tool_definition(
        name="list_folders",
        title="List Folders",
        description="List Granola folders plus the synthetic '__unlisted__' bucket.",
        input_schema=_object_schema({}),
        output_schema=_object_schema({"items": _array_schema(FOLDER_SUMMARY_SCHEMA)}, required=("items",)),
    ),
    _tool_definition(
        name="get_folder",
        title="Get Folder",
        description="Return folder metadata and recent meetings for a folder id or exact title.",
        input_schema=_object_schema(
            {"folder_id_or_title": FOLDER_REF_PARAM_SCHEMA},
            required=("folder_id_or_title",),
        ),
        output_schema=FOLDER_WITH_RECENT_MEETINGS_SCHEMA,
    ),
    _tool_definition(
        name="search_folder",
        title="Search Folder",
        description="Run a search constrained to one folder, with optional date and transcript filters.",
        input_schema=_object_schema(
            {
                "folder_id_or_title": FOLDER_REF_PARAM_SCHEMA,
                "query": QUERY_PARAM_SCHEMA,
                "date_from": DATE_PARAM_SCHEMA,
                "date_to": DATE_PARAM_SCHEMA,
                "has_transcript": BOOLEAN_PARAM_SCHEMA,
                "limit": LIMIT_10_SCHEMA,
            },
            required=("folder_id_or_title", "query"),
        ),
        output_schema=_object_schema(
            {
                "folder": FOLDER_DETAIL_SCHEMA,
                "filters": FILTERS_SCHEMA,
                "results": _array_schema(SEARCH_ROW_SCHEMA),
            },
            required=("folder", "filters", "results"),
        ),
    ),
    _tool_definition(
        name="search_evidence",
        title="Search Evidence",
        description="Return exact snippets from notes and transcript segments before making factual claims.",
        input_schema=_object_schema(
            {
                "query": QUERY_PARAM_SCHEMA,
                "meeting_id": MEETING_ID_PARAM_SCHEMA,
                "folder": STRING_PARAM_SCHEMA,
                "date_from": DATE_PARAM_SCHEMA,
                "date_to": DATE_PARAM_SCHEMA,
                "limit": LIMIT_10_SCHEMA,
            },
            required=("query",),
        ),
        output_schema=_object_schema(
            {
                "query": {"type": "string"},
                "filters": FILTERS_SCHEMA,
                "items": _array_schema(EVIDENCE_ITEM_SCHEMA),
                "meetings_considered": {"type": "integer"},
            },
            required=("query", "filters", "items", "meetings_considered"),
        ),
    ),
    _tool_definition(
        name="search_unlisted",
        title="Search Unlisted Meetings",
        description="Search documents that are not assigned to any Granola folder.",
        input_schema=_object_schema(
            {
                "query": QUERY_PARAM_SCHEMA,
                "limit": LIMIT_10_SCHEMA,
            },
            required=("query",),
        ),
        output_schema=_object_schema(
            {
                "folder": FOLDER_DETAIL_SCHEMA,
                "filters": FILTERS_SCHEMA,
                "results": _array_schema(SEARCH_ROW_SCHEMA),
            },
            required=("folder", "filters", "results"),
        ),
    ),
    _tool_definition(
        name="get_folder_attachments",
        title="Get Folder Attachments",
        description="Return attachment metadata for a folder id or exact title.",
        input_schema=_object_schema(
            {"folder_id_or_title": FOLDER_REF_PARAM_SCHEMA},
            required=("folder_id_or_title",),
        ),
        output_schema=_object_schema(
            {
                "folder": FOLDER_DETAIL_SCHEMA,
                "attachments": _array_schema(ATTACHMENT_SCHEMA),
            },
            required=("folder", "attachments"),
        ),
    ),
    _tool_definition(
        name="stats",
        title="Index Stats",
        description="Return counts and last-sync metadata for the local Granola MCP index.",
        input_schema=_object_schema({}),
        output_schema=_object_schema(
            {
                "meetings": {"type": "integer"},
                "folders": {"type": "integer"},
                "transcripts": {"type": "integer"},
                "meetings_missing_transcript": {"type": "integer"},
                "source": _nullable_schema(ANY_OBJECT_SCHEMA),
                "last_report_path": NULLABLE_STRING_SCHEMA,
                "last_hydrate_queue_path": NULLABLE_STRING_SCHEMA,
            },
            required=(
                "meetings",
                "folders",
                "transcripts",
                "meetings_missing_transcript",
                "source",
                "last_report_path",
                "last_hydrate_queue_path",
            ),
        ),
    ),
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


class UnknownToolError(Exception):
    """Raised when a client requests a tool that is not defined by this server."""


class ToolArgumentError(ValueError):
    """Raised when tool arguments fail the published input schema."""


class InvalidParamsError(ValueError):
    """Raised when request params do not match the MCP protocol schema."""


TOOL_SCHEMAS = {tool["name"]: tool.get("inputSchema", {}) for tool in TOOLS}


@dataclass(slots=True)
class ToolRouter:
    config: ArchiveConfig
    database: ArchiveDatabase

    def call_tool(self, name: str, arguments: dict[str, Any] | None) -> Any:
        arguments = _validate_tool_arguments(name, arguments or {})
        if name == "search_meetings":
            _validate_date(arguments.get("date_from"), "date_from")
            _validate_date(arguments.get("date_to"), "date_to")
            return self.database.search_meetings(
                query=arguments["query"],
                folder=arguments.get("folder"),
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
                has_transcript=arguments.get("has_transcript"),
                limit=arguments.get("limit", 10),
            )
        if name == "list_meetings":
            _validate_date(arguments.get("date_from"), "date_from")
            _validate_date(arguments.get("date_to"), "date_to")
            return self.database.list_meetings(
                folder=arguments.get("folder"),
                date_from=arguments.get("date_from"),
                date_to=arguments.get("date_to"),
                has_transcript=arguments.get("has_transcript"),
                limit=arguments.get("limit", 25),
            )
        if name == "get_meeting":
            return self.database.get_meeting(arguments["meeting_id"])
        if name == "get_meeting_transcript":
            return self.database.get_meeting_transcript(
                arguments["meeting_id"],
                full=arguments.get("full", False),
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
                limit=arguments.get("limit", 10),
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
                limit=arguments.get("limit", 10),
            )
        if name == "search_unlisted":
            return self.database.search_unlisted(
                arguments["query"],
                limit=arguments.get("limit", 10),
            )
        if name == "get_folder_attachments":
            return self.database.get_folder_attachments(arguments["folder_id_or_title"])
        if name == "stats":
            return self.database.stats(manifest=load_manifest(self.config))
        raise UnknownToolError(f"unknown tool {name}")


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
        raw_params = message.get("params")
        if raw_params is None:
            params: dict[str, Any] = {}
        elif isinstance(raw_params, dict):
            params = raw_params
        else:
            if message_id is None:
                return None
            return _error(message_id, -32602, "Invalid params")

        if method == "initialize":
            if self._protocol_version is not None:
                return _error(message_id, -32600, "Invalid Request")
            try:
                requested_version = _validate_initialize_params(params)
            except InvalidParamsError as exc:
                return _error(message_id, -32602, str(exc))
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
            if not isinstance(tool_name, str) or not tool_name:
                return _error(message_id, -32602, "Invalid params")
            arguments = params.get("arguments")
            if arguments is None:
                arguments = {}
            elif not isinstance(arguments, dict):
                return _error(message_id, -32602, "Invalid params")
            try:
                payload = self.router.call_tool(tool_name, arguments)
                return _result(message_id, _tool_payload(payload))
            except UnknownToolError as exc:
                return _error(message_id, -32602, str(exc))
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


def _validate_tool_arguments(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    schema = TOOL_SCHEMAS.get(name)
    if schema is None:
        raise UnknownToolError(f"unknown tool {name}")

    properties = schema.get("properties", {})
    required = schema.get("required", [])
    allow_additional_properties = bool(schema.get("additionalProperties", True))

    for field_name in required:
        if field_name not in arguments:
            raise ToolArgumentError(f"{name} requires a {field_name!r} argument")

    if not allow_additional_properties:
        unknown_arguments = sorted(field_name for field_name in arguments if field_name not in properties)
        if unknown_arguments:
            joined = ", ".join(repr(field_name) for field_name in unknown_arguments)
            raise ToolArgumentError(f"{name} does not accept argument(s): {joined}")

    for field_name, value in arguments.items():
        property_schema = properties.get(field_name)
        if property_schema is None or value is None:
            continue
        field_type = property_schema.get("type")
        if field_type == "string":
            if not isinstance(value, str):
                raise ToolArgumentError(f"{field_name} must be a string")
            continue
        if field_type == "boolean":
            if not isinstance(value, bool):
                raise ToolArgumentError(f"{field_name} must be a boolean")
            continue
        if field_type == "integer":
            if isinstance(value, bool) or not isinstance(value, int):
                raise ToolArgumentError(f"{field_name} must be an integer")
            maximum = property_schema.get("maximum")
            if maximum is not None and value > maximum:
                raise ToolArgumentError(f"{field_name} must be <= {maximum}")
            minimum = property_schema.get("minimum")
            if minimum is not None and value < minimum:
                raise ToolArgumentError(f"{field_name} must be >= {minimum}")
    return arguments


def _validate_initialize_params(params: dict[str, Any]) -> str:
    protocol_version = params.get("protocolVersion")
    capabilities = params.get("capabilities")
    client_info = params.get("clientInfo")

    if not isinstance(protocol_version, str) or not protocol_version:
        raise InvalidParamsError("initialize requires a non-empty 'protocolVersion' string")
    if not isinstance(capabilities, dict):
        raise InvalidParamsError("initialize requires a 'capabilities' object")
    if not isinstance(client_info, dict):
        raise InvalidParamsError("initialize requires a 'clientInfo' object")

    client_name = client_info.get("name")
    client_version = client_info.get("version")
    client_title = client_info.get("title")

    if not isinstance(client_name, str) or not client_name:
        raise InvalidParamsError("initialize requires clientInfo.name to be a non-empty string")
    if not isinstance(client_version, str) or not client_version:
        raise InvalidParamsError("initialize requires clientInfo.version to be a non-empty string")
    if client_title is not None and not isinstance(client_title, str):
        raise InvalidParamsError("initialize requires clientInfo.title to be a string when provided")
    return protocol_version
