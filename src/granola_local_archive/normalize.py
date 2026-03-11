from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from .config import UNLISTED_FOLDER_ID, UNLISTED_FOLDER_TITLE
from .models import FolderAttachmentRecord, FolderRecord, MeetingRecord, NormalizedCache
from .utils import canonical_json_hash


def parse_cache_file(cache_path: Path) -> NormalizedCache:
    with cache_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    cache = payload.get("cache", {})
    if isinstance(cache, str):
        cache = json.loads(cache)
    state = cache.get("state", {})

    documents = state.get("documents", {})
    transcripts = state.get("transcripts", {})
    folder_docs = state.get("documentLists", {})
    folder_meta = state.get("documentListsMetadata", {})
    folder_attachments = state.get("documentListsAttachments", {})
    meetings_metadata = state.get("meetingsMetadata", {})
    parse_errors: list[str] = []

    memberships: dict[str, list[str]] = defaultdict(list)
    for folder_id, document_ids in folder_docs.items():
        if not isinstance(document_ids, list):
            parse_errors.append(f"folder {folder_id} has non-list document membership")
            continue
        for document_id in document_ids:
            if document_id not in documents:
                continue
            memberships[document_id].append(folder_id)

    folders: dict[str, FolderRecord] = {}
    folder_ids = set(folder_docs) | set(folder_meta)
    for folder_id in folder_ids:
        try:
            metadata = folder_meta.get(folder_id, {}) or {}
            attachments = [
                FolderAttachmentRecord(
                    id=str(item.get("id", "")),
                    folder_id=folder_id,
                    name=str(item.get("name", "")),
                    type=str(item.get("type", "")),
                    mime_type=item.get("mime_type"),
                    content_summary=item.get("content_summary"),
                    content_markdown=item.get("content_markdown"),
                    size_in_bytes=item.get("size_in_bytes"),
                    created_at=item.get("created_at"),
                    updated_at=item.get("updated_at"),
                    metadata=item.get("metadata") or {},
                )
                for item in folder_attachments.get(folder_id, [])
                if isinstance(item, dict)
            ]
            record = FolderRecord(
                id=folder_id,
                title=str(metadata.get("title") or folder_id),
                description=metadata.get("description"),
                workspace_id=metadata.get("workspace_id"),
                workspace_display_name=metadata.get("workspace_display_name"),
                company_domain=metadata.get("company_domain"),
                created_at=metadata.get("created_at"),
                updated_at=metadata.get("updated_at"),
                is_space=bool(metadata.get("is_space")),
                is_default_folder=bool(metadata.get("is_default_folder")),
                parent_folder_id=metadata.get("parent_document_list_id"),
                document_ids=list(folder_docs.get(folder_id, [])),
                attachments=attachments,
                metadata={key: value for key, value in metadata.items() if key not in {"title", "description"}},
            )
            record.folder_hash = canonical_json_hash(record.to_dict())
            folders[folder_id] = record
        except Exception as exc:  # pragma: no cover - defensive
            parse_errors.append(f"failed to parse folder {folder_id}: {exc}")

    unlisted_document_ids = sorted(document_id for document_id in documents if document_id not in memberships)
    folders[UNLISTED_FOLDER_ID] = FolderRecord(
        id=UNLISTED_FOLDER_ID,
        title=UNLISTED_FOLDER_TITLE,
        description="Synthetic folder for documents without a Granola list",
        workspace_id=None,
        workspace_display_name=None,
        company_domain=None,
        created_at=None,
        updated_at=None,
        is_space=False,
        is_default_folder=False,
        parent_folder_id=None,
        document_ids=unlisted_document_ids,
        attachments=[],
        metadata={"synthetic": True},
    )
    folders[UNLISTED_FOLDER_ID].folder_hash = canonical_json_hash(folders[UNLISTED_FOLDER_ID].to_dict())

    meetings: dict[str, MeetingRecord] = {}
    for meeting_id, document in documents.items():
        if not isinstance(document, dict):
            parse_errors.append(f"document {meeting_id} is not an object")
            continue

        try:
            folder_ids_for_meeting = memberships.get(meeting_id, [UNLISTED_FOLDER_ID])
            folder_titles = [folders[folder_id].title for folder_id in folder_ids_for_meeting if folder_id in folders]
            meeting_metadata = meetings_metadata.get(meeting_id) if isinstance(meetings_metadata, dict) else None
            transcript_segments = transcripts.get(meeting_id, [])
            if not isinstance(transcript_segments, list):
                transcript_segments = []

            notes_plain = _clean_text(document.get("notes_plain"))
            notes_markdown = _clean_text(document.get("notes_markdown"))
            flattened_notes = _clean_text(_flatten_prosemirror(document.get("notes")))
            notes_text = notes_markdown or notes_plain or flattened_notes or ""

            attendees = _collect_attendees(document, meeting_metadata)
            transcript_text = _format_transcript_text(transcript_segments)

            record = MeetingRecord(
                id=meeting_id,
                title=str(document.get("title") or f"Untitled {meeting_id[:8]}"),
                created_at=document.get("created_at"),
                updated_at=document.get("updated_at"),
                valid_meeting=bool(document.get("valid_meeting")),
                transcribe=bool(document.get("transcribe")),
                notes_text=notes_text,
                notes_markdown=notes_markdown or None,
                notes_plain=notes_plain or None,
                attendees=attendees,
                folder_ids=folder_ids_for_meeting,
                folder_titles=folder_titles,
                transcript_segment_count=len(transcript_segments),
                transcript_text=transcript_text,
                transcript_segments=transcript_segments,
                metadata={
                    "status": document.get("status"),
                    "visibility": document.get("visibility"),
                    "workspace_id": document.get("workspace_id"),
                    "summary": document.get("summary"),
                    "overview": document.get("overview"),
                    "type": document.get("type"),
                    "metadata": document.get("metadata") or {},
                    "google_calendar_event": document.get("google_calendar_event") or {},
                    "meeting_metadata": meeting_metadata or {},
                    "attachments": document.get("attachments") or [],
                },
            )
            record.meeting_hash = canonical_json_hash(record.to_meeting_sidecar())
            record.transcript_hash = canonical_json_hash(record.to_transcript_sidecar()) if transcript_segments else None
            meetings[meeting_id] = record
        except Exception as exc:  # pragma: no cover - defensive
            parse_errors.append(f"failed to parse meeting {meeting_id}: {exc}")

    return NormalizedCache(meetings=meetings, folders=folders, parse_errors=parse_errors)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _flatten_prosemirror(node: Any) -> str:
    if not isinstance(node, dict):
        return ""

    node_type = node.get("type")
    if node_type == "text":
        return str(node.get("text", ""))

    children = node.get("content", [])
    parts = [_flatten_prosemirror(child) for child in children if isinstance(child, dict)]
    text = "".join(parts).strip()

    if node_type == "heading":
        return f"{text}\n\n" if text else ""
    if node_type == "paragraph":
        return f"{text}\n\n" if text else ""
    if node_type == "listItem":
        return f"- {text}\n" if text else ""
    if node_type in {"bulletList", "orderedList", "doc"}:
        return "".join(parts)
    return text


def _collect_attendees(document: dict[str, Any], meeting_metadata: dict[str, Any] | None) -> list[str]:
    attendees: list[str] = []
    seen: set[str] = set()

    def add_candidate(candidate: Any) -> None:
        if isinstance(candidate, str):
            normalized = candidate.strip()
        elif isinstance(candidate, dict):
            normalized = str(candidate.get("name") or candidate.get("email") or "").strip()
        else:
            normalized = ""
        if normalized and normalized not in seen:
            seen.add(normalized)
            attendees.append(normalized)

    people = document.get("people") or []
    if isinstance(people, list):
        for person in people:
            add_candidate(person)
    elif isinstance(people, dict):
        add_candidate(people)

    calendar_event = document.get("google_calendar_event") or {}
    if isinstance(calendar_event, dict):
        calendar_attendees = calendar_event.get("attendees", [])
        if isinstance(calendar_attendees, list):
            for attendee in calendar_attendees:
                add_candidate(attendee)
        elif isinstance(calendar_attendees, dict):
            add_candidate(calendar_attendees)

    if isinstance(meeting_metadata, dict):
        meeting_attendees = meeting_metadata.get("attendees", [])
        if isinstance(meeting_attendees, list):
            for attendee in meeting_attendees:
                add_candidate(attendee)
        elif isinstance(meeting_attendees, dict):
            add_candidate(meeting_attendees)

    return attendees


def _format_transcript_text(segments: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for segment in segments:
        if not isinstance(segment, dict):
            continue
        text = _clean_text(segment.get("text"))
        if not text:
            continue
        timestamp = segment.get("start_timestamp") or "unknown"
        source = segment.get("source") or "unknown"
        lines.append(f"[{timestamp}] {source}: {text}")
    return "\n".join(lines)
