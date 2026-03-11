from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class FolderAttachmentRecord:
    id: str
    folder_id: str
    name: str
    type: str
    mime_type: str | None
    content_summary: str | None
    content_markdown: str | None
    size_in_bytes: int | None
    created_at: str | None
    updated_at: str | None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FolderRecord:
    id: str
    title: str
    description: str | None
    workspace_id: str | None
    workspace_display_name: str | None
    company_domain: str | None
    created_at: str | None
    updated_at: str | None
    is_space: bool
    is_default_folder: bool
    parent_folder_id: str | None
    document_ids: list[str]
    attachments: list[FolderAttachmentRecord] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    folder_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["attachments"] = [item.to_dict() for item in self.attachments]
        return payload


@dataclass(slots=True)
class MeetingRecord:
    id: str
    title: str
    created_at: str | None
    updated_at: str | None
    valid_meeting: bool
    transcribe: bool
    notes_text: str
    notes_markdown: str | None
    notes_plain: str | None
    attendees: list[str]
    folder_ids: list[str]
    folder_titles: list[str]
    transcript_segment_count: int
    transcript_text: str
    transcript_segments: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    meeting_hash: str = ""
    transcript_hash: str | None = None

    def to_meeting_sidecar(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("transcript_segments", None)
        payload.pop("transcript_text", None)
        return payload

    def to_transcript_sidecar(self) -> dict[str, Any]:
        return {
            "meeting_id": self.id,
            "title": self.title,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "source": self.metadata.get("transcript_source") or "cache",
            "segment_count": self.transcript_segment_count,
            "text": self.transcript_text,
            "segments": self.transcript_segments,
        }


@dataclass(slots=True)
class NormalizedCache:
    meetings: dict[str, MeetingRecord]
    folders: dict[str, FolderRecord]
    parse_errors: list[str]

    def counts(self) -> dict[str, int]:
        return {
            "meetings": len(self.meetings),
            "folders": len(self.folders),
            "transcripts": sum(1 for meeting in self.meetings.values() if meeting.transcript_segment_count > 0),
        }
