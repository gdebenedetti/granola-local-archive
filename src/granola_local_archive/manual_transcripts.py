from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .config import ArchiveConfig
from .models import MeetingRecord, NormalizedCache
from .utils import canonical_json_hash, now_utc_iso, read_json, write_json


MANUAL_TRANSCRIPT_SCHEMA_VERSION = 1
_TIMESTAMP_SPEAKER_RE = re.compile(
    r"^\[?(?P<timestamp>\d{1,2}:\d{2}(?::\d{2})?)\]?\s+(?P<speaker>[^:]{1,80}):\s*(?P<text>.+)$"
)
_SPEAKER_TIMESTAMP_RE = re.compile(
    r"^(?P<speaker>[^:(]{1,80})\s+\(?(?P<timestamp>\d{1,2}:\d{2}(?::\d{2})?)\)?:\s*(?P<text>.+)$"
)
_SPEAKER_RE = re.compile(r"^(?P<speaker>[^:]{1,80}):\s*(?P<text>.+)$")


def load_manual_transcript_overrides(config: ArchiveConfig) -> dict[str, dict[str, Any]]:
    overrides: dict[str, dict[str, Any]] = {}
    for path in sorted(config.manual_transcripts_dir.glob("*.json")):
        payload = read_json(path)
        if not isinstance(payload, dict):
            continue
        if payload.get("schema_version") != MANUAL_TRANSCRIPT_SCHEMA_VERSION:
            continue
        meeting_id = str(payload.get("meeting_id") or "").strip()
        if meeting_id:
            overrides[meeting_id] = payload
    return overrides


def save_manual_transcript_override(config: ArchiveConfig, payload: dict[str, Any]) -> Path:
    meeting_id = str(payload["meeting_id"])
    path = config.manual_transcripts_dir / f"{meeting_id}.json"
    write_json(path, payload)
    return path


def build_manual_transcript_override(
    meeting: MeetingRecord,
    raw_text: str,
    source: str = "manual_copy",
    imported_at: str | None = None,
) -> dict[str, Any]:
    cleaned = _extract_transcript_body(raw_text.replace("\r\n", "\n").replace("\r", "\n")).strip()
    if not cleaned:
        raise ValueError("manual transcript text is empty")
    imported_at = imported_at or now_utc_iso()
    segments = _parse_segments(meeting.id, cleaned)
    transcript_text = _format_transcript_text(segments)
    return {
        "schema_version": MANUAL_TRANSCRIPT_SCHEMA_VERSION,
        "meeting_id": meeting.id,
        "title": meeting.title,
        "created_at": meeting.created_at,
        "imported_at": imported_at,
        "source": source,
        "raw_text": cleaned,
        "text": transcript_text,
        "segments": segments,
    }


def apply_manual_transcript_overrides(config: ArchiveConfig, normalized: NormalizedCache) -> NormalizedCache:
    overrides = load_manual_transcript_overrides(config)
    for meeting_id, payload in overrides.items():
        meeting = normalized.meetings.get(meeting_id)
        if meeting is None:
            continue
        apply_manual_transcript_override(meeting, payload)
    return normalized


def apply_manual_transcript_override(meeting: MeetingRecord, payload: dict[str, Any]) -> MeetingRecord:
    segments = payload.get("segments") or []
    if not isinstance(segments, list):
        raise ValueError("manual transcript override has invalid segments")

    normalized_segments: list[dict[str, Any]] = []
    for index, segment in enumerate(segments, start=1):
        if not isinstance(segment, dict):
            continue
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        normalized_segments.append(
            {
                "id": str(segment.get("id") or f"manual-{index:04d}"),
                "document_id": meeting.id,
                "start_timestamp": segment.get("start_timestamp"),
                "end_timestamp": segment.get("end_timestamp"),
                "text": text,
                "source": str(segment.get("source") or payload.get("source") or "manual_copy"),
                "is_final": True,
            }
        )

    meeting.transcript_segments = normalized_segments
    meeting.transcript_segment_count = len(normalized_segments)
    meeting.transcript_text = _format_transcript_text(normalized_segments)
    meeting.metadata["transcript_source"] = payload.get("source") or "manual_copy"
    meeting.metadata["manual_transcript"] = {
        "imported_at": payload.get("imported_at"),
        "raw_text_length": len(str(payload.get("raw_text") or "")),
    }
    meeting.transcript_hash = canonical_json_hash(meeting.to_transcript_sidecar()) if normalized_segments else None
    meeting.meeting_hash = canonical_json_hash(meeting.to_meeting_sidecar())
    return meeting


def _parse_segments(meeting_id: str, raw_text: str) -> list[dict[str, Any]]:
    non_empty_lines = [line.strip() for line in raw_text.split("\n") if line.strip()]
    structured_count = sum(1 for line in non_empty_lines if _match_segment(line) is not None)
    if non_empty_lines and structured_count >= max(1, len(non_empty_lines) // 2):
        return _parse_structured_lines(meeting_id, non_empty_lines)
    return _parse_paragraphs(meeting_id, raw_text)


def _extract_transcript_body(raw_text: str) -> str:
    marker = "\nTranscript:"
    if marker in raw_text:
        raw_text = raw_text.split(marker, 1)[1]
    elif raw_text.startswith("Transcript:"):
        raw_text = raw_text.split("Transcript:", 1)[1]

    lines = raw_text.split("\n")
    while lines and not lines[0].strip():
        lines.pop(0)

    header_prefixes = (
        "Meeting Title:",
        "Date:",
        "Meeting participants:",
    )
    while lines and any(lines[0].startswith(prefix) for prefix in header_prefixes):
        lines.pop(0)
        while lines and not lines[0].strip():
            lines.pop(0)

    return "\n".join(lines)


def _parse_structured_lines(meeting_id: str, lines: list[str]) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    for line in lines:
        matched = _match_segment(line)
        if matched is None:
            if segments:
                segments[-1]["text"] = f"{segments[-1]['text']} {line}".strip()
            else:
                segments.append(_make_segment(meeting_id, 1, line, "manual_copy", None))
            continue
        timestamp, speaker, text = matched
        segments.append(_make_segment(meeting_id, len(segments) + 1, text, speaker, timestamp))
    return segments


def _parse_paragraphs(meeting_id: str, raw_text: str) -> list[dict[str, Any]]:
    paragraphs = [part.strip() for part in raw_text.split("\n\n") if part.strip()]
    return [
        _make_segment(meeting_id, index, paragraph.replace("\n", " ").strip(), "manual_copy", None)
        for index, paragraph in enumerate(paragraphs, start=1)
        if paragraph.strip()
    ]


def _match_segment(line: str) -> tuple[str | None, str, str] | None:
    for pattern in (_TIMESTAMP_SPEAKER_RE, _SPEAKER_TIMESTAMP_RE, _SPEAKER_RE):
        match = pattern.match(line)
        if match is None:
            continue
        timestamp = match.groupdict().get("timestamp")
        speaker = match.groupdict().get("speaker") or "manual_copy"
        text = match.groupdict().get("text") or ""
        return timestamp, speaker.strip(), text.strip()
    return None


def _make_segment(
    meeting_id: str,
    index: int,
    text: str,
    source: str,
    timestamp: str | None,
) -> dict[str, Any]:
    return {
        "id": f"manual-{index:04d}",
        "document_id": meeting_id,
        "start_timestamp": timestamp,
        "end_timestamp": None,
        "text": text,
        "source": source,
        "is_final": True,
    }


def _format_transcript_text(segments: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for segment in segments:
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        timestamp = segment.get("start_timestamp") or "manual"
        source = str(segment.get("source") or "manual_copy")
        lines.append(f"[{timestamp}] {source}: {text}")
    return "\n".join(lines)
