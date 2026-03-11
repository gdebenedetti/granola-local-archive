from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import ArchiveConfig, UNLISTED_FOLDER_ID
from .models import FolderRecord, MeetingRecord, NormalizedCache
from .utils import read_json_gz


SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meetings (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  created_at TEXT,
  updated_at TEXT,
  valid_meeting INTEGER NOT NULL,
  transcribe INTEGER NOT NULL,
  notes_text TEXT NOT NULL,
  notes_markdown TEXT,
  notes_plain TEXT,
  attendees_json TEXT NOT NULL,
  folder_ids_json TEXT NOT NULL,
  folder_titles_json TEXT NOT NULL,
  transcript_segment_count INTEGER NOT NULL,
  transcript_hash TEXT,
  meeting_hash TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  meeting_sidecar_path TEXT NOT NULL,
  transcript_sidecar_path TEXT
);

CREATE TABLE IF NOT EXISTS folders (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT,
  workspace_id TEXT,
  workspace_display_name TEXT,
  company_domain TEXT,
  created_at TEXT,
  updated_at TEXT,
  is_space INTEGER NOT NULL,
  is_default_folder INTEGER NOT NULL,
  parent_folder_id TEXT,
  document_count INTEGER NOT NULL,
  folder_hash TEXT NOT NULL,
  folder_sidecar_path TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS folder_membership (
  folder_id TEXT NOT NULL,
  meeting_id TEXT NOT NULL,
  PRIMARY KEY (folder_id, meeting_id)
);

CREATE TABLE IF NOT EXISTS attachments (
  id TEXT PRIMARY KEY,
  folder_id TEXT NOT NULL,
  name TEXT,
  type TEXT,
  mime_type TEXT,
  content_summary TEXT,
  content_markdown TEXT,
  size_in_bytes INTEGER,
  created_at TEXT,
  updated_at TEXT,
  metadata_json TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS meeting_fts USING fts5(
  meeting_id UNINDEXED,
  title,
  notes_text,
  transcript_text,
  folder_titles,
  attendees,
  tokenize='unicode61'
);
"""


class ArchiveDatabase:
    def __init__(self, config: ArchiveConfig):
        self.config = config
        self.connection = sqlite3.connect(config.database_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)

    def close(self) -> None:
        self.connection.close()

    def apply_delta(
        self,
        normalized: NormalizedCache,
        changed_meeting_ids: Iterable[str],
        removed_meeting_ids: Iterable[str],
        changed_folder_ids: Iterable[str],
        removed_folder_ids: Iterable[str],
        meeting_sidecar_paths: dict[str, str],
        transcript_sidecar_paths: dict[str, str | None],
        folder_sidecar_paths: dict[str, str],
    ) -> None:
        changed_meeting_ids = list(changed_meeting_ids)
        removed_meeting_ids = list(removed_meeting_ids)
        changed_folder_ids = list(changed_folder_ids)
        removed_folder_ids = list(removed_folder_ids)
        with self.connection:
            for meeting_id in removed_meeting_ids:
                self.connection.execute("DELETE FROM folder_membership WHERE meeting_id = ?", (meeting_id,))
                self.connection.execute("DELETE FROM meeting_fts WHERE meeting_id = ?", (meeting_id,))
                self.connection.execute("DELETE FROM meetings WHERE id = ?", (meeting_id,))

            for folder_id in removed_folder_ids:
                self.connection.execute("DELETE FROM attachments WHERE folder_id = ?", (folder_id,))
                self.connection.execute("DELETE FROM folder_membership WHERE folder_id = ?", (folder_id,))
                self.connection.execute("DELETE FROM folders WHERE id = ?", (folder_id,))

            for folder_id in changed_folder_ids:
                folder = normalized.folders[folder_id]
                self.connection.execute(
                    """
                    INSERT INTO folders (
                      id, title, description, workspace_id, workspace_display_name,
                      company_domain, created_at, updated_at, is_space,
                      is_default_folder, parent_folder_id, document_count,
                      folder_hash, folder_sidecar_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                      title = excluded.title,
                      description = excluded.description,
                      workspace_id = excluded.workspace_id,
                      workspace_display_name = excluded.workspace_display_name,
                      company_domain = excluded.company_domain,
                      created_at = excluded.created_at,
                      updated_at = excluded.updated_at,
                      is_space = excluded.is_space,
                      is_default_folder = excluded.is_default_folder,
                      parent_folder_id = excluded.parent_folder_id,
                      document_count = excluded.document_count,
                      folder_hash = excluded.folder_hash,
                      folder_sidecar_path = excluded.folder_sidecar_path
                    """,
                    (
                        folder.id,
                        folder.title,
                        folder.description,
                        folder.workspace_id,
                        folder.workspace_display_name,
                        folder.company_domain,
                        folder.created_at,
                        folder.updated_at,
                        int(folder.is_space),
                        int(folder.is_default_folder),
                        folder.parent_folder_id,
                        len(folder.document_ids),
                        folder.folder_hash,
                        folder_sidecar_paths[folder.id],
                    ),
                )
                self.connection.execute("DELETE FROM attachments WHERE folder_id = ?", (folder.id,))
                for attachment in folder.attachments:
                    self.connection.execute(
                        """
                        INSERT INTO attachments (
                          id, folder_id, name, type, mime_type, content_summary,
                          content_markdown, size_in_bytes, created_at, updated_at,
                          metadata_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                          folder_id = excluded.folder_id,
                          name = excluded.name,
                          type = excluded.type,
                          mime_type = excluded.mime_type,
                          content_summary = excluded.content_summary,
                          content_markdown = excluded.content_markdown,
                          size_in_bytes = excluded.size_in_bytes,
                          created_at = excluded.created_at,
                          updated_at = excluded.updated_at,
                          metadata_json = excluded.metadata_json
                        """,
                        (
                            attachment.id,
                            attachment.folder_id,
                            attachment.name,
                            attachment.type,
                            attachment.mime_type,
                            attachment.content_summary,
                            attachment.content_markdown,
                            attachment.size_in_bytes,
                            attachment.created_at,
                            attachment.updated_at,
                            json.dumps(attachment.metadata, sort_keys=True),
                        ),
                    )

            for meeting_id in changed_meeting_ids:
                meeting = normalized.meetings[meeting_id]
                self.connection.execute(
                    """
                    INSERT INTO meetings (
                      id, title, created_at, updated_at, valid_meeting, transcribe,
                      notes_text, notes_markdown, notes_plain, attendees_json,
                      folder_ids_json, folder_titles_json, transcript_segment_count,
                      transcript_hash, meeting_hash, metadata_json,
                      meeting_sidecar_path, transcript_sidecar_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                      title = excluded.title,
                      created_at = excluded.created_at,
                      updated_at = excluded.updated_at,
                      valid_meeting = excluded.valid_meeting,
                      transcribe = excluded.transcribe,
                      notes_text = excluded.notes_text,
                      notes_markdown = excluded.notes_markdown,
                      notes_plain = excluded.notes_plain,
                      attendees_json = excluded.attendees_json,
                      folder_ids_json = excluded.folder_ids_json,
                      folder_titles_json = excluded.folder_titles_json,
                      transcript_segment_count = excluded.transcript_segment_count,
                      transcript_hash = excluded.transcript_hash,
                      meeting_hash = excluded.meeting_hash,
                      metadata_json = excluded.metadata_json,
                      meeting_sidecar_path = excluded.meeting_sidecar_path,
                      transcript_sidecar_path = excluded.transcript_sidecar_path
                    """,
                    (
                        meeting.id,
                        meeting.title,
                        meeting.created_at,
                        meeting.updated_at,
                        int(meeting.valid_meeting),
                        int(meeting.transcribe),
                        meeting.notes_text,
                        meeting.notes_markdown,
                        meeting.notes_plain,
                        json.dumps(meeting.attendees, sort_keys=True),
                        json.dumps(meeting.folder_ids, sort_keys=True),
                        json.dumps(meeting.folder_titles, sort_keys=True),
                        meeting.transcript_segment_count,
                        meeting.transcript_hash,
                        meeting.meeting_hash,
                        json.dumps(meeting.metadata, sort_keys=True),
                        meeting_sidecar_paths[meeting.id],
                        transcript_sidecar_paths.get(meeting.id),
                    ),
                )
                self.connection.execute("DELETE FROM folder_membership WHERE meeting_id = ?", (meeting.id,))
                for folder_id in meeting.folder_ids:
                    self.connection.execute(
                        "INSERT OR REPLACE INTO folder_membership (folder_id, meeting_id) VALUES (?, ?)",
                        (folder_id, meeting.id),
                    )
                self.connection.execute("DELETE FROM meeting_fts WHERE meeting_id = ?", (meeting.id,))
                self.connection.execute(
                    """
                    INSERT INTO meeting_fts (
                      meeting_id, title, notes_text, transcript_text, folder_titles, attendees
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        meeting.id,
                        meeting.title,
                        meeting.notes_text,
                        meeting.transcript_text,
                        " ".join(meeting.folder_titles),
                        " ".join(meeting.attendees),
                    ),
                )

    def search_meetings(
        self,
        query: str,
        folder: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        has_transcript: bool | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 50))
        params, filters, _ = self._build_meeting_filters(
            folder=folder,
            date_from=date_from,
            date_to=date_to,
            has_transcript=has_transcript,
        )

        fts_query = _prepare_fts_query(query)
        if fts_query:
            sql = """
                SELECT
                  meetings.id,
                  meetings.title,
                  meetings.created_at,
                  meetings.updated_at,
                  meetings.transcript_segment_count,
                  meetings.folder_titles_json,
                  meetings.attendees_json,
                  snippet(meeting_fts, 2, '[', ']', '...', 12) AS notes_snippet,
                  snippet(meeting_fts, 3, '[', ']', '...', 12) AS transcript_snippet,
                  bm25(meeting_fts) AS score
                FROM meeting_fts
                JOIN meetings ON meetings.id = meeting_fts.meeting_id
                WHERE meeting_fts MATCH ?
            """
            query_params = [fts_query] + params
            if filters:
                sql += " AND " + " AND ".join(filters)
            sql += " ORDER BY score, meetings.created_at DESC LIMIT ?"
            query_params.append(limit)
            rows = self.connection.execute(sql, query_params).fetchall()
        else:
            sql = """
                SELECT
                  meetings.id,
                  meetings.title,
                  meetings.created_at,
                  meetings.updated_at,
                  meetings.transcript_segment_count,
                  meetings.folder_titles_json,
                  meetings.attendees_json,
                  '' AS notes_snippet,
                  '' AS transcript_snippet,
                  0.0 AS score
                FROM meetings
            """
            if filters:
                sql += " WHERE " + " AND ".join(filters)
            sql += " ORDER BY meetings.created_at DESC LIMIT ?"
            rows = self.connection.execute(sql, params + [limit]).fetchall()

        return [self._serialize_search_row(row) for row in rows]

    def list_meetings(
        self,
        folder: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        has_transcript: bool | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        limit = max(1, min(limit, 100))
        params, filters, folder_record = self._build_meeting_filters(
            folder=folder,
            date_from=date_from,
            date_to=date_to,
            has_transcript=has_transcript,
        )
        sql = """
            SELECT
              meetings.id,
              meetings.title,
              meetings.created_at,
              meetings.updated_at,
              meetings.transcript_segment_count,
              meetings.folder_titles_json,
              meetings.attendees_json,
              '' AS notes_snippet,
              '' AS transcript_snippet,
              0.0 AS score
            FROM meetings
        """
        if filters:
            sql += " WHERE " + " AND ".join(filters)
        sql += " ORDER BY meetings.created_at DESC LIMIT ?"
        rows = self.connection.execute(sql, params + [limit]).fetchall()
        return {
            "filters": self._serialize_filters(folder_record, date_from, date_to, has_transcript),
            "items": [self._serialize_search_row(row) for row in rows],
        }

    def get_meeting(self, meeting_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM meetings WHERE id = ?", (meeting_id,)).fetchone()
        if row is None:
            raise KeyError(f"meeting {meeting_id} not found")
        payload = dict(row)
        payload["attendees"] = json.loads(payload.pop("attendees_json"))
        payload["folder_ids"] = json.loads(payload.pop("folder_ids_json"))
        payload["folder_titles"] = json.loads(payload.pop("folder_titles_json"))
        payload["metadata"] = json.loads(payload.pop("metadata_json"))
        sidecar_path = self.config.resolve_archive_path(payload.pop("meeting_sidecar_path"))
        payload["sidecar"] = read_json_gz(sidecar_path) if sidecar_path else None
        return payload

    def load_meeting_record(self, meeting_id: str) -> MeetingRecord:
        row = self.connection.execute("SELECT * FROM meetings WHERE id = ?", (meeting_id,)).fetchone()
        if row is None:
            raise KeyError(f"meeting {meeting_id} not found")

        transcript_segments: list[dict[str, Any]] = []
        transcript_text = ""
        transcript_path = self.config.resolve_archive_path(row["transcript_sidecar_path"])
        if transcript_path and transcript_path.exists():
            transcript_payload = read_json_gz(transcript_path)
            transcript_segments = transcript_payload.get("segments") or []
            transcript_text = str(transcript_payload.get("text") or "")

        return MeetingRecord(
            id=row["id"],
            title=row["title"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            valid_meeting=bool(row["valid_meeting"]),
            transcribe=bool(row["transcribe"]),
            notes_text=row["notes_text"],
            notes_markdown=row["notes_markdown"],
            notes_plain=row["notes_plain"],
            attendees=json.loads(row["attendees_json"]),
            folder_ids=json.loads(row["folder_ids_json"]),
            folder_titles=json.loads(row["folder_titles_json"]),
            transcript_segment_count=row["transcript_segment_count"],
            transcript_text=transcript_text,
            transcript_segments=transcript_segments,
            metadata=json.loads(row["metadata_json"]),
            meeting_hash=row["meeting_hash"],
            transcript_hash=row["transcript_hash"],
        )

    def resolve_meeting(self, reference: str, created_at: str | None = None) -> dict[str, Any]:
        by_id = self.connection.execute(
            "SELECT id, title, created_at FROM meetings WHERE id = ?",
            (reference,),
        ).fetchone()
        if by_id is not None:
            return dict(by_id)

        rows = self._matching_meetings(reference, created_at)
        if len(rows) == 1:
            return dict(rows[0])
        if not rows:
            raise KeyError(f"meeting {reference!r} not found")
        sample = ", ".join(f"{row['title']} ({row['created_at']})" for row in rows[:5])
        raise ValueError(f"meeting reference {reference!r} is ambiguous: {sample}")

    def get_meeting_transcript(self, meeting_id: str, full: bool = False) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT title, created_at, transcript_segment_count, transcript_sidecar_path FROM meetings WHERE id = ?",
            (meeting_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"meeting {meeting_id} not found")
        if not row["transcript_sidecar_path"]:
            return {
                "meeting_id": meeting_id,
                "title": row["title"],
                "created_at": row["created_at"],
                "segment_count": 0,
                "available": False,
            }
        payload = read_json_gz(self.config.resolve_archive_path(row["transcript_sidecar_path"]))
        if not full:
            payload = {
                "meeting_id": payload["meeting_id"],
                "title": payload["title"],
                "created_at": payload["created_at"],
                "source": payload.get("source"),
                "segment_count": payload["segment_count"],
                "available": True,
                "preview": payload["text"][:4000],
            }
        else:
            payload["available"] = True
        return payload

    def list_folders(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT id, title, description, document_count, updated_at, is_space
            FROM folders
            ORDER BY CASE WHEN id = ? THEN 1 ELSE 0 END, title COLLATE NOCASE
            """,
            (UNLISTED_FOLDER_ID,),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_folder(self, folder_id_or_title: str) -> dict[str, Any]:
        folder = self._resolve_folder(folder_id_or_title)
        meetings = self.connection.execute(
            """
            SELECT meetings.id, meetings.title, meetings.created_at, meetings.transcript_segment_count
            FROM folder_membership
            JOIN meetings ON meetings.id = folder_membership.meeting_id
            WHERE folder_membership.folder_id = ?
            ORDER BY meetings.created_at DESC
            LIMIT 25
            """,
            (folder["id"],),
        ).fetchall()
        payload = dict(folder)
        payload["recent_meetings"] = [dict(row) for row in meetings]
        return payload

    def search_folder(self, folder_id_or_title: str, query: str, limit: int = 10) -> dict[str, Any]:
        folder = self._resolve_folder(folder_id_or_title)
        return {
            "folder": dict(folder),
            "results": self.search_meetings(query=query, folder=folder["id"], limit=limit),
        }

    def search_folder_with_filters(
        self,
        folder_id_or_title: str,
        query: str,
        date_from: str | None = None,
        date_to: str | None = None,
        has_transcript: bool | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        folder = self._resolve_folder(folder_id_or_title)
        return {
            "folder": dict(folder),
            "filters": self._serialize_filters(folder, date_from, date_to, has_transcript),
            "results": self.search_meetings(
                query=query,
                folder=folder["id"],
                date_from=date_from,
                date_to=date_to,
                has_transcript=has_transcript,
                limit=limit,
            ),
        }

    def search_unlisted(self, query: str, limit: int = 10) -> dict[str, Any]:
        return self.search_folder(UNLISTED_FOLDER_ID, query, limit=limit)

    def search_evidence(
        self,
        query: str,
        meeting_id: str | None = None,
        folder: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        limit = max(1, min(limit, 50))
        if meeting_id:
            meeting_rows = [
                self.connection.execute(
                    """
                    SELECT id, title, created_at, updated_at, notes_text, transcript_sidecar_path
                    FROM meetings
                    WHERE id = ?
                    """,
                    (meeting_id,),
                ).fetchone()
            ]
            folder_record = None
        else:
            params, filters, folder_record = self._build_meeting_filters(
                folder=folder,
                date_from=date_from,
                date_to=date_to,
                has_transcript=None,
            )
            sql = """
                SELECT
                  meetings.id,
                  meetings.title,
                  meetings.created_at,
                  meetings.updated_at,
                  meetings.notes_text,
                  meetings.transcript_sidecar_path
                FROM meeting_fts
                JOIN meetings ON meetings.id = meeting_fts.meeting_id
                WHERE meeting_fts MATCH ?
            """
            fts_query = _prepare_fts_query(query)
            if not fts_query:
                raise ValueError("query must contain searchable terms")
            query_params: list[Any] = [fts_query] + params
            if filters:
                sql += " AND " + " AND ".join(filters)
            sql += " ORDER BY bm25(meeting_fts), meetings.created_at DESC LIMIT ?"
            query_params.append(max(limit * 5, 20))
            meeting_rows = self.connection.execute(sql, query_params).fetchall()

        terms = _prepare_search_terms(query)
        evidence_items: list[dict[str, Any]] = []
        for row in meeting_rows:
            if row is None:
                continue
            notes_text = str(row["notes_text"] or "")
            notes_match = _extract_text_match(notes_text, terms, query)
            if notes_match is not None:
                evidence_items.append(
                    {
                        "meeting_id": row["id"],
                        "title": row["title"],
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                        "source_kind": "notes",
                        "snippet": notes_match["snippet"],
                        "matched_terms": notes_match["matched_terms"],
                        "match_count": notes_match["match_count"],
                    }
                )

            transcript_path = row["transcript_sidecar_path"]
            if not transcript_path:
                continue
            transcript_payload = read_json_gz(self.config.resolve_archive_path(transcript_path))
            transcript_matches = _extract_segment_matches(
                transcript_payload.get("segments") or [],
                terms,
                query,
                limit=max(limit, 5),
            )
            for match in transcript_matches:
                evidence_items.append(
                    {
                        "meeting_id": row["id"],
                        "title": row["title"],
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                        "source_kind": "transcript",
                        "snippet": match["snippet"],
                        "matched_terms": match["matched_terms"],
                        "match_count": match["match_count"],
                        "start_timestamp": match["start_timestamp"],
                        "end_timestamp": match["end_timestamp"],
                        "speaker": match["speaker"],
                    }
                )

        evidence_items.sort(
            key=lambda item: (
                -int(item["match_count"]),
                0 if item["source_kind"] == "transcript" else 1,
                _descending_iso_sort_key(item["created_at"]),
                item.get("start_timestamp") or "",
            )
        )
        return {
            "query": query,
            "filters": self._serialize_filters(folder_record, date_from, date_to, None, meeting_id=meeting_id),
            "items": evidence_items[:limit],
            "meetings_considered": len([row for row in meeting_rows if row is not None]),
        }

    def get_folder_attachments(self, folder_id_or_title: str) -> dict[str, Any]:
        folder = self._resolve_folder(folder_id_or_title)
        attachments = self.connection.execute(
            """
            SELECT id, name, type, mime_type, content_summary, content_markdown, size_in_bytes, created_at, updated_at
            FROM attachments
            WHERE folder_id = ?
            ORDER BY created_at DESC, name COLLATE NOCASE
            """,
            (folder["id"],),
        ).fetchall()
        return {"folder": dict(folder), "attachments": [dict(row) for row in attachments]}

    def stats(self, manifest: dict[str, Any] | None = None) -> dict[str, Any]:
        meeting_count = self.connection.execute("SELECT COUNT(*) FROM meetings").fetchone()[0]
        folder_count = self.connection.execute("SELECT COUNT(*) FROM folders").fetchone()[0]
        transcript_count = self.connection.execute(
            "SELECT COUNT(*) FROM meetings WHERE transcript_segment_count > 0"
        ).fetchone()[0]
        missing_count = self.connection.execute(
            "SELECT COUNT(*) FROM meetings WHERE valid_meeting = 1 AND transcript_segment_count = 0"
        ).fetchone()[0]
        payload = {
            "meetings": meeting_count,
            "folders": folder_count,
            "transcripts": transcript_count,
            "meetings_missing_transcript": missing_count,
        }
        if manifest:
            payload["source"] = manifest.get("source", {})
            payload["last_report_path"] = manifest.get("last_report_path")
            payload["last_hydrate_queue_path"] = manifest.get("last_hydrate_queue_path")
        return payload

    def build_hydrate_queue(
        self,
        limit: int = 25,
        days: int = 30,
        priority_titles: tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT id, title, created_at, updated_at, folder_titles_json, transcript_segment_count
            FROM meetings
            WHERE valid_meeting = 1 AND transcript_segment_count = 0
            ORDER BY created_at DESC
            """
        ).fetchall()
        now = datetime.now(timezone.utc)
        priority_set = {title.casefold() for title in priority_titles}
        queue: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        for row in rows:
            folder_titles = json.loads(row["folder_titles_json"])
            created_at = _parse_iso(row["created_at"])
            is_recent = created_at is not None and created_at >= now - timedelta(days=days)
            is_priority = any(title.casefold() in priority_set for title in folder_titles)
            sort_key = (0 if is_recent else 1, 0 if is_priority else 1, -(created_at.timestamp() if created_at else 0))
            queue.append(
                (
                    sort_key,
                    {
                        "meeting_id": row["id"],
                        "title": row["title"],
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                        "folder_titles": folder_titles,
                        "reason": {
                            "recent": is_recent,
                            "priority_folder": is_priority,
                        },
                    },
                )
            )
        queue.sort(key=lambda item: item[0])
        return [item[1] for item in queue[:limit]]

    def _resolve_folder(self, folder_id_or_title: str) -> sqlite3.Row:
        by_id = self.connection.execute("SELECT * FROM folders WHERE id = ?", (folder_id_or_title,)).fetchone()
        if by_id is not None:
            return by_id

        exact = self.connection.execute(
            "SELECT * FROM folders WHERE lower(title) = lower(?)",
            (folder_id_or_title,),
        ).fetchall()
        if len(exact) == 1:
            return exact[0]
        if len(exact) > 1:
            raise ValueError(f"folder title {folder_id_or_title!r} is ambiguous")

        like = self.connection.execute(
            "SELECT * FROM folders WHERE lower(title) LIKE lower(?)",
            (f"%{folder_id_or_title}%",),
        ).fetchall()
        if len(like) == 1:
            return like[0]
        if not like:
            raise KeyError(f"folder {folder_id_or_title!r} not found")
        raise ValueError(f"folder title {folder_id_or_title!r} is ambiguous")

    def _build_meeting_filters(
        self,
        folder: str | None,
        date_from: str | None,
        date_to: str | None,
        has_transcript: bool | None,
    ) -> tuple[list[Any], list[str], sqlite3.Row | None]:
        params: list[Any] = []
        filters: list[str] = []
        folder_record = None
        if folder:
            folder_record = self._resolve_folder(folder)
            filters.append(
                "EXISTS (SELECT 1 FROM folder_membership fm WHERE fm.meeting_id = meetings.id AND fm.folder_id = ?)"
            )
            params.append(folder_record["id"])
        if date_from:
            filters.append("date(meetings.created_at) >= date(?)")
            params.append(date_from)
        if date_to:
            filters.append("date(meetings.created_at) <= date(?)")
            params.append(date_to)
        if has_transcript is True:
            filters.append("meetings.transcript_segment_count > 0")
        elif has_transcript is False:
            filters.append("meetings.transcript_segment_count = 0")
        return params, filters, folder_record

    def _serialize_filters(
        self,
        folder_record: sqlite3.Row | None,
        date_from: str | None,
        date_to: str | None,
        has_transcript: bool | None,
        meeting_id: str | None = None,
    ) -> dict[str, Any]:
        return {
            "meeting_id": meeting_id,
            "folder_id": folder_record["id"] if folder_record is not None else None,
            "folder_title": folder_record["title"] if folder_record is not None else None,
            "date_from": date_from,
            "date_to": date_to,
            "has_transcript": has_transcript,
        }

    def _matching_meetings(self, reference: str, created_at: str | None) -> list[sqlite3.Row]:
        where_clauses: list[str] = []
        params: list[Any] = []
        if created_at:
            where_clauses.append("date(created_at) = date(?)")
            params.append(created_at)

        exact_sql = "SELECT id, title, created_at FROM meetings WHERE lower(title) = lower(?)"
        exact_params = [reference] + params
        if where_clauses:
            exact_sql += " AND " + " AND ".join(where_clauses)
        exact_rows = self.connection.execute(exact_sql, exact_params).fetchall()
        if exact_rows:
            return exact_rows

        like_sql = "SELECT id, title, created_at FROM meetings WHERE lower(title) LIKE lower(?)"
        like_params = [f"%{reference}%"] + params
        if where_clauses:
            like_sql += " AND " + " AND ".join(where_clauses)
        return self.connection.execute(like_sql, like_params).fetchall()

    def _serialize_search_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "meeting_id": row["id"],
            "title": row["title"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "transcript_segment_count": row["transcript_segment_count"],
            "folder_titles": json.loads(row["folder_titles_json"]),
            "attendees": json.loads(row["attendees_json"]),
            "notes_snippet": row["notes_snippet"],
            "transcript_snippet": row["transcript_snippet"],
            "score": row["score"],
        }


def _prepare_fts_query(query: str) -> str | None:
    tokens = re.findall(r"[A-Za-z0-9_-]+", query or "")
    if not tokens:
        return None
    return " AND ".join(f'"{token}"' for token in tokens)


def _prepare_search_terms(query: str) -> list[str]:
    return [token.casefold() for token in re.findall(r"[A-Za-z0-9_-]+", query or "")]


def _extract_text_match(text: str, terms: list[str], raw_query: str) -> dict[str, Any] | None:
    if not text:
        return None
    lowered = text.casefold()
    normalized_tokens = {token.casefold() for token in re.findall(r"[A-Za-z0-9_-]+", text)}
    matched_terms = sorted({term for term in terms if term in normalized_tokens})
    if not matched_terms:
        phrase = raw_query.strip().casefold()
        if not phrase or phrase not in lowered:
            return None
        matched_terms = [phrase]
    anchor = raw_query.strip().casefold()
    start_index = lowered.find(anchor) if anchor else -1
    if start_index < 0:
        positions = [_find_whole_term_position(text, term) for term in matched_terms]
        start_index = min((position for position in positions if position >= 0), default=0)
    snippet = _snippet_around(text, start_index)
    return {
        "snippet": snippet,
        "matched_terms": matched_terms,
        "match_count": len(matched_terms),
    }


def _extract_segment_matches(
    segments: list[dict[str, Any]],
    terms: list[str],
    raw_query: str,
    limit: int,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for segment in segments:
        text = str(segment.get("text") or "")
        match = _extract_text_match(text, terms, raw_query)
        if match is None:
            continue
        matches.append(
            {
                "snippet": match["snippet"],
                "matched_terms": match["matched_terms"],
                "match_count": match["match_count"],
                "start_timestamp": segment.get("start_timestamp"),
                "end_timestamp": segment.get("end_timestamp"),
                "speaker": segment.get("speaker") or segment.get("source"),
            }
        )
    matches.sort(
        key=lambda item: (
            -int(item["match_count"]),
            item["start_timestamp"] or "",
        )
    )
    return matches[:limit]


def _snippet_around(text: str, start_index: int, width: int = 220) -> str:
    if not text:
        return ""
    if start_index < 0:
        start_index = 0
    snippet_start = max(0, start_index - width // 3)
    snippet_end = min(len(text), start_index + (2 * width // 3))
    snippet = text[snippet_start:snippet_end].strip()
    if snippet_start > 0:
        snippet = f"...{snippet}"
    if snippet_end < len(text):
        snippet = f"{snippet}..."
    return re.sub(r"\s+", " ", snippet)


def _find_whole_term_position(text: str, term: str) -> int:
    pattern = re.compile(rf"(?<![A-Za-z0-9_-]){re.escape(term)}(?![A-Za-z0-9_-])", re.IGNORECASE)
    match = pattern.search(text)
    return -1 if match is None else match.start()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _descending_iso_sort_key(value: str | None) -> float:
    parsed = _parse_iso(value)
    if parsed is None:
        return 0.0
    return -parsed.timestamp()
