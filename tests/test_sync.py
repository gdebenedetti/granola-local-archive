from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from granola_local_archive.config import ArchiveConfig
from granola_local_archive.index import ArchiveDatabase
from granola_local_archive.syncer import SyncService
from granola_local_archive.utils import read_json, write_json


def _cache_payload(documents: dict, transcripts: dict, document_lists: dict, folder_metadata: dict, attachments: dict | None = None):
    return {
        "cache": {
            "state": {
                "documents": documents,
                "transcripts": transcripts,
                "documentLists": document_lists,
                "documentListsMetadata": folder_metadata,
                "documentListsAttachments": attachments or {},
                "meetingsMetadata": {},
            }
        }
    }


def _write_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


class SyncTests(unittest.TestCase):
    def test_config_detects_latest_cache_version(self) -> None:
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            project = Path(project_root)
            granola = Path(granola_root)
            older = granola / "cache-v2.json"
            newer_version = granola / "cache-v10.json"
            _write_cache(
                older,
                _cache_payload(
                    documents={},
                    transcripts={},
                    document_lists={},
                    folder_metadata={},
                ),
            )
            _write_cache(
                newer_version,
                _cache_payload(
                    documents={},
                    transcripts={},
                    document_lists={},
                    folder_metadata={},
                ),
            )
            # Ensure version wins even if the lower-version file was written more recently.
            newer_mtime_ns = newer_version.stat().st_mtime_ns
            os.utime(older, ns=(newer_mtime_ns + 1_000_000, newer_mtime_ns + 1_000_000))

            config = ArchiveConfig.from_project_root(project, granola)

            self.assertEqual(config.cache_path.name, "cache-v10.json")

    def test_hourly_sync_skips_when_cache_is_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            project = Path(project_root)
            granola = Path(granola_root)
            cache_path = granola / "cache-v4.json"
            _write_cache(
                cache_path,
                _cache_payload(
                    documents={
                        "meeting-a": {
                            "id": "meeting-a",
                            "title": "Roadmap Review",
                            "created_at": "2026-03-09T10:00:00Z",
                            "updated_at": "2026-03-09T10:00:00Z",
                            "valid_meeting": True,
                            "transcribe": False,
                            "notes_markdown": "Discussed roadmap and hiring.",
                        }
                    },
                    transcripts={},
                    document_lists={"folder-a": ["meeting-a"]},
                    folder_metadata={"folder-a": {"id": "folder-a", "title": "Project Alpha"}},
                ),
            )
            config = ArchiveConfig.from_project_root(project, granola)
            service = SyncService(config)

            first = service.sync(mode="hourly")
            second = service.sync(mode="hourly")

            self.assertTrue(first.cache_changed)
            self.assertIsNotNone(first.snapshot_path)
            self.assertFalse(second.cache_changed)
            self.assertEqual(second.skipped_reason, "cache stat unchanged")

    def test_daily_sync_report_detects_new_meeting_and_new_transcript(self) -> None:
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            project = Path(project_root)
            granola = Path(granola_root)
            cache_path = granola / "cache-v4.json"
            folder_meta = {"folder-a": {"id": "folder-a", "title": "Project Alpha"}}
            base_documents = {
                "meeting-a": {
                    "id": "meeting-a",
                    "title": "Weekly Review",
                    "created_at": "2026-03-01T10:00:00Z",
                    "updated_at": "2026-03-01T10:00:00Z",
                    "valid_meeting": True,
                    "transcribe": False,
                    "notes_markdown": "Reviewed roadmap milestones.",
                }
            }
            _write_cache(
                cache_path,
                _cache_payload(base_documents, {}, {"folder-a": ["meeting-a"]}, folder_meta),
            )

            config = ArchiveConfig.from_project_root(project, granola)
            service = SyncService(config)
            service.sync(mode="hourly")

            updated_documents = {
                **base_documents,
                "meeting-b": {
                    "id": "meeting-b",
                    "title": "Hiring Sync",
                    "created_at": "2026-03-10T09:00:00Z",
                    "updated_at": "2026-03-10T09:00:00Z",
                    "valid_meeting": True,
                    "transcribe": False,
                    "notes_markdown": "New meeting about hiring plan.",
                },
            }
            updated_transcripts = {
                "meeting-a": [
                    {
                        "id": "segment-1",
                        "document_id": "meeting-a",
                        "start_timestamp": "2026-03-01T10:00:00Z",
                        "end_timestamp": "2026-03-01T10:00:05Z",
                        "text": "Roadmap risks were reviewed.",
                        "source": "system",
                        "is_final": True,
                    }
                ]
            }
            _write_cache(
                cache_path,
                _cache_payload(
                    updated_documents,
                    updated_transcripts,
                    {"folder-a": ["meeting-a", "meeting-b"]},
                    folder_meta,
                ),
            )

            result = service.sync(mode="daily")
            report = read_json(project / "archive" / result.report_path)

            self.assertTrue(result.cache_changed)
            self.assertEqual(result.new_meetings, 1)
            self.assertEqual(result.new_transcripts, 1)
            self.assertEqual(len(report["new_meetings"]), 1)
            self.assertEqual(report["new_meetings"][0]["id"], "meeting-b")
            self.assertEqual(len(report["new_transcripts"]), 1)
            self.assertEqual(report["new_transcripts"][0]["meeting_id"], "meeting-a")

    def test_sync_preserves_archived_transcript_when_cache_drops_it_for_existing_meeting(self) -> None:
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            project = Path(project_root)
            granola = Path(granola_root)
            cache_path = granola / "cache-v4.json"
            folder_meta = {"folder-a": {"id": "folder-a", "title": "Project Alpha"}}
            documents = {
                "meeting-a": {
                    "id": "meeting-a",
                    "title": "Weekly Review",
                    "created_at": "2026-03-01T10:00:00Z",
                    "updated_at": "2026-03-01T10:00:00Z",
                    "valid_meeting": True,
                    "transcribe": False,
                    "notes_markdown": "Reviewed roadmap milestones.",
                }
            }
            transcripts = {
                "meeting-a": [
                    {
                        "id": "segment-1",
                        "document_id": "meeting-a",
                        "start_timestamp": "2026-03-01T10:00:00Z",
                        "end_timestamp": "2026-03-01T10:00:05Z",
                        "text": "Roadmap risks were reviewed.",
                        "source": "system",
                        "is_final": True,
                    }
                ]
            }

            _write_cache(
                cache_path,
                _cache_payload(documents, transcripts, {"folder-a": ["meeting-a"]}, folder_meta),
            )

            config = ArchiveConfig.from_project_root(project, granola)
            service = SyncService(config)
            service.sync(mode="hourly")

            _write_cache(
                cache_path,
                _cache_payload(documents, {}, {"folder-a": ["meeting-a"]}, folder_meta),
            )
            service.sync(mode="hourly")

            database = ArchiveDatabase(config)
            try:
                transcript = database.get_meeting_transcript("meeting-a", full=True)
            finally:
                database.close()

            self.assertTrue(transcript["available"])
            self.assertEqual(transcript["segment_count"], 1)
            self.assertTrue((project / "archive" / "current" / "transcripts" / "meeting-a.json.gz").exists())

    def test_sync_restores_archived_transcript_from_history_when_current_sidecar_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            project = Path(project_root)
            granola = Path(granola_root)
            cache_path = granola / "cache-v4.json"
            folder_meta = {"folder-a": {"id": "folder-a", "title": "Project Alpha"}}
            documents = {
                "meeting-a": {
                    "id": "meeting-a",
                    "title": "Weekly Review",
                    "created_at": "2026-03-01T10:00:00Z",
                    "updated_at": "2026-03-01T10:00:00Z",
                    "valid_meeting": True,
                    "transcribe": False,
                    "notes_markdown": "Reviewed roadmap milestones.",
                }
            }
            transcripts = {
                "meeting-a": [
                    {
                        "id": "segment-1",
                        "document_id": "meeting-a",
                        "start_timestamp": "2026-03-01T10:00:00Z",
                        "end_timestamp": "2026-03-01T10:00:05Z",
                        "text": "Roadmap risks were reviewed.",
                        "source": "system",
                        "is_final": True,
                    }
                ]
            }
            _write_cache(
                cache_path,
                _cache_payload(documents, transcripts, {"folder-a": ["meeting-a"]}, folder_meta),
            )

            config = ArchiveConfig.from_project_root(project, granola)
            service = SyncService(config)
            service.sync(mode="hourly")

            current_transcript_path = project / "archive" / "current" / "transcripts" / "meeting-a.json.gz"
            history_dir = project / "archive" / "history" / "transcripts" / "meeting-a"
            history_dir.mkdir(parents=True, exist_ok=True)
            history_transcript_path = history_dir / "20260313-000000.deleted.json.gz"
            shutil.copy2(current_transcript_path, history_transcript_path)
            current_transcript_path.unlink()

            manifest = read_json(config.manifest_path)
            manifest["meetings"]["meeting-a"]["transcript_hash"] = None
            manifest["meetings"]["meeting-a"]["transcript_segment_count"] = 0
            write_json(config.manifest_path, manifest)

            database = ArchiveDatabase(config)
            try:
                with database.connection:
                    database.connection.execute(
                        """
                        UPDATE meetings
                        SET transcript_segment_count = 0,
                            transcript_hash = NULL,
                            transcript_sidecar_path = NULL
                        WHERE id = ?
                        """,
                        ("meeting-a",),
                    )
                    database.connection.execute(
                        """
                        INSERT OR REPLACE INTO meeting_fts (
                          rowid, meeting_id, title, notes_text, transcript_text, folder_titles, attendees
                        )
                        SELECT rowid, meeting_id, title, notes_text, ?, folder_titles, attendees
                        FROM meeting_fts
                        WHERE meeting_id = ?
                        """,
                        ("", "meeting-a"),
                    )
            finally:
                database.close()

            _write_cache(
                cache_path,
                _cache_payload(documents, {}, {"folder-a": ["meeting-a"]}, folder_meta),
            )

            result = service.sync(mode="hourly", force=True)

            database = ArchiveDatabase(config)
            try:
                transcript = database.get_meeting_transcript("meeting-a", full=True)
            finally:
                database.close()

            self.assertEqual(result.changed_meetings, 1)
            self.assertTrue(transcript["available"])
            self.assertEqual(transcript["segment_count"], 1)
            self.assertTrue(current_transcript_path.exists())
