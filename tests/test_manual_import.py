from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from granola_local_archive.config import ArchiveConfig
from granola_local_archive.index import ArchiveDatabase
from granola_local_archive.syncer import SyncService


def _write_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _payload(documents: dict, transcripts: dict | None = None, folder_docs: dict | None = None, folder_meta: dict | None = None) -> dict:
    return {
        "cache": {
            "state": {
                "documents": documents,
                "transcripts": transcripts or {},
                "documentLists": folder_docs or {},
                "documentListsMetadata": folder_meta or {},
                "documentListsAttachments": {},
                "meetingsMetadata": {},
            }
        }
    }


class ManualImportTests(unittest.TestCase):
    def test_manual_transcript_import_survives_future_sync(self) -> None:
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            project = Path(project_root)
            granola = Path(granola_root)
            cache_path = granola / "cache-v4.json"
            documents = {
                "meeting-a": {
                    "id": "meeting-a",
                    "title": "Discovery Interview",
                    "created_at": "2025-10-08T21:00:29Z",
                    "updated_at": "2025-10-08T22:04:29Z",
                    "valid_meeting": True,
                    "transcribe": False,
                    "notes_markdown": "Interview notes.",
                }
            }
            _write_cache(cache_path, _payload(documents))

            config = ArchiveConfig.from_project_root(project, granola)
            service = SyncService(config)
            service.sync(mode="hourly")
            result = service.import_manual_transcript(
                "meeting-a",
                "Speaker A: Primera linea copiada\nSpeaker B: Segunda linea copiada",
            )

            updated_documents = {
                **documents,
                "meeting-b": {
                    "id": "meeting-b",
                    "title": "Other Meeting",
                    "created_at": "2025-10-09T10:00:00Z",
                    "updated_at": "2025-10-09T10:00:00Z",
                    "valid_meeting": True,
                    "transcribe": False,
                    "notes_markdown": "Fresh cache change.",
                },
            }
            _write_cache(cache_path, _payload(updated_documents))
            service.sync(mode="hourly")

            database = ArchiveDatabase(config)
            try:
                transcript = database.get_meeting_transcript("meeting-a", full=True)
                queue = database.build_hydrate_queue(limit=25)
            finally:
                database.close()

            self.assertEqual(result.segment_count, 2)
            self.assertTrue(transcript["available"])
            self.assertEqual(transcript["source"], "manual_copy")
            self.assertEqual(transcript["segment_count"], 2)
            self.assertFalse(any(item["meeting_id"] == "meeting-a" for item in queue))

    def test_manual_import_strips_granola_export_headers(self) -> None:
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            project = Path(project_root)
            granola = Path(granola_root)
            cache_path = granola / "cache-v4.json"
            documents = {
                "meeting-a": {
                    "id": "meeting-a",
                    "title": "Discovery Interview",
                    "created_at": "2025-10-08T21:00:29Z",
                    "updated_at": "2025-10-08T22:04:29Z",
                    "valid_meeting": True,
                    "transcribe": False,
                    "notes_markdown": "Interview notes.",
                }
            }
            _write_cache(cache_path, _payload(documents))

            config = ArchiveConfig.from_project_root(project, granola)
            service = SyncService(config)
            service.sync(mode="hourly")
            service.import_manual_transcript(
                "meeting-a",
                "Meeting Title: Discovery Interview\nDate: Oct 8\nMeeting participants: Alex Taylor\n\nTranscript:\n\nMe: Hola\nThem: Buenas",
            )

            database = ArchiveDatabase(config)
            try:
                transcript = database.get_meeting_transcript("meeting-a", full=True)
            finally:
                database.close()

            self.assertEqual(transcript["segment_count"], 2)
            self.assertNotIn("Meeting Title:", transcript["text"])
            self.assertEqual(transcript["segments"][0]["text"], "Hola")

    def test_sync_preserves_archived_meeting_when_cache_prunes_it(self) -> None:
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            project = Path(project_root)
            granola = Path(granola_root)
            cache_path = granola / "cache-v4.json"
            documents = {
                "meeting-a": {
                    "id": "meeting-a",
                    "title": "Older Meeting",
                    "created_at": "2025-07-01T10:00:00Z",
                    "updated_at": "2025-07-01T10:00:00Z",
                    "valid_meeting": True,
                    "transcribe": False,
                    "notes_markdown": "Archive me.",
                }
            }
            _write_cache(
                cache_path,
                _payload(
                    documents=documents,
                    folder_docs={"folder-a": ["meeting-a"]},
                    folder_meta={"folder-a": {"id": "folder-a", "title": "Sample Folder"}},
                ),
            )

            config = ArchiveConfig.from_project_root(project, granola)
            service = SyncService(config)
            service.sync(mode="hourly")

            _write_cache(cache_path, _payload(documents={}))
            service.sync(mode="hourly")

            database = ArchiveDatabase(config)
            try:
                meeting = database.get_meeting("meeting-a")
            finally:
                database.close()

            self.assertEqual(meeting["id"], "meeting-a")
            self.assertTrue((project / "archive" / "current" / "meetings" / "meeting-a.json.gz").exists())
