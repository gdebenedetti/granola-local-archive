from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from granola_local_archive.config import ArchiveConfig
from granola_local_archive.syncer import SyncService
from granola_local_archive.utils import read_json


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
