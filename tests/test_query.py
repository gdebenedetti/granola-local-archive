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


class QueryTests(unittest.TestCase):
    def test_folder_and_unlisted_queries_use_sqlite_index(self) -> None:
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            project = Path(project_root)
            granola = Path(granola_root)
            cache_path = granola / "cache-v4.json"
            payload = {
                "cache": {
                    "state": {
                        "documents": {
                            "meeting-a": {
                                "id": "meeting-a",
                                "title": "Roadmap Review",
                                "created_at": "2026-03-09T10:00:00Z",
                                "updated_at": "2026-03-09T10:00:00Z",
                                "valid_meeting": True,
                                "transcribe": False,
                                "notes_markdown": "Roadmap and architecture decisions.",
                            },
                            "meeting-c": {
                                "id": "meeting-c",
                                "title": "Roadmap Review Old",
                                "created_at": "2026-02-01T10:00:00Z",
                                "updated_at": "2026-02-01T10:00:00Z",
                                "valid_meeting": True,
                                "transcribe": False,
                                "notes_markdown": "Old roadmap and legacy backlog items.",
                            },
                            "meeting-b": {
                                "id": "meeting-b",
                                "title": "Loose Note",
                                "created_at": "2026-03-09T11:00:00Z",
                                "updated_at": "2026-03-09T11:00:00Z",
                                "valid_meeting": True,
                                "transcribe": False,
                                "notes_markdown": "Unlisted note with budgeting details.",
                            },
                        },
                        "transcripts": {
                            "meeting-a": [
                                {
                                    "id": "seg-1",
                                    "document_id": "meeting-a",
                                    "start_timestamp": "2026-03-09T10:01:00Z",
                                    "end_timestamp": "2026-03-09T10:02:00Z",
                                    "text": "We agreed on the roadmap.",
                                    "source": "system",
                                    "is_final": True,
                                }
                            ],
                            "meeting-c": [
                                {
                                    "id": "seg-2",
                                    "document_id": "meeting-c",
                                    "start_timestamp": "2026-02-01T10:01:00Z",
                                    "end_timestamp": "2026-02-01T10:02:00Z",
                                    "text": "The old roadmap should be deferred.",
                                    "source": "system",
                                    "is_final": True,
                                }
                            ]
                        },
                        "documentLists": {"folder-a": ["meeting-a", "meeting-c"]},
                        "documentListsMetadata": {"folder-a": {"id": "folder-a", "title": "Project Alpha"}},
                        "documentListsAttachments": {},
                        "meetingsMetadata": {},
                    }
                }
            }
            _write_cache(cache_path, payload)

            config = ArchiveConfig.from_project_root(project, granola)
            SyncService(config).sync(mode="hourly")

            database = ArchiveDatabase(config)
            try:
                folder_results = database.search_folder("Project Alpha", "roadmap", limit=5)
                filtered_folder_results = database.search_folder_with_filters(
                    "Project Alpha",
                    "roadmap",
                    date_from="2026-03-01",
                    date_to="2026-03-10",
                    limit=5,
                )
                listed = database.list_meetings(
                    folder="Project Alpha",
                    date_from="2026-03-01",
                    date_to="2026-03-10",
                    limit=5,
                )
                evidence = database.search_evidence(
                    "roadmap",
                    folder="Project Alpha",
                    date_from="2026-03-01",
                    date_to="2026-03-10",
                    limit=5,
                )
                unlisted_results = database.search_unlisted("budgeting", limit=5)
                transcript = database.get_meeting_transcript("meeting-a", full=False)
            finally:
                database.close()

            self.assertEqual(len(folder_results["results"]), 2)
            self.assertEqual(folder_results["results"][0]["meeting_id"], "meeting-a")
            self.assertEqual(len(filtered_folder_results["results"]), 1)
            self.assertEqual(filtered_folder_results["results"][0]["meeting_id"], "meeting-a")
            self.assertEqual(len(listed["items"]), 1)
            self.assertEqual(listed["items"][0]["meeting_id"], "meeting-a")
            self.assertGreaterEqual(len(evidence["items"]), 1)
            self.assertTrue(all(item["meeting_id"] == "meeting-a" for item in evidence["items"]))
            self.assertTrue(any(item["source_kind"] == "transcript" for item in evidence["items"]))
            self.assertEqual(len(unlisted_results["results"]), 1)
            self.assertEqual(unlisted_results["results"][0]["meeting_id"], "meeting-b")
            self.assertTrue(transcript["available"])
            self.assertEqual(transcript["segment_count"], 1)
