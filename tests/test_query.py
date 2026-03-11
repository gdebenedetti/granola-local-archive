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


def _minimal_cache_payload() -> dict:
    """Return a minimal Granola cache payload with one foldered meeting (with transcript)
    and one unlisted meeting (without transcript)."""
    return {
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
                    ]
                },
                "documentLists": {"folder-a": ["meeting-a"]},
                "documentListsMetadata": {"folder-a": {"id": "folder-a", "title": "Project Alpha"}},
                "documentListsAttachments": {},
                "meetingsMetadata": {},
            }
        }
    }


def _build_archive(project: Path, granola: Path) -> ArchiveConfig:
    cache_path = granola / "cache-v4.json"
    _write_cache(cache_path, _minimal_cache_payload())
    config = ArchiveConfig.from_project_root(project, granola)
    SyncService(config).sync(mode="hourly")
    return config


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

    def test_transcript_preview_includes_truncation_fields(self) -> None:
        """get_meeting_transcript(full=False) must set is_truncated=True and full_length>4000
        when the transcript text exceeds the 4000-char preview window."""
        # "word " * 801 = 4005 chars; formatted line adds a ~31-char prefix, so full_length > 4000
        long_text = "word " * 801
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
                            "notes_markdown": "Roadmap.",
                        }
                    },
                    "transcripts": {
                        "meeting-a": [
                            {
                                "id": "seg-1",
                                "document_id": "meeting-a",
                                "start_timestamp": "2026-03-09T10:01:00Z",
                                "end_timestamp": "2026-03-09T10:02:00Z",
                                "text": long_text,
                                "source": "system",
                                "is_final": True,
                            }
                        ]
                    },
                    "documentLists": {"folder-a": ["meeting-a"]},
                    "documentListsMetadata": {"folder-a": {"id": "folder-a", "title": "Project Alpha"}},
                    "documentListsAttachments": {},
                    "meetingsMetadata": {},
                }
            }
        }
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            cache_path = Path(granola_root) / "cache-v4.json"
            _write_cache(cache_path, payload)
            config = ArchiveConfig.from_project_root(Path(project_root), Path(granola_root))
            SyncService(config).sync(mode="hourly")
            database = ArchiveDatabase(config)
            try:
                transcript = database.get_meeting_transcript("meeting-a", full=False)
            finally:
                database.close()

        self.assertTrue(transcript["available"])
        self.assertTrue(transcript["is_truncated"])
        self.assertGreater(transcript["full_length"], 4000)
        self.assertEqual(len(transcript["preview"]), 4000)

    def test_search_evidence_nonexistent_meeting_raises(self) -> None:
        """search_evidence with a meeting_id that does not exist must raise KeyError."""
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            config = _build_archive(Path(project_root), Path(granola_root))
            database = ArchiveDatabase(config)
            try:
                with self.assertRaises(KeyError):
                    database.search_evidence("anything", meeting_id="no-such-meeting-id")
            finally:
                database.close()

    def test_search_unlisted_output_shape_consistent_with_search_folder(self) -> None:
        """search_unlisted must return a 'filters' key, matching the shape of search_folder_with_filters."""
        with tempfile.TemporaryDirectory() as project_root, tempfile.TemporaryDirectory() as granola_root:
            config = _build_archive(Path(project_root), Path(granola_root))
            database = ArchiveDatabase(config)
            try:
                unlisted = database.search_unlisted("budgeting", limit=5)
                folder = database.search_folder_with_filters("Project Alpha", "roadmap", limit=5)
            finally:
                database.close()

        self.assertIn("filters", unlisted)
        self.assertIn("filters", folder)
        self.assertIn("results", unlisted)
        self.assertIn("results", folder)
