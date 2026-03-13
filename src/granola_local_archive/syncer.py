from __future__ import annotations

import fcntl
from dataclasses import asdict, dataclass, field
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .config import ArchiveConfig
from .index import ArchiveDatabase
from .manual_transcripts import (
    apply_manual_transcript_override,
    apply_manual_transcript_overrides,
    build_manual_transcript_override,
    save_manual_transcript_override,
)
from .models import MeetingRecord, NormalizedCache
from .normalize import parse_cache_file
from .storage import (
    create_weekly_cold_backup,
    current_cache_hash,
    current_cache_stat,
    load_manifest,
    remove_current_file,
    save_manifest,
    snapshot_cache,
    write_versioned_current_file,
)
from .utils import canonical_json_hash, now_utc_iso, read_json_gz, write_json


@dataclass(slots=True)
class SyncResult:
    mode: str
    cache_changed: bool
    snapshot_path: str | None
    cold_backup_path: str | None
    report_path: str | None
    hydrate_queue_path: str | None
    changed_meetings: int
    changed_folders: int
    removed_meetings: int
    removed_folders: int
    new_meetings: int
    new_transcripts: int
    meetings_missing_transcript: int
    parse_errors: list[str] = field(default_factory=list)
    skipped_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ManualImportResult:
    meeting_id: str
    title: str
    created_at: str | None
    segment_count: int
    override_path: str
    transcript_sidecar_path: str
    hydrate_queue_path: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SyncService:
    def __init__(self, config: ArchiveConfig):
        self.config = config
        self.config.ensure_directories()

    def sync(self, mode: str = "hourly", force: bool = False) -> SyncResult:
        with self._sync_lock():
            timestamp = now_utc_iso()
            manifest = load_manifest(self.config)
            source = manifest["source"]
            database = ArchiveDatabase(self.config)
            try:
                current_stat = current_cache_stat(self.config)
                stat_changed = force or current_stat != {
                    "size": source.get("size"),
                    "mtime_ns": source.get("mtime_ns"),
                }

                normalized: NormalizedCache | None = None
                current_hash: str | None = None
                snapshot_path: Path | None = None
                changed_meeting_ids: set[str] = set()
                changed_folder_ids: set[str] = set()
                removed_meeting_ids: set[str] = set()
                removed_folder_ids: set[str] = set()
                new_meeting_ids: set[str] = set()
                new_transcript_ids: set[str] = set()
                parse_errors: list[str] = []
                meeting_sidecar_paths: dict[str, str] = {}
                transcript_sidecar_paths: dict[str, str | None] = {}
                folder_sidecar_paths: dict[str, str] = {}

                if stat_changed:
                    current_hash = current_cache_hash(self.config)
                    if current_hash != source.get("sha256") or force:
                        snapshot_path = snapshot_cache(self.config, current_hash, _stamp(timestamp))
                        normalized = parse_cache_file(self.config.cache_path)
                        normalized = apply_manual_transcript_overrides(self.config, normalized)
                        normalized = _preserve_archived_transcripts(self.config, normalized)
                        parse_errors.extend(normalized.parse_errors)
                        (
                            changed_meeting_ids,
                            removed_meeting_ids,
                            new_meeting_ids,
                            new_transcript_ids,
                        ) = _compute_meeting_changes(manifest, normalized)
                        changed_folder_ids, removed_folder_ids = _compute_folder_changes(manifest, normalized)
                        (
                            meeting_sidecar_paths,
                            transcript_sidecar_paths,
                            folder_sidecar_paths,
                        ) = self._write_sidecars(
                            normalized,
                            changed_meeting_ids,
                            removed_meeting_ids,
                            changed_folder_ids,
                            removed_folder_ids,
                            timestamp,
                        )
                        database.apply_delta(
                            normalized=normalized,
                            changed_meeting_ids=changed_meeting_ids,
                            removed_meeting_ids=removed_meeting_ids,
                            changed_folder_ids=changed_folder_ids,
                            removed_folder_ids=removed_folder_ids,
                            meeting_sidecar_paths=meeting_sidecar_paths,
                            transcript_sidecar_paths=transcript_sidecar_paths,
                            folder_sidecar_paths=folder_sidecar_paths,
                        )
                        _update_manifest(
                            manifest=manifest,
                            config=self.config,
                            normalized=normalized,
                            current_hash=current_hash,
                            current_stat=current_stat,
                            snapshot_path=snapshot_path,
                        )
                    else:
                        source["size"] = current_stat["size"]
                        source["mtime_ns"] = current_stat["mtime_ns"]

                queue = database.build_hydrate_queue(priority_titles=self.config.priority_folder_titles)
                write_json(
                    self.config.hydrate_queue_path,
                    {"generated_at": timestamp, "items": queue},
                )
                manifest["last_hydrate_queue_path"] = self.config.relative_to_archive(self.config.hydrate_queue_path)

                report_path: Path | None = None
                cold_backup_path: Path | None = None
                if mode == "daily":
                    report_path = self._write_daily_report(
                        timestamp=timestamp,
                        database=database,
                        changed_meeting_ids=changed_meeting_ids,
                        new_meeting_ids=new_meeting_ids,
                        new_transcript_ids=new_transcript_ids,
                        changed_folder_ids=changed_folder_ids,
                        parse_errors=parse_errors,
                    )
                    manifest["last_report_path"] = self.config.relative_to_archive(report_path)
                    cold_backup_path = create_weekly_cold_backup(self.config, timestamp)
                    if cold_backup_path is not None:
                        source["last_cold_backup_path"] = self.config.relative_to_archive(cold_backup_path)
                        source["last_cold_backup_week"] = cold_backup_path.name.removeprefix("granola-").removesuffix(".tar.gz")

                save_manifest(self.config, manifest)
                stats = database.stats(manifest=manifest)
                return SyncResult(
                    mode=mode,
                    cache_changed=normalized is not None,
                    snapshot_path=self.config.relative_to_archive(snapshot_path),
                    cold_backup_path=self.config.relative_to_archive(cold_backup_path),
                    report_path=self.config.relative_to_archive(report_path),
                    hydrate_queue_path=self.config.relative_to_archive(self.config.hydrate_queue_path),
                    changed_meetings=len(changed_meeting_ids),
                    changed_folders=len(changed_folder_ids),
                    removed_meetings=len(removed_meeting_ids),
                    removed_folders=len(removed_folder_ids),
                    new_meetings=len(new_meeting_ids),
                    new_transcripts=len(new_transcript_ids),
                    meetings_missing_transcript=stats["meetings_missing_transcript"],
                    parse_errors=parse_errors,
                    skipped_reason=None if stat_changed else "cache stat unchanged",
                )
            finally:
                database.close()

    def import_manual_transcript(self, meeting_id: str, raw_text: str, source: str = "manual_copy") -> ManualImportResult:
        with self._sync_lock():
            timestamp = now_utc_iso()
            database = ArchiveDatabase(self.config)
            try:
                meeting = database.load_meeting_record(meeting_id)
                override = build_manual_transcript_override(
                    meeting=meeting,
                    raw_text=raw_text,
                    source=source,
                    imported_at=timestamp,
                )
                override_path = save_manual_transcript_override(self.config, override)
                meeting = apply_manual_transcript_override(meeting, override)
                meeting_sidecar_paths, transcript_sidecar_paths, _ = self._write_sidecars(
                    normalized=NormalizedCache(meetings={meeting.id: meeting}, folders={}, parse_errors=[]),
                    changed_meeting_ids={meeting.id},
                    removed_meeting_ids=set(),
                    changed_folder_ids=set(),
                    removed_folder_ids=set(),
                    timestamp=timestamp,
                )
                database.apply_delta(
                    normalized=NormalizedCache(meetings={meeting.id: meeting}, folders={}, parse_errors=[]),
                    changed_meeting_ids=[meeting.id],
                    removed_meeting_ids=[],
                    changed_folder_ids=[],
                    removed_folder_ids=[],
                    meeting_sidecar_paths=meeting_sidecar_paths,
                    transcript_sidecar_paths=transcript_sidecar_paths,
                    folder_sidecar_paths={},
                )

                manifest = load_manifest(self.config)
                manifest.setdefault("meetings", {})[meeting.id] = _meeting_manifest_entry(meeting)
                queue = database.build_hydrate_queue(priority_titles=self.config.priority_folder_titles)
                write_json(
                    self.config.hydrate_queue_path,
                    {"generated_at": timestamp, "items": queue},
                )
                manifest["last_hydrate_queue_path"] = self.config.relative_to_archive(self.config.hydrate_queue_path)
                save_manifest(self.config, manifest)
                return ManualImportResult(
                    meeting_id=meeting.id,
                    title=meeting.title,
                    created_at=meeting.created_at,
                    segment_count=meeting.transcript_segment_count,
                    override_path=self.config.relative_to_archive(override_path) or "",
                    transcript_sidecar_path=transcript_sidecar_paths[meeting.id] or "",
                    hydrate_queue_path=self.config.relative_to_archive(self.config.hydrate_queue_path) or "",
                )
            finally:
                database.close()

    @contextmanager
    def _sync_lock(self):
        lock_path = self.config.state_dir / "sync.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _write_sidecars(
        self,
        normalized: NormalizedCache,
        changed_meeting_ids: set[str],
        removed_meeting_ids: set[str],
        changed_folder_ids: set[str],
        removed_folder_ids: set[str],
        timestamp: str,
    ) -> tuple[dict[str, str], dict[str, str | None], dict[str, str]]:
        meeting_sidecar_paths: dict[str, str] = {}
        transcript_sidecar_paths: dict[str, str | None] = {}
        folder_sidecar_paths: dict[str, str] = {}
        stamp = _stamp(timestamp)

        for meeting_id in changed_meeting_ids:
            meeting = normalized.meetings[meeting_id]
            meeting_path = self.config.current_meetings_dir / f"{meeting_id}.json.gz"
            write_versioned_current_file(
                meeting_path,
                self.config.history_meetings_dir,
                meeting_id,
                meeting.to_meeting_sidecar(),
                stamp,
            )
            meeting_sidecar_paths[meeting_id] = self.config.relative_to_archive(meeting_path)

            transcript_path = self.config.current_transcripts_dir / f"{meeting_id}.json.gz"
            if meeting.transcript_segment_count > 0:
                write_versioned_current_file(
                    transcript_path,
                    self.config.history_transcripts_dir,
                    meeting_id,
                    meeting.to_transcript_sidecar(),
                    stamp,
                )
                transcript_sidecar_paths[meeting_id] = self.config.relative_to_archive(transcript_path)
            else:
                remove_current_file(transcript_path, self.config.history_transcripts_dir, meeting_id, stamp)
                transcript_sidecar_paths[meeting_id] = None

        for meeting_id in removed_meeting_ids:
            remove_current_file(
                self.config.current_meetings_dir / f"{meeting_id}.json.gz",
                self.config.history_meetings_dir,
                meeting_id,
                stamp,
            )
            remove_current_file(
                self.config.current_transcripts_dir / f"{meeting_id}.json.gz",
                self.config.history_transcripts_dir,
                meeting_id,
                stamp,
            )

        for folder_id in changed_folder_ids:
            folder = normalized.folders[folder_id]
            folder_path = self.config.current_folders_dir / f"{folder_id}.json.gz"
            write_versioned_current_file(
                folder_path,
                self.config.history_folders_dir,
                folder_id,
                folder.to_dict(),
                stamp,
            )
            folder_sidecar_paths[folder_id] = self.config.relative_to_archive(folder_path)

        for folder_id in removed_folder_ids:
            remove_current_file(
                self.config.current_folders_dir / f"{folder_id}.json.gz",
                self.config.history_folders_dir,
                folder_id,
                stamp,
            )

        return meeting_sidecar_paths, transcript_sidecar_paths, folder_sidecar_paths

    def _write_daily_report(
        self,
        timestamp: str,
        database: ArchiveDatabase,
        changed_meeting_ids: set[str],
        new_meeting_ids: set[str],
        new_transcript_ids: set[str],
        changed_folder_ids: set[str],
        parse_errors: list[str],
    ) -> Path:
        manifest = load_manifest(self.config)
        report = {
            "generated_at": timestamp,
            "new_meetings": [database.get_meeting(meeting_id) for meeting_id in sorted(new_meeting_ids)],
            "new_transcripts": [database.get_meeting_transcript(meeting_id, full=False) for meeting_id in sorted(new_transcript_ids)],
            "meetings_missing_transcript": database.build_hydrate_queue(limit=500, priority_titles=self.config.priority_folder_titles),
            "folders_changed": [database.get_folder(folder_id) for folder_id in sorted(changed_folder_ids)],
            "parse_errors": parse_errors,
            "stats": database.stats(manifest=manifest),
            "changed_meeting_ids": sorted(changed_meeting_ids),
        }
        report_path = self.config.reports_dir / f"{timestamp[:10]}.json"
        write_json(report_path, report)
        return report_path


def _compute_meeting_changes(
    manifest: dict[str, Any],
    normalized: NormalizedCache,
) -> tuple[set[str], set[str], set[str], set[str]]:
    previous = manifest.get("meetings", {})
    current = {
        meeting_id: {
            "meeting_hash": meeting.meeting_hash,
            "transcript_hash": meeting.transcript_hash,
            "title": meeting.title,
            "created_at": meeting.created_at,
            "updated_at": meeting.updated_at,
            "folder_ids": meeting.folder_ids,
            "transcript_segment_count": meeting.transcript_segment_count,
            "valid_meeting": meeting.valid_meeting,
        }
        for meeting_id, meeting in normalized.meetings.items()
    }
    changed = {
        meeting_id
        for meeting_id, data in current.items()
        if previous.get(meeting_id) != data
    }
    removed: set[str] = set()
    new_meetings = {meeting_id for meeting_id in current if meeting_id not in previous}
    new_transcripts = {
        meeting_id
        for meeting_id, data in current.items()
        if data["transcript_hash"] and previous.get(meeting_id, {}).get("transcript_hash") != data["transcript_hash"]
    }
    return changed, removed, new_meetings, new_transcripts


def _compute_folder_changes(
    manifest: dict[str, Any],
    normalized: NormalizedCache,
) -> tuple[set[str], set[str]]:
    previous = manifest.get("folders", {})
    current = {
        folder_id: {
            "folder_hash": folder.folder_hash,
            "title": folder.title,
            "document_ids": folder.document_ids,
            "updated_at": folder.updated_at,
        }
        for folder_id, folder in normalized.folders.items()
    }
    changed = {folder_id for folder_id, data in current.items() if previous.get(folder_id) != data}
    removed: set[str] = set()
    return changed, removed


def _update_manifest(
    manifest: dict[str, Any],
    config: ArchiveConfig,
    normalized: NormalizedCache,
    current_hash: str,
    current_stat: dict[str, Any],
    snapshot_path: Path,
) -> None:
    manifest["source"].update(
        {
            "cache_path": str(config.cache_path),
            "size": current_stat["size"],
            "mtime_ns": current_stat["mtime_ns"],
            "sha256": current_hash,
            "last_snapshot_path": config.relative_to_archive(snapshot_path),
            "last_snapshot_at": now_utc_iso(),
        }
    )
    meetings = manifest.setdefault("meetings", {})
    for meeting_id, meeting in normalized.meetings.items():
        meetings[meeting_id] = _meeting_manifest_entry(meeting)

    folders = manifest.setdefault("folders", {})
    for folder_id, folder in normalized.folders.items():
        folders[folder_id] = _folder_manifest_entry(folder)


def _stamp(timestamp: str) -> str:
    return timestamp.replace("-", "").replace(":", "").replace("T", "-").replace("Z", "")


def _meeting_manifest_entry(meeting: MeetingRecord) -> dict[str, Any]:
    return {
        "meeting_hash": meeting.meeting_hash,
        "transcript_hash": meeting.transcript_hash,
        "title": meeting.title,
        "created_at": meeting.created_at,
        "updated_at": meeting.updated_at,
        "folder_ids": meeting.folder_ids,
        "transcript_segment_count": meeting.transcript_segment_count,
        "valid_meeting": meeting.valid_meeting,
    }


def _folder_manifest_entry(folder: Any) -> dict[str, Any]:
    return {
        "folder_hash": folder.folder_hash,
        "title": folder.title,
        "document_ids": folder.document_ids,
        "updated_at": folder.updated_at,
    }


def _preserve_archived_transcripts(config: ArchiveConfig, normalized: NormalizedCache) -> NormalizedCache:
    for meeting in normalized.meetings.values():
        if meeting.transcript_segment_count > 0:
            continue
        payload = _load_archived_transcript_payload(config, meeting.id)
        if payload is None:
            continue
        _apply_archived_transcript_payload(meeting, payload)
    return normalized


def _load_archived_transcript_payload(config: ArchiveConfig, meeting_id: str) -> dict[str, Any] | None:
    current_path = config.current_transcripts_dir / f"{meeting_id}.json.gz"
    if current_path.exists():
        payload = _safe_read_transcript_payload(current_path)
        if payload is not None:
            return payload

    history_dir = config.history_transcripts_dir / meeting_id
    if not history_dir.exists():
        return None

    for history_path in sorted(history_dir.glob("*.json.gz"), reverse=True):
        payload = _safe_read_transcript_payload(history_path)
        if payload is not None:
            return payload
    return None


def _safe_read_transcript_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = read_json_gz(path)
    except Exception:  # pragma: no cover - defensive
        return None
    if not isinstance(payload, dict):
        return None
    segments = payload.get("segments") or []
    if not isinstance(segments, list):
        return None
    segment_count = payload.get("segment_count")
    if not segments and not segment_count:
        return None
    return payload


def _apply_archived_transcript_payload(meeting: MeetingRecord, payload: dict[str, Any]) -> None:
    segments = payload.get("segments") or []
    normalized_segments = [
        {
            "id": str(segment.get("id") or f"archived-{index:04d}"),
            "document_id": meeting.id,
            "start_timestamp": segment.get("start_timestamp"),
            "end_timestamp": segment.get("end_timestamp"),
            "text": str(segment.get("text") or "").strip(),
            "source": str(segment.get("source") or payload.get("source") or "cache"),
            "is_final": bool(segment.get("is_final", True)),
        }
        for index, segment in enumerate(segments, start=1)
        if isinstance(segment, dict) and str(segment.get("text") or "").strip()
    ]
    if not normalized_segments:
        return

    meeting.transcript_segments = normalized_segments
    meeting.transcript_segment_count = int(payload.get("segment_count") or len(normalized_segments))
    meeting.transcript_text = str(payload.get("text") or _format_transcript_text(normalized_segments))
    meeting.metadata["transcript_source"] = payload.get("source") or meeting.metadata.get("transcript_source") or "cache"
    meeting.transcript_hash = canonical_json_hash(meeting.to_transcript_sidecar())
    meeting.meeting_hash = canonical_json_hash(meeting.to_meeting_sidecar())


def _format_transcript_text(segments: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for segment in segments:
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        timestamp = segment.get("start_timestamp") or "unknown"
        source = str(segment.get("source") or "unknown")
        lines.append(f"[{timestamp}] {source}: {text}")
    return "\n".join(lines)
