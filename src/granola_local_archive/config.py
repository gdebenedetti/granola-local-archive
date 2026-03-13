from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path


UNLISTED_FOLDER_ID = "__unlisted__"
UNLISTED_FOLDER_TITLE = "Unlisted"
_CACHE_FILE_RE = re.compile(r"^cache-v(?P<version>\d+)\.json$")


def _load_priority_folder_titles() -> tuple[str, ...]:
    raw_value = os.getenv("GRANOLA_PRIORITY_FOLDERS", "")
    return tuple(part.strip() for part in raw_value.split(",") if part.strip())


def _discover_cache_path(granola_dir: Path) -> Path:
    candidates: list[tuple[int, int, Path]] = []
    for path in granola_dir.glob("cache-v*.json"):
        match = _CACHE_FILE_RE.match(path.name)
        if match is None:
            continue
        try:
            version = int(match.group("version"))
        except (TypeError, ValueError):
            continue
        try:
            mtime_ns = path.stat().st_mtime_ns
        except FileNotFoundError:
            continue
        candidates.append((version, mtime_ns, path))

    if candidates:
        _, _, latest = max(candidates, key=lambda item: (item[0], item[1]))
        return latest

    return granola_dir / "cache-v4.json"


@dataclass(slots=True)
class ArchiveConfig:
    project_root: Path
    granola_dir: Path
    cache_path: Path
    archive_root: Path
    manual_transcripts_dir: Path
    snapshots_dir: Path
    cold_backups_dir: Path
    current_meetings_dir: Path
    current_transcripts_dir: Path
    current_folders_dir: Path
    history_meetings_dir: Path
    history_transcripts_dir: Path
    history_folders_dir: Path
    reports_dir: Path
    logs_dir: Path
    state_dir: Path
    manifest_path: Path
    database_path: Path
    hydrate_queue_path: Path
    priority_folder_titles: tuple[str, ...]

    @classmethod
    def from_project_root(cls, project_root: Path, granola_dir: Path | None = None) -> "ArchiveConfig":
        project_root = project_root.expanduser().resolve()
        granola_dir = (granola_dir or Path("~/Library/Application Support/Granola")).expanduser().resolve()
        cache_path = _discover_cache_path(granola_dir)
        archive_root = project_root / "archive"
        current_dir = archive_root / "current"
        history_dir = archive_root / "history"
        state_dir = archive_root / "state"
        return cls(
            project_root=project_root,
            granola_dir=granola_dir,
            cache_path=cache_path,
            archive_root=archive_root,
            manual_transcripts_dir=archive_root / "manual" / "transcripts",
            snapshots_dir=archive_root / "snapshots",
            cold_backups_dir=archive_root / "cold-backups",
            current_meetings_dir=current_dir / "meetings",
            current_transcripts_dir=current_dir / "transcripts",
            current_folders_dir=current_dir / "folders",
            history_meetings_dir=history_dir / "meetings",
            history_transcripts_dir=history_dir / "transcripts",
            history_folders_dir=history_dir / "folders",
            reports_dir=archive_root / "reports",
            logs_dir=archive_root / "logs",
            state_dir=state_dir,
            manifest_path=state_dir / "manifest.json",
            database_path=state_dir / "archive.sqlite3",
            hydrate_queue_path=state_dir / "hydrate-queue.json",
            priority_folder_titles=_load_priority_folder_titles(),
        )

    def ensure_directories(self) -> None:
        for path in (
            self.manual_transcripts_dir,
            self.snapshots_dir,
            self.cold_backups_dir,
            self.current_meetings_dir,
            self.current_transcripts_dir,
            self.current_folders_dir,
            self.history_meetings_dir,
            self.history_transcripts_dir,
            self.history_folders_dir,
            self.reports_dir,
            self.logs_dir,
            self.state_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def relative_to_archive(self, path: Path | None) -> str | None:
        if path is None:
            return None
        return path.resolve().relative_to(self.archive_root).as_posix()

    def resolve_archive_path(self, relative_path: str | None) -> Path | None:
        if not relative_path:
            return None
        return (self.archive_root / relative_path).resolve()
