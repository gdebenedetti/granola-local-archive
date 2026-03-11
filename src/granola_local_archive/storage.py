from __future__ import annotations

import gzip
import io
import shutil
import tarfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from .config import ArchiveConfig
from .utils import ensure_parent, now_utc_iso, read_json, sha256_file, write_json


SCHEMA_VERSION = 1
EXCLUDED_BACKUP_PREFIXES = (
    "Cache/",
    "Code Cache/",
    "GPUCache/",
    "DawnGraphiteCache/",
    "DawnWebGPUCache/",
    "Crashpad/",
    "File System/",
    "Local Storage/",
    "IndexedDB/",
    "Session Storage/",
    "Shared Dictionary/",
    "WebStorage/",
    "blob_storage/",
    "shared_proto_db/",
    "sentry/",
    "telemetry/",
    "VideoDecodeStats/",
)
EXCLUDED_BACKUP_NAMES = {
    ".DS_Store",
    "SingletonCookie",
    "SingletonLock",
    "SingletonSocket",
    "Cookies-journal",
    "Trust Tokens-journal",
    "SharedStorage-wal",
}


def default_manifest() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "last_checked_at": None,
        "source": {
            "cache_path": None,
            "size": None,
            "mtime_ns": None,
            "sha256": None,
            "last_snapshot_path": None,
            "last_snapshot_at": None,
            "last_cold_backup_path": None,
            "last_cold_backup_week": None,
        },
        "meetings": {},
        "folders": {},
        "last_report_path": None,
        "last_hydrate_queue_path": None,
    }


def load_manifest(config: ArchiveConfig) -> dict[str, Any]:
    manifest = read_json(config.manifest_path, default_manifest())
    if not isinstance(manifest, dict):
        return default_manifest()
    if manifest.get("schema_version") != SCHEMA_VERSION:
        return default_manifest()
    return manifest


def save_manifest(config: ArchiveConfig, manifest: dict[str, Any]) -> None:
    manifest["schema_version"] = SCHEMA_VERSION
    manifest["last_checked_at"] = now_utc_iso()
    write_json(config.manifest_path, manifest)


def current_cache_stat(config: ArchiveConfig) -> dict[str, Any]:
    stat = config.cache_path.stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def current_cache_hash(config: ArchiveConfig) -> str:
    return sha256_file(config.cache_path)


def snapshot_cache(config: ArchiveConfig, cache_hash: str, timestamp: str | None = None) -> Path:
    timestamp = timestamp or now_utc_iso().replace(":", "").replace("-", "")
    filename = f"cache-{timestamp}-{cache_hash[:12]}.json.gz"
    destination = config.snapshots_dir / filename
    if destination.exists():
        return destination

    ensure_parent(destination)
    with config.cache_path.open("rb") as source_handle, gzip.open(destination, "wb") as target_handle:
        shutil.copyfileobj(source_handle, target_handle)
    return destination


def write_versioned_current_file(current_path: Path, history_root: Path, identifier: str, payload: Any, timestamp: str) -> None:
    if current_path.exists():
        history_dir = history_root / identifier
        history_dir.mkdir(parents=True, exist_ok=True)
        history_path = history_dir / f"{timestamp}.json.gz"
        shutil.copy2(current_path, history_path)

    from .utils import write_json_gz

    write_json_gz(current_path, payload)


def remove_current_file(current_path: Path, history_root: Path, identifier: str, timestamp: str) -> None:
    if not current_path.exists():
        return
    history_dir = history_root / identifier
    history_dir.mkdir(parents=True, exist_ok=True)
    history_path = history_dir / f"{timestamp}.deleted.json.gz"
    shutil.copy2(current_path, history_path)
    current_path.unlink()


def create_weekly_cold_backup(config: ArchiveConfig, timestamp: str | None = None) -> Path | None:
    now = datetime.now(timezone.utc)
    week_key = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
    destination = config.cold_backups_dir / f"granola-{week_key}.tar.gz"
    if destination.exists():
        return destination

    timestamp = timestamp or now_utc_iso()
    ensure_parent(destination)
    with tarfile.open(destination, "w:gz") as archive_handle:
        for file_path in sorted(config.granola_dir.rglob("*")):
            if not file_path.is_file():
                continue
            relative = file_path.relative_to(config.granola_dir).as_posix()
            if _should_skip_backup(relative):
                continue
            info = archive_handle.gettarinfo(str(file_path), arcname=relative)
            info.pax_headers["comment"] = f"created={timestamp}"
            with file_path.open("rb") as handle:
                archive_handle.addfile(info, fileobj=handle)
    return destination


def _should_skip_backup(relative_path: str) -> bool:
    posix = PurePosixPath(relative_path)
    if posix.name in EXCLUDED_BACKUP_NAMES:
        return True
    if any(relative_path.startswith(prefix) for prefix in EXCLUDED_BACKUP_PREFIXES):
        return True
    if posix.suffix in {".lock", ".journal"}:
        return True
    return False
