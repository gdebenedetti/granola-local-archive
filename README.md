# Granola Local Archive

Unofficial project. Not affiliated with, endorsed by, or maintained by Granola.

This project is intended for macOS installations where the Granola desktop app has already written local cache data to disk.

Local MCP server and SQLite-backed index for chatting with Granola-generated notes, summaries, and transcripts already present on the local machine.

## What It Does

- Exposes local Granola content to Codex, Cursor, and other MCP clients so you can ask questions about meetings and folders.
- Reads local notes, summaries, transcripts, folders, and folder attachments from the Granola cache already on disk.
- Indexes notes and transcript text in SQLite FTS5 for fast local search.
- Returns grounded snippets and meeting metadata through MCP tools optimized for date range and folder-scoped queries.
- Takes compressed snapshots of `~/Library/Application Support/Granola/cache-v4.json` only when it changes.
- Stores current sidecars plus versioned history for meetings, transcripts, and folders.
- Generates a daily hydration queue for meetings that still have no local transcript.
- Preserves already archived meetings even if they later disappear from Granola's live cache.

## Layout

Most runtime state is stored under `archive/`:

- `snapshots/`: gzip snapshots of raw `cache-v4.json`
- `cold-backups/`: weekly tarballs of the Granola application support directory, excluding ephemeral caches
- `current/`: current sidecars for meetings, transcripts, and folders
- `history/`: prior versions of those sidecars
- `reports/`: daily reconciliation reports
- `state/`: SQLite index, manifest, and hydrate queue

## Setup

Clean clone bootstrap:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip setuptools
.venv/bin/python -m pip install -e . --no-build-isolation
```

The wrapper scripts below assume `.venv/` exists:

```bash
zsh ./ops/run-archive.sh stats
```

Run the first full local sync:

```bash
zsh ./ops/run-archive.sh sync --mode daily
```

Cheap hourly check:

```bash
zsh ./ops/run-archive.sh sync --mode hourly
```

See the current transcript hydration queue:

```bash
zsh ./ops/run-archive.sh hydrate-queue
```

Main use case:

```bash
zsh ./ops/run-mcp.sh
```

## Manual Transcript Hydration

Granola appears to fetch some older transcripts on demand and then persist them back into `cache-v4.json`.

Recommended flow:

1. Run `hydrate-queue` and pick 5 to 10 meetings.
2. Open each meeting in Granola and expand the transcript.
3. Wait a few seconds for the note to finish hydrating.
4. Run `sync --mode hourly`.
5. The new transcript will be detected and archived automatically.

If Granola shows a transcript in the UI but never persists it into `cache-v4.json`, import a copied transcript manually:

```bash
pbpaste | zsh ./ops/run-archive.sh import-transcript --meeting "Weekly Sync" --created-at 2025-10-08
```

You can also read from a file:

```bash
zsh ./ops/run-archive.sh import-transcript --meeting-id meeting-123 --file /tmp/transcript.txt
```

Manual transcript imports are stored under `archive/manual/transcripts/` and are re-applied on future syncs, so a later cache update will not remove them.

## MCP Usage

Start the local MCP server:

```bash
zsh ./ops/run-mcp.sh
```

Available tools:

- `search_meetings`
- `list_meetings`
- `get_meeting`
- `get_meeting_transcript`
- `list_folders`
- `get_folder`
- `search_folder`
- `search_evidence`
- `search_unlisted`
- `get_folder_attachments`
- `stats`

Recommended usage for grounded answers:

1. `list_meetings` for strict folder/date scoping
2. `search_evidence` for exact snippets before claiming decisions, people, or next steps
3. `get_meeting` or `get_meeting_transcript` only when deeper context is needed

Conceptually, the archive layer is there to make the MCP reliable over time. The primary user-facing surface is the MCP server, not the backup job itself.

### Cursor Example

```json
{
  "mcpServers": {
    "granola-local": {
      "command": "/bin/zsh",
      "args": ["/path/to/granola-local-archive/ops/run-mcp.sh"]
    }
  }
}
```

### Codex Example

Use the same stdio command:

```text
/bin/zsh /path/to/granola-local-archive/ops/run-mcp.sh
```

## launchd Templates

Templates are under `ops/launchd/` and are rendered locally during install:

- hourly sync every 3600 seconds
- daily reconciliation at 07:30 local time

Use `./ops/manage-launchd.sh` to install or remove them:

```bash
zsh ./ops/manage-launchd.sh install
zsh ./ops/manage-launchd.sh status
zsh ./ops/manage-launchd.sh run-now daily
zsh ./ops/manage-launchd.sh uninstall
```

`clear-logs` truncates the archive scheduler logs without touching snapshots or reports.

## Shareable Repo Hygiene

- Runtime data under `archive/` is local state, not project source.
- macOS installation state under `~/Library/LaunchAgents` is local state, not project source.
- The project reads optional hydration priority folders from `GRANOLA_PRIORITY_FOLDERS`, for example:

```bash
export GRANOLA_PRIORITY_FOLDERS="Folder A,Folder B"
```

- Before publishing, run:

```bash
zsh ./ops/check-public-hygiene.sh
```
