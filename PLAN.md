# RepoStats Dashboard — Implementation Plan

## Context

Build a local web dashboard that aggregates git statistics across multiple repositories. The user configures repos in a TOML file, and the dashboard lets them toggle which repos are included in the aggregation. This gives a single-pane view of codebase health, activity, and contributor patterns across a portfolio of repos.

**Scale assumption:** repos can be large (8+ years of history, 100+ contributors, tens of thousands of commits). The design must handle initial imports gracefully — with progress feedback, concurrency, and optional history depth limiting.

## Tech Stack

| Layer           | Choice                          | Why                                                                    |
| --------------- | ------------------------------- | ---------------------------------------------------------------------- |
| Backend         | **FastAPI**                     | Async (parallel collection across repos), Pydantic models, lightweight |
| Frontend        | **Jinja2 + HTMX + Chart.js**    | No build step, no npm, server-rendered with interactive toggles        |
| Data collection | **subprocess → `git` + `cloc`** | No gitpython dependency, full control, fast                            |
| Storage         | **SQLite** (`aiosqlite`, WAL)   | Stdlib engine, async access, instant filtering via SQL queries         |
| Config          | **TOML** (`tomllib` stdlib)     | No extra dependency, pleasant to hand-edit                             |
| Server          | **uvicorn**                     | Standard ASGI server for FastAPI                                       |

## Architecture

```
┌─────────────────┐        ┌──────────┐        ┌──────────────┐
│ Background       │──────▶│  SQLite   │◀──────│  FastAPI      │
│ Collector        │ write  │  (.db)   │  read  │  (web UI)     │
│ (periodic scan)  │        └──────────┘        └──────────────┘
└─────────────────┘
```

Data collection is **separated from serving**. A background async task scans repos
on a configurable interval and writes normalized data into SQLite. The web UI only
reads from the database, so toggling repos/tags/time ranges is an instant SQL query
with no subprocess calls.

### Database Schema

SQLite is opened with `PRAGMA journal_mode=WAL` to allow concurrent reads (web UI) while the collector writes.

```sql
-- One row per commit
CREATE TABLE commits (
    hash        TEXT NOT NULL,
    repo        TEXT NOT NULL,
    author      TEXT NOT NULL,     -- canonical name (after mailmap/email normalization)
    email       TEXT NOT NULL,     -- canonical email
    date        TEXT NOT NULL,     -- ISO-8601
    insertions  INTEGER NOT NULL DEFAULT 0,
    deletions   INTEGER NOT NULL DEFAULT 0,
    files_changed INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo, hash)
);
CREATE INDEX idx_commits_date ON commits(date);
CREATE INDEX idx_commits_repo ON commits(repo);
CREATE INDEX idx_commits_author ON commits(author);
CREATE INDEX idx_commits_author_repo ON commits(author, repo);  -- cross-repo contributor queries

-- Per-file change tracking for hotspot analysis
CREATE TABLE file_changes (
    repo        TEXT NOT NULL,
    commit_hash TEXT NOT NULL,
    file_path   TEXT NOT NULL,
    insertions  INTEGER NOT NULL DEFAULT 0,
    deletions   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo, commit_hash, file_path),
    FOREIGN KEY (repo, commit_hash) REFERENCES commits(repo, hash)
);
CREATE INDEX idx_file_changes_path ON file_changes(repo, file_path);
CREATE INDEX idx_file_changes_commit ON file_changes(repo, commit_hash);  -- coupling analysis

-- Periodic cloc snapshots
CREATE TABLE cloc_snapshots (
    repo        TEXT NOT NULL,
    language    TEXT NOT NULL,
    code        INTEGER NOT NULL DEFAULT 0,
    comment     INTEGER NOT NULL DEFAULT 0,
    blank       INTEGER NOT NULL DEFAULT 0,
    files       INTEGER NOT NULL DEFAULT 0,
    scanned_at  TEXT NOT NULL,  -- ISO-8601
    PRIMARY KEY (repo, language, scanned_at)
);

-- Pre-computed file coupling (files that frequently change together)
-- Populated at scan time from file_changes; excludes commits touching >30 files
CREATE TABLE file_coupling (
    repo        TEXT NOT NULL,
    file_a      TEXT NOT NULL,
    file_b      TEXT NOT NULL,     -- file_a < file_b lexicographically to avoid duplicate pairs
    coupling_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo, file_a, file_b)
);
CREATE INDEX idx_file_coupling_count ON file_coupling(repo, coupling_count DESC);

-- Tracks scan state for incremental collection
CREATE TABLE scan_log (
    repo              TEXT PRIMARY KEY,
    last_commit_hash  TEXT,
    last_scanned_at   TEXT,     -- ISO-8601
    commit_count      INTEGER NOT NULL DEFAULT 0,
    total_commits     INTEGER,  -- NULL until first full scan completes; used for progress reporting
    status            TEXT NOT NULL DEFAULT 'pending'  -- pending | scanning | done | error
);
```

**Migration strategy:** This is a local cache, not a source of truth. If the schema changes, delete `repostats.db` and re-scan. The collector is designed for fast re-import.

**Stale repo data:** If a repo is removed from `config.toml`, its data remains in the DB. This is intentional — the DB is a disposable cache and can be rebuilt at any time. No automatic pruning.

**Connection management:** SQLite serializes writes, so the collector uses a **single shared writer connection** (with an `asyncio.Lock` around write operations) to avoid `SQLITE_BUSY` errors from concurrent scanners. The web UI uses a separate read-only connection. Both connections are managed in `db.py` and opened during the FastAPI lifespan.

### Author Identity Normalization

Git users frequently commit under different name/email combinations across repos. To make cross-repo contributor metrics accurate, identities are normalized at collection time:

1. If `mailmap_path` is set in config, load and parse the `.mailmap` file (standard git mailmap format)
2. If no mailmap is configured, fall back to **email-based matching** — all commits sharing the same email are attributed to the same canonical author name (the most recent one seen)
3. Normalized `author` and `email` are what get stored in the `commits` table

### Background Collector Flow

1. On startup, the FastAPI lifespan spawns a background `asyncio` task
2. The task loops every `scan_interval_minutes` (configurable)
3. On startup, check that each configured repo path exists and is a git repo — log a warning and skip invalid ones
4. Repos are scanned **concurrently** using `asyncio.gather` with a semaphore (max 4 parallel scans) to bound subprocess load
5. For each configured repo:
   a. Check `scan_log` for `last_commit_hash`
   b. If no prior scan → full `git log` import (respecting `history_depth_days` if set)
   c. If prior scan exists → incremental `git log <last_hash>..HEAD`
      - If `last_hash` is no longer reachable (force-push/rebase → non-zero exit from git), clear that repo's data and fall back to a full re-scan
   d. Parse commit data, apply author normalization, bulk-insert into `commits` and `file_changes` (all inserts for a repo wrapped in a single transaction for performance)
   e. Compute file coupling pairs and upsert into `file_coupling` (see schema below)
   f. Run `cloc` and insert a new `cloc_snapshots` row (skip gracefully if `cloc` is not installed)
   g. Update `scan_log` with new hash, timestamp, and commit count
   h. Progress is tracked in `scan_log.commit_count` / `scan_log.total_commits` and exposed via the UI
6. The UI can also trigger an immediate scan via `POST /api/refresh`

### Git Log Parsing

Use a null-byte-delimited format to avoid ambiguity:

```
git log --no-merges --format='%H%x00%an%x00%ae%x00%aI' --numstat
```

- `--no-merges` excludes merge commits — they have no meaningful numstat and would inflate insertion/deletion counts
- `%H` = full hash, `%an` = author name, `%ae` = author email, `%aI` = ISO-8601 date
- `%x00` = null byte separator — safe because these fields never contain null bytes
- `--numstat` appends `insertions\tdeletions\tfile_path` lines after each commit
- For initial imports with history limiting: add `--after=<date>` based on `history_depth_days`

### cloc Dependency

`cloc` is an optional external dependency. On startup, check if `cloc` is available in `$PATH`:
- **If present:** language stats are collected on each scan cycle
- **If absent:** log a warning once, skip `cloc_snapshots` collection, and hide language breakdown widgets in the UI

## Metrics (V1)

**Core:**

- Lines of Code (by language, via `cloc`)
- Additions / Deletions (total and per time period)
- Number of commits

**Activity:**

- Commit frequency over time (weekly/monthly timeline)
- Commit time heatmap (hour × day-of-week grid)
- Active contributors (unique committers in last 30/90/180 days)
- Top contributors (by commits and by lines changed)
- Bus factor (contributors accounting for 80% of commits)
- Weekend/off-hours commit ratio

**Contributors (cross-repo):**

- Commits per user across repos (table: user × repo matrix, top 20 by default with "show all" expand)
- Cross-repo contributors (people active in multiple repos)
- Contributor tenure (first commit → last commit span per person)
- New vs returning contributors per period

**Codebase health:**

- Language breakdown (% by language)
- File count by language
- Code-to-comment ratio
- Knowledge silos (files/directories where only 1-2 people have committed)

**Change patterns:**

- File hotspots (most frequently modified files)
- File coupling (files that frequently change together in the same commit)
  - Pre-computed into `file_coupling` table at scan time; excludes commits touching >30 files (bulk renames, large refactors)
  - UI shows top 20 most-coupled pairs
- Average commit size (insertions + deletions per commit)
- Commit size distribution (histogram)
- Recent velocity (net code growth over last 30 days)
- Code churn (adds + removes)

**Age:**

- Repo age (first commit date)
- Last commit date / days since last commit

## Config Format (`config.toml`)

```toml
[settings]
scan_interval_minutes = 60     # How often the background collector runs
db_path = "repostats.db"       # SQLite database location (relative to project root)
port = 3300                    # Web UI port
history_depth_days = 0         # 0 = full history; e.g. 365 = only import last year of commits
mailmap_path = ""              # Path to a .mailmap file for author normalization; empty = email-based fallback

[[repos]]
name = "risk-ng"
path = "/Users/rick/workspaces/risk-ng"
tags = ["python", "core"]

[[repos]]
name = "chdb"
path = "/Users/rick/workspaces/chdb"
tags = ["database"]
```

- **Tags** enable bulk filtering in the sidebar. Name is optional (defaults to directory name).
- **`history_depth_days`**: For large repos, limits the initial import to recent history. Incremental scans always pick up everything since the last scan regardless of this setting.
- **`mailmap_path`**: Points to a `.mailmap` file (standard git format). If empty or missing, author normalization falls back to email-based matching.

## Project Structure

```
repostats/
├── pyproject.toml
├── config.toml
├── repostats/
│   ├── __init__.py
│   ├── main.py              # FastAPI app + uvicorn entry point + background collector task
│   ├── config.py            # TOML loading → Pydantic models
│   ├── models.py            # Pydantic models (RepoStats, AggregatedStats, etc.)
│   ├── collector.py         # Git/cloc subprocess calls + output parsing + author normalization
│   ├── mailmap.py           # .mailmap parser + email-based fallback normalization
│   ├── db.py                # SQLite schema creation (WAL mode), connection management, query helpers
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── dashboard.py     # GET / → full page
│   │   └── api.py           # POST /api/aggregate, POST /api/refresh, GET /api/scan-status
│   ├── templates/
│   │   ├── base.html
│   │   ├── dashboard.html
│   │   └── partials/
│   │       ├── summary_cards.html      # LOC, commit count, active contributors, bus factor
│   │       ├── repo_table.html         # Sortable table: repo name, LOC, commits, last commit, adds/dels, age
│   │       ├── commit_timeline.html    # Chart.js stacked bar (weekly/monthly), velocity, churn
│   │       ├── commit_heatmap.html     # Hour × day-of-week grid, weekend/off-hours ratio
│   │       ├── commit_size.html        # Average commit size, size distribution histogram
│   │       ├── language_breakdown.html # Horizontal bar by language, code-to-comment ratio, file counts
│   │       ├── contributor_matrix.html # Top-N user × repo matrix (with "show all" expand), tenure, new vs returning
│   │       ├── change_patterns.html    # File hotspots, file coupling (top pairs), knowledge silos
│   │       └── scan_status.html        # Last scan time, progress bar, conditional HTMX polling
│   └── static/
│       └── style.css
├── tests/
│   ├── test_collector.py
│   ├── test_db.py
│   └── test_models.py
└── repostats.db             # Generated at runtime, gitignored
```

## Frontend Layout

```
┌──────────────────────────────────────────────────────────────┐
│  RepoStats Dashboard          [Scan: 2m ago] [Refresh] [Time ▾]│
├──────────────┬───────────────────────────────────────────────┤
│  SIDEBAR     │  SUMMARY CARDS                                │
│              │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐        │
│  Tags:       │  │ LOC  │ │Commit│ │Active│ │ Bus  │        │
│  [x] python  │  │ 120k │ │ 1.2k │ │Contb │ │Factor│        │
│  [x] database│  └──────┘ └──────┘ └──────┘ └──────┘        │
│              │                                               │
│  Repos:      │  COMMIT TIMELINE (stacked bar, per repo)     │
│  [x] risk-ng │  ▇▇▅▇▃▇▅▇▇▅▇▃                               │
│  [x] chdb    │                                               │
│  [x] rtdb    │  COMMIT HEATMAP (hour × day-of-week grid)    │
│  [ ] foo     │  ░░▒▓█▓▒░░░░░░░░░░░░░▒▓▓▒░                  │
│              │                                               │
│  [All][None] │  LANGUAGE BREAKDOWN (horizontal bar)          │
│              │  Python ████████ 40%                          │
│              │  Rust   █████ 28%                              │
│              │                                               │
│              │  CONTRIBUTOR × REPO MATRIX                    │
│              │         │risk-ng│ chdb │ rtdb │ total │       │
│              │  alice  │  120  │  45  │   —  │  165  │       │
│              │  bob    │   80  │  12  │  90  │  182  │       │
│              │                                               │
│              │  REPO TABLE (sortable columns)                │
│              │  Name │ LOC │ Commits │ Last │ Adds │ ...     │
└──────────────┴───────────────────────────────────────────────┘
```

- Checkbox toggles fire HTMX `POST /api/aggregate` → server returns the main content area as the primary swap target, with additional partials (summary cards, scan status) using `hx-swap-oob="true"` so everything updates in a single response
- Tag checkboxes bulk-toggle all repos with that tag
- Time range dropdown filters commit-based metrics (cloc snapshots are point-in-time and always show the latest scan, unaffected by time range)
- **Time range options:** 30 days, 90 days, 180 days, 1 year, All time
- Scan status indicator shows last scan time, in-progress state, and progress (e.g., "scanning risk-ng: 4,200 / 12,000 commits")

### API Endpoints

| Endpoint               | Method | Purpose                                                                 |
| ---------------------- | ------ | ----------------------------------------------------------------------- |
| `GET /`                | GET    | Full dashboard page (Jinja2 rendered)                                   |
| `POST /api/aggregate`  | POST   | Returns updated dashboard partials for selected repos + time range      |
| `POST /api/refresh`    | POST   | Triggers an immediate background scan; returns scan status partial      |
| `GET /api/scan-status` | GET    | Returns scan status partial (polled by HTMX `hx-trigger="every 3s"`)   |

**`POST /api/aggregate` request** (form-encoded via HTMX checkboxes):
```
repos=risk-ng&repos=chdb&time_range=90d
```
Returns: all dashboard partials as a single HTMX multi-swap response (`hx-swap-oob`).

**`GET /api/scan-status` response:** returns the `scan_status.html` partial. The UI polls this only while a scan is in progress (the partial includes `hx-trigger="every 3s"` when status is `scanning`, and omits it when idle).

## Implementation Phases (V1)

### Phase 1: Foundation

1. Create `pyproject.toml` (deps: fastapi, uvicorn, jinja2, pydantic, aiosqlite)
2. Create directory structure
3. Implement `config.py` — load TOML, validate repo paths exist and are git repos, validate `mailmap_path`
4. Define all models in `models.py`

### Phase 2: Storage

5. Implement `db.py` — schema creation (with WAL mode), connection management, query helpers
6. Implement aggregation queries — sum/merge stats across selected repos via SQL

### Phase 3: Data Collection

7. Implement `mailmap.py` — parse `.mailmap` file, email-based fallback normalization
8. Implement `collector.py` — async subprocess calls to git/cloc, null-byte-delimited output parsing
9. Full scan mode: import git history (respecting `history_depth_days`) into `commits` + `file_changes`
10. Incremental scan mode: `git log <last_hash>..HEAD` for repos already in the DB; if `last_hash` is unreachable (force-push), clear repo data and fall back to full re-scan
11. Concurrent scanning with `asyncio.gather` + semaphore (max 4), progress tracking in `scan_log`
12. File coupling computation: after inserting `file_changes`, compute pairwise co-occurrence counts (excluding commits with >30 files) and upsert into `file_coupling`
13. `cloc` runner: check availability on startup, snapshot language stats into `cloc_snapshots`, skip gracefully if missing

### Phase 4: Backend

14. Wire up FastAPI app in `main.py` — lifespan creates DB, checks `cloc`, spawns background collector task
15. Implement routes — `GET /`, `POST /api/aggregate`, `POST /api/refresh`, `GET /api/scan-status`
16. `POST /api/refresh` triggers an immediate scan outside the scheduled interval
17. Add structured logging (`logging` stdlib) for collector progress, scan errors, and startup diagnostics

### Phase 5: Frontend

18. Build templates — base layout, dashboard, HTMX partials (using `hx-swap-oob` for multi-fragment updates)
19. Add Chart.js charts — commit timeline, commit heatmap, language breakdown, commit size histogram
20. Add `style.css` — CSS Grid layout, cards, table styling

### Phase 6: Polish

21. Time range filter (30d / 90d / 180d / 1y / all), sortable table, error handling for missing/invalid repos
22. Scan status indicator with progress (polled via `GET /api/scan-status`, e.g., "scanning risk-ng: 4,200 / 12,000 commits")
23. Contributor matrix: top-20 default with "show all" expand
24. Tests for git log parser, mailmap normalization, DB queries, coupling computation, and aggregation logic

## V2 — Deferred Metrics

These metrics require schema additions or expensive collection steps beyond the V1 scope. They'll be added once the core dashboard is stable and battle-tested against real repos.

**Rework ratio** — "% of changes to code less than 30 days old." Requires `git blame`-level line provenance tracking. Schema addition: periodic blame snapshots or a `line_age` table. Expensive to collect for large repos.

**Stale files** — "files not touched in 6+ months with significant LOC." Requires per-file LOC (current `cloc_snapshots` is per-language aggregate). Schema addition: `file_stats` table with (repo, file_path, loc, last_commit_date, last_author).

**Average file age** — "mean last-modified date across files." Same `file_stats` table as stale files.

**Directory-level growth trends** — derivable from `file_changes` by extracting directory paths, but needs pre-computed rollups to be fast at query time.

## Verification

1. `pip install -e .` and run `python -m repostats.main`
2. On first startup, background collector runs a full scan — check logs for progress reporting per repo
3. Open `http://localhost:3300` — dashboard loads with all configured repos
4. Toggle repos on/off — summary cards, charts, and table update without page reload (instant, reads from DB)
5. Click Refresh — triggers an immediate re-scan, new commits appear in the UI
6. Verify scan progress is visible in the UI during a long initial import
7. Stop and restart the server — data persists in `repostats.db`, incremental scan picks up only new commits
8. Test with `cloc` uninstalled — language widgets should be hidden, everything else works
9. Test with an invalid repo path in config — should log a warning and skip, not crash
10. Test mailmap normalization — same person with different emails should appear as one contributor
11. Test force-push recovery — simulate an unreachable `last_commit_hash` in `scan_log`, verify the collector clears and re-scans
12. Run `pytest tests/` — parser, mailmap, DB, coupling computation, and aggregation tests pass
