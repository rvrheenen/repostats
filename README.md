# RepoStats

Local web dashboard that aggregates git statistics across multiple repositories. Configure your repos in a TOML file, and get a single-pane view of commit activity, contributor patterns, language breakdowns, and codebase health.

## Features

- **Summary metrics** — total LOC, commits, active contributors, bus factor, velocity
- **Commit timeline** — stacked bar chart per repo (weekly granularity)
- **Commit heatmap** — hour x day-of-week grid with weekend/off-hours ratios
- **Language breakdown** — horizontal bar chart + table (requires `cloc`)
- **Contributor matrix** — top-20 author x repo table with tenure, cross-repo stats
- **Repo table** — sortable columns for commits, insertions, deletions, contributors
- **Change patterns** — file hotspots, file coupling pairs, knowledge silos
- **Commit size** — average/median stats with distribution histogram
- **Background scanning** — periodic + on-demand, incremental updates, force-push recovery
- **Author normalization** — `.mailmap` support or automatic email-based grouping
- **HTMX interactions** — toggle repos, change time range, refresh scans without page reloads

## Requirements

- Python 3.11+
- `git` in PATH
- `cloc` in PATH (optional — language stats hidden if missing)

## Setup

```bash
# Clone and create venv
git clone <repo-url> && cd repostats
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Configure repos
cp config.toml.example config.toml
# Edit config.toml — add your repo paths
```

## Configuration

```toml
[settings]
scan_interval_minutes = 60     # Background scan interval
db_path = "repostats.db"       # SQLite database location
port = 3300                    # Web UI port
history_depth_days = 0         # 0 = full history; 365 = last year only
mailmap_path = ""              # Path to .mailmap file; empty = email-based fallback

[[repos]]
name = "my-project"
path = "/path/to/my-project"

[[repos]]
name = "another-repo"
path = "/path/to/another-repo"
```

## Usage

```bash
source .venv/bin/activate
python -m repostats.main
```

Open `http://localhost:3300`. On first startup the background collector runs a full scan of all configured repos — progress is shown in the top bar.

Use the sidebar to toggle repos and change the time range. Click **Refresh** to trigger an immediate re-scan.

## Running Tests

```bash
source .venv/bin/activate
pytest tests/ -v
```

## Architecture

```
Background Collector  ──write──>  SQLite (.db)  <──read──  FastAPI (web UI)
(periodic async task)              WAL mode                (Jinja2 + HTMX)
```

- **Collector** scans repos concurrently (max 4) via `git log` subprocess calls
- **SQLite** stores commits, file changes, cloc snapshots, file coupling, scan state
- **Web UI** reads from DB — toggling repos/time ranges is an instant SQL query
- Data is a disposable cache: delete `repostats.db` and re-scan at any time
