"""SQLite database: schema creation, connection management, query helpers."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS commits (
    hash        TEXT NOT NULL,
    repo        TEXT NOT NULL,
    author      TEXT NOT NULL,
    email       TEXT NOT NULL,
    date        TEXT NOT NULL,
    insertions  INTEGER NOT NULL DEFAULT 0,
    deletions   INTEGER NOT NULL DEFAULT 0,
    files_changed INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo, hash)
);
CREATE INDEX IF NOT EXISTS idx_commits_date ON commits(date);
CREATE INDEX IF NOT EXISTS idx_commits_repo ON commits(repo);
CREATE INDEX IF NOT EXISTS idx_commits_author ON commits(author);
CREATE INDEX IF NOT EXISTS idx_commits_author_repo ON commits(author, repo);

CREATE TABLE IF NOT EXISTS file_changes (
    repo        TEXT NOT NULL,
    commit_hash TEXT NOT NULL,
    file_path   TEXT NOT NULL,
    insertions  INTEGER NOT NULL DEFAULT 0,
    deletions   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo, commit_hash, file_path),
    FOREIGN KEY (repo, commit_hash) REFERENCES commits(repo, hash)
);
CREATE INDEX IF NOT EXISTS idx_file_changes_path ON file_changes(repo, file_path);
CREATE INDEX IF NOT EXISTS idx_file_changes_commit ON file_changes(repo, commit_hash);

CREATE TABLE IF NOT EXISTS cloc_snapshots (
    repo        TEXT NOT NULL,
    language    TEXT NOT NULL,
    code        INTEGER NOT NULL DEFAULT 0,
    comment     INTEGER NOT NULL DEFAULT 0,
    blank       INTEGER NOT NULL DEFAULT 0,
    files       INTEGER NOT NULL DEFAULT 0,
    scanned_at  TEXT NOT NULL,
    PRIMARY KEY (repo, language, scanned_at)
);

CREATE TABLE IF NOT EXISTS file_coupling (
    repo        TEXT NOT NULL,
    file_a      TEXT NOT NULL,
    file_b      TEXT NOT NULL,
    coupling_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo, file_a, file_b)
);
CREATE INDEX IF NOT EXISTS idx_file_coupling_count ON file_coupling(repo, coupling_count DESC);

CREATE TABLE IF NOT EXISTS scan_log (
    repo              TEXT PRIMARY KEY,
    last_commit_hash  TEXT,
    last_scanned_at   TEXT,
    commit_count      INTEGER NOT NULL DEFAULT 0,
    total_commits     INTEGER,
    status            TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS file_stats (
    repo              TEXT NOT NULL,
    file_path         TEXT NOT NULL,
    loc               INTEGER NOT NULL DEFAULT 0,
    language          TEXT,
    last_commit_hash  TEXT NOT NULL,
    last_commit_date  TEXT NOT NULL,
    last_author       TEXT NOT NULL,
    first_commit_date TEXT NOT NULL,
    author_count      INTEGER NOT NULL DEFAULT 1,
    scanned_at        TEXT NOT NULL,
    PRIMARY KEY (repo, file_path)
);
CREATE INDEX IF NOT EXISTS idx_file_stats_stale ON file_stats(repo, last_commit_date);
CREATE INDEX IF NOT EXISTS idx_file_stats_loc ON file_stats(repo, loc DESC);
"""


def _repo_filter(
    repos: list[str], after: str | None = None
) -> tuple[str, str, list[str | int]]:
    """Build WHERE clause parts for repo + optional date filtering.

    Returns (placeholders, date_filter, params).
    """
    placeholders = ",".join("?" * len(repos))
    params: list[str | int] = []
    params.extend(repos)
    date_filter = ""
    if after:
        date_filter = "AND date >= ?"
        params.append(after)
    return placeholders, date_filter, params


class Database:
    """Manages SQLite connections: a shared writer and a read-only reader."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._writer: aiosqlite.Connection | None = None
        self._reader: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def open(self) -> None:
        self._writer = await aiosqlite.connect(self.db_path)
        await self._writer.execute("PRAGMA journal_mode=WAL")
        await self._writer.execute("PRAGMA foreign_keys=ON")
        await self._writer.execute("PRAGMA busy_timeout=5000")
        self._writer.row_factory = aiosqlite.Row
        await self._writer.executescript(SCHEMA_SQL)
        await self._writer.commit()

        self._reader = await aiosqlite.connect(self.db_path)
        await self._reader.execute("PRAGMA query_only=ON")
        await self._reader.execute("PRAGMA busy_timeout=5000")
        self._reader.row_factory = aiosqlite.Row
        logger.info("database opened: %s", self.db_path)

    async def close(self) -> None:
        if self._writer:
            await self._writer.close()
        if self._reader:
            await self._reader.close()
        logger.info("database closed")

    @property
    def writer(self) -> aiosqlite.Connection:
        if self._writer is None:
            raise RuntimeError("database not opened")
        return self._writer

    @property
    def reader(self) -> aiosqlite.Connection:
        if self._reader is None:
            raise RuntimeError("database not opened")
        return self._reader

    @property
    def write_lock(self) -> asyncio.Lock:
        return self._write_lock

    # ------------------------------------------------------------------
    # Query helpers (all read from self.reader)
    # ------------------------------------------------------------------

    async def get_repo_summaries(
        self, repos: list[str], after: str | None = None
    ) -> list[dict[str, Any]]:
        """Per-repo summary: commits, insertions, deletions, contributors, date range."""
        if not repos:
            return []
        placeholders, date_filter, params = _repo_filter(repos, after)
        sql = f"""
            SELECT
                repo AS name,
                COUNT(*) AS total_commits,
                SUM(insertions) AS total_insertions,
                SUM(deletions) AS total_deletions,
                COUNT(DISTINCT author) AS active_contributors,
                MIN(date) AS first_commit_date,
                MAX(date) AS last_commit_date
            FROM commits
            WHERE repo IN ({placeholders}) {date_filter}
            GROUP BY repo
        """
        async with self.reader.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_aggregated_stats(
        self, repos: list[str], after: str | None = None
    ) -> dict[str, Any]:
        """Aggregated stats across selected repos."""
        empty: dict[str, Any] = {
            "total_commits": 0, "total_insertions": 0, "total_deletions": 0,
            "active_contributors_30d": 0, "active_contributors_90d": 0,
            "active_contributors_180d": 0, "bus_factor": 0,
        }
        if not repos:
            return empty
        placeholders, date_filter, params = _repo_filter(repos, after)

        sql = f"""
            SELECT
                COUNT(*) AS total_commits,
                SUM(insertions) AS total_insertions,
                SUM(deletions) AS total_deletions
            FROM commits
            WHERE repo IN ({placeholders}) {date_filter}
        """
        async with self.reader.execute(sql, params) as cur:
            row = await cur.fetchone()
        stats: dict[str, Any] = dict(row) if row else dict(empty)

        # Active contributors at various windows
        now = datetime.now(timezone.utc)
        for days, key in [(30, "active_contributors_30d"), (90, "active_contributors_90d"), (180, "active_contributors_180d")]:
            cutoff = (now - timedelta(days=days)).isoformat()
            p: list[str | int] = []
            p.extend(repos)
            p.append(cutoff)
            sql2 = f"""
                SELECT COUNT(DISTINCT author) AS cnt
                FROM commits
                WHERE repo IN ({placeholders}) AND date >= ?
            """
            async with self.reader.execute(sql2, p) as cur:
                r = await cur.fetchone()
            stats[key] = r["cnt"] if r else 0

        # Bus factor: fewest contributors accounting for 80% of commits
        sql_bf = f"""
            SELECT author, COUNT(*) AS cnt
            FROM commits
            WHERE repo IN ({placeholders}) {date_filter}
            GROUP BY author
            ORDER BY cnt DESC
        """
        async with self.reader.execute(sql_bf, params) as cur:
            author_rows = await cur.fetchall()
        total = stats["total_commits"] or 0
        threshold = total * 0.8
        running = 0
        bus_factor = 0
        for ar in author_rows:
            running += ar["cnt"]
            bus_factor += 1
            if running >= threshold:
                break
        stats["bus_factor"] = bus_factor

        return stats

    async def get_total_loc(self, repos: list[str]) -> int:
        """Total lines of code from latest cloc snapshot per repo."""
        if not repos:
            return 0
        placeholders = ",".join("?" * len(repos))
        sql = f"""
            SELECT SUM(code) AS total
            FROM cloc_snapshots
            WHERE (repo, scanned_at) IN (
                SELECT repo, MAX(scanned_at) FROM cloc_snapshots
                WHERE repo IN ({placeholders})
                GROUP BY repo
            )
        """
        async with self.reader.execute(sql, list(repos)) as cur:
            row = await cur.fetchone()
        return (row["total"] or 0) if row else 0

    async def get_commit_timeline(
        self, repos: list[str], after: str | None = None, granularity: str = "weekly"
    ) -> list[dict[str, Any]]:
        """Commit counts per period per repo for timeline chart."""
        if not repos:
            return []
        placeholders, date_filter, params = _repo_filter(repos, after)

        if granularity == "weekly":
            period_expr = "strftime('%Y-W%W', date)"
        else:
            period_expr = "strftime('%Y-%m', date)"

        sql = f"""
            SELECT {period_expr} AS period, repo,
                   COUNT(*) AS commits,
                   SUM(insertions) AS insertions,
                   SUM(deletions) AS deletions
            FROM commits
            WHERE repo IN ({placeholders}) {date_filter}
            GROUP BY period, repo
            ORDER BY period
        """
        async with self.reader.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_commit_heatmap(
        self, repos: list[str], after: str | None = None
    ) -> list[dict[str, Any]]:
        """Hour x day-of-week commit counts."""
        if not repos:
            return []
        placeholders, date_filter, params = _repo_filter(repos, after)

        # SQLite strftime %w: 0=Sunday, we convert to 0=Monday
        sql = f"""
            SELECT
                CAST(strftime('%H', date) AS INTEGER) AS hour,
                (CAST(strftime('%w', date) AS INTEGER) + 6) % 7 AS day_of_week,
                COUNT(*) AS count
            FROM commits
            WHERE repo IN ({placeholders}) {date_filter}
            GROUP BY day_of_week, hour
        """
        async with self.reader.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_contributor_matrix(
        self, repos: list[str], after: str | None = None, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Top contributors with per-repo commit counts and lines changed."""
        if not repos:
            return []
        placeholders, date_filter, params = _repo_filter(repos, after)

        # Single query: top N authors with per-repo breakdown
        sql = f"""
            WITH top_authors AS (
                SELECT author
                FROM commits
                WHERE repo IN ({placeholders}) {date_filter}
                GROUP BY author
                ORDER BY COUNT(*) DESC
                LIMIT ?
            )
            SELECT c.author, c.repo,
                   COUNT(*) AS commits,
                   SUM(c.insertions) AS insertions,
                   SUM(c.deletions) AS deletions,
                   MIN(c.date) AS first_commit,
                   MAX(c.date) AS last_commit
            FROM commits c
            JOIN top_authors ta ON ta.author = c.author
            WHERE c.repo IN ({placeholders}) {date_filter}
            GROUP BY c.author, c.repo
        """
        # params used twice (once in CTE, once in main WHERE), plus limit
        full_params: list[str | int] = []
        full_params.extend(params)
        full_params.append(limit)
        full_params.extend(params)
        async with self.reader.execute(sql, full_params) as cur:
            rows = await cur.fetchall()

        # Assemble into per-author dicts
        authors: dict[str, dict[str, Any]] = {}
        for r in rows:
            author: str = r["author"]
            if author not in authors:
                authors[author] = {
                    "author": author,
                    "repo_commits": {},
                    "repo_lines": {},
                    "total_commits": 0,
                    "total_lines": 0,
                    "first_commit": r["first_commit"],
                    "last_commit": r["last_commit"],
                }
            entry = authors[author]
            entry["repo_commits"][r["repo"]] = r["commits"]
            entry["repo_lines"][r["repo"]] = r["insertions"] + r["deletions"]
            entry["total_commits"] += r["commits"]
            entry["total_lines"] += r["insertions"] + r["deletions"]
            if r["first_commit"] < entry["first_commit"]:
                entry["first_commit"] = r["first_commit"]
            if r["last_commit"] > entry["last_commit"]:
                entry["last_commit"] = r["last_commit"]

        result = list(authors.values())
        result.sort(key=lambda a: a["total_commits"], reverse=True)
        return result

    async def get_cross_repo_contributors(
        self, repos: list[str], after: str | None = None
    ) -> list[dict[str, Any]]:
        """Contributors active in multiple repos."""
        if not repos:
            return []
        placeholders, date_filter, params = _repo_filter(repos, after)
        sql = f"""
            SELECT author, COUNT(DISTINCT repo) AS repo_count, COUNT(*) AS total_commits
            FROM commits
            WHERE repo IN ({placeholders}) {date_filter}
            GROUP BY author
            HAVING repo_count > 1
            ORDER BY repo_count DESC, total_commits DESC
        """
        async with self.reader.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_new_vs_returning(
        self, repos: list[str], after: str | None = None
    ) -> dict[str, int]:
        """New vs returning contributors in the given period."""
        if not repos or not after:
            return {"new": 0, "returning": 0}
        placeholders = ",".join("?" * len(repos))

        # Authors who committed in period
        sql_period = f"""
            SELECT DISTINCT author FROM commits
            WHERE repo IN ({placeholders}) AND date >= ?
        """
        period_params: list[str | int] = []
        period_params.extend(repos)
        period_params.append(after)
        async with self.reader.execute(sql_period, period_params) as cur:
            period_authors = {r["author"] for r in await cur.fetchall()}

        # Authors who committed before period
        sql_before = f"""
            SELECT DISTINCT author FROM commits
            WHERE repo IN ({placeholders}) AND date < ?
        """
        async with self.reader.execute(sql_before, period_params) as cur:
            before_authors = {r["author"] for r in await cur.fetchall()}

        new = period_authors - before_authors
        returning = period_authors & before_authors
        return {"new": len(new), "returning": len(returning)}

    async def get_language_breakdown(self, repos: list[str]) -> list[dict[str, Any]]:
        """Language stats from latest cloc snapshot, aggregated across repos."""
        if not repos:
            return []
        placeholders = ",".join("?" * len(repos))
        sql = f"""
            SELECT language,
                   SUM(code) AS code,
                   SUM(comment) AS comment,
                   SUM(blank) AS blank,
                   SUM(files) AS files
            FROM cloc_snapshots
            WHERE (repo, scanned_at) IN (
                SELECT repo, MAX(scanned_at) FROM cloc_snapshots
                WHERE repo IN ({placeholders})
                GROUP BY repo
            )
            GROUP BY language
            ORDER BY code DESC
        """
        async with self.reader.execute(sql, list(repos)) as cur:
            rows = await cur.fetchall()
        results: list[dict[str, Any]] = [dict(r) for r in rows]
        total_code = sum(r["code"] for r in results) or 1
        for r in results:
            r["percentage"] = round(r["code"] / total_code * 100, 1)
        return results

    async def get_code_to_comment_ratio(self, repos: list[str]) -> float:
        """Ratio of code lines to comment lines from latest cloc snapshot."""
        if not repos:
            return 0.0
        placeholders = ",".join("?" * len(repos))
        sql = f"""
            SELECT SUM(code) AS code, SUM(comment) AS comment
            FROM cloc_snapshots
            WHERE (repo, scanned_at) IN (
                SELECT repo, MAX(scanned_at) FROM cloc_snapshots
                WHERE repo IN ({placeholders})
                GROUP BY repo
            )
        """
        async with self.reader.execute(sql, list(repos)) as cur:
            row = await cur.fetchone()
        if not row or not row["comment"]:
            return 0.0
        return round(row["code"] / row["comment"], 1)

    async def get_file_hotspots(
        self, repos: list[str], after: str | None = None, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Most frequently modified files."""
        if not repos:
            return []
        placeholders = ",".join("?" * len(repos))
        fc_date_filter = ""
        params: list[str | int] = []
        params.extend(repos)
        if after:
            fc_date_filter = """
                AND commit_hash IN (
                    SELECT hash FROM commits WHERE repo = file_changes.repo AND date >= ?
                )
            """
            params.append(after)

        sql = f"""
            SELECT repo, file_path, COUNT(*) AS change_count
            FROM file_changes
            WHERE repo IN ({placeholders}) {fc_date_filter}
            GROUP BY repo, file_path
            ORDER BY change_count DESC
            LIMIT ?
        """
        params.append(limit)
        async with self.reader.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_file_coupling(
        self, repos: list[str], limit: int = 20
    ) -> list[dict[str, Any]]:
        """Top file coupling pairs."""
        if not repos:
            return []
        placeholders = ",".join("?" * len(repos))
        sql = f"""
            SELECT repo, file_a, file_b, coupling_count
            FROM file_coupling
            WHERE repo IN ({placeholders})
            ORDER BY coupling_count DESC
            LIMIT ?
        """
        params: list[str | int] = []
        params.extend(repos)
        params.append(limit)
        async with self.reader.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_commit_size_stats(
        self, repos: list[str], after: str | None = None
    ) -> dict[str, Any]:
        """Average commit size and distribution histogram."""
        if not repos:
            return {"average_size": 0, "median_size": 0, "buckets": []}
        placeholders, date_filter, params = _repo_filter(repos, after)

        sql = f"""
            SELECT insertions + deletions AS size
            FROM commits
            WHERE repo IN ({placeholders}) {date_filter}
            ORDER BY size
        """
        async with self.reader.execute(sql, params) as cur:
            rows = await cur.fetchall()

        sizes: list[int] = [r["size"] for r in rows]
        if not sizes:
            return {"average_size": 0, "median_size": 0, "buckets": []}

        avg = sum(sizes) / len(sizes)
        mid = len(sizes) // 2
        median = sizes[mid] if len(sizes) % 2 else (sizes[mid - 1] + sizes[mid]) / 2

        bucket_ranges: list[tuple[str, int, float]] = [
            ("1-10", 1, 10), ("11-50", 11, 50), ("51-100", 51, 100),
            ("101-500", 101, 500), ("501-1000", 501, 1000), ("1000+", 1001, float("inf")),
        ]
        buckets: list[dict[str, str | int]] = []
        for label, lo, hi in bucket_ranges:
            count = sum(1 for s in sizes if lo <= s <= hi)
            buckets.append({"label": label, "count": count})

        return {"average_size": round(avg, 1), "median_size": median, "buckets": buckets}

    async def get_velocity(self, repos: list[str], days: int = 30) -> dict[str, int]:
        """Net code growth and churn over a recent period."""
        if not repos:
            return {"net_growth": 0, "churn": 0, "insertions": 0, "deletions": 0}
        placeholders = ",".join("?" * len(repos))
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        params: list[str | int] = []
        params.extend(repos)
        params.append(cutoff)
        sql = f"""
            SELECT
                COALESCE(SUM(insertions), 0) AS insertions,
                COALESCE(SUM(deletions), 0) AS deletions
            FROM commits
            WHERE repo IN ({placeholders}) AND date >= ?
        """
        async with self.reader.execute(sql, params) as cur:
            row = await cur.fetchone()
        ins: int = row["insertions"] if row else 0
        dels: int = row["deletions"] if row else 0
        return {
            "net_growth": ins - dels,
            "churn": ins + dels,
            "insertions": ins,
            "deletions": dels,
        }

    async def get_scan_status(self) -> list[dict[str, Any]]:
        """Get scan status for all repos."""
        sql = "SELECT * FROM scan_log ORDER BY repo"
        async with self.reader.execute(sql) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_knowledge_silos(
        self, repos: list[str], max_authors: int = 2, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Files/directories where only 1-2 people have committed."""
        if not repos:
            return []
        placeholders = ",".join("?" * len(repos))
        sql = f"""
            SELECT fc.repo, fc.file_path, COUNT(DISTINCT c.author) AS author_count,
                   GROUP_CONCAT(DISTINCT c.author) AS authors
            FROM file_changes fc
            JOIN commits c ON c.repo = fc.repo AND c.hash = fc.commit_hash
            WHERE fc.repo IN ({placeholders})
            GROUP BY fc.repo, fc.file_path
            HAVING author_count <= ?
            ORDER BY author_count, fc.file_path
            LIMIT ?
        """
        params: list[str | int] = []
        params.extend(repos)
        params.append(max_authors)
        params.append(limit)
        async with self.reader.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_stale_files(
        self, repos: list[str], threshold_days: int = 180, min_loc: int = 50, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Files not touched in threshold_days with at least min_loc lines of code."""
        if not repos:
            return []
        placeholders = ",".join("?" * len(repos))
        cutoff = (datetime.now(timezone.utc) - timedelta(days=threshold_days)).isoformat()
        sql = f"""
            SELECT repo, file_path, loc, language, last_commit_date, last_author, author_count
            FROM file_stats
            WHERE repo IN ({placeholders})
              AND last_commit_date < ?
              AND loc >= ?
            ORDER BY loc DESC
            LIMIT ?
        """
        params: list[str | int] = []
        params.extend(repos)
        params.append(cutoff)
        params.append(min_loc)
        params.append(limit)
        async with self.reader.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_average_file_age(self, repos: list[str]) -> float:
        """Mean days since last commit across all tracked files."""
        if not repos:
            return 0.0
        placeholders = ",".join("?" * len(repos))
        sql = f"""
            SELECT AVG(julianday('now') - julianday(last_commit_date)) AS avg_age
            FROM file_stats
            WHERE repo IN ({placeholders})
        """
        async with self.reader.execute(sql, list(repos)) as cur:
            row = await cur.fetchone()
        return round(row["avg_age"], 1) if row and row["avg_age"] is not None else 0.0

    async def get_file_age_distribution(self, repos: list[str]) -> list[dict[str, Any]]:
        """Count of files in age buckets: <30d, 30-90d, 90-180d, 180d-1y, 1y+."""
        if not repos:
            return [
                {"label": "<30d", "count": 0}, {"label": "30-90d", "count": 0},
                {"label": "90-180d", "count": 0}, {"label": "180d-1y", "count": 0},
                {"label": "1y+", "count": 0},
            ]
        placeholders = ",".join("?" * len(repos))
        sql = f"""
            SELECT
                SUM(CASE WHEN age < 30 THEN 1 ELSE 0 END) AS under_30,
                SUM(CASE WHEN age >= 30 AND age < 90 THEN 1 ELSE 0 END) AS d30_90,
                SUM(CASE WHEN age >= 90 AND age < 180 THEN 1 ELSE 0 END) AS d90_180,
                SUM(CASE WHEN age >= 180 AND age < 365 THEN 1 ELSE 0 END) AS d180_1y,
                SUM(CASE WHEN age >= 365 THEN 1 ELSE 0 END) AS over_1y
            FROM (
                SELECT julianday('now') - julianday(last_commit_date) AS age
                FROM file_stats
                WHERE repo IN ({placeholders})
            )
        """
        async with self.reader.execute(sql, list(repos)) as cur:
            row = await cur.fetchone()
        if not row:
            return [
                {"label": "<30d", "count": 0}, {"label": "30-90d", "count": 0},
                {"label": "90-180d", "count": 0}, {"label": "180d-1y", "count": 0},
                {"label": "1y+", "count": 0},
            ]
        return [
            {"label": "<30d", "count": row["under_30"] or 0},
            {"label": "30-90d", "count": row["d30_90"] or 0},
            {"label": "90-180d", "count": row["d90_180"] or 0},
            {"label": "180d-1y", "count": row["d180_1y"] or 0},
            {"label": "1y+", "count": row["over_1y"] or 0},
        ]

    async def get_weekend_ratio(
        self, repos: list[str], after: str | None = None
    ) -> dict[str, float | int]:
        """Weekend and off-hours commit ratios."""
        if not repos:
            return {"weekend_ratio": 0, "off_hours_ratio": 0}
        placeholders, date_filter, params = _repo_filter(repos, after)

        sql = f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN CAST(strftime('%w', date) AS INTEGER) IN (0, 6) THEN 1 ELSE 0 END) AS weekend,
                SUM(CASE WHEN CAST(strftime('%H', date) AS INTEGER) NOT BETWEEN 9 AND 17 THEN 1 ELSE 0 END) AS off_hours
            FROM commits
            WHERE repo IN ({placeholders}) {date_filter}
        """
        async with self.reader.execute(sql, params) as cur:
            row = await cur.fetchone()
        if not row or not row["total"]:
            return {"weekend_ratio": 0, "off_hours_ratio": 0}
        return {
            "weekend_ratio": round(row["weekend"] / row["total"] * 100, 1),
            "off_hours_ratio": round(row["off_hours"] / row["total"] * 100, 1),
        }
