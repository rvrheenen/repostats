"""Tests for SQLite database queries and aggregation logic."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from repostats.db import Database


@pytest_asyncio.fixture
async def db(tmp_path: object) -> Database:  # type: ignore[type-arg]
    """Create a temporary in-memory-like database."""
    import pathlib

    db_path = str(pathlib.Path(str(tmp_path)) / "test.db")
    database = Database(db_path)
    await database.open()
    yield database  # type: ignore[misc]
    await database.close()


async def _insert_commits(db: Database, rows: list[tuple[str, str, str, str, str, int, int, int]]) -> None:
    """Insert test commit rows."""
    async with db.write_lock:
        await db.writer.executemany(
            """INSERT OR IGNORE INTO commits
               (hash, repo, author, email, date, insertions, deletions, files_changed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        await db.writer.commit()


async def _insert_file_changes(db: Database, rows: list[tuple[str, str, str, int, int]]) -> None:
    """Insert test file_changes rows."""
    async with db.write_lock:
        await db.writer.executemany(
            """INSERT OR IGNORE INTO file_changes
               (repo, commit_hash, file_path, insertions, deletions)
               VALUES (?, ?, ?, ?, ?)""",
            rows,
        )
        await db.writer.commit()


async def _insert_cloc(db: Database, rows: list[tuple[str, str, int, int, int, int, str]]) -> None:
    """Insert test cloc_snapshots rows."""
    async with db.write_lock:
        await db.writer.executemany(
            """INSERT INTO cloc_snapshots
               (repo, language, code, comment, blank, files, scanned_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        await db.writer.commit()


# ------------------------------------------------------------------
# get_repo_summaries
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repo_summaries_empty(db: Database) -> None:
    result = await db.get_repo_summaries(["nonexistent"])
    assert result == []


@pytest.mark.asyncio
async def test_repo_summaries_basic(db: Database) -> None:
    await _insert_commits(db, [
        ("h1", "repo1", "Alice", "a@b.com", "2024-01-10T10:00:00+00:00", 10, 5, 1),
        ("h2", "repo1", "Bob", "b@b.com", "2024-01-11T10:00:00+00:00", 20, 3, 2),
        ("h3", "repo2", "Alice", "a@b.com", "2024-01-12T10:00:00+00:00", 5, 1, 1),
    ])
    result = await db.get_repo_summaries(["repo1", "repo2"])
    assert len(result) == 2

    r1 = next(r for r in result if r["name"] == "repo1")
    assert r1["total_commits"] == 2
    assert r1["total_insertions"] == 30
    assert r1["total_deletions"] == 8
    assert r1["active_contributors"] == 2


# ------------------------------------------------------------------
# get_aggregated_stats
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregated_stats_bus_factor(db: Database) -> None:
    """Bus factor: fewest contributors for 80% of commits."""
    now = datetime.now(timezone.utc)
    rows = []
    # Alice: 80 commits, Bob: 15, Charlie: 5
    for i in range(80):
        d = (now - timedelta(days=i % 20)).isoformat()
        rows.append((f"a{i}", "repo", "Alice", "a@b.com", d, 1, 0, 1))
    for i in range(15):
        d = (now - timedelta(days=i % 10)).isoformat()
        rows.append((f"b{i}", "repo", "Bob", "b@b.com", d, 1, 0, 1))
    for i in range(5):
        d = (now - timedelta(days=i)).isoformat()
        rows.append((f"c{i}", "repo", "Charlie", "c@b.com", d, 1, 0, 1))
    await _insert_commits(db, rows)

    stats = await db.get_aggregated_stats(["repo"])
    assert stats["total_commits"] == 100
    assert stats["bus_factor"] == 1  # Alice alone has 80% of commits


@pytest.mark.asyncio
async def test_aggregated_stats_active_contributors(db: Database) -> None:
    """Active contributor windows at 30/90/180 days."""
    now = datetime.now(timezone.utc)
    await _insert_commits(db, [
        ("h1", "repo", "Recent", "r@b.com", (now - timedelta(days=5)).isoformat(), 1, 0, 1),
        ("h2", "repo", "Medium", "m@b.com", (now - timedelta(days=60)).isoformat(), 1, 0, 1),
        ("h3", "repo", "Old", "o@b.com", (now - timedelta(days=200)).isoformat(), 1, 0, 1),
    ])
    stats = await db.get_aggregated_stats(["repo"])
    assert stats["active_contributors_30d"] == 1
    assert stats["active_contributors_90d"] == 2
    assert stats["active_contributors_180d"] == 2  # Old is beyond 180d


# ------------------------------------------------------------------
# get_total_loc
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_total_loc(db: Database) -> None:
    await _insert_cloc(db, [
        ("repo1", "Python", 5000, 200, 100, 50, "2024-01-15T10:00:00"),
        ("repo1", "SQL", 1000, 50, 20, 10, "2024-01-15T10:00:00"),
        ("repo2", "Python", 3000, 100, 50, 30, "2024-01-15T10:00:00"),
    ])
    total = await db.get_total_loc(["repo1", "repo2"])
    assert total == 9000


@pytest.mark.asyncio
async def test_total_loc_uses_latest_snapshot(db: Database) -> None:
    await _insert_cloc(db, [
        ("repo1", "Python", 5000, 200, 100, 50, "2024-01-01T10:00:00"),
        ("repo1", "Python", 8000, 300, 150, 60, "2024-02-01T10:00:00"),  # newer
    ])
    total = await db.get_total_loc(["repo1"])
    assert total == 8000


# ------------------------------------------------------------------
# get_commit_timeline
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_timeline(db: Database) -> None:
    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", "2024-01-10T10:00:00+00:00", 10, 5, 1),
        ("h2", "repo", "Bob", "b@b.com", "2024-01-17T10:00:00+00:00", 20, 3, 2),
    ])
    result = await db.get_commit_timeline(["repo"])
    assert len(result) >= 1
    assert all("period" in r and "repo" in r and "commits" in r for r in result)


# ------------------------------------------------------------------
# get_commit_heatmap
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_heatmap(db: Database) -> None:
    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", "2024-01-10T14:30:00+00:00", 1, 0, 1),
        ("h2", "repo", "Bob", "b@b.com", "2024-01-10T14:45:00+00:00", 1, 0, 1),
    ])
    result = await db.get_commit_heatmap(["repo"])
    assert len(result) >= 1
    assert all("hour" in r and "day_of_week" in r and "count" in r for r in result)


# ------------------------------------------------------------------
# get_contributor_matrix
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_contributor_matrix(db: Database) -> None:
    await _insert_commits(db, [
        ("h1", "repo1", "Alice", "a@b.com", "2024-01-10T10:00:00+00:00", 10, 5, 1),
        ("h2", "repo2", "Alice", "a@b.com", "2024-01-11T10:00:00+00:00", 20, 3, 2),
        ("h3", "repo1", "Bob", "b@b.com", "2024-01-12T10:00:00+00:00", 5, 1, 1),
    ])
    result = await db.get_contributor_matrix(["repo1", "repo2"])
    assert len(result) == 2

    alice = next(c for c in result if c["author"] == "Alice")
    assert alice["total_commits"] == 2
    assert alice["repo_commits"]["repo1"] == 1
    assert alice["repo_commits"]["repo2"] == 1


# ------------------------------------------------------------------
# get_cross_repo_contributors
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cross_repo_contributors(db: Database) -> None:
    await _insert_commits(db, [
        ("h1", "repo1", "Alice", "a@b.com", "2024-01-10T10:00:00+00:00", 1, 0, 1),
        ("h2", "repo2", "Alice", "a@b.com", "2024-01-11T10:00:00+00:00", 1, 0, 1),
        ("h3", "repo1", "Bob", "b@b.com", "2024-01-12T10:00:00+00:00", 1, 0, 1),
    ])
    result = await db.get_cross_repo_contributors(["repo1", "repo2"])
    assert len(result) == 1
    assert result[0]["author"] == "Alice"
    assert result[0]["repo_count"] == 2


# ------------------------------------------------------------------
# get_new_vs_returning
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_new_vs_returning(db: Database) -> None:
    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", "2023-01-01T10:00:00+00:00", 1, 0, 1),
        ("h2", "repo", "Alice", "a@b.com", "2024-06-01T10:00:00+00:00", 1, 0, 1),
        ("h3", "repo", "Bob", "b@b.com", "2024-06-05T10:00:00+00:00", 1, 0, 1),
    ])
    result = await db.get_new_vs_returning(["repo"], "2024-01-01T00:00:00")
    assert result["new"] == 1  # Bob
    assert result["returning"] == 1  # Alice


# ------------------------------------------------------------------
# get_language_breakdown
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_language_breakdown(db: Database) -> None:
    await _insert_cloc(db, [
        ("repo", "Python", 6000, 200, 100, 50, "2024-01-15T10:00:00"),
        ("repo", "SQL", 4000, 50, 20, 10, "2024-01-15T10:00:00"),
    ])
    result = await db.get_language_breakdown(["repo"])
    assert len(result) == 2
    py = next(r for r in result if r["language"] == "Python")
    assert py["percentage"] == 60.0


# ------------------------------------------------------------------
# get_file_hotspots
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_file_hotspots(db: Database) -> None:
    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", "2024-01-10T10:00:00+00:00", 10, 5, 2),
        ("h2", "repo", "Bob", "b@b.com", "2024-01-11T10:00:00+00:00", 5, 1, 1),
    ])
    await _insert_file_changes(db, [
        ("repo", "h1", "hot.py", 5, 3),
        ("repo", "h1", "cold.py", 5, 2),
        ("repo", "h2", "hot.py", 5, 1),
    ])
    result = await db.get_file_hotspots(["repo"])
    assert len(result) == 2
    assert result[0]["file_path"] == "hot.py"
    assert result[0]["change_count"] == 2


# ------------------------------------------------------------------
# get_file_coupling + compute_file_coupling
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_file_coupling(db: Database) -> None:
    """compute_file_coupling produces correct co-occurrence pairs."""
    from repostats.collector import compute_file_coupling

    # Commit h1 touches a.py + b.py, h2 touches a.py + b.py + c.py
    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", "2024-01-10T10:00:00+00:00", 10, 0, 2),
        ("h2", "repo", "Bob", "b@b.com", "2024-01-11T10:00:00+00:00", 15, 0, 3),
    ])
    await _insert_file_changes(db, [
        ("repo", "h1", "a.py", 5, 0),
        ("repo", "h1", "b.py", 5, 0),
        ("repo", "h2", "a.py", 5, 0),
        ("repo", "h2", "b.py", 5, 0),
        ("repo", "h2", "c.py", 5, 0),
    ])

    await compute_file_coupling(db, "repo")
    result = await db.get_file_coupling(["repo"])

    assert len(result) >= 1
    # a.py + b.py should be the most coupled (2 co-changes)
    top = result[0]
    assert top["coupling_count"] == 2
    assert {top["file_a"], top["file_b"]} == {"a.py", "b.py"}


@pytest.mark.asyncio
async def test_file_coupling_excludes_large_commits(db: Database) -> None:
    """Commits touching >30 files are excluded from coupling."""
    from repostats.collector import compute_file_coupling

    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", "2024-01-10T10:00:00+00:00", 100, 0, 31),
    ])
    # Insert 31 file changes (exceeds COUPLING_MAX_FILES=30)
    file_rows: list[tuple[str, str, str, int, int]] = [
        ("repo", "h1", f"file{i}.py", 3, 0) for i in range(31)
    ]
    await _insert_file_changes(db, file_rows)

    await compute_file_coupling(db, "repo")
    result = await db.get_file_coupling(["repo"])
    assert result == []  # excluded because commit touches >30 files


# ------------------------------------------------------------------
# get_commit_size_stats
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_size_stats(db: Database) -> None:
    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", "2024-01-10T10:00:00+00:00", 5, 5, 1),
        ("h2", "repo", "Bob", "b@b.com", "2024-01-11T10:00:00+00:00", 50, 50, 2),
        ("h3", "repo", "Charlie", "c@b.com", "2024-01-12T10:00:00+00:00", 200, 100, 3),
    ])
    result = await db.get_commit_size_stats(["repo"])
    assert result["average_size"] > 0
    assert result["median_size"] > 0
    assert len(result["buckets"]) == 6  # 6 predefined buckets

    # Check bucket distribution
    bucket_map = {b["label"]: b["count"] for b in result["buckets"]}
    assert bucket_map["1-10"] == 1   # 5+5=10
    assert bucket_map["51-100"] == 1  # 50+50=100
    assert bucket_map["101-500"] == 1  # 200+100=300


# ------------------------------------------------------------------
# get_velocity
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_velocity(db: Database) -> None:
    now = datetime.now(timezone.utc)
    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", (now - timedelta(days=5)).isoformat(), 100, 30, 5),
        ("h2", "repo", "Bob", "b@b.com", (now - timedelta(days=60)).isoformat(), 50, 20, 3),
    ])
    result = await db.get_velocity(["repo"], days=30)
    assert result["insertions"] == 100
    assert result["deletions"] == 30
    assert result["net_growth"] == 70
    assert result["churn"] == 130


# ------------------------------------------------------------------
# get_weekend_ratio
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_weekend_ratio(db: Database) -> None:
    # 2024-01-13 is a Saturday, 2024-01-15 is a Monday
    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", "2024-01-13T14:00:00+00:00", 1, 0, 1),
        ("h2", "repo", "Bob", "b@b.com", "2024-01-15T10:00:00+00:00", 1, 0, 1),
        ("h3", "repo", "Charlie", "c@b.com", "2024-01-15T14:00:00+00:00", 1, 0, 1),
        ("h4", "repo", "Dave", "d@b.com", "2024-01-15T22:00:00+00:00", 1, 0, 1),
    ])
    result = await db.get_weekend_ratio(["repo"])
    assert result["weekend_ratio"] == 25.0  # 1 of 4 on weekend


# ------------------------------------------------------------------
# get_knowledge_silos
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_knowledge_silos(db: Database) -> None:
    await _insert_commits(db, [
        ("h1", "repo", "Alice", "a@b.com", "2024-01-10T10:00:00+00:00", 10, 0, 1),
        ("h2", "repo", "Bob", "b@b.com", "2024-01-11T10:00:00+00:00", 10, 0, 1),
        ("h3", "repo", "Alice", "a@b.com", "2024-01-12T10:00:00+00:00", 10, 0, 1),
    ])
    await _insert_file_changes(db, [
        ("repo", "h1", "shared.py", 5, 0),
        ("repo", "h2", "shared.py", 5, 0),
        ("repo", "h1", "silo.py", 5, 0),
        ("repo", "h3", "silo.py", 5, 0),
    ])
    result = await db.get_knowledge_silos(["repo"], max_authors=1)
    # silo.py only has Alice (both h1 and h3 are Alice), shared.py has Alice+Bob
    assert len(result) == 1
    assert result[0]["file_path"] == "silo.py"
    assert result[0]["author_count"] == 1


# ------------------------------------------------------------------
# get_code_to_comment_ratio
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_code_to_comment_ratio(db: Database) -> None:
    await _insert_cloc(db, [
        ("repo", "Python", 1000, 100, 50, 10, "2024-01-15T10:00:00"),
    ])
    result = await db.get_code_to_comment_ratio(["repo"])
    assert result == 10.0  # 1000 / 100


# ------------------------------------------------------------------
# Edge cases
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_repos_list(db: Database) -> None:
    """All queries handle empty repos list gracefully."""
    assert await db.get_repo_summaries([]) == []
    assert (await db.get_aggregated_stats([]))["total_commits"] == 0
    assert await db.get_total_loc([]) == 0
    assert await db.get_commit_timeline([]) == []
    assert await db.get_commit_heatmap([]) == []
    assert await db.get_contributor_matrix([]) == []
    assert await db.get_cross_repo_contributors([]) == []
    assert await db.get_language_breakdown([]) == []
    assert await db.get_file_hotspots([]) == []
    assert await db.get_file_coupling([]) == []
    assert (await db.get_commit_size_stats([]))["average_size"] == 0
    assert (await db.get_velocity([]))["net_growth"] == 0
    assert (await db.get_weekend_ratio([]))["weekend_ratio"] == 0
    assert (await db.get_new_vs_returning([], "2024-01-01"))["new"] == 0
    assert await db.get_knowledge_silos([]) == []
    assert await db.get_code_to_comment_ratio([]) == 0.0
