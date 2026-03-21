"""Pydantic models for RepoStats data transfer."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class CommitRecord(BaseModel):
    """A single parsed commit from git log."""
    hash: str
    repo: str
    author: str
    email: str
    date: str  # ISO-8601
    insertions: int = 0
    deletions: int = 0
    files_changed: int = 0


class FileChange(BaseModel):
    """A single file change within a commit."""
    repo: str
    commit_hash: str
    file_path: str
    insertions: int = 0
    deletions: int = 0


class ClocEntry(BaseModel):
    """A single language entry from cloc output."""
    repo: str
    language: str
    code: int = 0
    comment: int = 0
    blank: int = 0
    files: int = 0
    scanned_at: str  # ISO-8601


class FileCoupling(BaseModel):
    """A pair of files that frequently change together."""
    repo: str
    file_a: str
    file_b: str
    coupling_count: int = 0


class ScanStatus(BaseModel):
    """Current scan state for a repo."""
    repo: str
    last_commit_hash: str | None = None
    last_scanned_at: str | None = None
    commit_count: int = 0
    total_commits: int | None = None
    status: Literal["pending", "scanning", "done", "error"] = "pending"


class RepoSummary(BaseModel):
    """Summary stats for a single repo."""
    name: str
    total_commits: int = 0
    total_insertions: int = 0
    total_deletions: int = 0
    active_contributors: int = 0
    first_commit_date: str | None = None
    last_commit_date: str | None = None


class AggregatedStats(BaseModel):
    """Aggregated stats across selected repos."""
    total_loc: int = 0
    total_commits: int = 0
    active_contributors_30d: int = 0
    active_contributors_90d: int = 0
    active_contributors_180d: int = 0
    bus_factor: int = 0
    total_insertions: int = 0
    total_deletions: int = 0
    repo_summaries: list[RepoSummary] = []


class CommitTimelinePoint(BaseModel):
    """A single data point for the commit timeline chart."""
    period: str  # e.g. "2024-W03" or "2024-03"
    repo: str
    commits: int = 0
    insertions: int = 0
    deletions: int = 0


class HeatmapCell(BaseModel):
    """A single cell in the hour x day-of-week heatmap."""
    day_of_week: int  # 0=Monday, 6=Sunday
    hour: int  # 0-23
    count: int = 0


class ContributorRow(BaseModel):
    """A row in the contributor x repo matrix."""
    author: str
    repo_commits: dict[str, int] = {}  # repo_name -> commit_count
    total_commits: int = 0
    first_commit: str | None = None
    last_commit: str | None = None


class FileHotspot(BaseModel):
    """A frequently modified file."""
    repo: str
    file_path: str
    change_count: int = 0


class LanguageBreakdown(BaseModel):
    """Language stats from cloc."""
    language: str
    code: int = 0
    comment: int = 0
    blank: int = 0
    files: int = 0
    percentage: float = 0.0


class FileStats(BaseModel):
    """Per-file metadata snapshot."""
    repo: str
    file_path: str
    loc: int = 0
    language: str | None = None
    last_commit_hash: str
    last_commit_date: str
    last_author: str
    first_commit_date: str
    author_count: int = 1


class FileAgeBucket(BaseModel):
    """A bucket in the file age distribution histogram."""
    label: str  # e.g. "<30d", "30-90d"
    count: int = 0


class CommitSizeBucket(BaseModel):
    """A bucket in the commit size histogram."""
    label: str  # e.g. "1-10", "11-50"
    count: int = 0


class CommitSizeStats(BaseModel):
    """Commit size distribution data."""
    average_size: float = 0.0
    median_size: float = 0.0
    buckets: list[CommitSizeBucket] = []
