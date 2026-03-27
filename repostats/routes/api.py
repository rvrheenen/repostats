"""API routes for HTMX partial updates."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, Response

from repostats.db import Database, _reader_override

router = APIRouter()


_DAYS_MAP = {
    "7d": 7, "14d": 14, "30d": 30, "90d": 90, "180d": 180,
    "1y": 365, "2y": 730, "3y": 1095, "5y": 1825,
}


def parse_time_range(time_range: str) -> tuple[str | None, str | None]:
    """Convert time range string to (after, before) ISO date cutoffs.

    Supports: 7d, 14d, 30d, 90d, 180d, 1y, 2y, 3y, 5y, all, yNNNN (specific year).
    """
    days = _DAYS_MAP.get(time_range)
    if days is not None:
        return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(), None

    # Year selection: "y2024" → 2024-01-01 to 2025-01-01
    if time_range.startswith("y") and time_range[1:].isdigit():
        year = int(time_range[1:])
        return f"{year}-01-01T00:00:00+00:00", f"{year + 1}-01-01T00:00:00+00:00"

    return None, None  # "all"


def _days_in_range(time_range: str) -> int | None:
    """Return the number of days covered by *time_range*, or None for 'all'."""
    days = _DAYS_MAP.get(time_range)
    if days is not None:
        return days
    if time_range.startswith("y") and time_range[1:].isdigit():
        year = int(time_range[1:])
        start = datetime(year, 1, 1)
        end = datetime(year + 1, 1, 1)
        return (end - start).days
    return None


async def _on_reader(reader: Any, coro: Any) -> Any:
    """Run *coro* with *reader* pinned as the Database reader for this task."""
    _reader_override.set(reader)
    return await coro


async def build_dashboard_context(
    db: Database,
    repos: list[str],
    time_range: str,
    cloc_available: bool,
) -> dict[str, Any]:
    """Build the full dashboard template context.

    Opens fresh reader connections for each request to avoid stale WAL
    snapshots, and runs queries concurrently for ~4× speedup.
    """
    after, before = parse_time_range(time_range)

    # The rework LAG window function must sort every row in the scan range.
    # For "all time" (no date bounds) this is prohibitively slow, so cap at 1 year.
    rework_after = after
    if rework_after is None and before is None:
        rework_after = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()

    # Open fresh readers — avoids stale WAL read-marks left by the background
    # scan, and enables true parallelism (each aiosqlite connection has its own
    # thread; SQLite WAL mode permits concurrent readers).
    readers = [await db._open_reader() for _ in range(4)]
    try:
        def _r(idx: int, coro: Any) -> Any:
            return _on_reader(readers[idx % len(readers)], coro)

        (
            stats, summaries, total_loc, timeline, heatmap, language,
            contributors, hotspots, coupling, commit_size, velocity,
            weekend, scan_status, silos, cross_repo, new_returning,
            code_to_comment_ratio, stale_files, avg_file_age,
            file_age_distribution, rework, repo_loc,
            surviving_all, repo_surviving,
        ) = await asyncio.gather(
            _r(0,  db.get_aggregated_stats(repos, after, before)),
            _r(1,  db.get_repo_summaries(repos, after, before)),
            _r(2,  db.get_total_loc(repos)),
            _r(3,  db.get_commit_timeline(repos, after, before)),
            _r(0,  db.get_commit_heatmap(repos, after, before)),
            _r(1,  db.get_language_breakdown(repos)),
            _r(2,  db.get_contributor_matrix(repos, after, before, limit=100)),
            _r(3,  db.get_file_hotspots(repos, after, before)),
            _r(0,  db.get_file_coupling(repos)),
            _r(1,  db.get_commit_size_stats(repos, after, before)),
            _r(2,  db.get_velocity(repos)),
            _r(3,  db.get_weekend_ratio(repos, after, before)),
            _r(0,  db.get_scan_status()),
            _r(1,  db.get_knowledge_silos(repos)),
            _r(2,  db.get_cross_repo_contributors(repos, after, before)),
            _r(3,  db.get_new_vs_returning(repos, after, before)),
            _r(0,  db.get_code_to_comment_ratio(repos)),
            _r(1,  db.get_stale_files(repos)),
            _r(2,  db.get_average_file_age(repos)),
            _r(3,  db.get_file_age_distribution(repos)),
            _r(0,  db.get_rework_ratio(repos, rework_after, before)),
            _r(1,  db.get_per_repo_loc(repos)),
            _r(2,  db.get_all_surviving_lines(repos)),
            _r(3,  db.get_per_repo_surviving(repos)),
        )
    finally:
        for r in readers:
            await r.close()

    grand_total_commits = sum(c["total_commits"] for c in contributors)
    grand_total_lines = sum(c["total_lines"] for c in contributors)

    # Merge surviving lines (from blame) into contributor matrix
    _surviving_by_author: dict[str, dict[str, int]] = {}
    for row in surviving_all:
        _surviving_by_author.setdefault(row["author"], {})[row["repo"]] = row["lines"]
    for c in contributors:
        c["repo_surviving"] = _surviving_by_author.get(c["author"], {})
        c["total_surviving"] = sum(c["repo_surviving"].values())
    grand_total_surviving = sum(c["total_surviving"] for c in contributors)
    total_surviving_lines = sum(repo_surviving.values())

    days = _days_in_range(time_range)
    if days is None and contributors:
        earliest = min(c["first_commit"] for c in contributors)
        days = max((datetime.now(timezone.utc) - datetime.fromisoformat(earliest)).days, 1)

    return {
        "stats": stats,
        "total_loc": total_loc,
        "summaries": summaries,
        "timeline": timeline,
        "heatmap": heatmap,
        "language": language,
        "contributors": contributors,
        "grand_total_commits": grand_total_commits,
        "grand_total_lines": grand_total_lines,
        "grand_total_surviving": grand_total_surviving,
        "total_surviving_lines": total_surviving_lines,
        "repo_surviving": repo_surviving,
        "days_in_range": days,
        "hotspots": hotspots,
        "coupling": coupling,
        "commit_size": commit_size,
        "velocity": velocity,
        "weekend": weekend,
        "scan_status": scan_status,
        "silos": silos,
        "cross_repo": cross_repo,
        "new_returning": new_returning,
        "code_to_comment_ratio": code_to_comment_ratio,
        "stale_files": stale_files,
        "avg_file_age": avg_file_age,
        "file_age_distribution": file_age_distribution,
        "rework": rework,
        "repo_loc": repo_loc,
        "selected_repos": repos,
        "time_range": time_range,
        "cloc_available": cloc_available,
    }


@router.post("/aggregate", response_class=HTMLResponse)
async def aggregate(
    request: Request,
    repos: Annotated[list[str], Form()] = [],  # noqa: B006
    time_range: Annotated[str, Form()] = "all",
) -> Response:
    """Return updated dashboard partials for selected repos + time range."""
    db: Database = request.app.state.db
    templates: Any = request.app.state.templates
    config: Any = request.app.state.config
    cloc_available: bool = request.app.state.cloc_available
    all_repos: list[str] = [r.name for r in config.repos]

    context = await build_dashboard_context(db, repos, time_range, cloc_available)
    context["all_repos"] = all_repos

    return templates.TemplateResponse(request, name="partials/dashboard_content.html", context=context)


@router.post("/language-detail", response_class=HTMLResponse)
async def language_detail(
    request: Request,
    language: Annotated[str, Form()],
    repos: Annotated[list[str], Form()] = [],  # noqa: B006
) -> Response:
    """Return detail panel for a single language."""
    db: Database = request.app.state.db
    templates: Any = request.app.state.templates

    if not repos or not language:
        return Response("")

    readers = [await db._open_reader() for _ in range(2)]
    try:
        def _r(idx: int, coro: Any) -> Any:
            return _on_reader(readers[idx], coro)

        by_repo, largest_files = await asyncio.gather(
            _r(0, db.get_language_detail_by_repo(repos, language)),
            _r(1, db.get_largest_files_for_language(repos, language)),
        )
    finally:
        for r in readers:
            await r.close()

    total_code = sum(r["code"] for r in by_repo) or 0
    total_comment = sum(r["comment"] for r in by_repo) or 0
    total_files = sum(r["files"] for r in by_repo) or 0
    comment_ratio = round(total_comment / (total_code + total_comment) * 100, 1) if (total_code + total_comment) else 0
    avg_file_size = round(total_code / total_files) if total_files else 0

    return templates.TemplateResponse(
        request,
        name="partials/language_detail.html",
        context={
            "language": language,
            "by_repo": by_repo,
            "largest_files": largest_files,
            "total_code": total_code,
            "comment_ratio": comment_ratio,
            "avg_file_size": avg_file_size,
        },
    )


@router.post("/contributor-detail", response_class=HTMLResponse)
async def contributor_detail(
    request: Request,
    author: Annotated[str, Form()],
    repos: Annotated[list[str], Form()] = [],  # noqa: B006
    time_range: Annotated[str, Form()] = "all",
) -> Response:
    """Return detail panel for a single contributor."""
    db: Database = request.app.state.db
    templates: Any = request.app.state.templates

    if not repos or not author:
        return Response("")

    after, before = parse_time_range(time_range)

    readers = [await db._open_reader() for _ in range(3)]
    try:
        def _r(idx: int, coro: Any) -> Any:
            return _on_reader(readers[idx % len(readers)], coro)

        by_repo, by_lang, top_files, timeline, heatmap_raw, surviving = await asyncio.gather(
            _r(0, db.get_contributor_repo_breakdown(repos, author, after, before)),
            _r(1, db.get_contributor_language_breakdown(repos, author, after, before)),
            _r(2, db.get_contributor_top_files(repos, author, after, before)),
            _r(0, db.get_contributor_timeline(repos, author, after, before)),
            _r(1, db.get_contributor_heatmap(repos, author, after, before)),
            _r(2, db.get_contributor_surviving_lines(repos, author)),
        )
    finally:
        for r in readers:
            await r.close()

    # Summary stats from by_repo
    total_commits = sum(r["commits"] for r in by_repo)
    total_lines = sum(r["insertions"] + r["deletions"] for r in by_repo)
    repo_count = len(by_repo)
    avg_commit_size = round(total_lines / total_commits) if total_commits else 0

    # Tenure
    if by_repo:
        earliest = min(r["first_commit"] for r in by_repo)
        latest = max(r["last_commit"] for r in by_repo)
        try:
            d1 = datetime.fromisoformat(earliest)
            d2 = datetime.fromisoformat(latest)
            delta_days = max((d2 - d1).days, 0)
            if delta_days >= 365:
                years = delta_days // 365
                months = (delta_days % 365) // 30
                tenure = f"{years}y {months}m" if months else f"{years}y"
            elif delta_days >= 30:
                tenure = f"{delta_days // 30}m {delta_days % 30}d"
            else:
                tenure = f"{delta_days} days"
        except (ValueError, TypeError):
            tenure = "\u2014"
    else:
        tenure = "\u2014"

    # Per-repo percentages + surviving lines
    for r in by_repo:
        r["percentage"] = round(r["commits"] / total_commits * 100, 1) if total_commits else 0
        r["surviving_lines"] = surviving.get(r["repo"], 0)
    total_surviving = sum(surviving.values())

    # Language percentages
    total_lang_lines = sum(r["lines_changed"] for r in by_lang) or 1
    for r in by_lang:
        r["percentage"] = round(r["lines_changed"] / total_lang_lines * 100, 1)

    # Heatmap grid
    heatmap_grid: dict[tuple[int, int], int] = {}
    heatmap_max = 0
    for row in heatmap_raw:
        heatmap_grid[(row["day_of_week"], row["hour"])] = row["count"]
        heatmap_max = max(heatmap_max, row["count"])

    return templates.TemplateResponse(
        request,
        name="partials/contributor_detail.html",
        context={
            "author": author,
            "total_commits": total_commits,
            "total_lines": total_lines,
            "total_surviving": total_surviving,
            "repo_count": repo_count,
            "avg_commit_size": avg_commit_size,
            "tenure": tenure,
            "by_repo": by_repo,
            "by_lang": by_lang,
            "top_files": top_files,
            "timeline": timeline,
            "heatmap_grid": heatmap_grid,
            "heatmap_max": heatmap_max,
        },
    )


@router.post("/repo-detail", response_class=HTMLResponse)
async def repo_detail(
    request: Request,
    repo: Annotated[str, Form()],
    time_range: Annotated[str, Form()] = "all",
) -> Response:
    """Return detail panel for a single repository."""
    db: Database = request.app.state.db
    templates: Any = request.app.state.templates

    if not repo:
        return Response("")

    repos = [repo]
    after, before = parse_time_range(time_range)

    rework_after = after
    if rework_after is None and before is None:
        rework_after = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()

    readers = [await db._open_reader() for _ in range(3)]
    try:
        def _r(idx: int, coro: Any) -> Any:
            return _on_reader(readers[idx % len(readers)], coro)

        (
            stats, language, contributors, total_loc,
            velocity, code_to_comment_ratio, hotspots, rework,
            repo_surviving_all,
        ) = await asyncio.gather(
            _r(0, db.get_aggregated_stats(repos, after, before)),
            _r(1, db.get_language_breakdown(repos)),
            _r(2, db.get_contributor_matrix(repos, after, before, limit=15)),
            _r(0, db.get_total_loc(repos)),
            _r(1, db.get_velocity(repos)),
            _r(2, db.get_code_to_comment_ratio(repos)),
            _r(0, db.get_file_hotspots(repos, after, before, limit=10)),
            _r(1, db.get_rework_ratio(repos, rework_after, before)),
            _r(2, db.get_all_surviving_lines(repos)),
        )
    finally:
        for r in readers:
            await r.close()

    # Merge surviving lines into contributors
    _repo_surv: dict[str, dict[str, int]] = {}
    for row in repo_surviving_all:
        _repo_surv.setdefault(row["author"], {})[row["repo"]] = row["lines"]
    for c in contributors:
        c["total_surviving"] = sum(_repo_surv.get(c["author"], {}).values())

    return templates.TemplateResponse(
        request,
        name="partials/repo_detail.html",
        context={
            "repo": repo,
            "stats": stats,
            "language": language,
            "contributors": contributors,
            "total_loc": total_loc,
            "velocity": velocity,
            "code_to_comment_ratio": code_to_comment_ratio,
            "active_contributors": len(contributors),
            "hotspots": hotspots,
            "rework_ratio": round(rework.get("rework_ratio", 0), 1),
        },
    )


@router.post("/refresh", response_class=HTMLResponse)
async def refresh(request: Request) -> Response:
    """Trigger an immediate background scan."""
    scan_event: asyncio.Event = request.app.state.scan_event
    scan_event.set()

    db: Database = request.app.state.db
    templates: Any = request.app.state.templates
    scan_status = await db.get_scan_status()

    return templates.TemplateResponse(
        request, name="partials/scan_status.html",
        context={"scan_status": scan_status},
    )


@router.get("/scan-status", response_class=HTMLResponse)
async def scan_status(request: Request) -> Response:
    """Return scan status partial (polled by HTMX)."""
    db: Database = request.app.state.db
    templates: Any = request.app.state.templates
    status = await db.get_scan_status()

    return templates.TemplateResponse(
        request, name="partials/scan_status.html",
        context={"scan_status": status},
    )
