"""API routes for HTMX partial updates."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, Response

from repostats.db import Database, _reader_override

router = APIRouter()


def parse_time_range(time_range: str) -> tuple[str | None, str | None]:
    """Convert time range string to (after, before) ISO date cutoffs.

    Supports: 7d, 14d, 30d, 90d, 180d, 1y, 2y, 3y, 5y, all, yNNNN (specific year).
    """
    days_map = {
        "7d": 7, "14d": 14, "30d": 30, "90d": 90, "180d": 180,
        "1y": 365, "2y": 730, "3y": 1095, "5y": 1825,
    }
    days = days_map.get(time_range)
    if days is not None:
        return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(), None

    # Year selection: "y2024" → 2024-01-01 to 2025-01-01
    if time_range.startswith("y") and time_range[1:].isdigit():
        year = int(time_range[1:])
        return f"{year}-01-01T00:00:00+00:00", f"{year + 1}-01-01T00:00:00+00:00"

    return None, None  # "all"


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
        ) = await asyncio.gather(
            _r(0,  db.get_aggregated_stats(repos, after, before)),
            _r(1,  db.get_repo_summaries(repos, after, before)),
            _r(2,  db.get_total_loc(repos)),
            _r(3,  db.get_commit_timeline(repos, after, before)),
            _r(0,  db.get_commit_heatmap(repos, after, before)),
            _r(1,  db.get_language_breakdown(repos)),
            _r(2,  db.get_contributor_matrix(repos, after, before)),
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
        )
    finally:
        for r in readers:
            await r.close()

    return {
        "stats": stats,
        "total_loc": total_loc,
        "summaries": summaries,
        "timeline": timeline,
        "heatmap": heatmap,
        "language": language,
        "contributors": contributors,
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
