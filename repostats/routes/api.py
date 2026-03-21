"""API routes for HTMX partial updates."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, Response

from repostats.db import Database

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


async def build_dashboard_context(
    db: Database,
    repos: list[str],
    time_range: str,
    cloc_available: bool,
) -> dict[str, Any]:
    """Build the full dashboard template context."""
    after, before = parse_time_range(time_range)

    stats = await db.get_aggregated_stats(repos, after, before)
    summaries = await db.get_repo_summaries(repos, after, before)
    total_loc = await db.get_total_loc(repos)
    timeline = await db.get_commit_timeline(repos, after, before)
    heatmap = await db.get_commit_heatmap(repos, after, before)
    language = await db.get_language_breakdown(repos)
    contributors = await db.get_contributor_matrix(repos, after, before)
    hotspots = await db.get_file_hotspots(repos, after, before)
    coupling = await db.get_file_coupling(repos)
    commit_size = await db.get_commit_size_stats(repos, after, before)
    velocity = await db.get_velocity(repos)
    weekend = await db.get_weekend_ratio(repos, after, before)
    scan_status = await db.get_scan_status()
    silos = await db.get_knowledge_silos(repos)
    cross_repo = await db.get_cross_repo_contributors(repos, after, before)
    new_returning = await db.get_new_vs_returning(repos, after, before)
    code_to_comment_ratio = await db.get_code_to_comment_ratio(repos)
    stale_files = await db.get_stale_files(repos)
    avg_file_age = await db.get_average_file_age(repos)
    file_age_distribution = await db.get_file_age_distribution(repos)
    directory_growth = await db.get_directory_growth(repos, after, before)
    directory_activity = await db.get_directory_activity(repos, after, before)
    rework = await db.get_rework_ratio(repos, after, before)
    repo_loc = await db.get_per_repo_loc(repos)

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
        "directory_growth": directory_growth,
        "directory_activity": directory_activity,
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
    context["request"] = request
    context["all_repos"] = all_repos

    return templates.TemplateResponse("partials/dashboard_content.html", context)


@router.post("/refresh", response_class=HTMLResponse)
async def refresh(request: Request) -> Response:
    """Trigger an immediate background scan."""
    scan_event: asyncio.Event = request.app.state.scan_event
    scan_event.set()

    db: Database = request.app.state.db
    templates: Any = request.app.state.templates
    scan_status = await db.get_scan_status()

    return templates.TemplateResponse(
        "partials/scan_status.html",
        {"request": request, "scan_status": scan_status},
    )


@router.get("/scan-status", response_class=HTMLResponse)
async def scan_status(request: Request) -> Response:
    """Return scan status partial (polled by HTMX)."""
    db: Database = request.app.state.db
    templates: Any = request.app.state.templates
    status = await db.get_scan_status()

    return templates.TemplateResponse(
        "partials/scan_status.html",
        {"request": request, "scan_status": status},
    )
