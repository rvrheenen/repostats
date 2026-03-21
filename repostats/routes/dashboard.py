"""Dashboard page route."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from repostats.db import Database
from repostats.routes.api import build_dashboard_context

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> Response:
    """Full dashboard page."""
    db: Database = request.app.state.db
    config: Any = request.app.state.config
    templates: Any = request.app.state.templates
    cloc_available: bool = request.app.state.cloc_available

    all_repos: list[str] = [r.name for r in config.repos]
    default_repos: list[str] = [r.name for r in config.repos if r.active]
    min_year, max_year = await db.get_year_range()
    available_years = list(range(max_year, min_year - 1, -1)) if min_year and max_year else []

    context = await build_dashboard_context(db, default_repos, "all", cloc_available)
    context["request"] = request
    context["all_repos"] = all_repos
    context["default_repos"] = default_repos
    context["available_years"] = available_years

    return templates.TemplateResponse("dashboard.html", context)
