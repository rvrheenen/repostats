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
    context = await build_dashboard_context(db, all_repos, "all", cloc_available)
    context["request"] = request
    context["all_repos"] = all_repos

    return templates.TemplateResponse("dashboard.html", context)
