"""FastAPI app, lifespan, and background collector task."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from repostats.collector import check_cloc_available, scan_all
from repostats.config import AppConfig, load_config
from repostats.db import Database

logger = logging.getLogger(__name__)

PKG_DIR = Path(__file__).parent


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup: open DB, check cloc, spawn collector. Shutdown: stop collector, close DB."""
    config = load_config()
    app.state.config = config

    db = Database(config.settings.db_path)
    await db.open()
    app.state.db = db

    cloc_available = check_cloc_available()
    app.state.cloc_available = cloc_available
    if not cloc_available:
        logger.warning("cloc not found — language stats unavailable")

    app.state.templates = Jinja2Templates(directory=str(PKG_DIR / "templates"))
    app.state.scan_event = asyncio.Event()

    task = asyncio.create_task(
        _collector_loop(
            db, config, cloc_available,
            app.state.scan_event, config.settings.scan_interval_minutes,
        )
    )
    app.state.collector_task = task

    logger.info(
        "repostats started: %d repos, port %d",
        len(config.repos), config.settings.port,
    )

    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await db.close()
    logger.info("repostats stopped")


async def _collector_loop(
    db: Database,
    config: AppConfig,
    cloc_available: bool,
    scan_event: asyncio.Event,
    interval_minutes: int,
) -> None:
    """Background loop: scan immediately on startup, then every interval or on demand."""
    try:
        await scan_all(db, config, cloc_available)
    except Exception:
        logger.exception("initial scan failed")

    while True:
        try:
            try:
                await asyncio.wait_for(
                    scan_event.wait(), timeout=interval_minutes * 60,
                )
                scan_event.clear()
                logger.info("manual scan triggered")
            except TimeoutError:
                logger.info("scheduled scan starting")

            await scan_all(db, config, cloc_available)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("scan cycle failed")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    from repostats.routes import api, dashboard

    app = FastAPI(title="RepoStats", lifespan=lifespan)
    app.include_router(dashboard.router)
    app.include_router(api.router, prefix="/api")
    app.mount("/static", StaticFiles(directory=str(PKG_DIR / "static")), name="static")
    return app


app = create_app()


def main() -> None:
    """CLI entry point: configure logging and start uvicorn."""
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = load_config()
    uvicorn.run(app, host="0.0.0.0", port=config.settings.port)


if __name__ == "__main__":
    main()
