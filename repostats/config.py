"""TOML configuration loading and validation."""

from __future__ import annotations

import logging
import tomllib
from pathlib import Path

from pydantic import BaseModel, ValidationError, field_validator

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config.toml")


class RepoConfig(BaseModel):
    name: str = ""
    path: str

    @field_validator("path")
    @classmethod
    def validate_path(cls, v: str) -> str:
        p = Path(v)
        if not p.is_dir():
            raise ValueError(f"repo path does not exist: {v}")
        if not (p / ".git").exists():
            raise ValueError(f"not a git repository: {v}")
        return v

    def model_post_init(self, __context: object) -> None:
        if not self.name:
            self.name = Path(self.path).name


class Settings(BaseModel):
    scan_interval_minutes: int = 60
    db_path: str = "repostats.db"
    port: int = 3300
    history_depth_days: int = 0
    mailmap_path: str = ""

    @field_validator("mailmap_path")
    @classmethod
    def validate_mailmap(cls, v: str) -> str:
        if v and not Path(v).is_file():
            raise ValueError(f"mailmap file not found: {v}")
        return v


class AppConfig(BaseModel):
    settings: Settings = Settings()
    repos: list[RepoConfig] = []


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    """Load and validate config from a TOML file."""
    config_path = path.resolve()
    if not config_path.is_file():
        raise FileNotFoundError(f"config file not found: {config_path}")

    with open(config_path, "rb") as f:
        raw = tomllib.load(f)

    settings = Settings(**raw.get("settings", {}))

    repos: list[RepoConfig] = []
    for i, repo_raw in enumerate(raw.get("repos", [])):
        try:
            repos.append(RepoConfig(**repo_raw))
        except (ValidationError, ValueError) as e:
            logger.warning("skipping repos[%d]: %s", i, e)

    return AppConfig(settings=settings, repos=repos)
