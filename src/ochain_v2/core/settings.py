"""
Application settings loaded from YAML with env-var overrides.

Priority (highest → lowest):
  1. Environment variables  (OCHAIN__APP__API_PORT=5052)
  2. config/settings.yaml   (created from settings.example.yaml)
  3. Hard-coded defaults

Usage:
    from ochain_v2.core.settings import get_settings
    cfg = get_settings()          # reads config/settings.yaml by default
    cfg = get_settings("custom.yaml")
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Nested config sections
# ---------------------------------------------------------------------------

class AppSettings(BaseModel):
    api_port: int = 5051
    collector_port: int = 5052
    debug: bool = False
    log_level: str = "INFO"
    log_file: str = "logs/ochain.log"


class DbSettings(BaseModel):
    duckdb_path: str = "data/ochain.duckdb"
    meta_sqlite_path: str = "data/ochain_meta.sqlite"
    archive_path: str = "data/archive"
    reader_pool_size: int = 4
    archive_after_days: int = 90


class CollectorSettings(BaseModel):
    interval_sec: int = 60
    pre_open_snapshot: bool = True
    pre_open_time: str = "09:00"
    market_open: str = "09:15"
    market_close: str = "15:30"
    holidays_file: str = "config/nse_holidays.yaml"
    instruments_file: str = "config/instruments.yaml"
    live_channel: str = "memory"
    redis_url: Optional[str] = None


class BrokerSettings(BaseModel):
    name: str = "dhan"
    credentials_path: str = os.path.join(
        os.path.expandvars("%LOCALAPPDATA%"), "OChain", "credentials.json"
    )
    rate_limit_per_sec: float = 5.0
    connect_timeout_sec: int = 10
    request_timeout_sec: int = 15
    circuit_breaker_threshold: int = 5
    circuit_breaker_pause_sec: int = 300


class InstrumentsSettings(BaseModel):
    active: list[str] = Field(default_factory=lambda: ["NIFTY", "BANKNIFTY"])
    expiries_per_symbol: int = 2


class CacheSettings(BaseModel):
    snapshot_ttl_sec: int = 3600
    window_ttl_sec: int = 60
    max_window_entries: int = 200


# ---------------------------------------------------------------------------
# Root settings (pydantic-settings v2)
# ---------------------------------------------------------------------------

class _YamlSource(PydanticBaseSettingsSource):
    """Custom source that loads a YAML file and returns it as a flat-ish dict."""

    def __init__(self, settings_cls: type[BaseSettings], yaml_path: Path) -> None:
        super().__init__(settings_cls)
        self._data: dict = {}
        if yaml_path.exists():
            with open(yaml_path) as f:
                self._data = yaml.safe_load(f) or {}

    def get_field_value(self, field: any, field_name: str) -> tuple:  # type: ignore[override]
        value = self._data.get(field_name)
        return value, field_name, False

    def __call__(self) -> dict:
        return {k: v for k, v in self._data.items() if v is not None}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OCHAIN__",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app: AppSettings = Field(default_factory=AppSettings)
    db: DbSettings = Field(default_factory=DbSettings)
    collector: CollectorSettings = Field(default_factory=CollectorSettings)
    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    instruments: InstrumentsSettings = Field(default_factory=InstrumentsSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)

    @classmethod
    def customise_sources(  # type: ignore[override]
        cls,
        settings_customise_sources: any,
        **kwargs: any,
    ) -> tuple:
        # Expose a hook but the primary factory is `from_yaml`.
        return settings_customise_sources(**kwargs)

    @classmethod
    def from_yaml(cls, path: str | Path = "config/settings.yaml") -> "Settings":
        p = Path(path)
        if not p.exists():
            return cls()
        with open(p) as f:
            data = yaml.safe_load(f) or {}
        # pydantic v2 coerces nested dicts into the sub-models automatically
        return cls.model_validate(data)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_instance: Optional[Settings] = None
_config_path: str = "config/settings.yaml"


def get_settings(config_path: str | None = None) -> Settings:
    global _instance, _config_path
    if config_path is not None:
        _config_path = config_path
        _instance = None  # force reload if path changed
    if _instance is None:
        _instance = Settings.from_yaml(_config_path)
    return _instance


def reset_settings() -> None:
    """Reset the singleton (useful in tests)."""
    global _instance
    _instance = None
