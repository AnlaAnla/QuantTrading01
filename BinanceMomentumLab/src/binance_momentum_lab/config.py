"""Environment-backed application configuration and startup safety checks."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .exceptions import DemoTradingUnavailableError, LiveTradingDisabledError


class AppMode(StrEnum):
    """Supported and reserved runtime modes."""

    MONITOR = "MONITOR"
    PAPER = "PAPER"
    DEMO = "DEMO"
    LIVE = "LIVE"


class Settings(BaseSettings):
    """Typed settings loaded from environment variables and an optional .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_mode: AppMode = AppMode.PAPER
    binance_mainnet_rest_url: HttpUrl = HttpUrl("https://fapi.binance.com")
    binance_mainnet_ws_url: str = "wss://fstream.binance.com"
    binance_demo_rest_url: HttpUrl = HttpUrl("https://demo-fapi.binance.com")
    binance_demo_ws_url: str = "wss://fstream.binancefuture.com"
    binance_api_key: str = ""
    binance_api_secret: str = ""
    paper_initial_balance: Decimal = Field(default=Decimal("10000"), gt=0)
    live_trading_enabled: bool = False
    demo_trading_enabled: bool = False

    scan_interval_seconds: float = Field(default=10, gt=0)
    min_24h_quote_volume: Decimal = Field(default=Decimal("100000000"), ge=0)
    min_24h_price_change_percent: Decimal = Field(default=Decimal("5"))
    min_5m_price_change_percent: Decimal = Field(default=Decimal("2"))
    min_5m_volume_zscore: Decimal = Field(default=Decimal("3"))
    volume_baseline_windows: int = Field(default=60, ge=2, le=288)
    max_candidates: int = Field(default=20, ge=1, le=100)
    kline_concurrency: int = Field(default=8, ge=1, le=50)

    http_timeout_seconds: float = Field(default=10, gt=0)
    http_max_retries: int = Field(default=3, ge=0, le=10)
    database_path: Path = Path("data/binance_momentum.duckdb")
    log_level: str = "INFO"
    demo_data: bool = False

    @field_validator("binance_mainnet_ws_url", "binance_demo_ws_url")
    @classmethod
    def validate_websocket_url(cls, value: str) -> str:
        """Reject non-TLS websocket endpoints."""
        if not value.startswith("wss://"):
            raise ValueError("Binance WebSocket URLs must use wss://")
        return value.rstrip("/")

    def startup_safety_check(self) -> None:
        """Fail closed for all execution modes unavailable in phase one."""
        if self.app_mode is AppMode.LIVE:
            raise LiveTradingDisabledError(
                "LIVE mode is hard-disabled in this release, regardless of LIVE_TRADING_ENABLED"
            )
        if self.app_mode is AppMode.DEMO:
            raise DemoTradingUnavailableError(
                "DEMO mode is reserved but no order adapter is implemented in phase one"
            )

    @property
    def public_config(self) -> dict[str, object]:
        """Return a secret-free configuration view safe for API responses."""
        return {
            "app_mode": self.app_mode.value,
            "mainnet_rest_url": str(self.binance_mainnet_rest_url),
            "mainnet_ws_url": self.binance_mainnet_ws_url,
            "paper_initial_balance": str(self.paper_initial_balance),
            "live_trading_enabled": False,
            "demo_trading_enabled": self.demo_trading_enabled,
            "scan_interval_seconds": self.scan_interval_seconds,
            "max_candidates": self.max_candidates,
            "demo_data": self.demo_data,
            "api_key_masked": mask_api_key(self.binance_api_key),
        }


def mask_api_key(value: str) -> str:
    """Mask an API key so logs and public diagnostics never expose it."""
    if not value:
        return ""
    if len(value) <= 8:
        return "****"
    return f"{value[:4]}...{value[-4:]}"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings instance."""
    return Settings()
