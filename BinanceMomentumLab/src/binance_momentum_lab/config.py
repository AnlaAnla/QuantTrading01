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

    ws_rotation_seconds: float = Field(default=85800, gt=0, le=86400)
    ws_reconnect_base_seconds: float = Field(default=1, gt=0)
    ws_reconnect_max_seconds: float = Field(default=30, gt=0)
    ws_queue_maxsize: int = Field(default=10000, ge=100, le=1_000_000)
    ws_stale_after_seconds: float = Field(default=3, gt=0)
    ws_ping_interval_seconds: float = Field(default=180, gt=0)
    ws_ping_timeout_seconds: float = Field(default=600, gt=0)
    order_book_depth_limit: int = Field(default=1000, ge=5, le=1000)
    parquet_root: Path = Path("data/raw_events")
    parquet_batch_size: int = Field(default=1000, ge=1, le=100_000)
    parquet_flush_seconds: float = Field(default=5, gt=0)

    feature_volume_baseline_windows: int = Field(default=60, ge=2, le=288)
    feature_cvd_window_seconds: int = Field(default=300, ge=60, le=3600)
    feature_oi_window_seconds: int = Field(default=300, ge=60, le=3600)
    feature_benchmark_symbol: str = "BTCUSDT"
    feature_btc_beta: Decimal = Decimal("1")
    oi_poll_interval_seconds: float = Field(default=15, gt=0)

    strategy_watch_return_5m_percent: Decimal = Decimal("1.5")
    strategy_watch_volume_zscore: Decimal = Decimal("2.5")
    strategy_ignition_return_5m_percent: Decimal = Decimal("3")
    strategy_long_volume_zscore: Decimal = Decimal("4")
    strategy_long_taker_buy_ratio: Decimal = Field(default=Decimal("0.58"), ge=0, le=1)
    strategy_long_oi_change_5m_percent: Decimal = Decimal("1")
    strategy_max_spread_bps: Decimal = Field(default=Decimal("8"), ge=0)
    strategy_pullback_min_ratio: Decimal = Field(default=Decimal("0.20"), ge=0, le=1)
    strategy_pullback_max_ratio: Decimal = Field(default=Decimal("0.50"), ge=0, le=1)
    strategy_pullback_volume_ratio_max: Decimal = Field(default=Decimal("0.80"), ge=0, le=1)
    strategy_buy_absorption_taker_ratio: Decimal = Field(default=Decimal("0.60"), ge=0, le=1)
    strategy_buy_absorption_max_return_1m_percent: Decimal = Decimal("0.10")
    strategy_short_taker_sell_ratio: Decimal = Field(default=Decimal("0.55"), ge=0, le=1)
    strategy_cooldown_seconds: int = Field(default=1800, ge=1)

    paper_taker_fee_rate: Decimal = Field(default=Decimal("0.0004"), ge=0, le=1)
    paper_network_latency_ms: int = Field(default=100, ge=0)
    paper_base_slippage_bps: Decimal = Field(default=Decimal("1"), ge=0)
    paper_volatility_slippage_factor: Decimal = Field(default=Decimal("0.20"), ge=0)
    paper_spread_slippage_factor: Decimal = Field(default=Decimal("0.25"), ge=0)
    paper_notional_slippage_bps_per_100k: Decimal = Field(default=Decimal("1"), ge=0)
    paper_take_profit_r_multiple: Decimal = Field(default=Decimal("2"), gt=0)
    paper_max_hold_seconds: int = Field(default=900, ge=1)
    paper_quantity_step: Decimal = Field(default=Decimal("0.001"), gt=0)
    risk_per_trade_fraction: Decimal = Field(default=Decimal("0.0025"), gt=0, le=1)
    risk_max_positions: int = Field(default=1, ge=1)
    risk_max_daily_loss_fraction: Decimal = Field(default=Decimal("0.01"), gt=0, le=1)
    risk_consecutive_loss_limit: int = Field(default=3, ge=1)
    risk_cooldown_seconds: int = Field(default=1800, ge=1)
    risk_data_max_age_seconds: float = Field(default=3, gt=0)
    risk_max_position_notional_multiple: Decimal = Field(default=Decimal("1"), gt=0)

    @field_validator("binance_mainnet_ws_url", "binance_demo_ws_url")
    @classmethod
    def validate_websocket_url(cls, value: str) -> str:
        """Reject non-TLS websocket endpoints."""
        if not value.startswith("wss://"):
            raise ValueError("Binance WebSocket URLs must use wss://")
        return value.rstrip("/")

    @field_validator("feature_benchmark_symbol")
    @classmethod
    def normalize_benchmark_symbol(cls, value: str) -> str:
        return value.upper()

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
            "strategy_thresholds": {
                "watch_return_5m_percent": str(self.strategy_watch_return_5m_percent),
                "watch_volume_zscore": str(self.strategy_watch_volume_zscore),
                "ignition_return_5m_percent": str(self.strategy_ignition_return_5m_percent),
                "long_volume_zscore": str(self.strategy_long_volume_zscore),
                "long_taker_buy_ratio": str(self.strategy_long_taker_buy_ratio),
                "long_oi_change_5m_percent": str(self.strategy_long_oi_change_5m_percent),
                "max_spread_bps": str(self.strategy_max_spread_bps),
                "pullback_min_ratio": str(self.strategy_pullback_min_ratio),
                "pullback_max_ratio": str(self.strategy_pullback_max_ratio),
                "short_taker_sell_ratio": str(self.strategy_short_taker_sell_ratio),
                "cooldown_seconds": self.strategy_cooldown_seconds,
            },
            "paper_risk": {
                "risk_per_trade_fraction": str(self.risk_per_trade_fraction),
                "max_positions": self.risk_max_positions,
                "max_daily_loss_fraction": str(self.risk_max_daily_loss_fraction),
                "consecutive_loss_limit": self.risk_consecutive_loss_limit,
                "data_max_age_seconds": self.risk_data_max_age_seconds,
                "network_latency_ms": self.paper_network_latency_ms,
                "taker_fee_rate": str(self.paper_taker_fee_rate),
            },
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
