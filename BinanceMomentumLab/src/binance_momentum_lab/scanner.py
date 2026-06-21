"""Configurable full-market prefilter and five-minute anomaly scanner."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel

from .binance.client import PublicMarketDataClient
from .binance.models import Kline, Ticker24h
from .config import Settings


class Candidate(BaseModel):
    """A symbol satisfying every configured phase-one screening threshold."""

    symbol: str
    last_price: Decimal
    price_change_24h_percent: Decimal
    quote_volume_24h: Decimal
    price_change_5m_percent: Decimal
    quote_volume_5m: Decimal
    volume_zscore: Decimal
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class FiveMinuteMetrics:
    """Metrics derived from the latest one-minute candlesticks."""

    price_change_percent: Decimal
    quote_volume: Decimal
    volume_zscore: Decimal


def population_zscore(value: Decimal, baseline: list[Decimal]) -> Decimal:
    """Compute a population Z-Score using Decimal arithmetic.

    A constant baseline has no measurable dispersion, so this function returns zero rather than
    manufacturing an infinite anomaly score.
    """
    if len(baseline) < 2:
        return Decimal(0)
    count = Decimal(len(baseline))
    mean = sum(baseline, start=Decimal(0)) / count
    variance = sum(((sample - mean) ** 2 for sample in baseline), start=Decimal(0)) / count
    if variance == 0:
        return Decimal(0)
    return (value - mean) / variance.sqrt()


def calculate_five_minute_metrics(klines: list[Kline], baseline_windows: int) -> FiveMinuteMetrics:
    """Aggregate recent and baseline non-overlapping five-minute windows."""
    required = (baseline_windows + 1) * 5
    if len(klines) < required:
        raise ValueError(f"Need {required} one-minute klines, received {len(klines)}")

    ordered = sorted(klines, key=lambda item: item.open_time)[-required:]
    baseline_rows = ordered[:-5]
    recent = ordered[-5:]
    baseline_volumes = [
        sum(
            (row.quote_volume for row in baseline_rows[index : index + 5]),
            start=Decimal(0),
        )
        for index in range(0, len(baseline_rows), 5)
    ]
    recent_volume = sum((row.quote_volume for row in recent), start=Decimal(0))
    open_price = recent[0].open_price
    if open_price <= 0:
        raise ValueError("Kline open price must be positive")
    price_change = (recent[-1].close_price / open_price - Decimal(1)) * Decimal(100)
    return FiveMinuteMetrics(
        price_change_percent=price_change,
        quote_volume=recent_volume,
        volume_zscore=population_zscore(recent_volume, baseline_volumes),
    )


class MarketScanner:
    """Scan all actively trading USDT perpetual contracts using an injected REST client."""

    def __init__(self, client: PublicMarketDataClient, settings: Settings) -> None:
        self._client = client
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.kline_concurrency)
        self.last_scan_at: datetime | None = None
        self.last_error: str | None = None

    async def scan_once(self) -> list[Candidate]:
        """Run one complete market scan and return ranked candidates."""
        try:
            exchange_info, tickers = await asyncio.gather(
                self._client.exchange_info(), self._client.tickers_24h()
            )
            universe = {
                item.symbol for item in exchange_info.symbols if item.is_trading_usdt_perpetual
            }
            prefiltered = [
                ticker
                for ticker in tickers
                if ticker.symbol in universe
                and ticker.quote_volume >= self._settings.min_24h_quote_volume
                and ticker.price_change_percent >= self._settings.min_24h_price_change_percent
            ]
            candidates = await asyncio.gather(
                *(self._evaluate_ticker(ticker) for ticker in prefiltered)
            )
            ranked = sorted(
                (candidate for candidate in candidates if candidate is not None),
                key=lambda item: (item.volume_zscore, item.price_change_5m_percent),
                reverse=True,
            )[: self._settings.max_candidates]
            self.last_scan_at = datetime.now(UTC)
            self.last_error = None
            return ranked
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            raise

    async def _evaluate_ticker(self, ticker: Ticker24h) -> Candidate | None:
        required = (self._settings.volume_baseline_windows + 1) * 5
        async with self._semaphore:
            klines = await self._client.klines(ticker.symbol, "1m", required + 1)
        closed_klines = [item for item in klines if item.close_time <= datetime.now(UTC)]
        metrics = calculate_five_minute_metrics(
            closed_klines, self._settings.volume_baseline_windows
        )
        if (
            metrics.price_change_percent < self._settings.min_5m_price_change_percent
            or metrics.volume_zscore < self._settings.min_5m_volume_zscore
        ):
            return None
        return Candidate(
            symbol=ticker.symbol,
            last_price=ticker.last_price,
            price_change_24h_percent=ticker.price_change_percent,
            quote_volume_24h=ticker.quote_volume,
            price_change_5m_percent=metrics.price_change_percent,
            quote_volume_5m=metrics.quote_volume,
            volume_zscore=metrics.volume_zscore,
            observed_at=ticker.close_time,
        )
