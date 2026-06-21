from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from binance_momentum_lab.binance.models import ExchangeInfo, Kline, Ticker24h
from binance_momentum_lab.config import Settings
from binance_momentum_lab.scanner import (
    MarketScanner,
    calculate_five_minute_metrics,
    population_zscore,
)

FIXTURES = Path(__file__).parent / "fixtures"


def make_klines(baseline_windows: int = 60) -> list[Kline]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows: list[Kline] = []
    minute = 0
    for window in range(1, baseline_windows + 1):
        for _ in range(5):
            open_time = start + timedelta(minutes=minute)
            rows.append(
                Kline(
                    open_time=open_time,
                    open_price=Decimal("100"),
                    high_price=Decimal("101"),
                    low_price=Decimal("99"),
                    close_price=Decimal("100"),
                    volume=Decimal("1"),
                    close_time=open_time + timedelta(minutes=1) - timedelta(milliseconds=1),
                    quote_volume=Decimal(window * 10),
                    trade_count=10,
                    taker_buy_base_volume=Decimal("0.6"),
                    taker_buy_quote_volume=Decimal(window * 6),
                )
            )
            minute += 1
    for offset in range(5):
        open_time = start + timedelta(minutes=minute)
        rows.append(
            Kline(
                open_time=open_time,
                open_price=Decimal("100") if offset == 0 else Decimal("103"),
                high_price=Decimal("104"),
                low_price=Decimal("100"),
                close_price=Decimal("103") if offset == 4 else Decimal("102"),
                volume=Decimal("100"),
                close_time=open_time + timedelta(minutes=1) - timedelta(milliseconds=1),
                quote_volume=Decimal("10000"),
                trade_count=100,
                taker_buy_base_volume=Decimal("60"),
                taker_buy_quote_volume=Decimal("6000"),
            )
        )
        minute += 1
    return rows


def test_population_zscore() -> None:
    score = population_zscore(Decimal("5"), [Decimal("1"), Decimal("2"), Decimal("3")])

    assert score.quantize(Decimal("0.001")) == Decimal("3.674")


def test_zero_variance_zscore_is_conservative() -> None:
    assert population_zscore(Decimal("100"), [Decimal("1"), Decimal("1")]) == 0


def test_calculates_five_minute_metrics() -> None:
    metrics = calculate_five_minute_metrics(make_klines(), 60)

    assert metrics.price_change_percent == Decimal("3.00")
    assert metrics.quote_volume == Decimal("50000")
    assert metrics.volume_zscore > Decimal("3")


class FakeMarketDataClient:
    def __init__(self) -> None:
        payload = json.loads((FIXTURES / "exchange_info.json").read_text(encoding="utf-8"))
        self.info = ExchangeInfo.model_validate(payload)
        self.requested_symbols: list[str] = []

    async def exchange_info(self) -> ExchangeInfo:
        return self.info

    async def tickers_24h(self) -> list[Ticker24h]:
        common = {"lastPrice": "103", "closeTime": 1_700_000_000_000}
        return [
            Ticker24h.model_validate(
                {
                    **common,
                    "symbol": "ALPHAUSDT",
                    "priceChangePercent": "6",
                    "quoteVolume": "200000000",
                }
            ),
            Ticker24h.model_validate(
                {
                    **common,
                    "symbol": "DELIVEREDUSDT",
                    "priceChangePercent": "20",
                    "quoteVolume": "900000000",
                }
            ),
        ]

    async def klines(self, symbol: str, interval: str, limit: int) -> list[Kline]:
        self.requested_symbols.append(symbol)
        assert interval == "1m"
        assert limit == 306
        return make_klines()


@pytest.mark.asyncio
async def test_scanner_filters_universe_and_returns_candidate() -> None:
    client = FakeMarketDataClient()
    scanner = MarketScanner(client, Settings(_env_file=None))

    candidates = await scanner.scan_once()

    assert [item.symbol for item in candidates] == ["ALPHAUSDT"]
    assert client.requested_symbols == ["ALPHAUSDT"]
    assert scanner.last_scan_at is not None
    assert scanner.last_error is None


@pytest.mark.asyncio
async def test_offline_demo_data_produces_candidates() -> None:
    from binance_momentum_lab.demo import DemoPublicMarketDataClient

    settings = Settings(_env_file=None, demo_data=True)
    candidates = await MarketScanner(DemoPublicMarketDataClient(), settings).scan_once()

    assert {candidate.symbol for candidate in candidates} == {"ALPHAUSDT", "PULSEUSDT"}
