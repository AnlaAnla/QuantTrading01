from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from binance_momentum_lab.binance.models import Kline
from binance_momentum_lab.config import Settings
from binance_momentum_lab.market_data.events import (
    AggTradeEvent,
    BookTickerEvent,
    KlineEvent,
    MarkPriceEvent,
)
from binance_momentum_lab.market_data.order_book import LocalOrderBook
from binance_momentum_lab.strategy.features import FeatureEngine, OpenInterestObservation


def kline(symbol: str, minute: int, close: str, volume: str, taker_ratio: str) -> KlineEvent:
    start = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=minute)
    quote_volume = Decimal(volume)
    return KlineEvent.model_validate(
        {
            "e": "kline",
            "E": int((start + timedelta(minutes=1)).timestamp() * 1000),
            "s": symbol,
            "k": {
                "t": int(start.timestamp() * 1000),
                "T": int((start + timedelta(minutes=1)).timestamp() * 1000) - 1,
                "s": symbol,
                "i": "1m",
                "f": minute * 10,
                "L": minute * 10 + 9,
                "o": close,
                "c": close,
                "h": str(Decimal(close) * Decimal("1.001")),
                "l": str(Decimal(close) * Decimal("0.999")),
                "v": "10",
                "n": 10,
                "x": True,
                "q": volume,
                "V": "6",
                "Q": str(quote_volume * Decimal(taker_ratio)),
            },
        }
    )


def trade(
    symbol: str, second: int, price: str, quantity: str, buyer_is_maker: bool
) -> AggTradeEvent:
    timestamp = datetime(2026, 1, 1, 0, 16, tzinfo=UTC) + timedelta(seconds=second)
    return AggTradeEvent.model_validate(
        {
            "e": "aggTrade",
            "E": int(timestamp.timestamp() * 1000),
            "s": symbol,
            "a": second,
            "p": price,
            "q": quantity,
            "f": second,
            "l": second,
            "T": int(timestamp.timestamp() * 1000),
            "m": buyer_is_maker,
        }
    )


def build_engine() -> FeatureEngine:
    settings = Settings(_env_file=None, feature_volume_baseline_windows=2)
    engine = FeatureEngine(settings)
    for minute in range(16):
        engine.ingest(kline("BTCUSDT", minute, str(100 + minute), str(100 + minute), "0.5"))
        engine.ingest(kline("ALPHAUSDT", minute, str(50 + minute * 2), str(50 + minute * 5), "0.6"))
    return engine


def test_multi_period_returns_volume_zscore_and_btc_residual() -> None:
    engine = build_engine()

    snapshot = engine.snapshot("ALPHAUSDT")

    assert snapshot.return_1m_percent == (Decimal("80") / Decimal("78") - 1) * 100
    assert snapshot.return_3m_percent == (Decimal("80") / Decimal("74") - 1) * 100
    assert snapshot.return_5m_percent == (Decimal("80") / Decimal("70") - 1) * 100
    assert snapshot.return_15m_percent == Decimal("60")
    assert snapshot.quote_volume_5m == sum(Decimal(50 + minute * 5) for minute in range(11, 16))
    assert snapshot.volume_zscore is not None
    assert snapshot.taker_buy_ratio == Decimal("0.6")
    assert snapshot.btc_return_5m_percent is not None
    assert snapshot.btc_residual_return_5m_percent == (
        snapshot.return_5m_percent - snapshot.btc_return_5m_percent
    )


def test_cvd_slope_and_anchored_vwap() -> None:
    engine = build_engine()
    anchor = datetime(2026, 1, 1, 0, 16, tzinfo=UTC)
    engine.anchor("ALPHAUSDT", anchor)
    engine.ingest(trade("ALPHAUSDT", 0, "80", "2", False))
    engine.ingest(trade("ALPHAUSDT", 10, "82", "1", True))

    snapshot = engine.snapshot("ALPHAUSDT")

    assert snapshot.cvd == Decimal("78")
    assert snapshot.cvd_slope == Decimal("7.8")
    assert snapshot.anchored_vwap == Decimal("242") / Decimal("3")
    assert snapshot.distance_from_anchored_vwap_percent is not None


def test_oi_funding_basis_spread_and_order_book_imbalance() -> None:
    engine = build_engine()
    base = datetime(2026, 1, 1, 0, 11, tzinfo=UTC)
    engine.ingest_open_interest(OpenInterestObservation("ALPHAUSDT", base, Decimal("1000")))
    engine.ingest_open_interest(
        OpenInterestObservation("ALPHAUSDT", base + timedelta(minutes=5), Decimal("1020"))
    )
    engine.ingest(
        MarkPriceEvent.model_validate(
            {
                "e": "markPriceUpdate",
                "E": int((base + timedelta(minutes=5)).timestamp() * 1000),
                "s": "ALPHAUSDT",
                "p": "80.8",
                "i": "80",
                "P": "80",
                "r": "0.0001",
                "T": 0,
            }
        )
    )
    engine.ingest(
        BookTickerEvent.model_validate(
            {
                "e": "bookTicker",
                "E": int((base + timedelta(minutes=5)).timestamp() * 1000),
                "T": int((base + timedelta(minutes=5)).timestamp() * 1000),
                "s": "ALPHAUSDT",
                "u": 1,
                "b": "79.9",
                "B": "2",
                "a": "80.1",
                "A": "1",
            }
        )
    )
    book = LocalOrderBook("ALPHAUSDT")
    book.synchronized = True
    book.bids = {Decimal("79.9"): Decimal("6"), Decimal("79.8"): Decimal("4")}
    book.asks = {Decimal("80.1"): Decimal("2"), Decimal("80.2"): Decimal("3")}

    snapshot = engine.snapshot("ALPHAUSDT", order_book=book)

    assert snapshot.open_interest_change_5m_percent == Decimal("2.00")
    assert snapshot.funding_rate == Decimal("0.0001")
    assert snapshot.basis_percent == Decimal("1.00")
    assert snapshot.spread_bps == Decimal("25.00")
    assert snapshot.order_book_imbalance_5 == Decimal("1") / Decimal("3")
    assert snapshot.order_book_imbalance_20 == Decimal("1") / Decimal("3")


def test_rest_kline_seed_warms_feature_history() -> None:
    engine = FeatureEngine(Settings(_env_file=None, feature_volume_baseline_windows=2))
    rows: list[Kline] = []
    start = datetime(2026, 1, 1, tzinfo=UTC)
    for minute in range(16):
        timestamp = start + timedelta(minutes=minute)
        rows.append(
            Kline(
                open_time=timestamp,
                open_price=Decimal("100"),
                high_price=Decimal("102"),
                low_price=Decimal("99"),
                close_price=Decimal(100 + minute),
                volume=Decimal("10"),
                close_time=timestamp + timedelta(minutes=1),
                quote_volume=Decimal(100 + minute),
                trade_count=10,
                taker_buy_base_volume=Decimal("6"),
                taker_buy_quote_volume=Decimal(60 + minute),
            )
        )

    engine.seed_klines("ALPHAUSDT", rows)
    snapshot = engine.snapshot("ALPHAUSDT")

    assert snapshot.return_15m_percent == Decimal("15")
    assert snapshot.volume_zscore is not None

    engine.seed_klines("ALPHAUSDT", list(reversed(rows)))
    repeated = engine.snapshot("ALPHAUSDT")
    assert repeated == snapshot
