"""Deterministic offline market data for screenshots and UI demonstrations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from .binance.models import ExchangeInfo, ExchangeSymbol, Kline, OpenInterest, Ticker24h
from .market_data.order_book import DepthSnapshot


class DemoPublicMarketDataClient:
    """In-memory implementation of the public market-data protocol."""

    async def exchange_info(self) -> ExchangeInfo:
        symbols = [
            ExchangeSymbol.model_validate(
                {
                    "symbol": symbol,
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "baseAsset": symbol.removesuffix("USDT"),
                    "marginAsset": "USDT",
                }
            )
            for symbol in ("ALPHAUSDT", "PULSEUSDT")
        ]
        return ExchangeInfo(symbols=symbols)

    async def tickers_24h(self) -> list[Ticker24h]:
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        return [
            Ticker24h.model_validate(
                {
                    "symbol": "ALPHAUSDT",
                    "priceChangePercent": "12.4",
                    "lastPrice": "1.284",
                    "quoteVolume": "428000000",
                    "closeTime": now_ms,
                }
            ),
            Ticker24h.model_validate(
                {
                    "symbol": "PULSEUSDT",
                    "priceChangePercent": "8.7",
                    "lastPrice": "0.0712",
                    "quoteVolume": "215000000",
                    "closeTime": now_ms,
                }
            ),
        ]

    async def klines(self, symbol: str, interval: str, limit: int) -> list[Kline]:
        if interval != "1m":
            raise ValueError("Demo data only supports 1m klines")
        start = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=limit)
        price = Decimal("1") if symbol == "ALPHAUSDT" else Decimal("0.05")
        rows: list[Kline] = []
        for index in range(limit):
            recent = index >= limit - 5
            open_price = price
            close_price = price * (Decimal("1.007") if recent else Decimal("1.0001"))
            quote_volume = Decimal("9000000") if recent else Decimal(100000 + index * 1000)
            open_time = start + timedelta(minutes=index)
            rows.append(
                Kline(
                    open_time=open_time,
                    open_price=open_price,
                    high_price=max(open_price, close_price) * Decimal("1.001"),
                    low_price=min(open_price, close_price) * Decimal("0.999"),
                    close_price=close_price,
                    volume=quote_volume / open_price,
                    close_time=open_time + timedelta(minutes=1) - timedelta(milliseconds=1),
                    quote_volume=quote_volume,
                    trade_count=100 + index,
                    taker_buy_base_volume=Decimal(0),
                    taker_buy_quote_volume=quote_volume * Decimal("0.62"),
                )
            )
            if recent:
                price = close_price
        return rows

    async def aclose(self) -> None:
        """Match the live client's lifecycle contract."""

    async def depth_snapshot(self, symbol: str, limit: int = 1000) -> DepthSnapshot:
        """Return a tiny deterministic book for offline runtime demonstrations."""
        del symbol, limit
        return DepthSnapshot(
            last_update_id=100,
            bids=((Decimal("1.0"), Decimal("10")),),
            asks=((Decimal("1.1"), Decimal("10")),),
        )

    async def open_interest(self, symbol: str) -> OpenInterest:
        return OpenInterest(
            symbol=symbol,
            value=Decimal("100000"),
            time_ms=int(datetime.now(UTC).timestamp() * 1000),
        )
