"""Typed models for the Binance USDⓈ-M public REST API."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def milliseconds_to_utc(value: int) -> datetime:
    """Convert a Binance millisecond timestamp to an aware UTC datetime."""
    return datetime.fromtimestamp(value / 1000, tz=UTC)


class ExchangeSymbol(BaseModel):
    """Relevant fields from an exchangeInfo symbol entry."""

    model_config = ConfigDict(populate_by_name=True)

    symbol: str
    status: str
    contract_type: str = Field(alias="contractType")
    quote_asset: str = Field(alias="quoteAsset")
    base_asset: str = Field(alias="baseAsset")
    margin_asset: str = Field(alias="marginAsset")

    @property
    def is_trading_usdt_perpetual(self) -> bool:
        """Return whether this symbol belongs in the scanner universe."""
        return (
            self.status == "TRADING"
            and self.contract_type == "PERPETUAL"
            and self.quote_asset == "USDT"
        )


class ExchangeInfo(BaseModel):
    """Response from GET /fapi/v1/exchangeInfo."""

    symbols: list[ExchangeSymbol]


class Ticker24h(BaseModel):
    """Relevant fields from a 24-hour ticker record."""

    model_config = ConfigDict(populate_by_name=True)

    symbol: str
    price_change_percent: Decimal = Field(alias="priceChangePercent")
    last_price: Decimal = Field(alias="lastPrice")
    quote_volume: Decimal = Field(alias="quoteVolume")
    close_time_ms: int = Field(alias="closeTime")

    @property
    def close_time(self) -> datetime:
        """Return the ticker close time as an aware UTC datetime."""
        return milliseconds_to_utc(self.close_time_ms)


class Kline(BaseModel):
    """One Binance candlestick parsed from its positional array representation."""

    open_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    close_time: datetime
    quote_volume: Decimal
    trade_count: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal

    @classmethod
    def from_payload(cls, row: list[Any]) -> Kline:
        """Parse the documented 12-item Kline array."""
        if len(row) < 11:
            raise ValueError(f"Expected at least 11 kline fields, received {len(row)}")
        return cls(
            open_time=milliseconds_to_utc(int(row[0])),
            open_price=Decimal(str(row[1])),
            high_price=Decimal(str(row[2])),
            low_price=Decimal(str(row[3])),
            close_price=Decimal(str(row[4])),
            volume=Decimal(str(row[5])),
            close_time=milliseconds_to_utc(int(row[6])),
            quote_volume=Decimal(str(row[7])),
            trade_count=int(row[8]),
            taker_buy_base_volume=Decimal(str(row[9])),
            taker_buy_quote_volume=Decimal(str(row[10])),
        )
