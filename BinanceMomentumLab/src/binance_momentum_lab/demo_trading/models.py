"""Typed demo orders, account state, and exchange precision rules."""

from __future__ import annotations

import hashlib
from decimal import ROUND_DOWN, Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..exceptions import DemoOrderValidationError


class DemoSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PositionSide(StrEnum):
    BOTH = "BOTH"
    LONG = "LONG"
    SHORT = "SHORT"


class DemoOrderType(StrEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


class DemoOrderIntent(StrEnum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"


class DemoOrderRequest(BaseModel):
    """Intent-level request; reduceOnly is derived and cannot be disabled for exits."""

    model_config = ConfigDict(frozen=True)

    idempotency_key: str = Field(min_length=1, max_length=200)
    symbol: str = Field(min_length=1)
    side: DemoSide
    order_type: DemoOrderType
    quantity: Decimal = Field(gt=0)
    intent: DemoOrderIntent
    price: Decimal | None = Field(default=None, gt=0)
    stop_price: Decimal | None = Field(default=None, gt=0)
    reference_price: Decimal | None = Field(default=None, gt=0)
    time_in_force: str | None = None

    @model_validator(mode="after")
    def validate_type_fields(self) -> DemoOrderRequest:
        if self.order_type is DemoOrderType.LIMIT and self.price is None:
            raise ValueError("LIMIT orders require price")
        if self.order_type in {DemoOrderType.STOP_MARKET, DemoOrderType.TAKE_PROFIT_MARKET}:
            if self.stop_price is None:
                raise ValueError(f"{self.order_type} orders require stop_price")
            if self.intent is not DemoOrderIntent.CLOSE:
                raise ValueError("Protective trigger orders must be closing orders")
        return self

    @property
    def reduce_only(self) -> bool:
        return self.intent is DemoOrderIntent.CLOSE

    @property
    def client_order_id(self) -> str:
        digest = hashlib.sha256(self.idempotency_key.encode()).hexdigest()[:24]
        return f"bml_{digest}"


class DemoOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    order_id: int = Field(alias="orderId")
    client_order_id: str = Field(alias="clientOrderId")
    side: DemoSide
    position_side: PositionSide = Field(alias="positionSide")
    order_type: str = Field(alias="type")
    status: str
    original_quantity: Decimal = Field(alias="origQty")
    executed_quantity: Decimal = Field(alias="executedQty")
    price: Decimal
    average_price: Decimal = Field(alias="avgPrice")
    stop_price: Decimal = Field(alias="stopPrice")
    reduce_only: bool = Field(alias="reduceOnly")
    update_time_ms: int | None = Field(default=None, alias="updateTime")


class DemoPosition(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    position_side: PositionSide = Field(alias="positionSide")
    quantity: Decimal = Field(alias="positionAmt")
    entry_price: Decimal = Field(alias="entryPrice")
    break_even_price: Decimal = Field(default=Decimal(0), alias="breakEvenPrice")
    unrealized_profit: Decimal = Field(default=Decimal(0), alias="unRealizedProfit")
    update_time_ms: int = Field(default=0, alias="updateTime")

    @property
    def key(self) -> tuple[str, PositionSide]:
        return self.symbol, self.position_side


class DemoBalance(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset: str
    balance: Decimal
    available_balance: Decimal = Field(alias="availableBalance")
    cross_wallet_balance: Decimal = Field(alias="crossWalletBalance")
    cross_unrealized_pnl: Decimal = Field(alias="crossUnPnl")
    update_time_ms: int = Field(alias="updateTime")


class SymbolRules(BaseModel):
    """Order filters sourced from exchangeInfo; precision fields are intentionally ignored."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    tick_size: Decimal
    lot_step_size: Decimal
    lot_min_quantity: Decimal
    market_step_size: Decimal
    market_min_quantity: Decimal
    min_notional: Decimal

    @classmethod
    def from_exchange_symbol(cls, payload: dict[str, Any]) -> SymbolRules:
        filters = {
            str(item.get("filterType")): item
            for item in payload.get("filters", [])
            if isinstance(item, dict)
        }
        try:
            price_filter = filters["PRICE_FILTER"]
            lot_filter = filters["LOT_SIZE"]
            market_filter = filters.get("MARKET_LOT_SIZE", lot_filter)
            notional_filter = filters["MIN_NOTIONAL"]
            return cls(
                symbol=str(payload["symbol"]),
                tick_size=Decimal(str(price_filter["tickSize"])),
                lot_step_size=Decimal(str(lot_filter["stepSize"])),
                lot_min_quantity=Decimal(str(lot_filter["minQty"])),
                market_step_size=Decimal(str(market_filter["stepSize"])),
                market_min_quantity=Decimal(str(market_filter["minQty"])),
                min_notional=Decimal(str(notional_filter["notional"])),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise DemoOrderValidationError("Incomplete exchangeInfo order filters") from exc

    def normalize_quantity(self, quantity: Decimal, *, market: bool) -> Decimal:
        step = self.market_step_size if market else self.lot_step_size
        minimum = self.market_min_quantity if market else self.lot_min_quantity
        normalized = _floor_to_step(quantity, step)
        if normalized < minimum:
            raise DemoOrderValidationError(
                f"{self.symbol} quantity {normalized} is below minQty {minimum}"
            )
        return normalized

    def normalize_price(self, price: Decimal) -> Decimal:
        return _floor_to_step(price, self.tick_size)

    def validate_notional(self, quantity: Decimal, price: Decimal) -> None:
        notional = quantity * price
        if notional < self.min_notional:
            raise DemoOrderValidationError(
                f"{self.symbol} notional {notional} is below minNotional {self.min_notional}"
            )


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise DemoOrderValidationError("Exchange step size must be positive")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step
