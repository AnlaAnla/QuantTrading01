"""Paper-only order, fill, position, market, and account models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from ..market_data.order_book import LocalOrderBook
from ..strategy.models import SignalSide


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


class FillRole(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


class ExitReason(StrEnum):
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    TIME_STOP = "TIME_STOP"
    EMERGENCY_STOP = "EMERGENCY_STOP"
    SIGNAL_REVERSAL = "SIGNAL_REVERSAL"


class PaperOrder(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: str
    signal_id: str | None = None
    symbol: str
    side: OrderSide
    position_side: SignalSide
    order_type: OrderType
    quantity: Decimal
    filled_quantity: Decimal = Decimal(0)
    trigger_price: Decimal | None = None
    reduce_only: bool = False
    created_at: datetime
    eligible_at: datetime
    status: OrderStatus = OrderStatus.PENDING
    exit_reason: ExitReason | None = None
    stop_reference_price: Decimal | None = None
    take_profit_reference_price: Decimal | None = None

    @property
    def remaining_quantity(self) -> Decimal:
        return self.quantity - self.filled_quantity


class PaperFill(BaseModel):
    model_config = ConfigDict(frozen=True)

    fill_id: str
    order_id: str
    symbol: str
    timestamp: datetime
    side: OrderSide
    role: FillRole
    quantity: Decimal
    price: Decimal
    notional: Decimal
    fee: Decimal
    slippage_bps: Decimal
    realized_pnl: Decimal


@dataclass(slots=True)
class PaperPosition:
    symbol: str
    side: SignalSide
    quantity: Decimal
    entry_price: Decimal
    opened_at: datetime
    entry_order_id: str
    stop_price: Decimal
    take_profit_price: Decimal
    time_stop_at: datetime
    realized_pnl: Decimal = Decimal(0)
    funding_pnl: Decimal = Decimal(0)

    def unrealized_pnl(self, mark_price: Decimal) -> Decimal:
        direction = Decimal(1) if self.side is SignalSide.LONG else Decimal(-1)
        return (mark_price - self.entry_price) * self.quantity * direction


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    symbol: str
    timestamp: datetime
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    order_book: LocalOrderBook | None = None
    volatility_percent: Decimal = Decimal(0)
    funding_rate: Decimal = Decimal(0)
    funding_event_id: str | None = None

    @property
    def midpoint(self) -> Decimal:
        return (self.bid_price + self.ask_price) / Decimal(2)

    @property
    def spread_bps(self) -> Decimal:
        return (self.ask_price - self.bid_price) / self.midpoint * Decimal(10000)


@dataclass(slots=True)
class PaperAccount:
    initial_balance: Decimal
    cash_balance: Decimal
    realized_pnl: Decimal = Decimal(0)
    cumulative_fees: Decimal = Decimal(0)
    cumulative_funding: Decimal = Decimal(0)


class AccountSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    cash_balance: Decimal
    equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    cumulative_fees: Decimal
    cumulative_funding: Decimal
    open_positions: int


@dataclass(slots=True)
class Liquidity:
    bids: list[list[Decimal]] = field(default_factory=list)
    asks: list[list[Decimal]] = field(default_factory=list)
