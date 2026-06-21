"""Fail-closed paper entry risk manager and deterministic position sizing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from enum import StrEnum

from ..config import AppMode, Settings
from ..strategy.models import SignalSide, StrategySignal


class RiskRejectCode(StrEnum):
    MODE_NOT_PAPER = "MODE_NOT_PAPER"
    EMERGENCY_STOP = "EMERGENCY_STOP"
    NEW_ENTRIES_PAUSED = "NEW_ENTRIES_PAUSED"
    DAILY_LOSS_LIMIT = "DAILY_LOSS_LIMIT"
    CONSECUTIVE_LOSS_COOLDOWN = "CONSECUTIVE_LOSS_COOLDOWN"
    STALE_MARKET_DATA = "STALE_MARKET_DATA"
    WEBSOCKET_UNHEALTHY = "WEBSOCKET_UNHEALTHY"
    ORDER_BOOK_UNSYNCHRONIZED = "ORDER_BOOK_UNSYNCHRONIZED"
    SPREAD_TOO_WIDE = "SPREAD_TOO_WIDE"
    MAX_POSITIONS = "MAX_POSITIONS"
    POSITION_EXISTS = "POSITION_EXISTS"
    INVALID_STOP = "INVALID_STOP"
    ZERO_POSITION_SIZE = "ZERO_POSITION_SIZE"


@dataclass(frozen=True, slots=True)
class RiskEnvironment:
    timestamp: datetime
    market_data_timestamp: datetime
    websocket_healthy: bool
    order_book_synchronized: bool
    spread_bps: Decimal


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    quantity: Decimal = Decimal(0)
    reject_codes: tuple[RiskRejectCode, ...] = ()


class RiskManager:
    """Approve paper entries only; exits are always allowed."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.emergency_stopped = False
        self.entries_paused = False
        self._daily_date: date | None = None
        self._daily_start_equity: Decimal | None = None
        self._daily_realized_pnl = Decimal(0)
        self._consecutive_losses = 0
        self._cooldown_until: datetime | None = None

    def evaluate_entry(
        self,
        signal: StrategySignal,
        environment: RiskEnvironment,
        *,
        equity: Decimal,
        open_symbols: set[str],
    ) -> RiskDecision:
        self._roll_day(environment.timestamp, equity)
        rejects: list[RiskRejectCode] = []
        if self._settings.app_mode is not AppMode.PAPER:
            rejects.append(RiskRejectCode.MODE_NOT_PAPER)
        if self.emergency_stopped:
            rejects.append(RiskRejectCode.EMERGENCY_STOP)
        if self.entries_paused:
            rejects.append(RiskRejectCode.NEW_ENTRIES_PAUSED)
        if self._daily_loss_exceeded():
            rejects.append(RiskRejectCode.DAILY_LOSS_LIMIT)
        if self._cooldown_until is not None and environment.timestamp < self._cooldown_until:
            rejects.append(RiskRejectCode.CONSECUTIVE_LOSS_COOLDOWN)
        age = (environment.timestamp - environment.market_data_timestamp).total_seconds()
        if age < 0 or age > self._settings.risk_data_max_age_seconds:
            rejects.append(RiskRejectCode.STALE_MARKET_DATA)
        if not environment.websocket_healthy:
            rejects.append(RiskRejectCode.WEBSOCKET_UNHEALTHY)
        if not environment.order_book_synchronized:
            rejects.append(RiskRejectCode.ORDER_BOOK_UNSYNCHRONIZED)
        if environment.spread_bps > self._settings.strategy_max_spread_bps:
            rejects.append(RiskRejectCode.SPREAD_TOO_WIDE)
        if len(open_symbols) >= self._settings.risk_max_positions:
            rejects.append(RiskRejectCode.MAX_POSITIONS)
        if signal.symbol in open_symbols:
            rejects.append(RiskRejectCode.POSITION_EXISTS)

        risk_per_unit = self._risk_per_unit(signal)
        if risk_per_unit <= 0:
            rejects.append(RiskRejectCode.INVALID_STOP)
            quantity = Decimal(0)
        else:
            quantity = self.position_size(
                equity,
                signal.entry_reference_price,
                signal.stop_reference_price,
            )
            if quantity <= 0:
                rejects.append(RiskRejectCode.ZERO_POSITION_SIZE)
        return RiskDecision(not rejects, quantity if not rejects else Decimal(0), tuple(rejects))

    def position_size(self, equity: Decimal, entry_price: Decimal, stop_price: Decimal) -> Decimal:
        distance = abs(entry_price - stop_price)
        if distance <= 0 or entry_price <= 0:
            return Decimal(0)
        risk_budget = equity * self._settings.risk_per_trade_fraction
        risk_quantity = risk_budget / distance
        notional_cap = equity * self._settings.risk_max_position_notional_multiple
        capped = min(risk_quantity, notional_cap / entry_price)
        step = self._settings.paper_quantity_step
        return (capped / step).to_integral_value(rounding=ROUND_DOWN) * step

    def record_closed_trade(self, timestamp: datetime, net_pnl: Decimal, equity: Decimal) -> None:
        self._roll_day(timestamp, equity - net_pnl)
        self._daily_realized_pnl += net_pnl
        if net_pnl < 0:
            self._consecutive_losses += 1
            if self._consecutive_losses >= self._settings.risk_consecutive_loss_limit:
                self._cooldown_until = timestamp + timedelta(
                    seconds=self._settings.risk_cooldown_seconds
                )
        else:
            self._consecutive_losses = 0
            self._cooldown_until = None

    def activate_emergency_stop(self) -> None:
        self.emergency_stopped = True

    def clear_emergency_stop(self) -> None:
        self.emergency_stopped = False

    def pause_entries(self) -> None:
        self.entries_paused = True

    def resume_entries(self) -> None:
        self.entries_paused = False

    def reset(self) -> None:
        self.emergency_stopped = False
        self.entries_paused = False
        self._daily_date = None
        self._daily_start_equity = None
        self._daily_realized_pnl = Decimal(0)
        self._consecutive_losses = 0
        self._cooldown_until = None

    def _roll_day(self, timestamp: datetime, equity: Decimal) -> None:
        utc_day = timestamp.astimezone(UTC).date()
        if self._daily_date != utc_day:
            self._daily_date = utc_day
            self._daily_start_equity = equity
            self._daily_realized_pnl = Decimal(0)

    def _daily_loss_exceeded(self) -> bool:
        return (
            self._daily_start_equity is not None
            and self._daily_realized_pnl
            <= -self._daily_start_equity * self._settings.risk_max_daily_loss_fraction
        )

    @staticmethod
    def _risk_per_unit(signal: StrategySignal) -> Decimal:
        if signal.side is SignalSide.LONG:
            return signal.entry_reference_price - signal.stop_reference_price
        return signal.stop_reference_price - signal.entry_reference_price
