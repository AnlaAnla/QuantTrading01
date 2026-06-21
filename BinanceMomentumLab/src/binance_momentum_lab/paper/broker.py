"""Local-only paper broker matched against contemporaneous visible liquidity."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from ..config import Settings
from ..exceptions import NonMonotonicMarketDataError
from ..strategy.models import SignalSide, StrategySignal
from .models import (
    AccountSnapshot,
    ExitReason,
    FillRole,
    Liquidity,
    MarketSnapshot,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperAccount,
    PaperFill,
    PaperOrder,
    PaperPosition,
)
from .risk import RiskDecision, RiskManager

ZERO = Decimal(0)
TEN_THOUSAND = Decimal(10000)


class PaperTradeStore(Protocol):
    def save_paper_order(self, order: PaperOrder) -> None: ...

    def save_paper_fill(self, fill: PaperFill) -> None: ...

    def save_paper_position(self, position: PaperPosition | None, symbol: str) -> None: ...

    def save_account_snapshot(self, snapshot: AccountSnapshot) -> None: ...

    def reset_paper(self) -> None: ...


class PaperBroker:
    """Deterministic derivatives ledger with no external account connectivity."""

    def __init__(
        self,
        settings: Settings,
        risk_manager: RiskManager,
        store: PaperTradeStore | None = None,
    ) -> None:
        self.settings = settings
        self.risk_manager = risk_manager
        self.store = store
        self.account = PaperAccount(
            initial_balance=settings.paper_initial_balance,
            cash_balance=settings.paper_initial_balance,
        )
        self.orders: dict[str, PaperOrder] = {}
        self.fills: list[PaperFill] = []
        self.positions: dict[str, PaperPosition] = {}
        self.equity_curve: list[AccountSnapshot] = []
        self.closed_trade_pnls: list[Decimal] = []
        self._last_market_time: dict[str, datetime] = {}
        self._last_markets: dict[str, MarketSnapshot] = {}
        self._funding_events: set[str] = set()

    def submit_entry(
        self,
        signal: StrategySignal,
        decision: RiskDecision,
        submitted_at: datetime,
    ) -> PaperOrder:
        """Create a delayed paper MARKET order after an external risk decision."""
        side = OrderSide.BUY if signal.side is SignalSide.LONG else OrderSide.SELL
        identity = f"paper-entry:{signal.signal_id}"
        rejected = not decision.approved or signal.symbol in self.positions
        order = PaperOrder(
            order_id=str(uuid5(NAMESPACE_URL, identity)),
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            side=side,
            position_side=signal.side,
            order_type=OrderType.MARKET,
            quantity=decision.quantity,
            reduce_only=False,
            created_at=submitted_at,
            eligible_at=submitted_at
            + timedelta(milliseconds=self.settings.paper_network_latency_ms),
            status=OrderStatus.REJECTED if rejected else OrderStatus.PENDING,
            stop_reference_price=signal.stop_reference_price,
            take_profit_reference_price=self._take_profit_reference(signal),
        )
        self._save_order(order)
        return order

    def submit_reduce_only(
        self,
        symbol: str,
        order_type: OrderType,
        quantity: Decimal,
        submitted_at: datetime,
        *,
        trigger_price: Decimal | None = None,
        exit_reason: ExitReason,
        identity_suffix: str,
    ) -> PaperOrder:
        position = self.positions.get(symbol)
        position_side = position.side if position is not None else SignalSide.LONG
        side = OrderSide.SELL if position_side is SignalSide.LONG else OrderSide.BUY
        rejected = position is None or quantity <= 0
        identity = f"paper-exit:{symbol}:{identity_suffix}"
        order = PaperOrder(
            order_id=str(uuid5(NAMESPACE_URL, identity)),
            symbol=symbol,
            side=side,
            position_side=position_side,
            order_type=order_type,
            quantity=min(quantity, position.quantity) if position is not None else ZERO,
            trigger_price=trigger_price,
            reduce_only=True,
            created_at=submitted_at,
            eligible_at=submitted_at
            + timedelta(milliseconds=self.settings.paper_network_latency_ms),
            status=OrderStatus.REJECTED if rejected else OrderStatus.PENDING,
            exit_reason=exit_reason,
        )
        self._save_order(order)
        return order

    def on_market(self, market: MarketSnapshot) -> list[PaperFill]:
        """Advance paper time using only this snapshot's visible prices and quantities."""
        previous = self._last_market_time.get(market.symbol)
        if previous is not None and market.timestamp < previous:
            raise NonMonotonicMarketDataError(
                f"{market.symbol} moved backward from {previous} to {market.timestamp}"
            )
        self._last_market_time[market.symbol] = market.timestamp
        self._last_markets[market.symbol] = market
        self._apply_funding(market)
        self._schedule_time_stop(market)
        liquidity = self._liquidity(market)
        new_fills: list[PaperFill] = []
        scheduled_orders = sorted(
            self.orders.values(),
            key=lambda item: (not item.reduce_only, item.created_at, item.order_id),
        )
        for scheduled in scheduled_orders:
            order = self.orders[scheduled.order_id]
            if order.symbol != market.symbol or order.status not in {
                OrderStatus.PENDING,
                OrderStatus.PARTIALLY_FILLED,
            }:
                continue
            if market.timestamp < order.eligible_at or not self._triggered(order, market):
                continue
            fill = self._execute(order, market, liquidity)
            if fill is not None:
                new_fills.append(fill)
        self._record_equity(market.timestamp)
        return new_fills

    def emergency_close_all(self, timestamp: datetime) -> list[PaperOrder]:
        self.risk_manager.activate_emergency_stop()
        orders = []
        for symbol, position in tuple(self.positions.items()):
            orders.append(
                self.submit_reduce_only(
                    symbol,
                    OrderType.MARKET,
                    position.quantity,
                    timestamp,
                    exit_reason=ExitReason.EMERGENCY_STOP,
                    identity_suffix=f"emergency:{timestamp}",
                )
            )
        return orders

    def equity(self) -> Decimal:
        unrealized = ZERO
        for symbol, position in self.positions.items():
            market = self._last_markets.get(symbol)
            if market is not None:
                unrealized += position.unrealized_pnl(market.midpoint)
        return self.account.cash_balance + unrealized

    def _execute(
        self, order: PaperOrder, market: MarketSnapshot, liquidity: Liquidity
    ) -> PaperFill | None:
        position = self.positions.get(order.symbol)
        if order.reduce_only:
            if position is None or position.side is not order.position_side:
                self._save_order(order.model_copy(update={"status": OrderStatus.CANCELED}))
                return None
            requested = min(order.remaining_quantity, position.quantity)
        else:
            if position is not None and position.entry_order_id != order.order_id:
                self._save_order(order.model_copy(update={"status": OrderStatus.REJECTED}))
                return None
            requested = order.remaining_quantity
        quantity, visible_vwap = self._consume(order.side, requested, liquidity)
        if quantity <= 0 or visible_vwap is None:
            return None
        notional_before_slippage = visible_vwap * quantity
        slippage_bps = self._slippage_bps(market, notional_before_slippage)
        direction = Decimal(1) if order.side is OrderSide.BUY else Decimal(-1)
        price = visible_vwap * (Decimal(1) + direction * slippage_bps / TEN_THOUSAND)
        notional = price * quantity
        fee = notional * self.settings.paper_taker_fee_rate
        role = FillRole.EXIT if order.reduce_only else FillRole.ENTRY
        realized = self._apply_fill_to_account(order, quantity, price, fee, market)
        filled_quantity = order.filled_quantity + quantity
        status = (
            OrderStatus.FILLED
            if filled_quantity >= order.quantity
            else OrderStatus.PARTIALLY_FILLED
        )
        updated_order = order.model_copy(
            update={"filled_quantity": filled_quantity, "status": status}
        )
        self._save_order(updated_order)
        fill = PaperFill(
            fill_id=str(
                uuid5(
                    NAMESPACE_URL,
                    f"paper-fill:{order.order_id}:{filled_quantity}:{market.timestamp.isoformat()}",
                )
            ),
            order_id=order.order_id,
            symbol=order.symbol,
            timestamp=market.timestamp,
            side=order.side,
            role=role,
            quantity=quantity,
            price=price,
            notional=notional,
            fee=fee,
            slippage_bps=slippage_bps,
            realized_pnl=realized,
        )
        self.fills.append(fill)
        if self.store is not None:
            self.store.save_paper_fill(fill)
        return fill

    def _apply_fill_to_account(
        self,
        order: PaperOrder,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
        market: MarketSnapshot,
    ) -> Decimal:
        self.account.cash_balance -= fee
        self.account.realized_pnl -= fee
        self.account.cumulative_fees += fee
        position = self.positions.get(order.symbol)
        if not order.reduce_only:
            if position is None:
                stop = order.stop_reference_price or price
                take_profit = order.take_profit_reference_price or price
                position = PaperPosition(
                    symbol=order.symbol,
                    side=order.position_side,
                    quantity=quantity,
                    entry_price=price,
                    opened_at=market.timestamp,
                    entry_order_id=order.order_id,
                    stop_price=stop,
                    take_profit_price=take_profit,
                    time_stop_at=market.timestamp
                    + timedelta(seconds=self.settings.paper_max_hold_seconds),
                    realized_pnl=-fee,
                )
                self.positions[order.symbol] = position
            else:
                total = position.quantity + quantity
                position.entry_price = (
                    position.entry_price * position.quantity + price * quantity
                ) / total
                position.quantity = total
                position.realized_pnl -= fee
            self._create_protective_orders(position, quantity, market.timestamp, order.order_id)
            self._save_position(position)
            return -fee

        if position is None:
            return -fee
        direction = Decimal(1) if position.side is SignalSide.LONG else Decimal(-1)
        gross = (price - position.entry_price) * quantity * direction
        net = gross - fee
        self.account.cash_balance += gross
        self.account.realized_pnl += gross
        position.realized_pnl += net
        position.quantity -= quantity
        if position.quantity <= 0:
            closed_pnl = position.realized_pnl + position.funding_pnl
            del self.positions[order.symbol]
            self._cancel_remaining_orders(order.symbol, order.order_id)
            self.risk_manager.record_closed_trade(market.timestamp, closed_pnl, self.equity())
            self.closed_trade_pnls.append(closed_pnl)
            self._save_position(None, order.symbol)
        else:
            self._save_position(position)
        return net

    def _create_protective_orders(
        self,
        position: PaperPosition,
        quantity: Decimal,
        timestamp: datetime,
        parent_order_id: str,
    ) -> None:
        self.submit_reduce_only(
            position.symbol,
            OrderType.STOP_MARKET,
            quantity,
            timestamp,
            trigger_price=position.stop_price,
            exit_reason=ExitReason.STOP_LOSS,
            identity_suffix=f"{parent_order_id}:stop:{position.quantity}",
        )
        self.submit_reduce_only(
            position.symbol,
            OrderType.TAKE_PROFIT_MARKET,
            quantity,
            timestamp,
            trigger_price=position.take_profit_price,
            exit_reason=ExitReason.TAKE_PROFIT,
            identity_suffix=f"{parent_order_id}:take-profit:{position.quantity}",
        )

    def _schedule_time_stop(self, market: MarketSnapshot) -> None:
        position = self.positions.get(market.symbol)
        if position is None or market.timestamp < position.time_stop_at:
            return
        exists = any(
            order.symbol == market.symbol
            and order.exit_reason is ExitReason.TIME_STOP
            and order.status in {OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED}
            for order in self.orders.values()
        )
        if not exists:
            self.submit_reduce_only(
                market.symbol,
                OrderType.MARKET,
                position.quantity,
                market.timestamp,
                exit_reason=ExitReason.TIME_STOP,
                identity_suffix=f"time:{position.opened_at.isoformat()}",
            )

    def _apply_funding(self, market: MarketSnapshot) -> None:
        event_id = market.funding_event_id
        position = self.positions.get(market.symbol)
        if event_id is None or event_id in self._funding_events or position is None:
            return
        direction = Decimal(-1) if position.side is SignalSide.LONG else Decimal(1)
        payment = market.midpoint * position.quantity * market.funding_rate * direction
        position.funding_pnl += payment
        self.account.cash_balance += payment
        self.account.realized_pnl += payment
        self.account.cumulative_funding += payment
        self._funding_events.add(event_id)
        self._save_position(position)

    @staticmethod
    def _triggered(order: PaperOrder, market: MarketSnapshot) -> bool:
        if order.order_type is OrderType.MARKET:
            return True
        if order.trigger_price is None:
            return False
        executable = market.bid_price if order.side is OrderSide.SELL else market.ask_price
        if order.order_type is OrderType.STOP_MARKET:
            return (
                executable <= order.trigger_price
                if order.side is OrderSide.SELL
                else executable >= order.trigger_price
            )
        return (
            executable >= order.trigger_price
            if order.side is OrderSide.SELL
            else executable <= order.trigger_price
        )

    def _slippage_bps(self, market: MarketSnapshot, notional: Decimal) -> Decimal:
        return (
            self.settings.paper_base_slippage_bps
            + abs(market.volatility_percent)
            * Decimal(100)
            * self.settings.paper_volatility_slippage_factor
            + market.spread_bps * self.settings.paper_spread_slippage_factor
            + notional / Decimal(100000) * self.settings.paper_notional_slippage_bps_per_100k
        )

    @staticmethod
    def _consume(
        side: OrderSide, requested: Decimal, liquidity: Liquidity
    ) -> tuple[Decimal, Decimal | None]:
        levels = liquidity.asks if side is OrderSide.BUY else liquidity.bids
        remaining = requested
        filled = ZERO
        notional = ZERO
        for level in levels:
            if remaining <= 0:
                break
            quantity = min(remaining, level[1])
            if quantity <= 0:
                continue
            filled += quantity
            notional += level[0] * quantity
            level[1] -= quantity
            remaining -= quantity
        return filled, notional / filled if filled > 0 else None

    @staticmethod
    def _liquidity(market: MarketSnapshot) -> Liquidity:
        book = market.order_book
        if book is not None and book.synchronized:
            return Liquidity(
                bids=[
                    [price, quantity] for price, quantity in sorted(book.bids.items(), reverse=True)
                ],
                asks=[[price, quantity] for price, quantity in sorted(book.asks.items())],
            )
        return Liquidity(
            bids=[[market.bid_price, market.bid_quantity]],
            asks=[[market.ask_price, market.ask_quantity]],
        )

    def _take_profit_reference(self, signal: StrategySignal) -> Decimal:
        risk = abs(signal.entry_reference_price - signal.stop_reference_price)
        if signal.side is SignalSide.LONG:
            return signal.entry_reference_price + risk * self.settings.paper_take_profit_r_multiple
        return signal.entry_reference_price - risk * self.settings.paper_take_profit_r_multiple

    def _cancel_remaining_orders(self, symbol: str, filled_order_id: str) -> None:
        for order in tuple(self.orders.values()):
            if (
                order.symbol == symbol
                and order.order_id != filled_order_id
                and order.status in {OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED}
            ):
                self._save_order(order.model_copy(update={"status": OrderStatus.CANCELED}))

    def _record_equity(self, timestamp: datetime) -> None:
        unrealized = self.equity() - self.account.cash_balance
        snapshot = AccountSnapshot(
            timestamp=timestamp,
            cash_balance=self.account.cash_balance,
            equity=self.account.cash_balance + unrealized,
            realized_pnl=self.account.realized_pnl,
            unrealized_pnl=unrealized,
            cumulative_fees=self.account.cumulative_fees,
            cumulative_funding=self.account.cumulative_funding,
            open_positions=len(self.positions),
        )
        self.equity_curve.append(snapshot)
        if self.store is not None:
            self.store.save_account_snapshot(snapshot)

    def _save_order(self, order: PaperOrder) -> None:
        self.orders[order.order_id] = order
        if self.store is not None:
            self.store.save_paper_order(order)

    def _save_position(self, position: PaperPosition | None, symbol: str | None = None) -> None:
        if self.store is not None:
            resolved = symbol if symbol is not None else position.symbol if position else None
            if resolved is None:
                raise ValueError("symbol is required when deleting a paper position")
            self.store.save_paper_position(position, resolved)

    def performance(self) -> dict[str, object]:
        equities = [item.equity for item in self.equity_curve]
        peak: Decimal | None = None
        max_drawdown = ZERO
        for equity in equities:
            peak = equity if peak is None else max(peak, equity)
            if peak > 0:
                max_drawdown = max(max_drawdown, (peak - equity) / peak)
        wins = [value for value in self.closed_trade_pnls if value > 0]
        losses = [value for value in self.closed_trade_pnls if value < 0]
        gross_profit = sum(wins, start=ZERO)
        gross_loss = abs(sum(losses, start=ZERO))
        return {
            "initial_balance": self.account.initial_balance,
            "equity": self.equity(),
            "realized_pnl": self.account.realized_pnl,
            "cumulative_fees": self.account.cumulative_fees,
            "cumulative_funding": self.account.cumulative_funding,
            "max_drawdown_fraction": max_drawdown,
            "closed_trades": len(self.closed_trade_pnls),
            "win_rate": Decimal(len(wins)) / Decimal(len(self.closed_trade_pnls))
            if self.closed_trade_pnls
            else ZERO,
            "profit_factor": gross_profit / gross_loss if gross_loss > 0 else None,
            "equity_curve_points": len(self.equity_curve),
        }

    def reset(self) -> None:
        if self.positions:
            raise ValueError("Cannot reset paper account while positions are open")
        self.account = PaperAccount(
            initial_balance=self.settings.paper_initial_balance,
            cash_balance=self.settings.paper_initial_balance,
        )
        self.orders.clear()
        self.fills.clear()
        self.equity_curve.clear()
        self.closed_trade_pnls.clear()
        self._last_market_time.clear()
        self._last_markets.clear()
        self._funding_events.clear()
        self.risk_manager.reset()
        if self.store is not None:
            self.store.reset_paper()
