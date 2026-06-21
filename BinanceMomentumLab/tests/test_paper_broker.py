from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from binance_momentum_lab.config import Settings
from binance_momentum_lab.exceptions import NonMonotonicMarketDataError
from binance_momentum_lab.market_data.order_book import LocalOrderBook
from binance_momentum_lab.paper.broker import PaperBroker
from binance_momentum_lab.paper.models import (
    ExitReason,
    FillRole,
    MarketSnapshot,
    OrderStatus,
    OrderType,
)
from binance_momentum_lab.paper.risk import RiskDecision, RiskManager
from binance_momentum_lab.strategy.models import StrategySignal
from binance_momentum_lab.strategy.replay import HistoricalFeatureReplay
from binance_momentum_lab.strategy.state_machine import StrategyStateMachine
from tests.test_risk_manager import long_signal
from tests.test_strategy_state_machine import replay_events


def market(
    signal: StrategySignal,
    milliseconds: int,
    *,
    bid: str = "105.0",
    ask: str = "105.2",
    funding_rate: str = "0",
    funding_event_id: str | None = None,
) -> MarketSnapshot:
    book = LocalOrderBook(signal.symbol)
    book.synchronized = True
    book.bids = {Decimal(bid): Decimal("100"), Decimal(bid) - 1: Decimal("100")}
    book.asks = {Decimal(ask): Decimal("100"), Decimal(ask) + 1: Decimal("100")}
    return MarketSnapshot(
        symbol=signal.symbol,
        timestamp=signal.timestamp + timedelta(milliseconds=milliseconds),
        bid_price=Decimal(bid),
        bid_quantity=Decimal("100"),
        ask_price=Decimal(ask),
        ask_quantity=Decimal("100"),
        order_book=book,
        volatility_percent=Decimal("1"),
        funding_rate=Decimal(funding_rate),
        funding_event_id=funding_event_id,
    )


def broker_settings(**updates: object) -> Settings:
    base = Settings(
        _env_file=None,
        paper_network_latency_ms=100,
        risk_max_position_notional_multiple=Decimal("10"),
    )
    return base.model_copy(update=updates)


def open_long(settings: Settings | None = None) -> tuple[PaperBroker, StrategySignal]:
    configured = settings or broker_settings()
    broker = PaperBroker(configured, RiskManager(configured))
    signal = long_signal()
    broker.submit_entry(signal, RiskDecision(True, Decimal("2")), signal.timestamp)
    fills = broker.on_market(market(signal, 0))
    if configured.paper_network_latency_ms > 0:
        assert not fills
        fills = broker.on_market(market(signal, configured.paper_network_latency_ms))
    assert len(fills) == 1
    return broker, signal


def test_entry_uses_visible_ask_then_adverse_slippage_and_fee() -> None:
    broker, signal = open_long()
    fill = broker.fills[0]

    assert fill.role is FillRole.ENTRY
    assert fill.price > Decimal("105.2")
    assert fill.slippage_bps > broker.settings.paper_base_slippage_bps
    assert fill.fee > 0
    assert broker.account.cash_balance == broker.account.initial_balance - fill.fee
    assert signal.symbol in broker.positions


def test_stop_loss_is_reduce_only_and_never_uses_candle_extremes() -> None:
    broker, signal = open_long()
    stop_orders = [
        order for order in broker.orders.values() if order.order_type is OrderType.STOP_MARKET
    ]
    assert len(stop_orders) == 1 and stop_orders[0].reduce_only

    fills = broker.on_market(market(signal, 200, bid="103.0", ask="103.2"))

    assert fills[-1].role is FillRole.EXIT
    assert fills[-1].realized_pnl < 0
    assert signal.symbol not in broker.positions
    assert broker.account.cumulative_fees > broker.fills[0].fee


def test_take_profit_and_positive_funding_charge_for_long() -> None:
    broker, signal = open_long()
    cash_before_funding = broker.account.cash_balance
    broker.on_market(
        market(
            signal,
            150,
            funding_rate="0.001",
            funding_event_id="funding-1",
        )
    )
    assert broker.account.cash_balance < cash_before_funding

    take_profit = next(
        order
        for order in broker.orders.values()
        if order.order_type is OrderType.TAKE_PROFIT_MARKET
    )
    target = take_profit.trigger_price
    assert target is not None
    fills = broker.on_market(
        market(signal, 250, bid=str(target + 1), ask=str(target + Decimal("1.2")))
    )

    assert fills[-1].realized_pnl > 0
    assert broker.account.cumulative_funding < 0
    assert signal.symbol not in broker.positions


def test_reduce_only_cannot_reverse_position() -> None:
    broker, signal = open_long(broker_settings(paper_network_latency_ms=0))
    position_quantity = broker.positions[signal.symbol].quantity

    broker.submit_reduce_only(
        signal.symbol,
        OrderType.MARKET,
        position_quantity * 10,
        signal.timestamp + timedelta(milliseconds=500),
        exit_reason=ExitReason.TIME_STOP,
        identity_suffix="manual-oversize",
    )
    broker.on_market(market(signal, 500))

    assert signal.symbol not in broker.positions
    assert not any(fill.quantity > position_quantity for fill in broker.fills)


def test_time_stop_closes_at_current_quote_after_hold_limit() -> None:
    settings = broker_settings(paper_network_latency_ms=0, paper_max_hold_seconds=1)
    broker, signal = open_long(settings)

    fills = broker.on_market(market(signal, 1000, bid="104.9", ask="105.1"))

    assert fills[-1].role is FillRole.EXIT
    assert signal.symbol not in broker.positions
    time_orders = [
        order for order in broker.orders.values() if order.exit_reason is ExitReason.TIME_STOP
    ]
    assert len(time_orders) == 1 and time_orders[0].reduce_only


def test_losing_position_cannot_be_averaged_down() -> None:
    broker, signal = open_long()
    second = broker.submit_entry(
        signal.model_copy(update={"signal_id": "second-signal"}),
        RiskDecision(True, Decimal("2")),
        signal.timestamp + timedelta(milliseconds=150),
    )

    assert second.status is OrderStatus.REJECTED
    assert broker.positions[signal.symbol].quantity == Decimal("2")


def test_non_monotonic_market_data_is_rejected() -> None:
    broker, signal = open_long()

    with pytest.raises(NonMonotonicMarketDataError):
        broker.on_market(market(signal, 50))


def test_visible_liquidity_produces_partial_then_complete_fill() -> None:
    settings = broker_settings(paper_network_latency_ms=0)
    broker = PaperBroker(settings, RiskManager(settings))
    signal = long_signal()
    broker.submit_entry(signal, RiskDecision(True, Decimal("150")), signal.timestamp)

    first = broker.on_market(replace(market(signal, 0), order_book=None))
    second = broker.on_market(replace(market(signal, 100), order_book=None))

    assert first[0].quantity == Decimal("100")
    assert second[0].quantity == Decimal("50")
    entry_order = next(order for order in broker.orders.values() if not order.reduce_only)
    assert entry_order.status is OrderStatus.FILLED
    assert broker.positions[signal.symbol].quantity == Decimal("150")


def test_stop_cancels_unfilled_entry_remainder_before_it_can_average_down() -> None:
    settings = broker_settings(paper_network_latency_ms=0)
    broker = PaperBroker(settings, RiskManager(settings))
    signal = long_signal()
    entry = broker.submit_entry(signal, RiskDecision(True, Decimal("150")), signal.timestamp)
    broker.on_market(replace(market(signal, 0), order_book=None))
    assert broker.orders[entry.order_id].status is OrderStatus.PARTIALLY_FILLED

    stopped_market = replace(market(signal, 100, bid="103.0", ask="103.02"), order_book=None)
    fills = broker.on_market(stopped_market)

    assert len(fills) == 1 and fills[0].role is FillRole.EXIT
    assert broker.orders[entry.order_id].status is OrderStatus.CANCELED
    assert signal.symbol not in broker.positions
    broker.on_market(replace(market(signal, 200), order_book=None))
    assert signal.symbol not in broker.positions


def test_short_receives_positive_funding_and_emergency_exit_is_reduce_only() -> None:
    settings = broker_settings()
    broker = PaperBroker(settings, RiskManager(settings))
    _, replay_signals = HistoricalFeatureReplay(StrategyStateMachine(settings)).run(replay_events())
    short_signal = replay_signals[1]
    broker.submit_entry(short_signal, RiskDecision(True, Decimal("2")), short_signal.timestamp)
    broker.on_market(market(short_signal, 100, bid="98.8", ask="98.82"))
    cash_before = broker.account.cash_balance
    broker.on_market(
        market(
            short_signal,
            150,
            bid="98.7",
            ask="98.72",
            funding_rate="0.001",
            funding_event_id="short-funding",
        )
    )
    assert broker.account.cash_balance > cash_before

    orders = broker.emergency_close_all(short_signal.timestamp + timedelta(milliseconds=200))
    assert orders and all(order.reduce_only for order in orders)
    broker.on_market(market(short_signal, 300, bid="98.6", ask="98.62"))
    assert short_signal.symbol not in broker.positions
