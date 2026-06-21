from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from binance_momentum_lab.config import Settings
from binance_momentum_lab.market_data.order_book import LocalOrderBook
from binance_momentum_lab.paper.broker import PaperBroker
from binance_momentum_lab.paper.models import FillRole, MarketSnapshot
from binance_momentum_lab.paper.risk import RiskEnvironment, RiskManager
from binance_momentum_lab.strategy.models import (
    FeatureSnapshot,
    SignalSide,
    StrategySignal,
    StrategyState,
)
from binance_momentum_lab.strategy.state_machine import StrategyStateMachine

FIXTURES = Path(__file__).parent / "fixtures"


def load_scenario(name: str) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads((FIXTURES / name).read_text(encoding="utf-8")),
    )


def fixture_market(symbol: str, payload: dict[str, Any]) -> MarketSnapshot:
    bid = Decimal(payload["bid"])
    ask = Decimal(payload["ask"])
    quantity = Decimal(payload["quantity"])
    book = LocalOrderBook(symbol)
    book.synchronized = True
    book.bids = {bid: quantity}
    book.asks = {ask: quantity}
    return MarketSnapshot(
        symbol=symbol,
        timestamp=datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00")),
        bid_price=bid,
        bid_quantity=quantity,
        ask_price=ask,
        ask_quantity=quantity,
        order_book=book,
        volatility_percent=Decimal("1"),
    )


def run_features(
    machine: StrategyStateMachine, payloads: list[dict[str, Any]]
) -> tuple[list[StrategyState], list[StrategySignal]]:
    states: list[StrategyState] = []
    signals: list[StrategySignal] = []
    for payload in payloads:
        result = machine.process(FeatureSnapshot.model_validate(payload))
        states.append(result.state)
        if result.signal is not None:
            signals.append(result.signal)
    return states, signals


def execute_signal(
    broker: PaperBroker, signal: StrategySignal, markets: list[MarketSnapshot]
) -> None:
    current = markets[0]
    broker.on_market(current)
    decision = broker.risk_manager.evaluate_entry(
        signal,
        RiskEnvironment(
            timestamp=signal.timestamp,
            market_data_timestamp=current.timestamp,
            websocket_healthy=True,
            order_book_synchronized=True,
            spread_bps=current.spread_bps,
        ),
        equity=broker.equity(),
        open_symbols=set(broker.positions),
    )
    assert decision.approved
    broker.submit_entry(signal, decision, signal.timestamp)
    for market in markets[1:]:
        broker.on_market(market)


def test_ignition_pullback_continuation_fixture_is_profitable() -> None:
    scenario = load_scenario("paper_ignition_pullback_continuation.json")
    settings = Settings(_env_file=None)
    machine = StrategyStateMachine(settings)
    states, signals = run_features(machine, scenario["features"])
    assert states[-1].value == scenario["expected_state"]
    assert len(signals) == 1 and signals[0].side.value == scenario["expected_side"]

    broker = PaperBroker(settings, RiskManager(settings))
    markets = [fixture_market(signals[0].symbol, item) for item in scenario["markets"]]
    execute_signal(broker, signals[0], markets)

    assert [fill.role for fill in broker.fills] == [FillRole.ENTRY, FillRole.EXIT]
    assert broker.closed_trade_pnls[0] > 0
    assert not broker.positions
    assert len(broker.orders) == 3
    assert broker.account.cumulative_fees > 0
    assert len(broker.equity_curve) == len(markets)


def test_ignition_immediate_failure_fixture_never_opens() -> None:
    scenario = load_scenario("paper_ignition_immediate_failure.json")
    settings = Settings(_env_file=None)
    machine = StrategyStateMachine(settings)
    states, signals = run_features(machine, scenario["features"])
    broker = PaperBroker(settings, RiskManager(settings))
    broker.on_market(fixture_market("FAILUSDT", scenario["markets"][0]))

    assert states[-1].value == scenario["expected_state"]
    assert len(signals) == scenario["expected_signals"]
    assert not broker.orders and not broker.fills and not broker.positions
    assert broker.account.realized_pnl == 0


def test_distribution_breakdown_failed_rebound_fixture_shorts_profitably() -> None:
    scenario = load_scenario("paper_distribution_breakdown_failed_rebound.json")
    settings = Settings(_env_file=None)
    machine = StrategyStateMachine(settings)
    context = machine.context_for("REVUSDT")
    initial = scenario["initial_context"]
    context.state = StrategyState(initial["state"])
    context.ignition_start_price = Decimal(initial["ignition_start_price"])
    context.ignition_high = Decimal(initial["ignition_high"])
    context.ignition_cvd_high = Decimal(initial["ignition_cvd_high"])
    context.peak_price = Decimal(initial["peak_price"])
    context.peak_cvd = Decimal(initial["peak_cvd"])
    context.previous_price = Decimal(initial["previous_price"])
    states, signals = run_features(machine, scenario["features"])

    assert states[-1].value == scenario["expected_state"]
    assert len(signals) == 1
    assert signals[0].side is SignalSide.SHORT

    broker = PaperBroker(settings, RiskManager(settings))
    markets = [fixture_market(signals[0].symbol, item) for item in scenario["markets"]]
    execute_signal(broker, signals[0], markets)

    assert [fill.role for fill in broker.fills] == [FillRole.ENTRY, FillRole.EXIT]
    assert broker.fills[0].side.value == "SELL"
    assert broker.closed_trade_pnls[0] > 0
    assert not broker.positions
    assert len(broker.orders) == 3
    assert len(broker.equity_curve) == len(markets)
