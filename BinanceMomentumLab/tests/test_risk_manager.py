from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from binance_momentum_lab.config import AppMode, Settings
from binance_momentum_lab.paper.risk import (
    RiskEnvironment,
    RiskManager,
    RiskRejectCode,
)
from binance_momentum_lab.strategy.models import StrategySignal
from binance_momentum_lab.strategy.replay import HistoricalFeatureReplay
from binance_momentum_lab.strategy.state_machine import StrategyStateMachine
from tests.test_strategy_state_machine import replay_events


def long_signal() -> StrategySignal:
    _, signals = HistoricalFeatureReplay(StrategyStateMachine(Settings(_env_file=None))).run(
        replay_events()
    )
    return signals[0]


def environment(timestamp: datetime | None = None) -> RiskEnvironment:
    now = timestamp or datetime(2026, 1, 1, tzinfo=UTC)
    return RiskEnvironment(
        timestamp=now,
        market_data_timestamp=now,
        websocket_healthy=True,
        order_book_synchronized=True,
        spread_bps=Decimal("2"),
    )


def test_risk_position_size_uses_equity_and_stop_distance() -> None:
    settings = Settings(
        _env_file=None,
        risk_per_trade_fraction=Decimal("0.01"),
        risk_max_position_notional_multiple=Decimal("10"),
        paper_quantity_step=Decimal("0.001"),
    )
    manager = RiskManager(settings)

    assert manager.position_size(Decimal("10000"), Decimal("100"), Decimal("98")) == Decimal("50")


def test_entry_fails_closed_for_market_health_and_existing_position() -> None:
    manager = RiskManager(Settings(_env_file=None))
    signal = long_signal()
    now = signal.timestamp
    unsafe = RiskEnvironment(
        timestamp=now,
        market_data_timestamp=now - timedelta(seconds=4),
        websocket_healthy=False,
        order_book_synchronized=False,
        spread_bps=Decimal("100"),
    )

    decision = manager.evaluate_entry(
        signal, unsafe, equity=Decimal("10000"), open_symbols={signal.symbol}
    )

    assert not decision.approved
    assert {
        RiskRejectCode.STALE_MARKET_DATA,
        RiskRejectCode.WEBSOCKET_UNHEALTHY,
        RiskRejectCode.ORDER_BOOK_UNSYNCHRONIZED,
        RiskRejectCode.SPREAD_TOO_WIDE,
        RiskRejectCode.POSITION_EXISTS,
    } <= set(decision.reject_codes)


def test_daily_loss_consecutive_loss_cooldown_and_emergency_stop() -> None:
    settings = Settings(
        _env_file=None,
        risk_consecutive_loss_limit=3,
        risk_cooldown_seconds=60,
    )
    manager = RiskManager(settings)
    signal = long_signal()
    now = signal.timestamp
    for offset in range(3):
        manager.record_closed_trade(
            now + timedelta(seconds=offset), Decimal("-40"), Decimal("9880")
        )

    decision = manager.evaluate_entry(
        signal,
        environment(now + timedelta(seconds=3)),
        equity=Decimal("9880"),
        open_symbols=set(),
    )
    assert RiskRejectCode.DAILY_LOSS_LIMIT in decision.reject_codes
    assert RiskRejectCode.CONSECUTIVE_LOSS_COOLDOWN in decision.reject_codes

    manager.activate_emergency_stop()
    stopped = manager.evaluate_entry(
        signal,
        environment(now + timedelta(seconds=4)),
        equity=Decimal("9880"),
        open_symbols=set(),
    )
    assert RiskRejectCode.EMERGENCY_STOP in stopped.reject_codes


def test_monitor_mode_cannot_open_paper_position() -> None:
    manager = RiskManager(Settings(_env_file=None, app_mode=AppMode.MONITOR))
    signal = long_signal()

    decision = manager.evaluate_entry(
        signal, environment(signal.timestamp), equity=Decimal("10000"), open_symbols=set()
    )

    assert RiskRejectCode.MODE_NOT_PAPER in decision.reject_codes
