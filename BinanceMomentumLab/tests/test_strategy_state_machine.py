from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

from binance_momentum_lab.config import Settings
from binance_momentum_lab.strategy.models import (
    FeatureSnapshot,
    ReasonCode,
    SignalSide,
    StrategyState,
)
from binance_momentum_lab.strategy.replay import HistoricalFeatureReplay
from binance_momentum_lab.strategy.state_machine import StrategyStateMachine

FIXTURES = Path(__file__).parent / "fixtures"


def replay_events() -> list[FeatureSnapshot]:
    payload = cast(
        list[dict[str, object]],
        json.loads((FIXTURES / "strategy_replay.json").read_text(encoding="utf-8")),
    )
    return [FeatureSnapshot.model_validate(item) for item in payload]


def test_full_state_path_emits_explainable_long_and_short_signals() -> None:
    machine = StrategyStateMachine(Settings(_env_file=None))
    results, signals = HistoricalFeatureReplay(machine).run(replay_events())

    assert [result.state for result in results] == [
        StrategyState.WATCH,
        StrategyState.IGNITION,
        StrategyState.IGNITION,
        StrategyState.PULLBACK,
        StrategyState.CONTINUATION,
        StrategyState.CONTINUATION,
        StrategyState.DISTRIBUTION,
        StrategyState.BREAKDOWN,
        StrategyState.BREAKDOWN,
        StrategyState.COOLDOWN,
    ]
    assert [signal.side for signal in signals] == [SignalSide.LONG, SignalSide.SHORT]
    long_signal, short_signal = signals
    assert long_signal.stop_reference_price == replay_events()[3].price
    assert ReasonCode.VALID_PULLBACK_DEPTH in long_signal.reason_codes
    assert ReasonCode.PULLBACK_VOLUME_CONTRACTION in long_signal.reason_codes
    assert ReasonCode.BEARISH_CVD_DIVERGENCE in short_signal.reason_codes
    assert ReasonCode.FAILED_VWAP_RECLAIM in short_signal.reason_codes
    assert all(signal.feature_snapshot.symbol == signal.symbol for signal in signals)


def test_same_history_replays_to_identical_states_and_signal_ids() -> None:
    settings = Settings(_env_file=None)

    first_results, first_signals = HistoricalFeatureReplay(StrategyStateMachine(settings)).run(
        replay_events()
    )
    second_results, second_signals = HistoricalFeatureReplay(StrategyStateMachine(settings)).run(
        replay_events()
    )

    assert [item.model_dump_json() for item in first_results] == [
        item.model_dump_json() for item in second_results
    ]
    assert [item.signal_id for item in first_signals] == [item.signal_id for item in second_signals]


def test_signal_reason_codes_are_structured_enums_not_free_text() -> None:
    _, signals = HistoricalFeatureReplay(StrategyStateMachine(Settings(_env_file=None))).run(
        replay_events()
    )

    assert signals
    assert all(isinstance(code, ReasonCode) for signal in signals for code in signal.reason_codes)


def test_buy_absorption_can_enter_distribution_without_cvd_divergence() -> None:
    machine = StrategyStateMachine(Settings(_env_file=None))
    events = replay_events()[:5]
    for event in events:
        machine.process(event)
    absorbed = events[-1].model_copy(
        update={
            "timestamp": events[-1].timestamp + timedelta(minutes=1),
            "price": events[-1].price,
            "return_1m_percent": Decimal("0.05"),
            "taker_buy_ratio": Decimal("0.65"),
            "cvd": Decimal("130"),
        }
    )

    result = machine.process(absorbed)

    assert result.state is StrategyState.DISTRIBUTION
    assert machine.context_for("ALPHAUSDT").divergence_reason is ReasonCode.BUY_ABSORPTION


def test_cooldown_expires_deterministically_to_normal() -> None:
    settings = Settings(_env_file=None)
    machine = StrategyStateMachine(settings)
    events = replay_events()
    for event in events:
        machine.process(event)
    expired = events[-1].model_copy(
        update={
            "timestamp": events[-1].timestamp
            + timedelta(seconds=settings.strategy_cooldown_seconds + 1)
        }
    )

    result = machine.process(expired)

    assert result.previous_state is StrategyState.COOLDOWN
    assert result.state is StrategyState.NORMAL
