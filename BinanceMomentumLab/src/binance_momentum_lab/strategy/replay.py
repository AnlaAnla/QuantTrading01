"""Deterministic historical feature-event replay."""

from __future__ import annotations

from collections.abc import Iterable

from .models import FeatureSnapshot, StateMachineResult, StrategySignal
from .state_machine import StrategyStateMachine


class HistoricalFeatureReplay:
    """Replay already timestamped feature events in stable input order."""

    def __init__(self, machine: StrategyStateMachine) -> None:
        self._machine = machine

    def run(
        self, events: Iterable[FeatureSnapshot]
    ) -> tuple[list[StateMachineResult], list[StrategySignal]]:
        ordered = sorted(enumerate(events), key=lambda item: (item[1].timestamp, item[0]))
        results: list[StateMachineResult] = []
        signals: list[StrategySignal] = []
        for _, event in ordered:
            result = self._machine.process(event)
            results.append(result)
            if result.signal is not None:
                signals.append(result.signal)
        return results, signals
