"""Deterministic explainable momentum and reversal state machine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from ..config import Settings
from .models import (
    FeatureSnapshot,
    InvalidationReason,
    ReasonCode,
    SignalSide,
    StateMachineResult,
    StrategySignal,
    StrategyState,
)

ZERO = Decimal(0)
ONE = Decimal(1)
STRATEGY_NAME = "binance_momentum_state_machine_v1"


@dataclass(slots=True)
class SymbolStrategyContext:
    state: StrategyState = StrategyState.NORMAL
    entered_at: datetime | None = None
    ignition_start_price: Decimal | None = None
    ignition_high: Decimal | None = None
    ignition_cvd_high: Decimal | None = None
    pullback_low: Decimal | None = None
    pullback_breakout_level: Decimal | None = None
    pullback_volume_valid: bool = False
    peak_price: Decimal | None = None
    peak_cvd: Decimal | None = None
    divergence_reason: ReasonCode | None = None
    breakdown_low: Decimal | None = None
    rebound_high: Decimal | None = None
    rebound_seen: bool = False
    previous_price: Decimal | None = None
    cooldown_until: datetime | None = None


class StrategyStateMachine:
    """One deterministic state context per symbol; emits research signals only."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._contexts: dict[str, SymbolStrategyContext] = {}

    def context_for(self, symbol: str) -> SymbolStrategyContext:
        return self._contexts.setdefault(symbol.upper(), SymbolStrategyContext())

    def process(self, snapshot: FeatureSnapshot) -> StateMachineResult:
        context = self.context_for(snapshot.symbol)
        previous = context.state
        signal: StrategySignal | None = None
        anchor_requested = False

        if context.state is StrategyState.COOLDOWN:
            if context.cooldown_until is not None and snapshot.timestamp >= context.cooldown_until:
                self._reset(context, snapshot.timestamp)
            context.previous_price = snapshot.price
            return StateMachineResult(previous_state=previous, state=context.state)

        if context.state is StrategyState.NORMAL and self._watch_conditions(snapshot):
            self._transition(context, StrategyState.WATCH, snapshot.timestamp)
            anchor_requested = True
        elif context.state is StrategyState.WATCH and self._ignition_conditions(snapshot):
            self._start_ignition(context, snapshot)
        elif context.state is StrategyState.IGNITION:
            self._process_ignition(context, snapshot)
        elif context.state is StrategyState.PULLBACK:
            signal = self._process_pullback(context, snapshot)
        elif context.state is StrategyState.CONTINUATION:
            self._process_continuation(context, snapshot)
        elif context.state is StrategyState.DISTRIBUTION:
            self._process_distribution(context, snapshot)
        elif context.state is StrategyState.BREAKDOWN:
            signal = self._process_breakdown(context, snapshot)

        context.previous_price = snapshot.price
        return StateMachineResult(
            previous_state=previous,
            state=context.state,
            signal=signal,
            anchor_requested=anchor_requested,
        )

    def _watch_conditions(self, snapshot: FeatureSnapshot) -> bool:
        return self._at_least(
            snapshot.return_5m_percent, self._settings.strategy_watch_return_5m_percent
        ) and self._at_least(snapshot.volume_zscore, self._settings.strategy_watch_volume_zscore)

    def _ignition_conditions(self, snapshot: FeatureSnapshot) -> bool:
        return all(
            (
                self._at_least(
                    snapshot.return_5m_percent,
                    self._settings.strategy_ignition_return_5m_percent,
                ),
                self._at_least(snapshot.volume_zscore, self._settings.strategy_long_volume_zscore),
                self._at_least(
                    snapshot.taker_buy_ratio,
                    self._settings.strategy_long_taker_buy_ratio,
                ),
                self._at_least(
                    snapshot.open_interest_change_5m_percent,
                    self._settings.strategy_long_oi_change_5m_percent,
                ),
                snapshot.anchored_vwap is not None and snapshot.price > snapshot.anchored_vwap,
                self._spread_valid(snapshot),
            )
        )

    def _start_ignition(self, context: SymbolStrategyContext, snapshot: FeatureSnapshot) -> None:
        self._transition(context, StrategyState.IGNITION, snapshot.timestamp)
        context.ignition_start_price = snapshot.anchored_vwap or snapshot.price
        context.ignition_high = snapshot.price
        context.ignition_cvd_high = snapshot.cvd
        context.peak_price = snapshot.price
        context.peak_cvd = snapshot.cvd

    def _process_ignition(self, context: SymbolStrategyContext, snapshot: FeatureSnapshot) -> None:
        if context.ignition_high is None or context.ignition_start_price is None:
            return
        context.ignition_high = max(context.ignition_high, snapshot.price)
        if snapshot.cvd is not None:
            context.ignition_cvd_high = max(context.ignition_cvd_high or snapshot.cvd, snapshot.cvd)
        rise = context.ignition_high - context.ignition_start_price
        drawdown = context.ignition_high - snapshot.price
        if rise <= 0 or drawdown <= 0:
            return
        ratio = drawdown / rise
        if ratio > self._settings.strategy_pullback_max_ratio:
            self._transition(context, StrategyState.BREAKDOWN, snapshot.timestamp)
            context.breakdown_low = snapshot.price
            return
        if ratio < self._settings.strategy_pullback_min_ratio:
            return
        context.pullback_low = snapshot.price
        context.pullback_breakout_level = context.ignition_high
        context.pullback_volume_valid = self._volume_contracted(snapshot)
        self._transition(context, StrategyState.PULLBACK, snapshot.timestamp)

    def _process_pullback(
        self, context: SymbolStrategyContext, snapshot: FeatureSnapshot
    ) -> StrategySignal | None:
        if (
            context.ignition_high is None
            or context.ignition_start_price is None
            or context.pullback_low is None
            or context.pullback_breakout_level is None
        ):
            return None
        context.pullback_low = min(context.pullback_low, snapshot.price)
        rise = context.ignition_high - context.ignition_start_price
        drawdown = context.ignition_high - context.pullback_low
        if rise > 0 and drawdown / rise > self._settings.strategy_pullback_max_ratio:
            self._transition(context, StrategyState.BREAKDOWN, snapshot.timestamp)
            context.breakdown_low = snapshot.price
            return None
        if (
            snapshot.price >= context.pullback_breakout_level
            and context.pullback_volume_valid
            and self._ignition_conditions(snapshot)
        ):
            self._transition(context, StrategyState.CONTINUATION, snapshot.timestamp)
            context.peak_price = snapshot.price
            context.peak_cvd = snapshot.cvd
            return self._long_signal(context, snapshot)
        return None

    def _process_continuation(
        self, context: SymbolStrategyContext, snapshot: FeatureSnapshot
    ) -> None:
        previous_peak_price = context.peak_price or snapshot.price
        previous_peak_cvd = context.peak_cvd
        price_new_high = snapshot.price > previous_peak_price
        cvd_divergence = (
            price_new_high
            and snapshot.cvd is not None
            and previous_peak_cvd is not None
            and snapshot.cvd <= previous_peak_cvd
        )
        buy_absorption = self._at_least(
            snapshot.taker_buy_ratio,
            self._settings.strategy_buy_absorption_taker_ratio,
        ) and self._at_most(
            snapshot.return_1m_percent,
            self._settings.strategy_buy_absorption_max_return_1m_percent,
        )
        context.peak_price = max(previous_peak_price, snapshot.price)
        if snapshot.cvd is not None:
            context.peak_cvd = max(previous_peak_cvd or snapshot.cvd, snapshot.cvd)
        if cvd_divergence or buy_absorption:
            context.divergence_reason = (
                ReasonCode.BEARISH_CVD_DIVERGENCE if cvd_divergence else ReasonCode.BUY_ABSORPTION
            )
            self._transition(context, StrategyState.DISTRIBUTION, snapshot.timestamp)

    def _process_distribution(
        self, context: SymbolStrategyContext, snapshot: FeatureSnapshot
    ) -> None:
        if snapshot.anchored_vwap is not None and snapshot.price < snapshot.anchored_vwap:
            self._transition(context, StrategyState.BREAKDOWN, snapshot.timestamp)
            context.breakdown_low = snapshot.price
            context.rebound_high = None
            context.rebound_seen = False

    def _process_breakdown(
        self, context: SymbolStrategyContext, snapshot: FeatureSnapshot
    ) -> StrategySignal | None:
        if snapshot.anchored_vwap is None:
            return None
        if snapshot.price >= snapshot.anchored_vwap:
            context.rebound_seen = False
            context.rebound_high = None
            self._transition(context, StrategyState.DISTRIBUTION, snapshot.timestamp)
            return None
        previous_price = context.previous_price
        if previous_price is not None and snapshot.price > previous_price:
            context.rebound_seen = True
            context.rebound_high = max(context.rebound_high or snapshot.price, snapshot.price)
            return None
        context.breakdown_low = min(context.breakdown_low or snapshot.price, snapshot.price)
        sell_ratio = (
            ONE - snapshot.taker_buy_ratio if snapshot.taker_buy_ratio is not None else None
        )
        lower_high = (
            context.rebound_high is not None
            and context.ignition_high is not None
            and context.rebound_high < context.ignition_high
        )
        if all(
            (
                context.divergence_reason is not None,
                context.rebound_seen,
                context.rebound_high is not None and context.rebound_high < snapshot.anchored_vwap,
                lower_high,
                self._at_least(sell_ratio, self._settings.strategy_short_taker_sell_ratio),
                self._spread_valid(snapshot),
                previous_price is not None and snapshot.price < previous_price,
            )
        ):
            signal = self._short_signal(context, snapshot)
            self._transition(context, StrategyState.COOLDOWN, snapshot.timestamp)
            context.cooldown_until = snapshot.timestamp + timedelta(
                seconds=self._settings.strategy_cooldown_seconds
            )
            return signal
        return None

    def _long_signal(
        self, context: SymbolStrategyContext, snapshot: FeatureSnapshot
    ) -> StrategySignal:
        reasons = (
            ReasonCode.RETURN_5M_CONFIRMED,
            ReasonCode.VOLUME_ZSCORE_CONFIRMED,
            ReasonCode.TAKER_BUY_DOMINANCE,
            ReasonCode.OPEN_INTEREST_EXPANSION,
            ReasonCode.ABOVE_ANCHORED_VWAP,
            ReasonCode.VALID_PULLBACK_DEPTH,
            ReasonCode.PULLBACK_VOLUME_CONTRACTION,
            ReasonCode.BREAKOUT_PULLBACK_HIGH,
            ReasonCode.SPREAD_WITHIN_LIMIT,
        )
        return self._signal(
            snapshot,
            SignalSide.LONG,
            StrategyState.CONTINUATION,
            context.pullback_low or snapshot.price,
            reasons,
            InvalidationReason.PRICE_BELOW_PULLBACK_LOW,
        )

    def _short_signal(
        self, context: SymbolStrategyContext, snapshot: FeatureSnapshot
    ) -> StrategySignal:
        divergence = context.divergence_reason or ReasonCode.BEARISH_CVD_DIVERGENCE
        reasons = (
            divergence,
            ReasonCode.BELOW_ANCHORED_VWAP,
            ReasonCode.FAILED_VWAP_RECLAIM,
            ReasonCode.LOWER_LOCAL_HIGH,
            ReasonCode.TAKER_SELL_DOMINANCE,
            ReasonCode.SPREAD_WITHIN_LIMIT,
        )
        return self._signal(
            snapshot,
            SignalSide.SHORT,
            StrategyState.BREAKDOWN,
            context.rebound_high or snapshot.price,
            reasons,
            InvalidationReason.PRICE_ABOVE_FAILED_REBOUND_HIGH,
        )

    @staticmethod
    def _signal(
        snapshot: FeatureSnapshot,
        side: SignalSide,
        state: StrategyState,
        stop: Decimal,
        reasons: tuple[ReasonCode, ...],
        invalidation: InvalidationReason,
    ) -> StrategySignal:
        identity = (
            f"{STRATEGY_NAME}:{snapshot.symbol}:{snapshot.timestamp.isoformat()}:{side.value}"
        )
        confidence = min(ONE, Decimal(len(reasons)) / Decimal(9)).quantize(Decimal("0.0001"))
        return StrategySignal(
            signal_id=str(uuid5(NAMESPACE_URL, identity)),
            symbol=snapshot.symbol,
            timestamp=snapshot.timestamp,
            side=side,
            strategy_name=STRATEGY_NAME,
            state=state,
            entry_reference_price=snapshot.price,
            stop_reference_price=stop,
            confidence_score=confidence,
            feature_snapshot=snapshot,
            reason_codes=reasons,
            invalidation_reason=invalidation,
        )

    def _volume_contracted(self, snapshot: FeatureSnapshot) -> bool:
        return (
            snapshot.quote_volume_1m is not None
            and snapshot.previous_5m_average_quote_volume is not None
            and snapshot.quote_volume_1m
            <= snapshot.previous_5m_average_quote_volume
            * self._settings.strategy_pullback_volume_ratio_max
        )

    def _spread_valid(self, snapshot: FeatureSnapshot) -> bool:
        return self._at_most(snapshot.spread_bps, self._settings.strategy_max_spread_bps)

    @staticmethod
    def _at_least(value: Decimal | None, threshold: Decimal) -> bool:
        return value is not None and value >= threshold

    @staticmethod
    def _at_most(value: Decimal | None, threshold: Decimal) -> bool:
        return value is not None and value <= threshold

    @staticmethod
    def _transition(
        context: SymbolStrategyContext, state: StrategyState, timestamp: datetime
    ) -> None:
        context.state = state
        context.entered_at = timestamp

    @staticmethod
    def _reset(context: SymbolStrategyContext, timestamp: datetime) -> None:
        fresh = SymbolStrategyContext(state=StrategyState.NORMAL, entered_at=timestamp)
        for name in fresh.__dataclass_fields__:
            setattr(context, name, getattr(fresh, name))
