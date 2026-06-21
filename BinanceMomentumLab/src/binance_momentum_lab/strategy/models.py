"""Typed feature, state, and explainable signal models."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class StrategyState(StrEnum):
    NORMAL = "NORMAL"
    WATCH = "WATCH"
    IGNITION = "IGNITION"
    PULLBACK = "PULLBACK"
    CONTINUATION = "CONTINUATION"
    DISTRIBUTION = "DISTRIBUTION"
    BREAKDOWN = "BREAKDOWN"
    COOLDOWN = "COOLDOWN"


class SignalSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class ReasonCode(StrEnum):
    RETURN_5M_CONFIRMED = "RETURN_5M_CONFIRMED"
    VOLUME_ZSCORE_CONFIRMED = "VOLUME_ZSCORE_CONFIRMED"
    TAKER_BUY_DOMINANCE = "TAKER_BUY_DOMINANCE"
    OPEN_INTEREST_EXPANSION = "OPEN_INTEREST_EXPANSION"
    ABOVE_ANCHORED_VWAP = "ABOVE_ANCHORED_VWAP"
    VALID_PULLBACK_DEPTH = "VALID_PULLBACK_DEPTH"
    PULLBACK_VOLUME_CONTRACTION = "PULLBACK_VOLUME_CONTRACTION"
    BREAKOUT_PULLBACK_HIGH = "BREAKOUT_PULLBACK_HIGH"
    SPREAD_WITHIN_LIMIT = "SPREAD_WITHIN_LIMIT"
    BEARISH_CVD_DIVERGENCE = "BEARISH_CVD_DIVERGENCE"
    BUY_ABSORPTION = "BUY_ABSORPTION"
    BELOW_ANCHORED_VWAP = "BELOW_ANCHORED_VWAP"
    FAILED_VWAP_RECLAIM = "FAILED_VWAP_RECLAIM"
    LOWER_LOCAL_HIGH = "LOWER_LOCAL_HIGH"
    TAKER_SELL_DOMINANCE = "TAKER_SELL_DOMINANCE"


class InvalidationReason(StrEnum):
    PRICE_BELOW_PULLBACK_LOW = "PRICE_BELOW_PULLBACK_LOW"
    PRICE_ABOVE_FAILED_REBOUND_HIGH = "PRICE_ABOVE_FAILED_REBOUND_HIGH"


class FeatureSnapshot(BaseModel):
    """A fully serializable point-in-time strategy input."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    price: Decimal
    return_1m_percent: Decimal | None = None
    return_3m_percent: Decimal | None = None
    return_5m_percent: Decimal | None = None
    return_15m_percent: Decimal | None = None
    quote_volume_1m: Decimal | None = None
    quote_volume_5m: Decimal | None = None
    previous_5m_average_quote_volume: Decimal | None = None
    volume_zscore: Decimal | None = None
    taker_buy_ratio: Decimal | None = None
    cvd: Decimal | None = None
    cvd_slope: Decimal | None = None
    anchored_vwap: Decimal | None = None
    distance_from_anchored_vwap_percent: Decimal | None = None
    open_interest_change_5m_percent: Decimal | None = None
    funding_rate: Decimal | None = None
    basis_percent: Decimal | None = None
    spread_bps: Decimal | None = None
    order_book_imbalance_5: Decimal | None = None
    order_book_imbalance_20: Decimal | None = None
    btc_return_5m_percent: Decimal | None = None
    btc_residual_return_5m_percent: Decimal | None = None
    price_high_5m: Decimal | None = None
    prior_price_high_5m: Decimal | None = None


class StrategySignal(BaseModel):
    """Explainable research signal; it has no order or execution semantics."""

    model_config = ConfigDict(frozen=True)

    signal_id: str
    symbol: str
    timestamp: datetime
    side: SignalSide
    strategy_name: str
    state: StrategyState
    entry_reference_price: Decimal
    stop_reference_price: Decimal
    confidence_score: Decimal
    feature_snapshot: FeatureSnapshot
    reason_codes: tuple[ReasonCode, ...]
    invalidation_reason: InvalidationReason


class StateMachineResult(BaseModel):
    previous_state: StrategyState
    state: StrategyState
    signal: StrategySignal | None = None
    anchor_requested: bool = False
