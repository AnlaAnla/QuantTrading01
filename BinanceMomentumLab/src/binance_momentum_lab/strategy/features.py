"""Incremental Decimal feature engine over normalized market observations."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ..binance.models import Kline
from ..config import Settings
from ..market_data.events import (
    AggTradeEvent,
    BookTickerEvent,
    KlineEvent,
    KlinePayload,
    MarkPriceEvent,
    ParsedEvent,
)
from ..market_data.order_book import LocalOrderBook
from ..scanner import population_zscore
from .models import FeatureSnapshot

ZERO = Decimal(0)
ONE_HUNDRED = Decimal(100)
TEN_THOUSAND = Decimal(10000)


@dataclass(frozen=True, slots=True)
class OpenInterestObservation:
    symbol: str
    timestamp: datetime
    value: Decimal


@dataclass(frozen=True, slots=True)
class SignedTrade:
    timestamp: datetime
    price: Decimal
    quantity: Decimal
    signed_quote: Decimal


@dataclass(slots=True)
class SymbolFeatureState:
    klines: deque[KlinePayload] = field(default_factory=lambda: deque(maxlen=2000))
    trades: deque[SignedTrade] = field(default_factory=lambda: deque(maxlen=100_000))
    open_interest: deque[OpenInterestObservation] = field(
        default_factory=lambda: deque(maxlen=2000)
    )
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    funding_rate: Decimal | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    last_price: Decimal | None = None
    last_timestamp: datetime | None = None
    anchor_time: datetime | None = None


class FeatureEngine:
    """Calculate deterministic snapshots without I/O or wall-clock reads."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._states: dict[str, SymbolFeatureState] = {}

    def state_for(self, symbol: str) -> SymbolFeatureState:
        return self._states.setdefault(symbol.upper(), SymbolFeatureState())

    def anchor(self, symbol: str, timestamp: datetime) -> None:
        self.state_for(symbol).anchor_time = timestamp.astimezone(UTC)

    def ingest(self, event: ParsedEvent) -> None:
        state = self.state_for(event.symbol)
        state.last_timestamp = event.event_time
        if isinstance(event, AggTradeEvent):
            quote = event.price * event.quantity
            state.trades.append(
                SignedTrade(
                    timestamp=event.event_time,
                    price=event.price,
                    quantity=event.quantity,
                    signed_quote=-quote if event.buyer_is_maker else quote,
                )
            )
            state.last_price = event.price
        elif isinstance(event, KlineEvent):
            if event.kline.is_closed:
                self._upsert_kline(state, event.kline)
                latest = state.klines[-1]
                state.last_price = latest.close_price
                state.last_timestamp = datetime.fromtimestamp(latest.close_time_ms / 1000, tz=UTC)
        elif isinstance(event, MarkPriceEvent):
            state.mark_price = event.mark_price
            state.index_price = event.index_price
            state.funding_rate = event.funding_rate
            if state.last_price is None:
                state.last_price = event.mark_price
        elif isinstance(event, BookTickerEvent):
            state.best_bid = event.best_bid_price
            state.best_ask = event.best_ask_price

    def ingest_open_interest(self, observation: OpenInterestObservation) -> None:
        if observation.timestamp.tzinfo is None:
            raise ValueError("Open-interest timestamps must be timezone-aware")
        state = self.state_for(observation.symbol)
        state.open_interest.append(observation)
        state.last_timestamp = observation.timestamp.astimezone(UTC)

    def seed_klines(self, symbol: str, klines: list[Kline]) -> None:
        """Warm a new candidate from documented REST Klines before live updates arrive."""
        state = self.state_for(symbol)
        seeded: list[KlinePayload] = []
        for item in sorted(klines, key=lambda value: value.open_time):
            start_ms = int(item.open_time.timestamp() * 1000)
            payload = KlinePayload(
                start_time_ms=start_ms,
                close_time_ms=int(item.close_time.timestamp() * 1000),
                symbol=symbol.upper(),
                interval="1m",
                first_trade_id=0,
                last_trade_id=0,
                open_price=item.open_price,
                close_price=item.close_price,
                high_price=item.high_price,
                low_price=item.low_price,
                base_volume=item.volume,
                trade_count=item.trade_count,
                is_closed=True,
                quote_volume=item.quote_volume,
                taker_buy_base_volume=item.taker_buy_base_volume,
                taker_buy_quote_volume=item.taker_buy_quote_volume,
            )
            seeded.append(payload)
        merged = {item.start_time_ms: item for item in state.klines}
        merged.update({item.start_time_ms: item for item in seeded})
        state.klines.clear()
        state.klines.extend(sorted(merged.values(), key=lambda item: item.start_time_ms))
        if state.klines:
            latest = state.klines[-1]
            state.last_price = latest.close_price
            state.last_timestamp = datetime.fromtimestamp(latest.close_time_ms / 1000, tz=UTC)

    def snapshot(
        self,
        symbol: str,
        *,
        timestamp: datetime | None = None,
        order_book: LocalOrderBook | None = None,
    ) -> FeatureSnapshot:
        normalized = symbol.upper()
        state = self.state_for(normalized)
        if state.last_price is None or state.last_timestamp is None:
            raise ValueError(f"No price observations for {normalized}")
        observed_at = (timestamp or state.last_timestamp).astimezone(UTC)
        self._prune(state, observed_at)
        returns = {minutes: self._return(state.klines, minutes) for minutes in (1, 3, 5, 15)}
        volume = self._volume_features(state.klines)
        cvd, cvd_slope = self._cvd(state.trades, observed_at)
        anchored_vwap = self._anchored_vwap(state)
        oi_change = self._open_interest_change(state.open_interest, observed_at)
        spread = self._spread_bps(state.best_bid, state.best_ask)
        imbalance_5 = self._imbalance(order_book, 5)
        imbalance_20 = self._imbalance(order_book, 20)
        btc_return = self._benchmark_return(normalized)
        return_5m = returns[5]
        residual = (
            return_5m - self._settings.feature_btc_beta * btc_return
            if return_5m is not None and btc_return is not None
            else None
        )
        distance = (
            (state.last_price / anchored_vwap - 1) * ONE_HUNDRED
            if anchored_vwap is not None and anchored_vwap > 0
            else None
        )
        basis = (
            (state.mark_price / state.index_price - 1) * ONE_HUNDRED
            if state.mark_price is not None
            and state.index_price is not None
            and state.index_price > 0
            else None
        )
        recent = list(state.klines)[-5:]
        prior = list(state.klines)[-10:-5]
        return FeatureSnapshot(
            symbol=normalized,
            timestamp=observed_at,
            price=state.last_price,
            return_1m_percent=returns[1],
            return_3m_percent=returns[3],
            return_5m_percent=return_5m,
            return_15m_percent=returns[15],
            quote_volume_1m=volume["quote_volume_1m"],
            quote_volume_5m=volume["quote_volume_5m"],
            previous_5m_average_quote_volume=volume["previous_average"],
            volume_zscore=volume["zscore"],
            taker_buy_ratio=volume["taker_buy_ratio"],
            cvd=cvd,
            cvd_slope=cvd_slope,
            anchored_vwap=anchored_vwap,
            distance_from_anchored_vwap_percent=distance,
            open_interest_change_5m_percent=oi_change,
            funding_rate=state.funding_rate,
            basis_percent=basis,
            spread_bps=spread,
            order_book_imbalance_5=imbalance_5,
            order_book_imbalance_20=imbalance_20,
            btc_return_5m_percent=btc_return,
            btc_residual_return_5m_percent=residual,
            price_high_5m=max((item.high_price for item in recent), default=None),
            prior_price_high_5m=max((item.high_price for item in prior), default=None),
        )

    def _return(self, klines: deque[KlinePayload], minutes: int) -> Decimal | None:
        if len(klines) <= minutes:
            return None
        previous = klines[-minutes - 1].close_price
        if previous <= 0:
            return None
        return (klines[-1].close_price / previous - 1) * ONE_HUNDRED

    def _volume_features(self, klines: deque[KlinePayload]) -> dict[str, Decimal | None]:
        rows = list(klines)
        required = (self._settings.feature_volume_baseline_windows + 1) * 5
        if len(rows) < 5:
            return {
                "quote_volume_1m": rows[-1].quote_volume if rows else None,
                "quote_volume_5m": None,
                "previous_average": None,
                "zscore": None,
                "taker_buy_ratio": None,
            }
        recent = rows[-5:]
        quote_volume = sum((item.quote_volume for item in recent), start=ZERO)
        taker_quote = sum((item.taker_buy_quote_volume for item in recent), start=ZERO)
        output: dict[str, Decimal | None] = {
            "quote_volume_1m": recent[-1].quote_volume,
            "quote_volume_5m": quote_volume,
            "previous_average": None,
            "zscore": None,
            "taker_buy_ratio": taker_quote / quote_volume if quote_volume > 0 else None,
        }
        if len(rows) < required:
            return output
        baseline_rows = rows[-required:-5]
        windows = [
            sum((item.quote_volume for item in baseline_rows[index : index + 5]), start=ZERO)
            for index in range(0, len(baseline_rows), 5)
        ]
        output["previous_average"] = sum(windows, start=ZERO) / Decimal(len(windows))
        output["zscore"] = population_zscore(quote_volume, windows)
        return output

    def _cvd(
        self, trades: deque[SignedTrade], timestamp: datetime
    ) -> tuple[Decimal | None, Decimal | None]:
        cutoff = timestamp - timedelta(seconds=self._settings.feature_cvd_window_seconds)
        selected = [trade for trade in trades if cutoff <= trade.timestamp <= timestamp]
        if not selected:
            return None, None
        cvd = sum((trade.signed_quote for trade in selected), start=ZERO)
        elapsed = Decimal(str((selected[-1].timestamp - selected[0].timestamp).total_seconds()))
        slope = cvd / elapsed if elapsed > 0 else ZERO
        return cvd, slope

    def _anchored_vwap(self, state: SymbolFeatureState) -> Decimal | None:
        if state.anchor_time is None:
            return None
        selected = [trade for trade in state.trades if trade.timestamp >= state.anchor_time]
        quantity = sum((trade.quantity for trade in selected), start=ZERO)
        if quantity <= 0:
            return None
        notional = sum((trade.price * trade.quantity for trade in selected), start=ZERO)
        return notional / quantity

    def _open_interest_change(
        self, observations: deque[OpenInterestObservation], timestamp: datetime
    ) -> Decimal | None:
        if not observations:
            return None
        latest = observations[-1]
        cutoff = timestamp - timedelta(seconds=self._settings.feature_oi_window_seconds)
        baseline = next((item for item in reversed(observations) if item.timestamp <= cutoff), None)
        if baseline is None or baseline.value <= 0:
            return None
        return (latest.value / baseline.value - 1) * ONE_HUNDRED

    @staticmethod
    def _spread_bps(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
        if bid is None or ask is None or bid <= 0 or ask < bid:
            return None
        midpoint = (bid + ask) / 2
        return (ask - bid) / midpoint * TEN_THOUSAND

    @staticmethod
    def _imbalance(book: LocalOrderBook | None, levels: int) -> Decimal | None:
        if book is None or not book.synchronized:
            return None
        bid_quantity = sum(
            (quantity for _, quantity in sorted(book.bids.items(), reverse=True)[:levels]),
            start=ZERO,
        )
        ask_quantity = sum(
            (quantity for _, quantity in sorted(book.asks.items())[:levels]), start=ZERO
        )
        total = bid_quantity + ask_quantity
        return (bid_quantity - ask_quantity) / total if total > 0 else None

    def _benchmark_return(self, symbol: str) -> Decimal | None:
        benchmark = self._settings.feature_benchmark_symbol
        if benchmark not in self._states:
            return None
        value = self._return(self._states[benchmark].klines, 5)
        if symbol == benchmark:
            return value
        return value

    @staticmethod
    def _upsert_kline(state: SymbolFeatureState, payload: KlinePayload) -> None:
        if not state.klines or state.klines[-1].start_time_ms < payload.start_time_ms:
            state.klines.append(payload)
            return
        merged = {item.start_time_ms: item for item in state.klines}
        merged[payload.start_time_ms] = payload
        state.klines.clear()
        state.klines.extend(sorted(merged.values(), key=lambda item: item.start_time_ms))

    def _prune(self, state: SymbolFeatureState, timestamp: datetime) -> None:
        trade_cutoff = timestamp - timedelta(
            seconds=max(self._settings.feature_cvd_window_seconds, 900)
        )
        while state.trades and state.trades[0].timestamp < trade_cutoff:
            state.trades.popleft()
        oi_cutoff = timestamp - timedelta(
            seconds=max(self._settings.feature_oi_window_seconds * 3, 900)
        )
        while state.open_interest and state.open_interest[0].timestamp < oi_cutoff:
            state.open_interest.popleft()
