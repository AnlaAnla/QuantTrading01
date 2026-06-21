"""Local order book synchronized by REST snapshot and diff-depth events."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ..exceptions import OrderBookNotSynchronizedError, OrderBookSequenceGapError
from .events import DepthEvent


@dataclass(frozen=True, slots=True)
class DepthSnapshot:
    last_update_id: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]


@dataclass(slots=True)
class LocalOrderBook:
    """One symbol's absolute-quantity local order book."""

    symbol: str
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_update_id: int | None = None
    synchronized: bool = False
    _buffer: list[DepthEvent] = field(default_factory=list)

    def buffer(self, event: DepthEvent) -> None:
        """Buffer updates received before the REST snapshot is installed."""
        if event.symbol != self.symbol:
            raise ValueError(f"Expected {self.symbol}, received {event.symbol}")
        self._buffer.append(event)

    def initialize(self, snapshot: DepthSnapshot) -> None:
        """Align buffered updates with lastUpdateId exactly as documented by Binance."""
        self.bids = {price: qty for price, qty in snapshot.bids if qty != 0}
        self.asks = {price: qty for price, qty in snapshot.asks if qty != 0}
        relevant = [
            event for event in self._buffer if event.final_update_id >= snapshot.last_update_id
        ]
        first_index = next(
            (
                index
                for index, event in enumerate(relevant)
                if event.first_update_id <= snapshot.last_update_id <= event.final_update_id
            ),
            None,
        )
        if first_index is None:
            self.reset()
            raise OrderBookSequenceGapError(
                f"No buffered event bridges snapshot lastUpdateId={snapshot.last_update_id}"
            )
        self.last_update_id = snapshot.last_update_id
        self.synchronized = True
        try:
            for offset, event in enumerate(relevant[first_index:]):
                self.apply(event, first_event=offset == 0)
        except OrderBookSequenceGapError:
            self.reset()
            raise
        self._buffer.clear()

    def apply(self, event: DepthEvent, *, first_event: bool = False) -> None:
        """Apply absolute level quantities and enforce the `pu == previous u` invariant."""
        if not self.synchronized or self.last_update_id is None:
            raise OrderBookNotSynchronizedError(f"{self.symbol} order book is not synchronized")
        if event.final_update_id < self.last_update_id:
            return
        if not first_event and event.previous_final_update_id != self.last_update_id:
            self.synchronized = False
            raise OrderBookSequenceGapError(
                f"{self.symbol} expected pu={self.last_update_id}, "
                f"got {event.previous_final_update_id}"
            )
        self._apply_levels(self.bids, event.bids)
        self._apply_levels(self.asks, event.asks)
        self.last_update_id = event.final_update_id

    def best_bid(self) -> tuple[Decimal, Decimal]:
        if not self.synchronized or not self.bids:
            raise OrderBookNotSynchronizedError(f"{self.symbol} has no synchronized bids")
        price = max(self.bids)
        return price, self.bids[price]

    def best_ask(self) -> tuple[Decimal, Decimal]:
        if not self.synchronized or not self.asks:
            raise OrderBookNotSynchronizedError(f"{self.symbol} has no synchronized asks")
        price = min(self.asks)
        return price, self.asks[price]

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None
        self.synchronized = False
        self._buffer.clear()

    @staticmethod
    def _apply_levels(side: dict[Decimal, Decimal], levels: list[tuple[Decimal, Decimal]]) -> None:
        for price, quantity in levels:
            if quantity == 0:
                side.pop(price, None)
            else:
                side[price] = quantity
