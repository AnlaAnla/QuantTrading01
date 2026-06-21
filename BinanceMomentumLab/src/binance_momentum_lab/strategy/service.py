"""Runtime bridge from real-time events and public OI into features and state."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol

from ..binance.models import Kline, OpenInterest
from ..config import Settings
from ..market_data.events import KlineEvent, ParsedEnvelope
from ..market_data.order_book import LocalOrderBook
from ..storage import DuckDBStore
from .features import FeatureEngine, OpenInterestObservation
from .models import StrategySignal
from .state_machine import StrategyStateMachine

LOGGER = logging.getLogger(__name__)
SignalListener = Callable[[StrategySignal], Awaitable[None]]


class OpenInterestClient(Protocol):
    async def open_interest(self, symbol: str) -> OpenInterest: ...

    async def klines(self, symbol: str, interval: str, limit: int) -> list[Kline]: ...


class StrategyFeatureService:
    """Evaluate closed 1m bars and persist research outputs without execution."""

    def __init__(
        self,
        settings: Settings,
        client: OpenInterestClient,
        store: DuckDBStore,
        order_books: dict[str, LocalOrderBook],
    ) -> None:
        self._settings = settings
        self._client = client
        self._store = store
        self._order_books = order_books
        self.engine = FeatureEngine(settings)
        self.machine = StrategyStateMachine(settings)
        self._symbols: frozenset[str] = frozenset()
        self._oi_task: asyncio.Task[None] | None = None
        self.signals: list[StrategySignal] = []
        self._signal_listeners: list[SignalListener] = []

    def start(self) -> None:
        self._oi_task = asyncio.create_task(self._poll_open_interest(), name="open-interest-poller")

    async def stop(self) -> None:
        if self._oi_task is not None:
            self._oi_task.cancel()
            try:
                await self._oi_task
            except asyncio.CancelledError:
                pass

    async def update_symbols(self, symbols: frozenset[str]) -> None:
        added = symbols - self._symbols
        self._symbols = symbols
        if added:
            results = await asyncio.gather(
                *(self._seed_symbol(symbol) for symbol in sorted(added)),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    LOGGER.warning("feature_seed_failed error=%s", result)
        await self._poll_once()

    def subscribe_signal(self, listener: SignalListener) -> None:
        self._signal_listeners.append(listener)

    async def on_event(self, envelope: ParsedEnvelope) -> None:
        self.engine.ingest(envelope.event)
        event = envelope.event
        if (
            not isinstance(event, KlineEvent)
            or not event.kline.is_closed
            or event.symbol == self._settings.feature_benchmark_symbol
        ):
            return
        snapshot = self.engine.snapshot(
            event.symbol,
            timestamp=event.event_time,
            order_book=self._order_books.get(event.symbol),
        )
        result = self.machine.process(snapshot)
        if result.anchor_requested:
            self.engine.anchor(event.symbol, snapshot.timestamp)
        await asyncio.to_thread(self._store.save_feature_snapshot, snapshot)
        await asyncio.to_thread(
            self._store.save_strategy_state,
            snapshot.symbol,
            result.state,
            snapshot.timestamp,
        )
        if result.signal is not None:
            self.signals.append(result.signal)
            await asyncio.to_thread(self._store.save_signal, result.signal)
            for listener in self._signal_listeners:
                await listener(result.signal)

    async def _poll_open_interest(self) -> None:
        while True:
            await self._poll_once()
            await asyncio.sleep(self._settings.oi_poll_interval_seconds)

    async def _poll_once(self) -> None:
        results = await asyncio.gather(
            *(self._fetch_open_interest(symbol) for symbol in sorted(self._symbols)),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                LOGGER.warning("open_interest_poll_failed error=%s", result)

    async def _fetch_open_interest(self, symbol: str) -> None:
        result = await self._client.open_interest(symbol)
        self.engine.ingest_open_interest(
            OpenInterestObservation(result.symbol, result.timestamp, result.value)
        )

    async def _seed_symbol(self, symbol: str) -> None:
        limit = (self._settings.feature_volume_baseline_windows + 1) * 5 + 16
        klines = await self._client.klines(symbol, "1m", limit)
        self.engine.seed_klines(symbol, klines)
