"""Dynamic routed WebSocket supervisor and event processing pipeline."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from ..binance.client import PublicMarketDataClient
from ..config import Settings
from ..exceptions import WebSocketProtocolError
from ..storage import DuckDBStore
from .book_manager import LocalOrderBookManager
from .deduplication import BoundedDeduplicator
from .events import DepthEvent, ParsedEnvelope, event_identity, parse_combined_message
from .health import MarketDataHealth
from .parquet import ParquetEventWriter, RawEvent
from .routes import StreamRoute, combined_stream_url, streams_for_symbols
from .websocket import (
    ConnectionFactory,
    RawWebSocketMessage,
    RoutedWebSocketSession,
    open_websocket,
)

LOGGER = logging.getLogger(__name__)
EventListener = Callable[[ParsedEnvelope], Awaitable[None]]


class RealtimeMarketDataService:
    """Own sessions, parsing, deduplication, books, health, and raw persistence."""

    def __init__(
        self,
        settings: Settings,
        snapshot_client: PublicMarketDataClient,
        store: DuckDBStore,
        *,
        connection_factory: ConnectionFactory = open_websocket,
    ) -> None:
        self._settings = settings
        self._store = store
        self._connection_factory = connection_factory
        self._queue: asyncio.Queue[RawWebSocketMessage | None] = asyncio.Queue(
            maxsize=settings.ws_queue_maxsize
        )
        self.health = MarketDataHealth(settings.ws_stale_after_seconds)
        self.books = LocalOrderBookManager(
            snapshot_client, self.health, settings.order_book_depth_limit
        )
        self.writer = ParquetEventWriter(
            settings.parquet_root,
            settings.parquet_batch_size,
            settings.parquet_flush_seconds,
        )
        self._deduplicator = BoundedDeduplicator()
        self._sessions: dict[StreamRoute, RoutedWebSocketSession] = {}
        self._consumer: asyncio.Task[None] | None = None
        self._reporter: asyncio.Task[None] | None = None
        self._listeners: list[EventListener] = []
        self._update_lock = asyncio.Lock()

    def start(self) -> None:
        self.writer.start()
        self._consumer = asyncio.create_task(self._consume(), name="market-event-consumer")
        self._reporter = asyncio.create_task(self._report_health(), name="ws-health-reporter")

    def subscribe(self, listener: EventListener) -> None:
        self._listeners.append(listener)

    async def update_symbols(self, symbols: frozenset[str]) -> None:
        """Rebuild routed sessions only when the candidate manager reports a change."""
        async with self._update_lock:
            old_sessions = tuple(self._sessions.values())
            self._sessions.clear()
            await asyncio.gather(*(session.stop() for session in old_sessions))
            if not symbols:
                return
            partitioned = streams_for_symbols(set(symbols))
            for route, streams in partitioned.items():
                url = combined_stream_url(self._settings.binance_mainnet_ws_url, route, streams)
                session = RoutedWebSocketSession(
                    route,
                    url,
                    self._queue,
                    self.health,
                    rotation_seconds=self._settings.ws_rotation_seconds,
                    reconnect_base_seconds=self._settings.ws_reconnect_base_seconds,
                    reconnect_max_seconds=self._settings.ws_reconnect_max_seconds,
                    ping_interval_seconds=self._settings.ws_ping_interval_seconds,
                    ping_timeout_seconds=self._settings.ws_ping_timeout_seconds,
                    connection_factory=self._connection_factory,
                )
                self._sessions[route] = session
                session.start()

    async def stop(self) -> None:
        await asyncio.gather(*(session.stop() for session in self._sessions.values()))
        self._sessions.clear()
        await self.books.stop()
        await self._queue.put(None)
        if self._consumer is not None:
            await self._consumer
        if self._reporter is not None:
            self._reporter.cancel()
            try:
                await self._reporter
            except asyncio.CancelledError:
                pass
        await self.writer.stop()

    async def _consume(self) -> None:
        while True:
            message = await self._queue.get()
            if message is None:
                return
            try:
                envelope = parse_combined_message(message.payload, message.route)
            except WebSocketProtocolError:
                LOGGER.exception("invalid_websocket_message route=%s", message.route)
                continue
            if not self._deduplicator.accept(event_identity(envelope)):
                continue
            self.health.observe(
                message.route,
                envelope.stream,
                envelope.event.event_time_ms,
                message.received_at,
            )
            await self.writer.write(RawEvent.from_envelope(envelope, message.received_at))
            if isinstance(envelope.event, DepthEvent):
                await self.books.on_depth(envelope.event)
            for listener in self._listeners:
                await listener(envelope)

    async def _report_health(self) -> None:
        while True:
            self.health.evaluate_staleness()
            snapshot = self.health.snapshot()
            routes = snapshot["routes"]
            order_books = snapshot["order_books"]
            if isinstance(routes, dict):
                for route, value in routes.items():
                    if isinstance(value, dict):
                        await asyncio.to_thread(
                            self._store.record_health,
                            f"websocket:{route}",
                            str(value["status"]),
                            value.get("detail"),
                        )
            if isinstance(order_books, dict):
                for symbol, value in order_books.items():
                    if isinstance(value, dict):
                        await asyncio.to_thread(
                            self._store.record_health,
                            f"order_book:{symbol}",
                            str(value["status"]),
                            value.get("detail"),
                        )
            await asyncio.sleep(1)
