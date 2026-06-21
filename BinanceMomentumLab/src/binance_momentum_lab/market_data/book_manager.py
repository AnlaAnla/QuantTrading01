"""Asynchronous REST-snapshot coordination for local order books."""

from __future__ import annotations

import asyncio
from typing import Protocol

from ..exceptions import OrderBookSequenceGapError
from .events import DepthEvent
from .health import MarketDataHealth
from .order_book import DepthSnapshot, LocalOrderBook


class SnapshotClient(Protocol):
    async def depth_snapshot(self, symbol: str, limit: int = 1000) -> DepthSnapshot: ...


class LocalOrderBookManager:
    def __init__(
        self, client: SnapshotClient, health: MarketDataHealth, depth_limit: int = 1000
    ) -> None:
        self._client = client
        self._health = health
        self._depth_limit = depth_limit
        self.books: dict[str, LocalOrderBook] = {}
        self._sync_tasks: dict[str, asyncio.Task[None]] = {}

    async def on_depth(self, event: DepthEvent) -> None:
        book = self.books.setdefault(event.symbol, LocalOrderBook(event.symbol))
        if book.synchronized:
            try:
                book.apply(event)
            except OrderBookSequenceGapError as exc:
                self._health.order_book_gap(event.symbol, str(exc))
                book.buffer(event)
                self._start_sync(event.symbol)
            return
        book.buffer(event)
        self._start_sync(event.symbol)

    async def stop(self) -> None:
        if self._sync_tasks:
            await asyncio.gather(*self._sync_tasks.values(), return_exceptions=True)

    def _start_sync(self, symbol: str) -> None:
        existing = self._sync_tasks.get(symbol)
        if existing is None or existing.done():
            self._sync_tasks[symbol] = asyncio.create_task(
                self._synchronize(symbol), name=f"order-book-sync-{symbol}"
            )

    async def _synchronize(self, symbol: str) -> None:
        book = self.books[symbol]
        snapshot = await self._client.depth_snapshot(symbol, self._depth_limit)
        try:
            book.initialize(snapshot)
        except OrderBookSequenceGapError as exc:
            self._health.order_book_gap(symbol, str(exc))
            return
        self._health.order_book_synced(symbol)
