"""Application runtime orchestration for periodic public-market scans."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime

from .binance.client import BinancePublicRESTClient
from .config import Settings
from .demo import DemoPublicMarketDataClient
from .scanner import Candidate, MarketScanner
from .storage import DuckDBStore

LOGGER = logging.getLogger(__name__)


class ScannerRuntime:
    """Own the public REST client and graceful periodic scanner task."""

    def __init__(
        self,
        settings: Settings,
        store: DuckDBStore,
        on_candidates: Callable[[list[Candidate]], Awaitable[None]] | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.client: BinancePublicRESTClient | DemoPublicMarketDataClient
        if settings.demo_data:
            self.client = DemoPublicMarketDataClient()
        else:
            self.client = BinancePublicRESTClient(
                str(settings.binance_mainnet_rest_url),
                timeout_seconds=settings.http_timeout_seconds,
                max_retries=settings.http_max_retries,
            )
        self.scanner = MarketScanner(self.client, settings)
        self.task: asyncio.Task[None] | None = None
        self.rest_status = "starting"
        self.last_error: str | None = None
        self.started_at = datetime.now(UTC)
        self._on_candidates = on_candidates

    def start(self) -> None:
        """Start the periodic scan loop without blocking API startup."""
        self.task = asyncio.create_task(self._run(), name="market-scanner")

    async def stop(self, *, close_client: bool = True) -> None:
        """Cancel work and close the HTTP pool gracefully."""
        if self.task is not None:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
        if close_client:
            await self.client.aclose()

    async def _run(self) -> None:
        while True:
            try:
                candidates = await self.scanner.scan_once()
                await asyncio.to_thread(self.store.replace_candidates, candidates)
                if self._on_candidates is not None:
                    await self._on_candidates(candidates)
                self.rest_status = "healthy"
                self.last_error = None
                await asyncio.to_thread(self.store.record_health, "rest", "healthy", None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.rest_status = "degraded"
                self.last_error = f"{type(exc).__name__}: {exc}"
                LOGGER.exception("market_scan_failed")
                await asyncio.to_thread(
                    self.store.record_health, "rest", "degraded", self.last_error
                )
            await asyncio.sleep(self.settings.scan_interval_seconds)
