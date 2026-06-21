"""Application runtime orchestration for periodic public-market scans."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime

from .binance.client import BinancePublicRESTClient
from .config import Settings
from .demo import DemoPublicMarketDataClient
from .scanner import MarketScanner
from .storage import DuckDBStore

LOGGER = logging.getLogger(__name__)


class ScannerRuntime:
    """Own the public REST client and graceful periodic scanner task."""

    def __init__(self, settings: Settings, store: DuckDBStore) -> None:
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

    def start(self) -> None:
        """Start the periodic scan loop without blocking API startup."""
        self.task = asyncio.create_task(self._run(), name="market-scanner")

    async def stop(self) -> None:
        """Cancel work and close the HTTP pool gracefully."""
        if self.task is not None:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
        await self.client.aclose()

    async def _run(self) -> None:
        while True:
            try:
                candidates = await self.scanner.scan_once()
                await asyncio.to_thread(self.store.replace_candidates, candidates)
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
