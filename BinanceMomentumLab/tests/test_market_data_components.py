from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from binance_momentum_lab.market_data.book_manager import LocalOrderBookManager
from binance_momentum_lab.market_data.candidates import DynamicCandidateManager
from binance_momentum_lab.market_data.deduplication import BoundedDeduplicator
from binance_momentum_lab.market_data.events import parse_combined_message
from binance_momentum_lab.market_data.health import MarketDataHealth
from binance_momentum_lab.market_data.order_book import DepthSnapshot
from binance_momentum_lab.market_data.parquet import ParquetEventWriter, RawEvent
from binance_momentum_lab.market_data.routes import StreamRoute
from tests.test_market_events import payloads
from tests.test_order_book import depth_event


@pytest.mark.asyncio
async def test_candidate_manager_notifies_only_on_normalized_change() -> None:
    manager = DynamicCandidateManager()
    observations: list[frozenset[str]] = []

    async def listener(symbols: frozenset[str]) -> None:
        observations.append(symbols)

    manager.subscribe(listener)
    assert await manager.update(["btcusdt", "ETHUSDT"])
    assert not await manager.update(["ETHUSDT", "BTCUSDT"])
    assert observations == [frozenset({"BTCUSDT", "ETHUSDT"})]


def test_bounded_duplicate_suppression_evicts_oldest() -> None:
    deduplicator = BoundedDeduplicator(capacity=2)
    assert deduplicator.accept("a")
    assert not deduplicator.accept("a")
    assert deduplicator.accept("b")
    assert deduplicator.accept("c")
    assert deduplicator.accept("a")


def test_latency_and_out_of_order_events_degrade_health() -> None:
    health = MarketDataHealth(stale_after_seconds=3)
    now = datetime.now(UTC)
    health.connected(StreamRoute.MARKET)
    health.observe(
        StreamRoute.MARKET,
        "btcusdt@aggTrade",
        int((now - timedelta(seconds=4)).timestamp() * 1000),
        now,
    )
    route = health.snapshot()["routes"]
    assert isinstance(route, dict)
    assert route["market"]["status"] == "degraded"

    health.observe(StreamRoute.MARKET, "btcusdt@aggTrade", 200, now)
    health.observe(StreamRoute.MARKET, "btcusdt@aggTrade", 100, now)
    route = health.snapshot()["routes"]
    assert isinstance(route, dict)
    assert "out_of_order" in route["market"]["detail"]


def test_connected_but_silent_route_becomes_degraded() -> None:
    health = MarketDataHealth(stale_after_seconds=3)
    health.connected(StreamRoute.PUBLIC)

    health.evaluate_staleness(datetime.now(UTC) + timedelta(seconds=4))

    route = health.snapshot()["routes"]
    assert isinstance(route, dict)
    assert route["public"]["status"] == "degraded"
    assert "silent" in route["public"]["detail"]


@pytest.mark.asyncio
async def test_order_book_sequence_gap_updates_health() -> None:
    class SnapshotClient:
        async def depth_snapshot(self, symbol: str, limit: int = 1000) -> DepthSnapshot:
            del symbol, limit
            return DepthSnapshot(
                last_update_id=100,
                bids=(),
                asks=(),
            )

    health = MarketDataHealth(stale_after_seconds=3)
    manager = LocalOrderBookManager(SnapshotClient(), health)
    await manager.on_depth(depth_event(U=99, u=102, pu=98))
    await manager.stop()
    assert manager.books["BTCUSDT"].synchronized

    await manager.on_depth(depth_event(U=103, u=104, pu=999))
    await manager.stop()

    order_books = health.snapshot()["order_books"]
    assert isinstance(order_books, dict)
    assert order_books["BTCUSDT"]["status"] == "degraded"


@pytest.mark.asyncio
async def test_raw_events_are_written_to_partitioned_parquet(tmp_path: Path) -> None:
    envelope = parse_combined_message(payloads()["aggTrade"], StreamRoute.MARKET)
    writer = ParquetEventWriter(tmp_path, batch_size=1, flush_seconds=60)
    writer.start()
    await writer.write(RawEvent.from_envelope(envelope, datetime.now(UTC)))
    await writer.stop()

    files = await asyncio.to_thread(lambda: list(tmp_path.glob("date=*/symbol=BTCUSDT/*.parquet")))
    assert len(files) == 1
    row = (
        duckdb.connect()
        .execute("SELECT symbol, route FROM read_parquet(?)", [str(files[0])])
        .fetchone()
    )
    assert row == ("BTCUSDT", "market")
