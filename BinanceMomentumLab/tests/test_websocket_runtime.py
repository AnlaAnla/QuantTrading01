from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from binance_momentum_lab.config import Settings
from binance_momentum_lab.demo import DemoPublicMarketDataClient
from binance_momentum_lab.market_data.health import MarketDataHealth
from binance_momentum_lab.market_data.routes import StreamRoute
from binance_momentum_lab.market_data.service import RealtimeMarketDataService
from binance_momentum_lab.market_data.websocket import (
    RawWebSocketMessage,
    RoutedWebSocketSession,
    WebSocketConnection,
)
from binance_momentum_lab.storage import DuckDBStore
from tests.test_market_events import payloads


class BlockingConnection:
    def __init__(self) -> None:
        self.closed = False

    async def recv(self) -> str:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        self.closed = True


class OneMessageConnection:
    def __init__(self) -> None:
        self.sent = False

    async def recv(self) -> str:
        if not self.sent:
            self.sent = True
            return json.dumps(payloads()["aggTrade"])
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        return None


def make_session(
    factory: object,
    output: asyncio.Queue[RawWebSocketMessage | None],
    health: MarketDataHealth,
    *,
    rotation: float = 60,
    sleep: object = asyncio.sleep,
) -> RoutedWebSocketSession:
    return RoutedWebSocketSession(
        StreamRoute.MARKET,
        "wss://example.test/market/stream?streams=btcusdt@aggTrade",
        output,
        health,
        rotation_seconds=rotation,
        reconnect_base_seconds=0.001,
        reconnect_max_seconds=0.01,
        ping_interval_seconds=180,
        ping_timeout_seconds=600,
        connection_factory=factory,  # type: ignore[arg-type]
        sleep=sleep,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_receives_combined_message_with_bounded_queue() -> None:
    output: asyncio.Queue[RawWebSocketMessage | None] = asyncio.Queue(maxsize=1)

    async def factory(
        _url: str, _ping_interval: float, _ping_timeout: float, _max_queue: int
    ) -> WebSocketConnection:
        return OneMessageConnection()

    session = make_session(factory, output, MarketDataHealth(3))
    session.start()
    message = await asyncio.wait_for(output.get(), timeout=1)
    await session.stop()

    assert message is not None
    assert message.route is StreamRoute.MARKET
    assert message.payload["stream"] == "btcusdt@aggTrade"


@pytest.mark.asyncio
async def test_connection_rotates_before_24_hours_and_preserves_ping_settings() -> None:
    output: asyncio.Queue[RawWebSocketMessage | None] = asyncio.Queue()
    calls: list[tuple[float, float]] = []
    connections: list[BlockingConnection] = []

    async def factory(
        _url: str, ping_interval: float, ping_timeout: float, _max_queue: int
    ) -> WebSocketConnection:
        calls.append((ping_interval, ping_timeout))
        connection = BlockingConnection()
        connections.append(connection)
        return connection

    session = make_session(factory, output, MarketDataHealth(3), rotation=0.02)
    session.start()
    await asyncio.sleep(0.055)
    await session.stop()

    assert len(connections) >= 2
    assert all(connection.closed for connection in connections)
    assert calls[0] == (180, 600)


@pytest.mark.asyncio
async def test_reconnect_uses_backoff_after_connection_failure() -> None:
    output: asyncio.Queue[RawWebSocketMessage | None] = asyncio.Queue()
    attempts = 0
    delays: list[float] = []
    reconnected = asyncio.Event()

    async def factory(
        _url: str, _ping_interval: float, _ping_timeout: float, _max_queue: int
    ) -> WebSocketConnection:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("offline")
        reconnected.set()
        return BlockingConnection()

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    session = make_session(factory, output, MarketDataHealth(3), sleep=fake_sleep)
    session.start()
    await asyncio.wait_for(reconnected.wait(), timeout=1)
    await session.stop()

    assert delays and delays[0] >= 0.001


@pytest.mark.asyncio
async def test_service_opens_separate_routes_when_candidates_change(tmp_path: Path) -> None:
    urls: list[str] = []
    both_connected = asyncio.Event()

    async def factory(
        url: str, _ping_interval: float, _ping_timeout: float, _max_queue: int
    ) -> WebSocketConnection:
        urls.append(url)
        if len(urls) >= 2:
            both_connected.set()
        return BlockingConnection()

    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "service.duckdb",
        parquet_root=tmp_path / "raw",
    )
    store = DuckDBStore(settings.database_path)
    store.initialize()
    service = RealtimeMarketDataService(
        settings, DemoPublicMarketDataClient(), store, connection_factory=factory
    )
    service.start()
    await service.update_symbols(frozenset({"BTCUSDT"}))
    await asyncio.wait_for(both_connected.wait(), timeout=1)
    await service.stop()
    store.close()

    assert any("/market/stream?streams=" in url and "@aggTrade" in url for url in urls)
    assert any("/public/stream?streams=" in url and "@depth@100ms" in url for url in urls)
