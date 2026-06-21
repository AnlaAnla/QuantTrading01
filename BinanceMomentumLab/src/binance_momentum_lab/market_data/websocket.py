"""Routed WebSocket sessions with rotation, reconnect, and queue backpressure."""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from websockets.asyncio.client import connect

from .health import MarketDataHealth
from .routes import StreamRoute

LOGGER = logging.getLogger(__name__)


class WebSocketConnection(Protocol):
    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


ConnectionFactory = Callable[[str, float, float, int], Awaitable[WebSocketConnection]]
Sleep = Callable[[float], Awaitable[None]]


async def open_websocket(
    url: str, ping_interval: float, ping_timeout: float, max_queue: int
) -> WebSocketConnection:
    """Open a connection whose protocol automatically answers server ping frames with pong.

    A low-frequency client ping additionally verifies the reverse path. Binance's mandatory
    24-hour disconnect is handled independently by proactive session rotation.
    """
    connection = await connect(
        url,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
        max_queue=max_queue,
        open_timeout=10,
        close_timeout=10,
    )
    return cast(WebSocketConnection, connection)


@dataclass(frozen=True, slots=True)
class RawWebSocketMessage:
    route: StreamRoute
    payload: dict[str, Any]
    received_at: datetime


class RoutedWebSocketSession:
    """Maintain one routed combined-stream connection until stopped."""

    def __init__(
        self,
        route: StreamRoute,
        url: str,
        output: asyncio.Queue[RawWebSocketMessage | None],
        health: MarketDataHealth,
        *,
        rotation_seconds: float,
        reconnect_base_seconds: float,
        reconnect_max_seconds: float,
        ping_interval_seconds: float,
        ping_timeout_seconds: float,
        connection_factory: ConnectionFactory = open_websocket,
        sleep: Sleep = asyncio.sleep,
        random_source: random.Random | None = None,
    ) -> None:
        self.route = route
        self.url = url
        self._output = output
        self._health = health
        self._rotation_seconds = rotation_seconds
        self._reconnect_base = reconnect_base_seconds
        self._reconnect_max = reconnect_max_seconds
        self._ping_interval = ping_interval_seconds
        self._ping_timeout = ping_timeout_seconds
        self._factory = connection_factory
        self._sleep = sleep
        self._random = random_source or random.Random()
        self._task: asyncio.Task[None] | None = None
        self._connection: WebSocketConnection | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"ws-{self.route.value}")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._connection is not None:
            await self._connection.close()

    async def _run(self) -> None:
        attempt = 0
        while True:
            try:
                self._connection = await self._factory(
                    self.url,
                    self._ping_interval,
                    self._ping_timeout,
                    16,
                )
                self._health.connected(self.route)
                attempt = 0
                async with asyncio.timeout(self._rotation_seconds):
                    await self._receive_forever(self._connection)
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                self._health.disconnected(self.route, "proactive_24h_rotation")
                await self._close_current()
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                self._health.disconnected(self.route, detail)
                LOGGER.warning("websocket_reconnecting route=%s error=%s", self.route, detail)
                await self._close_current()
                delay = min(self._reconnect_base * (2**attempt), self._reconnect_max)
                jitter = self._random.uniform(0, delay * 0.25)
                attempt += 1
                await self._sleep(delay + jitter)

    async def _receive_forever(self, connection: WebSocketConnection) -> None:
        while True:
            raw = await connection.recv()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("Expected JSON object from combined stream")
            await self._output.put(
                RawWebSocketMessage(
                    route=self.route,
                    payload=payload,
                    received_at=datetime.now(UTC),
                )
            )

    async def _close_current(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
