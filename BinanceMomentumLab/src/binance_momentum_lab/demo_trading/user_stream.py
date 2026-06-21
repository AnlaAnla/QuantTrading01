"""Demo user-data WebSocket with listenKey keepalive, reconnect, and 24-hour rotation."""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, cast

from websockets.asyncio.client import connect

from ..config import DEMO_WS_URL, Settings
from ..exceptions import DemoEndpointViolationError
from .client import BinanceDemoTradingClient


class UserStreamConnection(Protocol):
    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


ConnectionFactory = Callable[[str, float, float], Awaitable[UserStreamConnection]]
EventHandler = Callable[[dict[str, Any]], Awaitable[None]]
Sleep = Callable[[float], Awaitable[None]]
LOGGER = logging.getLogger(__name__)


async def open_user_stream(
    url: str, ping_interval: float, ping_timeout: float
) -> UserStreamConnection:
    if not url.startswith(f"{DEMO_WS_URL}/private/ws/"):
        raise DemoEndpointViolationError("User data stream must use the demo private WebSocket")
    connection = await connect(
        url,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
        open_timeout=10,
        close_timeout=10,
        max_queue=256,
    )
    return cast(UserStreamConnection, connection)


class DemoUserDataStream:
    """Maintain the official listenKey stream without ever constructing a mainnet URL."""

    def __init__(
        self,
        settings: Settings,
        client: BinanceDemoTradingClient,
        handler: EventHandler,
        *,
        connection_factory: ConnectionFactory = open_user_stream,
        sleep: Sleep = asyncio.sleep,
        random_source: random.Random | None = None,
    ) -> None:
        base_url = settings.binance_demo_ws_url.rstrip("/")
        if base_url != DEMO_WS_URL:
            raise DemoEndpointViolationError(f"Demo WebSocket endpoint must be {DEMO_WS_URL}")
        self._base_url = base_url
        self._client = client
        self._handler = handler
        self._factory = connection_factory
        self._sleep = sleep
        self._random = random_source or random.Random()
        self._keepalive_seconds = settings.demo_user_stream_keepalive_seconds
        self._rotation_seconds = settings.ws_rotation_seconds
        self._reconnect_base = settings.ws_reconnect_base_seconds
        self._reconnect_max = settings.ws_reconnect_max_seconds
        self._ping_interval = settings.ws_ping_interval_seconds
        self._ping_timeout = settings.ws_ping_timeout_seconds
        self._listen_key: str | None = None
        self._connection: UserStreamConnection | None = None
        self._receive_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self.healthy = False
        self.last_error: str | None = "not_connected"

    @property
    def websocket_url(self) -> str | None:
        if self._listen_key is None:
            return None
        return f"{self._base_url}/private/ws/{self._listen_key}"

    async def start(self) -> None:
        self._listen_key = await self._client.start_user_stream()
        self._receive_task = asyncio.create_task(self._run(), name="demo-user-stream")
        self._keepalive_task = asyncio.create_task(
            self._keepalive(), name="demo-user-stream-keepalive"
        )

    async def stop(self) -> None:
        for task in (self._receive_task, self._keepalive_task):
            if task is not None:
                task.cancel()
        for task in (self._receive_task, self._keepalive_task):
            if task is not None:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await self._close_connection()
        self.healthy = False
        if self._listen_key is not None:
            await self._client.close_user_stream()
            self._listen_key = None

    async def _run(self) -> None:
        attempt = 0
        while True:
            try:
                url = self.websocket_url
                if url is None:
                    raise RuntimeError("Demo listenKey is not initialized")
                self._connection = await self._factory(url, self._ping_interval, self._ping_timeout)
                self.healthy = True
                self.last_error = None
                attempt = 0
                async with asyncio.timeout(self._rotation_seconds):
                    await self._receive(self._connection)
                await self._close_connection()
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                self.healthy = False
                self.last_error = "proactive_24h_rotation"
                await self._close_connection()
            except Exception as exc:
                self.healthy = False
                self.last_error = type(exc).__name__
                LOGGER.warning("demo_user_stream_reconnecting error_type=%s", type(exc).__name__)
                await self._close_connection()
                delay = min(self._reconnect_base * (2**attempt), self._reconnect_max)
                attempt += 1
                await self._sleep(delay + self._random.uniform(0, delay * 0.25))

    async def _receive(self, connection: UserStreamConnection) -> None:
        while True:
            raw = await connection.recv()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("Expected object from demo user data stream")
            if payload.get("e") == "listenKeyExpired":
                self.healthy = False
                self.last_error = "listen_key_expired"
                self._listen_key = await self._client.start_user_stream()
                return
            await self._handler(payload)

    async def _keepalive(self) -> None:
        while True:
            await self._sleep(self._keepalive_seconds)
            try:
                await self._client.keepalive_user_stream()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.healthy = False
                self.last_error = f"keepalive_{type(exc).__name__}"
                LOGGER.warning(
                    "demo_user_stream_keepalive_failed error_type=%s", type(exc).__name__
                )

    async def _close_connection(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
