"""Async public REST client for Binance USDⓈ-M futures."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any, Protocol, cast

import httpx

from ..exceptions import BinanceAPIError, BinanceRateLimitError
from ..market_data.order_book import DepthSnapshot
from .models import ExchangeInfo, Kline, OpenInterest, Ticker24h

Sleep = Callable[[float], Awaitable[None]]


class PublicMarketDataClient(Protocol):
    """Injectable scanner dependency exposing only public market data."""

    async def exchange_info(self) -> ExchangeInfo:
        """Fetch current exchange metadata."""
        ...

    async def tickers_24h(self) -> list[Ticker24h]:
        """Fetch all 24-hour tickers."""
        ...

    async def klines(self, symbol: str, interval: str, limit: int) -> list[Kline]:
        """Fetch candlesticks for one symbol."""
        ...

    async def depth_snapshot(self, symbol: str, limit: int = 1000) -> DepthSnapshot:
        """Fetch a REST order-book snapshot for local depth alignment."""
        ...


class BinancePublicRESTClient:
    """Rate-limit-aware adapter around documented USDⓈ-M public endpoints."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 10,
        max_retries: int = 3,
        client: httpx.AsyncClient | None = None,
        sleep: Sleep = asyncio.sleep,
        random_source: random.Random | None = None,
    ) -> None:
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
            headers={"User-Agent": "BinanceMomentumLab/0.1"},
        )
        self._max_retries = max_retries
        self._sleep = sleep
        self._random = random_source or random.Random()
        self.used_weight: int | None = None

    async def __aenter__(self) -> BinancePublicRESTClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close an internally owned HTTP session."""
        if self._owns_client:
            await self._client.aclose()

    async def exchange_info(self) -> ExchangeInfo:
        """Fetch GET /fapi/v1/exchangeInfo."""
        payload = await self._get_json("/fapi/v1/exchangeInfo")
        return ExchangeInfo.model_validate(payload)

    async def tickers_24h(self) -> list[Ticker24h]:
        """Fetch GET /fapi/v1/ticker/24hr without a symbol."""
        payload = await self._get_json("/fapi/v1/ticker/24hr")
        if not isinstance(payload, list):
            raise BinanceAPIError("Expected a list from 24hr ticker endpoint")
        return [Ticker24h.model_validate(item) for item in payload]

    async def klines(self, symbol: str, interval: str = "1m", limit: int = 305) -> list[Kline]:
        """Fetch GET /fapi/v1/klines for one symbol."""
        payload = await self._get_json(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )
        if not isinstance(payload, list):
            raise BinanceAPIError("Expected a list from kline endpoint")
        return [Kline.from_payload(cast(list[Any], row)) for row in payload]

    async def depth_snapshot(self, symbol: str, limit: int = 1000) -> DepthSnapshot:
        """Fetch GET /fapi/v1/depth for local order-book initialization."""
        payload = await self._get_json("/fapi/v1/depth", params={"symbol": symbol, "limit": limit})
        if not isinstance(payload, dict):
            raise BinanceAPIError("Expected an object from depth endpoint")
        try:
            return DepthSnapshot(
                last_update_id=int(payload["lastUpdateId"]),
                bids=tuple(
                    (Decimal(str(level[0])), Decimal(str(level[1]))) for level in payload["bids"]
                ),
                asks=tuple(
                    (Decimal(str(level[0])), Decimal(str(level[1]))) for level in payload["asks"]
                ),
            )
        except (KeyError, TypeError, ValueError, IndexError) as exc:
            raise BinanceAPIError("Invalid depth snapshot payload") from exc

    async def open_interest(self, symbol: str) -> OpenInterest:
        """Fetch GET /fapi/v1/openInterest (request weight 1)."""
        payload = await self._get_json("/fapi/v1/openInterest", params={"symbol": symbol})
        if not isinstance(payload, dict):
            raise BinanceAPIError("Expected an object from open-interest endpoint")
        return OpenInterest.model_validate(payload)

    async def _get_json(
        self,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        for attempt in range(self._max_retries + 1):
            response = await self._client.get(path, params=params)
            self._capture_used_weight(response.headers)

            if response.status_code in {418, 429}:
                if attempt >= self._max_retries:
                    raise BinanceRateLimitError(
                        f"Binance rate limit HTTP {response.status_code} after retries"
                    )
                await self._sleep(self._retry_delay(response, attempt))
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise BinanceAPIError(
                    f"Binance public API HTTP {response.status_code} at {path}"
                ) from exc
            return response.json()
        raise AssertionError("retry loop exhausted without returning or raising")

    def _capture_used_weight(self, headers: httpx.Headers) -> None:
        for name, value in headers.items():
            if name.lower().startswith("x-mbx-used-weight"):
                try:
                    self.used_weight = int(value)
                except ValueError:
                    self.used_weight = None
                return

    def _retry_delay(self, response: httpx.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
        exponential = min(2**attempt, 30)
        return float(exponential + self._random.uniform(0, exponential * 0.25))
