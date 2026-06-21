"""Authenticated Binance USDⓈ-M Demo REST client with fail-closed endpoint guards."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from decimal import Decimal
from typing import Any, cast
from urllib.parse import urlencode

import httpx

from ..binance.signing import hmac_sha256
from ..config import DEMO_REST_URL, Settings
from ..exceptions import (
    BinanceAPIError,
    BinanceRateLimitError,
    DemoEndpointViolationError,
    DemoExecutionStatusUnknownError,
    DemoOrderValidationError,
)
from .models import (
    DemoBalance,
    DemoOrder,
    DemoOrderRequest,
    DemoOrderType,
    DemoPosition,
    SymbolRules,
)

NowMilliseconds = Callable[[], int]


class BinanceDemoTradingClient:
    """Minimal authenticated adapter that can never target the production trading host."""

    def __init__(
        self,
        settings: Settings,
        *,
        client: httpx.AsyncClient | None = None,
        now_ms: NowMilliseconds | None = None,
    ) -> None:
        base_url = str(settings.binance_demo_rest_url).rstrip("/")
        if base_url != DEMO_REST_URL:
            raise DemoEndpointViolationError(f"Demo REST endpoint must be {DEMO_REST_URL}")
        if client is not None and str(client.base_url).rstrip("/") != DEMO_REST_URL:
            raise DemoEndpointViolationError("Injected HTTP client must use the demo REST endpoint")
        self._api_key = settings.binance_api_key
        self._api_secret = settings.binance_api_secret
        self._recv_window_ms = settings.demo_recv_window_ms
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=DEMO_REST_URL,
            timeout=settings.http_timeout_seconds,
            headers={"User-Agent": "BinanceMomentumLab/0.1-demo"},
        )
        self._now_ms = now_ms or (lambda: time.time_ns() // 1_000_000)
        self._time_offset_ms = 0
        self._rules: dict[str, SymbolRules] = {}
        self._order_locks: dict[str, asyncio.Lock] = {}

    @property
    def time_offset_ms(self) -> int:
        return self._time_offset_ms

    async def start(self) -> None:
        """Synchronize server time and cache authoritative exchange filters."""
        await self.sync_server_time()
        await self.refresh_exchange_info()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def sync_server_time(self) -> int:
        before = self._now_ms()
        payload = await self._public_request("GET", "/fapi/v1/time")
        after = self._now_ms()
        if not isinstance(payload, dict) or "serverTime" not in payload:
            raise BinanceAPIError("Invalid demo server-time response")
        midpoint = before + (after - before) // 2
        self._time_offset_ms = int(payload["serverTime"]) - midpoint
        return self._time_offset_ms

    async def refresh_exchange_info(self) -> dict[str, SymbolRules]:
        payload = await self._public_request("GET", "/fapi/v1/exchangeInfo")
        if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
            raise BinanceAPIError("Invalid demo exchangeInfo response")
        rules: dict[str, SymbolRules] = {}
        for item in payload["symbols"]:
            if not isinstance(item, dict) or item.get("status") != "TRADING":
                continue
            parsed = SymbolRules.from_exchange_symbol(item)
            rules[parsed.symbol] = parsed
        self._rules = rules
        return dict(rules)

    def rules_for(self, symbol: str) -> SymbolRules:
        try:
            return self._rules[symbol.upper()]
        except KeyError as exc:
            raise DemoOrderValidationError(f"No active exchangeInfo rules for {symbol}") from exc

    async def place_order(self, request: DemoOrderRequest) -> DemoOrder:
        """Place idempotently; a 503 unknown response is resolved by query, never resubmission."""
        symbol = request.symbol.upper()
        client_order_id = request.client_order_id
        lock = self._order_locks.setdefault(client_order_id, asyncio.Lock())
        async with lock:
            existing = await self.query_order(symbol, client_order_id)
            if existing is not None:
                return existing
            params = self._normalized_order_params(request, symbol, client_order_id)
            response = await self._signed_response("POST", "/fapi/v1/order", params)
            if response.status_code == 503 and _execution_unknown(response):
                recovered = await self.query_order(symbol, client_order_id)
                if recovered is not None:
                    return recovered
                raise DemoExecutionStatusUnknownError(
                    f"Execution status remains unknown for client order {client_order_id}"
                )
            payload = self._decode_success(response, "/fapi/v1/order")
            return DemoOrder.model_validate(payload)

    async def query_order(self, symbol: str, client_order_id: str) -> DemoOrder | None:
        response = await self._signed_response(
            "GET",
            "/fapi/v1/order",
            {"symbol": symbol.upper(), "origClientOrderId": client_order_id},
        )
        if response.status_code >= 400 and _error_code(response) == -2013:
            return None
        payload = self._decode_success(response, "/fapi/v1/order")
        return DemoOrder.model_validate(payload)

    async def cancel_order(self, symbol: str, client_order_id: str) -> DemoOrder:
        payload = await self._signed_request(
            "DELETE",
            "/fapi/v1/order",
            {"symbol": symbol.upper(), "origClientOrderId": client_order_id},
        )
        return DemoOrder.model_validate(payload)

    async def positions(self, symbol: str | None = None) -> list[DemoPosition]:
        params: dict[str, str] = {}
        if symbol is not None:
            params["symbol"] = symbol.upper()
        payload = await self._signed_request("GET", "/fapi/v3/positionRisk", params)
        if not isinstance(payload, list):
            raise BinanceAPIError("Expected list from demo positionRisk")
        return [DemoPosition.model_validate(item) for item in payload]

    async def balances(self) -> list[DemoBalance]:
        payload = await self._signed_request("GET", "/fapi/v3/balance", {})
        if not isinstance(payload, list):
            raise BinanceAPIError("Expected list from demo balance")
        return [DemoBalance.model_validate(item) for item in payload]

    async def is_hedge_mode(self) -> bool:
        payload = await self._signed_request("GET", "/fapi/v1/positionSide/dual", {})
        if not isinstance(payload, dict) or "dualSidePosition" not in payload:
            raise BinanceAPIError("Invalid position mode response")
        value = payload["dualSidePosition"]
        return value is True or str(value).lower() == "true"

    async def start_user_stream(self) -> str:
        payload = await self._api_key_request("POST", "/fapi/v1/listenKey")
        if not isinstance(payload, dict) or not payload.get("listenKey"):
            raise BinanceAPIError("Invalid listenKey response")
        return str(payload["listenKey"])

    async def keepalive_user_stream(self) -> None:
        await self._api_key_request("PUT", "/fapi/v1/listenKey")

    async def close_user_stream(self) -> None:
        await self._api_key_request("DELETE", "/fapi/v1/listenKey")

    def _normalized_order_params(
        self, request: DemoOrderRequest, symbol: str, client_order_id: str
    ) -> dict[str, str]:
        rules = self.rules_for(symbol)
        market = request.order_type is not DemoOrderType.LIMIT
        quantity = rules.normalize_quantity(request.quantity, market=market)
        price: Decimal | None = None
        if request.price is not None:
            price = rules.normalize_price(request.price)
        stop_price: Decimal | None = None
        if request.stop_price is not None:
            stop_price = rules.normalize_price(request.stop_price)
        notional_price = price or request.reference_price or stop_price
        if notional_price is None:
            raise DemoOrderValidationError(
                "MARKET orders require reference_price for minNotional validation"
            )
        rules.validate_notional(quantity, notional_price)
        params = {
            "symbol": symbol,
            "side": request.side.value,
            "type": request.order_type.value,
            "quantity": _decimal_text(quantity),
            "newClientOrderId": client_order_id,
            "newOrderRespType": "RESULT",
        }
        if request.time_in_force is not None:
            params["timeInForce"] = request.time_in_force
        elif request.order_type is DemoOrderType.LIMIT:
            params["timeInForce"] = "GTC"
        if price is not None:
            params["price"] = _decimal_text(price)
        if stop_price is not None:
            params["stopPrice"] = _decimal_text(stop_price)
        if request.reduce_only:
            params["reduceOnly"] = "true"
        return params

    async def _public_request(self, method: str, path: str) -> Any:
        response = await self._client.request(method, path)
        return self._decode_success(response, path)

    async def _api_key_request(self, method: str, path: str) -> Any:
        response = await self._client.request(method, path, headers={"X-MBX-APIKEY": self._api_key})
        return self._decode_success(response, path)

    async def _signed_request(self, method: str, path: str, params: dict[str, str]) -> Any:
        response = await self._signed_response(method, path, params)
        if response.status_code >= 400 and _error_code(response) == -1021:
            await self.sync_server_time()
            response = await self._signed_response(method, path, params)
        return self._decode_success(response, path)

    async def _signed_response(
        self, method: str, path: str, params: dict[str, str]
    ) -> httpx.Response:
        signed = dict(params)
        signed["recvWindow"] = str(self._recv_window_ms)
        signed["timestamp"] = str(self._now_ms() + self._time_offset_ms)
        query = urlencode(signed)
        signed["signature"] = hmac_sha256(self._api_secret, query)
        return await self._client.request(
            method,
            path,
            params=signed,
            headers={"X-MBX-APIKEY": self._api_key},
        )

    @staticmethod
    def _decode_success(response: httpx.Response, path: str) -> Any:
        if response.status_code in {418, 429}:
            raise BinanceRateLimitError(f"Demo rate limit HTTP {response.status_code} at {path}")
        if response.status_code >= 400:
            code = _error_code(response)
            message = _error_message(response)
            raise BinanceAPIError(
                f"Demo API HTTP {response.status_code} code={code} at {path}: {message}"
            )
        try:
            return cast(Any, response.json())
        except ValueError as exc:
            raise BinanceAPIError(f"Invalid JSON from demo endpoint {path}") from exc


def _error_code(response: httpx.Response) -> int | None:
    try:
        payload = response.json()
    except ValueError:
        return None
    if isinstance(payload, dict) and isinstance(payload.get("code"), int):
        return int(payload["code"])
    return None


def _error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text[:200]
    if isinstance(payload, dict):
        return str(payload.get("msg", "unknown error"))[:200]
    return "unknown error"


def _execution_unknown(response: httpx.Response) -> bool:
    return "unknown error" in _error_message(response).lower()


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")
