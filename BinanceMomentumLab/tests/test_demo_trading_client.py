from __future__ import annotations

from decimal import Decimal
from urllib.parse import urlencode

import httpx
import pytest

from binance_momentum_lab.binance.signing import hmac_sha256
from binance_momentum_lab.config import Settings
from binance_momentum_lab.demo_trading.client import BinanceDemoTradingClient
from binance_momentum_lab.demo_trading.models import (
    DemoOrderIntent,
    DemoOrderRequest,
    DemoOrderType,
    DemoSide,
    SymbolRules,
)
from binance_momentum_lab.exceptions import (
    DemoEndpointViolationError,
    DemoExecutionStatusUnknownError,
    DemoOrderValidationError,
)

DEMO_URL = "https://demo-fapi.binance.com"
SECRET = "demo-secret"


def demo_settings() -> Settings:
    return Settings(
        _env_file=None,
        app_mode="DEMO",
        demo_trading_enabled=True,
        binance_api_key="demo-key",
        binance_api_secret=SECRET,
    )


def exchange_info() -> dict[str, object]:
    return {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {
                        "filterType": "MARKET_LOT_SIZE",
                        "stepSize": "0.001",
                        "minQty": "0.001",
                    },
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            }
        ]
    }


def order_payload(*, reduce_only: bool = True) -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "orderId": 42,
        "clientOrderId": "bml_test",
        "side": "SELL",
        "positionSide": "BOTH",
        "type": "MARKET",
        "status": "FILLED",
        "origQty": "0.002",
        "executedQty": "0.002",
        "price": "0",
        "avgPrice": "50000",
        "stopPrice": "0",
        "reduceOnly": reduce_only,
        "updateTime": 1500,
    }


def assert_valid_signature(request: httpx.Request) -> None:
    items = [(key, value) for key, value in request.url.params.multi_items() if key != "signature"]
    assert request.url.params["signature"] == hmac_sha256(SECRET, urlencode(items))
    assert request.headers["X-MBX-APIKEY"] == "demo-key"


@pytest.mark.asyncio
async def test_time_sync_rules_signing_reduce_only_and_idempotency() -> None:
    posts = 0
    query_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posts, query_count
        if request.url.path == "/fapi/v1/time":
            return httpx.Response(200, json={"serverTime": 1500})
        if request.url.path == "/fapi/v1/exchangeInfo":
            return httpx.Response(200, json=exchange_info())
        assert_valid_signature(request)
        if request.method == "GET" and request.url.path == "/fapi/v1/order":
            query_count += 1
            if query_count == 1:
                return httpx.Response(400, json={"code": -2013, "msg": "Order does not exist."})
            payload = order_payload()
            payload["clientOrderId"] = request.url.params["origClientOrderId"]
            return httpx.Response(200, json=payload)
        if request.method == "POST" and request.url.path == "/fapi/v1/order":
            posts += 1
            assert request.url.params["quantity"] == "0.002"
            assert request.url.params["reduceOnly"] == "true"
            assert request.url.params["recvWindow"] == "5000"
            assert request.url.params["timestamp"] == "1500"
            payload = order_payload()
            payload["clientOrderId"] = request.url.params["newClientOrderId"]
            return httpx.Response(200, json=payload)
        raise AssertionError(f"unexpected request {request.method} {request.url}")

    http = httpx.AsyncClient(base_url=DEMO_URL, transport=httpx.MockTransport(handler))
    client = BinanceDemoTradingClient(demo_settings(), client=http, now_ms=lambda: 1000)
    await client.start()
    request = DemoOrderRequest(
        idempotency_key="signal-1-exit",
        symbol="btcusdt",
        side=DemoSide.SELL,
        order_type=DemoOrderType.MARKET,
        quantity=Decimal("0.0029"),
        reference_price=Decimal("50000"),
        intent=DemoOrderIntent.CLOSE,
    )

    first = await client.place_order(request)
    second = await client.place_order(request)

    assert first.client_order_id == request.client_order_id
    assert second.order_id == first.order_id
    assert first.reduce_only is True
    assert posts == 1
    await http.aclose()


@pytest.mark.asyncio
async def test_503_unknown_queries_before_returning_and_never_reposts() -> None:
    posts = 0
    queries = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posts, queries
        if request.method == "GET":
            queries += 1
            if queries == 1:
                return httpx.Response(400, json={"code": -2013, "msg": "Order not found"})
            payload = order_payload(reduce_only=False)
            payload["side"] = "BUY"
            payload["clientOrderId"] = request.url.params["origClientOrderId"]
            return httpx.Response(200, json=payload)
        posts += 1
        return httpx.Response(
            503,
            json={
                "code": -1000,
                "msg": "Unknown error, please check your request or try again later.",
            },
        )

    http = httpx.AsyncClient(base_url=DEMO_URL, transport=httpx.MockTransport(handler))
    client = BinanceDemoTradingClient(demo_settings(), client=http, now_ms=lambda: 1000)
    client._rules = {"BTCUSDT": SymbolRules.from_exchange_symbol(exchange_info()["symbols"][0])}  # type: ignore[index]
    request = DemoOrderRequest(
        idempotency_key="signal-open",
        symbol="BTCUSDT",
        side=DemoSide.BUY,
        order_type=DemoOrderType.MARKET,
        quantity=Decimal("0.001"),
        reference_price=Decimal("50000"),
        intent=DemoOrderIntent.OPEN,
    )

    recovered = await client.place_order(request)

    assert recovered.client_order_id == request.client_order_id
    assert posts == 1
    assert queries == 2
    await http.aclose()


@pytest.mark.asyncio
async def test_503_unknown_without_query_result_raises_without_duplicate_post() -> None:
    posts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if request.method == "GET":
            return httpx.Response(400, json={"code": -2013, "msg": "Order not found"})
        posts += 1
        return httpx.Response(503, json={"code": -1000, "msg": "Unknown error"})

    http = httpx.AsyncClient(base_url=DEMO_URL, transport=httpx.MockTransport(handler))
    client = BinanceDemoTradingClient(demo_settings(), client=http)
    client._rules = {"BTCUSDT": SymbolRules.from_exchange_symbol(exchange_info()["symbols"][0])}  # type: ignore[index]
    request = DemoOrderRequest(
        idempotency_key="unknown-open",
        symbol="BTCUSDT",
        side=DemoSide.BUY,
        order_type=DemoOrderType.MARKET,
        quantity=Decimal("0.001"),
        reference_price=Decimal("50000"),
        intent=DemoOrderIntent.OPEN,
    )

    with pytest.raises(DemoExecutionStatusUnknownError):
        await client.place_order(request)

    assert posts == 1
    await http.aclose()


def test_exchange_rules_use_filters_not_precision_fields() -> None:
    rules = SymbolRules.from_exchange_symbol(exchange_info()["symbols"][0])  # type: ignore[index]

    assert rules.normalize_price(Decimal("123.456")) == Decimal("123.40")
    assert rules.normalize_quantity(Decimal("0.0029"), market=True) == Decimal("0.002")
    with pytest.raises(DemoOrderValidationError, match="minQty"):
        rules.normalize_quantity(Decimal("0.0009"), market=False)
    with pytest.raises(DemoOrderValidationError, match="minNotional"):
        rules.validate_notional(Decimal("0.001"), Decimal("1000"))


@pytest.mark.asyncio
async def test_injected_mainnet_http_client_is_rejected() -> None:
    http = httpx.AsyncClient(base_url="https://fapi.binance.com")
    with pytest.raises(DemoEndpointViolationError):
        BinanceDemoTradingClient(demo_settings(), client=http)
    await http.aclose()


@pytest.mark.asyncio
async def test_query_cancel_positions_balances_and_listen_key_lifecycle() -> None:
    seen: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.url.path == "/fapi/v3/positionRisk":
            assert_valid_signature(request)
            return httpx.Response(
                200,
                json=[
                    {
                        "symbol": "BTCUSDT",
                        "positionSide": "BOTH",
                        "positionAmt": "0.01",
                        "entryPrice": "50000",
                        "breakEvenPrice": "50010",
                        "unRealizedProfit": "2",
                        "updateTime": 1000,
                    }
                ],
            )
        if request.url.path == "/fapi/v3/balance":
            assert_valid_signature(request)
            return httpx.Response(
                200,
                json=[
                    {
                        "asset": "USDT",
                        "balance": "1000",
                        "availableBalance": "900",
                        "crossWalletBalance": "1000",
                        "crossUnPnl": "2",
                        "updateTime": 1000,
                    }
                ],
            )
        if request.url.path == "/fapi/v1/positionSide/dual":
            assert_valid_signature(request)
            return httpx.Response(200, json={"dualSidePosition": False})
        if request.url.path == "/fapi/v1/order" and request.method == "DELETE":
            assert_valid_signature(request)
            payload = order_payload()
            payload["clientOrderId"] = request.url.params["origClientOrderId"]
            return httpx.Response(200, json=payload)
        if request.url.path == "/fapi/v1/listenKey":
            assert request.headers["X-MBX-APIKEY"] == "demo-key"
            assert "signature" not in request.url.params
            return httpx.Response(200, json={"listenKey": "demo-key-1"})
        raise AssertionError(f"unexpected request {request.method} {request.url}")

    http = httpx.AsyncClient(base_url=DEMO_URL, transport=httpx.MockTransport(handler))
    client = BinanceDemoTradingClient(demo_settings(), client=http, now_ms=lambda: 1000)

    positions = await client.positions()
    balances = await client.balances()
    assert await client.is_hedge_mode() is False
    canceled = await client.cancel_order("BTCUSDT", "bml_cancel")
    assert await client.start_user_stream() == "demo-key-1"
    await client.keepalive_user_stream()
    await client.close_user_stream()

    assert positions[0].quantity == Decimal("0.01")
    assert balances[0].available_balance == Decimal("900")
    assert canceled.client_order_id == "bml_cancel"
    assert ("PUT", "/fapi/v1/listenKey") in seen
    assert ("DELETE", "/fapi/v1/listenKey") in seen
    await http.aclose()
