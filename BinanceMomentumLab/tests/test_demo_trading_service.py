from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any, cast

import pytest

from binance_momentum_lab.config import Settings
from binance_momentum_lab.demo_trading.client import BinanceDemoTradingClient
from binance_momentum_lab.demo_trading.models import (
    DemoOrder,
    DemoOrderIntent,
    DemoOrderRequest,
    DemoOrderType,
    DemoPosition,
    DemoSide,
)
from binance_momentum_lab.demo_trading.service import DemoTradingAdapter
from binance_momentum_lab.demo_trading.user_stream import (
    DemoUserDataStream,
    UserStreamConnection,
)
from binance_momentum_lab.exceptions import DemoStateMismatchError
from binance_momentum_lab.storage import DuckDBStore


def position(quantity: str = "0.01") -> DemoPosition:
    return DemoPosition.model_validate(
        {
            "symbol": "BTCUSDT",
            "positionSide": "BOTH",
            "positionAmt": quantity,
            "entryPrice": "50000",
            "breakEvenPrice": "50010",
            "unRealizedProfit": "1",
            "updateTime": 1000,
        }
    )


def close_request() -> DemoOrderRequest:
    return DemoOrderRequest(
        idempotency_key="close-1",
        symbol="BTCUSDT",
        side=DemoSide.SELL,
        order_type=DemoOrderType.MARKET,
        quantity=Decimal("0.01"),
        reference_price=Decimal("50000"),
        intent=DemoOrderIntent.CLOSE,
    )


class StubDemoClient:
    def __init__(self, remote_positions: list[DemoPosition]) -> None:
        self.remote_positions = remote_positions
        self.placed: list[DemoOrderRequest] = []

    async def start(self) -> None:
        return None

    async def aclose(self) -> None:
        return None

    async def is_hedge_mode(self) -> bool:
        return False

    async def positions(self, symbol: str | None = None) -> list[DemoPosition]:
        return self.remote_positions

    async def place_order(self, request: DemoOrderRequest) -> DemoOrder:
        self.placed.append(request)
        return DemoOrder.model_validate(
            {
                "symbol": request.symbol,
                "orderId": 1,
                "clientOrderId": request.client_order_id,
                "side": request.side,
                "positionSide": "BOTH",
                "type": request.order_type,
                "status": "NEW",
                "origQty": request.quantity,
                "executedQty": "0",
                "price": "0",
                "avgPrice": "0",
                "stopPrice": "0",
                "reduceOnly": request.reduce_only,
                "updateTime": 1000,
            }
        )


@pytest.mark.asyncio
async def test_startup_mismatch_blocks_open_but_allows_reduce_only_close() -> None:
    store = DuckDBStore(":memory:")
    store.initialize()
    stub = StubDemoClient([position()])
    adapter = DemoTradingAdapter(cast(BinanceDemoTradingClient, stub), store)

    await adapter.start()

    assert adapter.positions_in_sync is False
    opening = close_request().model_copy(
        update={"idempotency_key": "open-1", "side": DemoSide.BUY, "intent": DemoOrderIntent.OPEN}
    )
    with pytest.raises(DemoStateMismatchError):
        await adapter.place_order(opening)
    closing = await adapter.place_order(close_request())
    assert closing.reduce_only is True
    assert len(stub.placed) == 1
    store.close()


@pytest.mark.asyncio
async def test_account_update_persists_position_and_restores_sync() -> None:
    store = DuckDBStore(":memory:")
    store.initialize()
    stub = StubDemoClient([position()])
    adapter = DemoTradingAdapter(cast(BinanceDemoTradingClient, stub), store)
    assert await adapter.reconcile_positions() is False

    await adapter.on_user_event(
        {
            "e": "ACCOUNT_UPDATE",
            "E": 1000,
            "T": 1000,
            "a": {
                "P": [
                    {
                        "s": "BTCUSDT",
                        "pa": "0.01",
                        "ep": "50000",
                        "bep": "50010",
                        "up": "1",
                        "ps": "BOTH",
                    }
                ]
            },
        }
    )

    assert adapter.positions_in_sync is True
    assert store.list_demo_positions()[0].quantity == Decimal("0.01")
    store.close()


@pytest.mark.asyncio
async def test_each_open_rechecks_remote_positions_to_fail_closed_after_missed_event() -> None:
    store = DuckDBStore(":memory:")
    store.initialize()
    stub = StubDemoClient([])
    adapter = DemoTradingAdapter(cast(BinanceDemoTradingClient, stub), store)
    assert await adapter.reconcile_positions() is True
    stub.remote_positions = [position()]
    opening = close_request().model_copy(
        update={
            "idempotency_key": "open-after-gap",
            "side": DemoSide.BUY,
            "intent": DemoOrderIntent.OPEN,
        }
    )

    with pytest.raises(DemoStateMismatchError):
        await adapter.place_order(opening)

    assert stub.placed == []
    store.close()


class StubListenClient:
    def __init__(self) -> None:
        self.closed = False

    async def start_user_stream(self) -> str:
        return "demo-listen-key"

    async def keepalive_user_stream(self) -> None:
        return None

    async def close_user_stream(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.sent = False
        self.closed = False
        self.wait_forever = asyncio.Event()

    async def recv(self) -> str:
        if not self.sent:
            self.sent = True
            return json.dumps(self.payload)
        await self.wait_forever.wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_user_stream_uses_demo_private_route_and_dispatches_events() -> None:
    settings = Settings(_env_file=None)
    listen_client = StubListenClient()
    connection = FakeConnection({"e": "ORDER_TRADE_UPDATE", "o": {"c": "bml_1"}})
    received = asyncio.Event()
    events: list[dict[str, Any]] = []
    urls: list[str] = []

    async def factory(
        url: str, _ping_interval: float, _ping_timeout: float
    ) -> UserStreamConnection:
        urls.append(url)
        return connection

    async def handler(payload: dict[str, Any]) -> None:
        events.append(payload)
        received.set()

    stream = DemoUserDataStream(
        settings,
        cast(BinanceDemoTradingClient, listen_client),
        handler,
        connection_factory=factory,
    )
    await stream.start()
    await asyncio.wait_for(received.wait(), timeout=1)
    assert stream.healthy is True
    await stream.stop()

    assert urls == ["wss://fstream.binancefuture.com/private/ws/demo-listen-key"]
    assert events[0]["e"] == "ORDER_TRADE_UPDATE"
    assert connection.closed is True
    assert listen_client.closed is True
    assert stream.healthy is False
