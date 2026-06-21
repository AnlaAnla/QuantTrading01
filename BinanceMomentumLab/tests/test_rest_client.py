from __future__ import annotations

import httpx
import pytest

from binance_momentum_lab.binance.client import BinancePublicRESTClient
from binance_momentum_lab.exceptions import BinanceRateLimitError


def ticker_payload() -> dict[str, str | int]:
    return {
        "symbol": "BTCUSDT",
        "priceChangePercent": "5.25",
        "lastPrice": "65000.10",
        "quoteVolume": "123456789.12",
        "closeTime": 1_700_000_000_000,
    }


@pytest.mark.asyncio
async def test_parses_tickers_as_decimal_and_reads_weight_header() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/fapi/v1/ticker/24hr"
        return httpx.Response(
            200,
            json=[ticker_payload()],
            headers={"X-MBX-USED-WEIGHT-1M": "42"},
        )

    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport, base_url="https://example.test")
    client = BinancePublicRESTClient("https://example.test", client=http_client)

    tickers = await client.tickers_24h()

    assert str(tickers[0].last_price) == "65000.10"
    assert client.used_weight == 42
    await http_client.aclose()


@pytest.mark.asyncio
async def test_rate_limit_retries_using_retry_after() -> None:
    calls = 0
    delays: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "2"})
        return httpx.Response(200, json=[ticker_payload()])

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://example.test"
    )
    client = BinancePublicRESTClient("https://example.test", client=http_client, sleep=fake_sleep)

    assert len(await client.tickers_24h()) == 1
    assert delays == [2.0]
    await http_client.aclose()


@pytest.mark.asyncio
async def test_rate_limit_fails_closed_after_retry_budget() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(418, headers={"Retry-After": "0"})

    async def fake_sleep(_delay: float) -> None:
        return None

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://example.test"
    )
    client = BinancePublicRESTClient(
        "https://example.test", client=http_client, sleep=fake_sleep, max_retries=1
    )

    with pytest.raises(BinanceRateLimitError, match="HTTP 418"):
        await client.tickers_24h()
    await http_client.aclose()


@pytest.mark.asyncio
async def test_parses_depth_snapshot_for_local_order_book() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/fapi/v1/depth"
        return httpx.Response(
            200,
            json={
                "lastUpdateId": 100,
                "bids": [["42000.0", "2.5"]],
                "asks": [["42001.0", "1.5"]],
            },
        )

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://example.test"
    )
    client = BinancePublicRESTClient("https://example.test", client=http_client)

    snapshot = await client.depth_snapshot("BTCUSDT")

    assert snapshot.last_update_id == 100
    assert str(snapshot.bids[0][1]) == "2.5"
    await http_client.aclose()
