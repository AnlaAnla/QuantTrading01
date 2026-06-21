"""Opt-in Binance Futures Demo checks; never collected into network work by default."""

from __future__ import annotations

import os

import pytest

from binance_momentum_lab.config import DEMO_REST_URL, DEMO_WS_URL, Settings
from binance_momentum_lab.demo_trading.client import BinanceDemoTradingClient

RUN_DEMO = os.getenv("RUN_BINANCE_DEMO_TESTS", "").lower() == "true"
pytestmark = pytest.mark.skipif(
    not RUN_DEMO,
    reason="Set RUN_BINANCE_DEMO_TESTS=true to explicitly allow Binance Demo network tests",
)


@pytest.mark.asyncio
async def test_demo_time_exchange_rules_balance_and_positions() -> None:
    settings = Settings(
        _env_file=None,
        app_mode="DEMO",
        demo_trading_enabled=True,
    )
    if not settings.binance_api_key or not settings.binance_api_secret:
        pytest.skip("Demo API credentials are not configured")
    settings.startup_safety_check()
    assert str(settings.binance_demo_rest_url).rstrip("/") == DEMO_REST_URL
    assert settings.binance_demo_ws_url == DEMO_WS_URL
    client = BinanceDemoTradingClient(settings)
    try:
        await client.start()
        balances = await client.balances()
        positions = await client.positions()
        assert client.rules_for("BTCUSDT").tick_size > 0
        assert isinstance(balances, list)
        assert isinstance(positions, list)
    finally:
        await client.aclose()
