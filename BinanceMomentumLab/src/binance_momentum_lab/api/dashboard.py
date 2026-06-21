"""Dashboard aggregation and top-level incremental patch generation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder

from ..logging_config import recent_errors
from ..market_data.service import RealtimeMarketDataService
from ..paper.broker import PaperBroker
from ..runtime import ScannerRuntime
from ..storage import DuckDBStore


async def dashboard_snapshot(app: FastAPI) -> dict[str, Any]:
    """Collect a coherent, secret-free browser dashboard snapshot."""
    store: DuckDBStore = app.state.store
    runtime: ScannerRuntime | None = app.state.runtime
    realtime: RealtimeMarketDataService | None = app.state.realtime
    broker: PaperBroker = app.state.paper_broker
    settings = app.state.settings
    (
        candidates,
        features,
        states,
        signals,
        orders,
        fills,
        positions,
        equity_curve,
        persisted_health,
    ) = await asyncio.gather(
        asyncio.to_thread(store.list_candidates),
        asyncio.to_thread(store.list_feature_snapshots, 100),
        asyncio.to_thread(store.list_strategy_states),
        asyncio.to_thread(store.list_signals, 100),
        asyncio.to_thread(store.list_paper_orders, 100),
        asyncio.to_thread(store.list_paper_fills, 100),
        asyncio.to_thread(store.list_positions),
        asyncio.to_thread(store.list_account_snapshots, 1000),
        asyncio.to_thread(store.list_system_health),
    )
    websocket_health: object
    if realtime is not None:
        websocket_health = realtime.health.snapshot()
    elif settings.demo_data:
        websocket_health = "disabled_demo_data"
    else:
        websocket_health = "disabled_for_test"
    now = datetime.now(UTC)
    demo_adapter = getattr(app.state, "demo_adapter", None)
    demo_in_sync = demo_adapter.positions_in_sync if demo_adapter is not None else None
    demo_stream_healthy = (
        demo_adapter.user_stream.healthy
        if demo_adapter is not None and demo_adapter.user_stream is not None
        else None
    )
    payload = {
        "system": {
            "status": (
                "degraded"
                if (runtime is not None and runtime.rest_status == "degraded")
                or demo_in_sync is False
                or demo_stream_healthy is False
                else "ok"
            ),
            "mode": settings.app_mode.value,
            "rest": runtime.rest_status if runtime is not None else "disabled_for_test",
            "websocket": websocket_health,
            "database": "healthy",
            "entries_paused": broker.risk_manager.entries_paused,
            "emergency_stop": broker.risk_manager.emergency_stopped,
            "checked_at_utc": now,
            "checked_at_asia_shanghai": now.astimezone(ZoneInfo("Asia/Shanghai")),
            "health_components": persisted_health,
            "demo_positions_in_sync": demo_in_sync,
            "demo_mismatch_detail": (
                demo_adapter.mismatch_detail if demo_adapter is not None else None
            ),
            "demo_user_stream": (
                {
                    "healthy": demo_stream_healthy,
                    "last_error": demo_adapter.user_stream.last_error,
                }
                if demo_adapter is not None and demo_adapter.user_stream is not None
                else None
            ),
        },
        "candidates": candidates,
        "features": features,
        "strategy_states": states,
        "signals": signals,
        "orders": orders,
        "fills": fills,
        "positions": positions,
        "performance": broker.performance(),
        "equity_curve": equity_curve,
        "errors": recent_errors(),
    }
    return cast(dict[str, Any], jsonable_encoder(payload))


def dashboard_patch(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    """Return only changed top-level dashboard domains."""
    return {
        key: value
        for key, value in current.items()
        if key not in previous or previous[key] != value
    }
