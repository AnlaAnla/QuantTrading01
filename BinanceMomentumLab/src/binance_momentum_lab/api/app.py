"""FastAPI application factory and phase-one endpoints."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from ..config import Settings, get_settings
from ..logging_config import configure_logging
from ..market_data.candidates import DynamicCandidateManager
from ..market_data.service import RealtimeMarketDataService
from ..paper.broker import PaperBroker
from ..paper.risk import RiskManager
from ..paper.service import PaperExecutionService
from ..runtime import ScannerRuntime
from ..scanner import Candidate
from ..storage import DuckDBStore
from ..strategy.service import StrategyFeatureService

WEB_DIR = Path(__file__).parents[3] / "web"


def create_app(settings: Settings | None = None, *, start_scanner: bool = True) -> FastAPI:
    """Build an independently testable application instance."""
    configured = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configured.startup_safety_check()
        configure_logging(configured.log_level)
        store = DuckDBStore(configured.database_path)
        await asyncio.to_thread(store.initialize)
        candidate_manager = DynamicCandidateManager()

        async def publish_candidates(items: list[Candidate]) -> None:
            symbols = {item.symbol for item in items}
            symbols.add(configured.feature_benchmark_symbol)
            await candidate_manager.update(symbols)

        runtime = (
            ScannerRuntime(configured, store, on_candidates=publish_candidates)
            if start_scanner
            else None
        )
        realtime = (
            RealtimeMarketDataService(configured, runtime.client, store)
            if runtime is not None and not configured.demo_data
            else None
        )
        if realtime is not None:
            candidate_manager.subscribe(realtime.update_symbols)
        strategy = (
            StrategyFeatureService(
                configured,
                runtime.client,
                store,
                realtime.books.books,
            )
            if runtime is not None and realtime is not None
            else None
        )
        if strategy is not None:
            candidate_manager.subscribe(strategy.update_symbols)
            assert realtime is not None
            realtime.subscribe(strategy.on_event)
        risk_manager = RiskManager(configured)
        paper_broker = PaperBroker(configured, risk_manager, store)
        paper_execution = (
            PaperExecutionService(
                configured,
                paper_broker,
                strategy,
                realtime.health,
                realtime.books.books,
            )
            if strategy is not None and realtime is not None
            else None
        )
        if paper_execution is not None:
            assert strategy is not None and realtime is not None
            strategy.subscribe_signal(paper_execution.on_signal)
            realtime.subscribe(paper_execution.on_event)
        app.state.settings = configured
        app.state.store = store
        app.state.runtime = runtime
        app.state.realtime = realtime
        app.state.strategy = strategy
        app.state.risk_manager = risk_manager
        app.state.paper_broker = paper_broker
        app.state.paper_execution = paper_execution
        app.state.emergency_stop = False
        if runtime is not None:
            if realtime is not None:
                realtime.start()
            if strategy is not None:
                strategy.start()
            runtime.start()
        try:
            yield
        finally:
            if runtime is not None:
                await runtime.stop(close_client=False)
            if strategy is not None:
                await strategy.stop()
            if realtime is not None:
                await realtime.stop()
            if runtime is not None:
                await runtime.client.aclose()
            await asyncio.to_thread(store.close)

    app = FastAPI(title="BinanceMomentumLab", version="0.1.0", lifespan=lifespan)

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(WEB_DIR / "index.html")

    @app.get("/api/health")
    async def health(request: Request) -> dict[str, Any]:
        runtime: ScannerRuntime | None = request.app.state.runtime
        realtime: RealtimeMarketDataService | None = request.app.state.realtime
        rest_status = runtime.rest_status if runtime else "disabled_for_test"
        last_error = runtime.last_error if runtime else None
        if realtime is not None:
            websocket_status: object = realtime.health.snapshot()
        elif configured.demo_data:
            websocket_status = "disabled_demo_data"
        else:
            websocket_status = "disabled_for_test"
        return {
            "status": "ok" if rest_status != "degraded" else "degraded",
            "mode": configured.app_mode.value,
            "rest": rest_status,
            "websocket": websocket_status,
            "database": "healthy",
            "emergency_stop": bool(request.app.state.emergency_stop),
            "last_error": last_error,
            "checked_at_utc": datetime.now(UTC),
            "checked_at_asia_shanghai": datetime.now(UTC).astimezone(ZoneInfo("Asia/Shanghai")),
        }

    @app.get("/api/config/public")
    async def public_config() -> dict[str, object]:
        return configured.public_config

    @app.get("/api/candidates")
    async def candidates(request: Request) -> list[dict[str, Any]]:
        store: DuckDBStore = request.app.state.store
        return await asyncio.to_thread(store.list_candidates)

    @app.get("/api/positions")
    async def positions(request: Request) -> list[dict[str, Any]]:
        return await asyncio.to_thread(request.app.state.store.list_positions)

    @app.get("/api/orders")
    async def orders(request: Request) -> list[dict[str, Any]]:
        return await asyncio.to_thread(request.app.state.store.list_paper_orders)

    @app.get("/api/trades")
    async def trades(request: Request) -> list[dict[str, Any]]:
        return await asyncio.to_thread(request.app.state.store.list_paper_fills)

    @app.get("/api/signals")
    async def signals(request: Request) -> list[dict[str, Any]]:
        store: DuckDBStore = request.app.state.store
        return await asyncio.to_thread(store.list_signals)

    @app.get("/api/performance")
    async def performance(request: Request) -> dict[str, object]:
        broker: PaperBroker = request.app.state.paper_broker
        return broker.performance()

    @app.post("/api/paper/reset")
    async def reset_paper(request: Request) -> dict[str, bool]:
        broker: PaperBroker = request.app.state.paper_broker
        try:
            await asyncio.to_thread(broker.reset)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        request.app.state.emergency_stop = False
        return {"reset": True}

    @app.post("/api/emergency-stop")
    async def emergency_stop(request: Request) -> dict[str, bool]:
        broker: PaperBroker = request.app.state.paper_broker
        broker.emergency_close_all(datetime.now(UTC))
        request.app.state.emergency_stop = True
        return {"emergency_stop": True}

    @app.websocket("/ws/dashboard")
    async def dashboard(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            while True:
                await websocket.send_json(await health_for_websocket(websocket, configured))
                await asyncio.sleep(2)
        except WebSocketDisconnect:
            return

    return app


async def health_for_websocket(websocket: WebSocket, settings: Settings) -> dict[str, object]:
    """Build a compact dashboard snapshot without a network dependency."""
    store: DuckDBStore = websocket.app.state.store
    return {
        "type": "dashboard",
        "mode": settings.app_mode.value,
        "candidates": await asyncio.to_thread(store.list_candidates),
        "timestamp_utc": datetime.now(UTC).isoformat(),
    }
