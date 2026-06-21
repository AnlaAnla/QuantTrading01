"""Demo adapter lifecycle, remote reconciliation, and opening-order safety gate."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

from ..exceptions import DemoOrderValidationError, DemoStateMismatchError
from ..storage import DuckDBStore
from .client import BinanceDemoTradingClient
from .models import DemoOrder, DemoOrderIntent, DemoOrderRequest, DemoPosition
from .user_stream import DemoUserDataStream


class DemoTradingAdapter:
    """Own remote demo state while keeping strategy signal generation execution-agnostic."""

    def __init__(
        self,
        client: BinanceDemoTradingClient,
        store: DuckDBStore,
        user_stream: DemoUserDataStream | None = None,
    ) -> None:
        self.client = client
        self.store = store
        self.user_stream = user_stream
        self.positions_in_sync = False
        self.mismatch_detail: str | None = "not_reconciled"
        self.latest_order_events: dict[str, dict[str, Any]] = {}

    async def start(self) -> None:
        await self.client.start()
        if await self.client.is_hedge_mode():
            raise DemoOrderValidationError(
                "DEMO account must use one-way position mode because every close is reduce-only"
            )
        await self.reconcile_positions()
        if self.user_stream is not None:
            await self.user_stream.start()

    async def stop(self) -> None:
        try:
            if self.user_stream is not None:
                await self.user_stream.stop()
        finally:
            await self.client.aclose()

    async def reconcile_positions(self) -> bool:
        remote = [position for position in await self.client.positions() if position.quantity != 0]
        local = await asyncio.to_thread(self.store.list_demo_positions)
        remote_state = _canonical_positions(remote)
        local_state = _canonical_positions(local)
        self.positions_in_sync = remote_state == local_state
        self.mismatch_detail = (
            None if self.positions_in_sync else (f"local={local_state!r} remote={remote_state!r}")
        )
        return self.positions_in_sync

    async def place_order(self, request: DemoOrderRequest) -> DemoOrder:
        if request.intent is DemoOrderIntent.OPEN:
            await self.reconcile_positions()
            if not self.positions_in_sync:
                raise DemoStateMismatchError(
                    "Opening blocked until local and remote demo positions are reconciled"
                )
        order = await self.client.place_order(request)
        if request.intent is DemoOrderIntent.CLOSE and not order.reduce_only:
            raise DemoOrderValidationError("Binance response did not preserve reduceOnly on close")
        return order

    async def on_user_event(self, payload: dict[str, Any]) -> None:
        event_type = payload.get("e")
        if event_type == "ORDER_TRADE_UPDATE":
            order = payload.get("o")
            if isinstance(order, dict) and order.get("c"):
                self.latest_order_events[str(order["c"])] = payload
            return
        if event_type != "ACCOUNT_UPDATE":
            return
        account = payload.get("a")
        if not isinstance(account, dict) or not isinstance(account.get("P"), list):
            return
        for raw in account["P"]:
            if not isinstance(raw, dict):
                continue
            position = DemoPosition.model_validate(
                {
                    "symbol": raw["s"],
                    "positionSide": raw.get("ps", "BOTH"),
                    "positionAmt": raw["pa"],
                    "entryPrice": raw["ep"],
                    "breakEvenPrice": raw.get("bep", "0"),
                    "unRealizedProfit": raw.get("up", "0"),
                    "updateTime": payload.get("T", payload.get("E", 0)),
                }
            )
            await asyncio.to_thread(self.store.save_demo_position, position)
        await self.reconcile_positions()


def _canonical_positions(
    positions: list[DemoPosition],
) -> dict[tuple[str, str], tuple[Decimal, Decimal]]:
    return {
        (position.symbol, position.position_side.value): (
            position.quantity,
            position.entry_price,
        )
        for position in positions
        if position.quantity != 0
    }
