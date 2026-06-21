"""Strict parsing for Binance USDⓈ-M WebSocket market events."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ..exceptions import WebSocketProtocolError
from .routes import StreamRoute


class EventModel(BaseModel):
    """Base model using Binance's compact field aliases."""

    model_config = ConfigDict(populate_by_name=True)

    event_type: str = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    symbol: str = Field(alias="s")

    @property
    def event_time(self) -> datetime:
        return datetime.fromtimestamp(self.event_time_ms / 1000, tz=UTC)


class AggTradeEvent(EventModel):
    event_type: Literal["aggTrade"] = Field(alias="e")
    aggregate_trade_id: int = Field(alias="a")
    price: Decimal = Field(alias="p")
    quantity: Decimal = Field(alias="q")
    first_trade_id: int = Field(alias="f")
    last_trade_id: int = Field(alias="l")
    trade_time_ms: int = Field(alias="T")
    buyer_is_maker: bool = Field(alias="m")


class MarkPriceEvent(EventModel):
    event_type: Literal["markPriceUpdate"] = Field(alias="e")
    mark_price: Decimal = Field(alias="p")
    index_price: Decimal = Field(alias="i")
    estimated_settle_price: Decimal = Field(alias="P")
    funding_rate: Decimal = Field(alias="r")
    next_funding_time_ms: int = Field(alias="T")


class BookTickerEvent(EventModel):
    event_type: Literal["bookTicker"] = Field(alias="e")
    update_id: int = Field(alias="u")
    transaction_time_ms: int = Field(alias="T")
    best_bid_price: Decimal = Field(alias="b")
    best_bid_quantity: Decimal = Field(alias="B")
    best_ask_price: Decimal = Field(alias="a")
    best_ask_quantity: Decimal = Field(alias="A")


class DepthEvent(EventModel):
    event_type: Literal["depthUpdate"] = Field(alias="e")
    transaction_time_ms: int = Field(alias="T")
    first_update_id: int = Field(alias="U")
    final_update_id: int = Field(alias="u")
    previous_final_update_id: int = Field(alias="pu")
    bids: list[tuple[Decimal, Decimal]] = Field(alias="b")
    asks: list[tuple[Decimal, Decimal]] = Field(alias="a")


class LiquidationOrder(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(alias="s")
    side: str = Field(alias="S")
    order_type: str = Field(alias="o")
    time_in_force: str = Field(alias="f")
    original_quantity: Decimal = Field(alias="q")
    price: Decimal = Field(alias="p")
    average_price: Decimal = Field(alias="ap")
    status: str = Field(alias="X")
    last_filled_quantity: Decimal = Field(alias="l")
    accumulated_filled_quantity: Decimal = Field(alias="z")
    trade_time_ms: int = Field(alias="T")


class ForceOrderEvent(BaseModel):
    """Largest liquidation order snapshot in a symbol's 1000ms window.

    This event is explicitly not a complete liquidation-volume feed.
    """

    model_config = ConfigDict(populate_by_name=True)

    event_type: Literal["forceOrder"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    order: LiquidationOrder = Field(alias="o")

    @property
    def symbol(self) -> str:
        return self.order.symbol

    @property
    def event_time(self) -> datetime:
        return datetime.fromtimestamp(self.event_time_ms / 1000, tz=UTC)


class KlinePayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    start_time_ms: int = Field(alias="t")
    close_time_ms: int = Field(alias="T")
    symbol: str = Field(alias="s")
    interval: Literal["1m"] = Field(alias="i")
    first_trade_id: int = Field(alias="f")
    last_trade_id: int = Field(alias="L")
    open_price: Decimal = Field(alias="o")
    close_price: Decimal = Field(alias="c")
    high_price: Decimal = Field(alias="h")
    low_price: Decimal = Field(alias="l")
    base_volume: Decimal = Field(alias="v")
    trade_count: int = Field(alias="n")
    is_closed: bool = Field(alias="x")
    quote_volume: Decimal = Field(alias="q")
    taker_buy_base_volume: Decimal = Field(alias="V")
    taker_buy_quote_volume: Decimal = Field(alias="Q")


class KlineEvent(EventModel):
    event_type: Literal["kline"] = Field(alias="e")
    kline: KlinePayload = Field(alias="k")


ParsedEvent = (
    AggTradeEvent | MarkPriceEvent | BookTickerEvent | DepthEvent | ForceOrderEvent | KlineEvent
)


class ParsedEnvelope(BaseModel):
    """Normalized combined-stream envelope with route provenance."""

    stream: str
    route: StreamRoute
    event: ParsedEvent
    raw_data: dict[str, Any]

    model_config = ConfigDict(arbitrary_types_allowed=True)


EVENT_MODELS: dict[str, type[BaseModel]] = {
    "aggTrade": AggTradeEvent,
    "markPriceUpdate": MarkPriceEvent,
    "bookTicker": BookTickerEvent,
    "depthUpdate": DepthEvent,
    "forceOrder": ForceOrderEvent,
    "kline": KlineEvent,
}
EVENT_ROUTES = {
    "aggTrade": StreamRoute.MARKET,
    "markPriceUpdate": StreamRoute.MARKET,
    "forceOrder": StreamRoute.MARKET,
    "kline": StreamRoute.MARKET,
    "bookTicker": StreamRoute.PUBLIC,
    "depthUpdate": StreamRoute.PUBLIC,
}


def parse_combined_message(payload: dict[str, Any], route: StreamRoute) -> ParsedEnvelope:
    """Parse one combined stream message and retain its unmodified data payload."""
    stream = payload.get("stream")
    data = payload.get("data")
    if not isinstance(stream, str) or not isinstance(data, dict):
        raise WebSocketProtocolError("Expected combined stream envelope with stream and data")
    event_type = data.get("e")
    model = EVENT_MODELS.get(str(event_type))
    if model is None:
        raise WebSocketProtocolError(f"Unsupported event type: {event_type!r}")
    expected_route = EVENT_ROUTES[str(event_type)]
    if route is not expected_route:
        raise WebSocketProtocolError(
            f"{event_type} belongs to /{expected_route.value}, not /{route.value}"
        )
    try:
        event = model.model_validate(data)
    except ValidationError as exc:
        raise WebSocketProtocolError(f"Invalid {event_type} payload") from exc
    return ParsedEnvelope(stream=stream, route=route, event=event, raw_data=data)


def event_identity(envelope: ParsedEnvelope) -> str:
    """Build a deterministic identity suitable for bounded duplicate suppression."""
    event = envelope.event
    if isinstance(event, AggTradeEvent):
        suffix = str(event.aggregate_trade_id)
    elif isinstance(event, (BookTickerEvent, DepthEvent)):
        suffix = str(
            event.update_id if isinstance(event, BookTickerEvent) else event.final_update_id
        )
    elif isinstance(event, ForceOrderEvent):
        suffix = f"{event.order.trade_time_ms}:{event.order.side}"
    elif isinstance(event, KlineEvent):
        suffix = f"{event.kline.start_time_ms}:{event.event_time_ms}"
    else:
        suffix = str(event.event_time_ms)
    return f"{event.event_type}:{event.symbol}:{suffix}"
