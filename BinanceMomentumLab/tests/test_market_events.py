import json
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from binance_momentum_lab.exceptions import WebSocketProtocolError
from binance_momentum_lab.market_data.events import (
    AggTradeEvent,
    DepthEvent,
    ForceOrderEvent,
    KlineEvent,
    event_identity,
    parse_combined_message,
)
from binance_momentum_lab.market_data.routes import StreamRoute

FIXTURES = Path(__file__).parent / "fixtures"


def payloads() -> dict[str, dict[str, object]]:
    return cast(
        dict[str, dict[str, object]],
        json.loads((FIXTURES / "ws_events.json").read_text(encoding="utf-8")),
    )


@pytest.mark.parametrize(
    "name", ["aggTrade", "markPrice", "bookTicker", "depth", "forceOrder", "kline"]
)
def test_parses_all_documented_event_types(name: str) -> None:
    route = StreamRoute.PUBLIC if name in {"bookTicker", "depth"} else StreamRoute.MARKET
    envelope = parse_combined_message(payloads()[name], route)

    assert envelope.event.symbol == "BTCUSDT"
    assert envelope.route is route


def test_decimal_fields_and_identities() -> None:
    aggregate = parse_combined_message(payloads()["aggTrade"], StreamRoute.MARKET)
    depth = parse_combined_message(payloads()["depth"], StreamRoute.PUBLIC)

    assert isinstance(aggregate.event, AggTradeEvent)
    assert aggregate.event.price == Decimal("42000.10")
    assert event_identity(aggregate) == "aggTrade:BTCUSDT:42"
    assert isinstance(depth.event, DepthEvent)
    assert event_identity(depth) == "depthUpdate:BTCUSDT:102"


def test_force_order_is_explicitly_a_snapshot_not_complete_volume() -> None:
    envelope = parse_combined_message(payloads()["forceOrder"], StreamRoute.MARKET)

    assert isinstance(envelope.event, ForceOrderEvent)
    documentation = ForceOrderEvent.__doc__
    assert documentation is not None
    assert "not a complete liquidation-volume feed" in documentation


def test_kline_is_one_minute() -> None:
    envelope = parse_combined_message(payloads()["kline"], StreamRoute.MARKET)

    assert isinstance(envelope.event, KlineEvent)
    assert envelope.event.kline.interval == "1m"


def test_rejects_unknown_or_malformed_messages() -> None:
    with pytest.raises(WebSocketProtocolError):
        parse_combined_message({"data": {}}, StreamRoute.MARKET)
    with pytest.raises(WebSocketProtocolError, match="Unsupported"):
        parse_combined_message({"stream": "x", "data": {"e": "unknown"}}, StreamRoute.MARKET)


def test_rejects_event_on_wrong_routed_endpoint() -> None:
    with pytest.raises(WebSocketProtocolError, match="belongs to /market"):
        parse_combined_message(payloads()["aggTrade"], StreamRoute.PUBLIC)
    with pytest.raises(WebSocketProtocolError, match="belongs to /public"):
        parse_combined_message(payloads()["depth"], StreamRoute.MARKET)
