import json
from decimal import Decimal
from pathlib import Path

import pytest

from binance_momentum_lab.exceptions import OrderBookSequenceGapError
from binance_momentum_lab.market_data.events import DepthEvent, parse_combined_message
from binance_momentum_lab.market_data.order_book import DepthSnapshot, LocalOrderBook
from binance_momentum_lab.market_data.routes import StreamRoute

FIXTURES = Path(__file__).parent / "fixtures"


def depth_event(**updates: object) -> DepthEvent:
    fixtures = json.loads((FIXTURES / "ws_events.json").read_text(encoding="utf-8"))
    data = fixtures["depth"]["data"]
    data.update(updates)
    event = parse_combined_message(
        {"stream": "btcusdt@depth@100ms", "data": data}, StreamRoute.PUBLIC
    ).event
    assert isinstance(event, DepthEvent)
    return event


def snapshot() -> DepthSnapshot:
    return DepthSnapshot(
        last_update_id=100,
        bids=((Decimal("42000"), Decimal("1")), (Decimal("41999"), Decimal("1"))),
        asks=((Decimal("42001"), Decimal("1")),),
    )


def test_snapshot_alignment_and_absolute_level_updates() -> None:
    book = LocalOrderBook("BTCUSDT")
    book.buffer(depth_event(U=99, u=102, pu=98))
    book.initialize(snapshot())

    assert book.synchronized
    assert book.last_update_id == 102
    assert book.best_bid() == (Decimal("42000.0"), Decimal("2.0"))
    assert book.best_ask() == (Decimal("42002.0"), Decimal("3.0"))
    assert Decimal("41999") not in book.bids


def test_continuation_requires_pu_equal_previous_u() -> None:
    book = LocalOrderBook("BTCUSDT")
    book.buffer(depth_event(U=99, u=102, pu=98))
    book.initialize(snapshot())

    book.apply(depth_event(U=103, u=104, pu=102))
    assert book.last_update_id == 104

    with pytest.raises(OrderBookSequenceGapError, match="expected pu=104"):
        book.apply(depth_event(U=105, u=106, pu=999))
    assert not book.synchronized


def test_missing_bridge_requires_resnapshot() -> None:
    book = LocalOrderBook("BTCUSDT")
    book.buffer(depth_event(U=101, u=102, pu=100))

    with pytest.raises(OrderBookSequenceGapError, match="No buffered event bridges"):
        book.initialize(snapshot())


def test_only_first_bridging_event_can_bypass_pu_check() -> None:
    book = LocalOrderBook("BTCUSDT")
    book.buffer(depth_event(U=99, u=100, pu=98))
    book.buffer(depth_event(U=101, u=102, pu=999))

    with pytest.raises(OrderBookSequenceGapError, match="expected pu=100"):
        book.initialize(snapshot())
    assert not book.synchronized
