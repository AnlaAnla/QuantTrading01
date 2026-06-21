"""Official Binance routed WebSocket stream mapping."""

from __future__ import annotations

from enum import StrEnum


class StreamRoute(StrEnum):
    """Binance USDⓈ-M routed public endpoint."""

    MARKET = "market"
    PUBLIC = "public"


MARKET_STREAM_TEMPLATES = (
    "{symbol}@aggTrade",
    "{symbol}@markPrice@1s",
    "{symbol}@forceOrder",
    "{symbol}@kline_1m",
)
PUBLIC_STREAM_TEMPLATES = (
    "{symbol}@bookTicker",
    "{symbol}@depth@100ms",
)


def streams_for_symbols(symbols: set[str]) -> dict[StreamRoute, tuple[str, ...]]:
    """Build deterministic stream names, partitioned by their official routes."""
    lowered = sorted(symbol.lower() for symbol in symbols)
    return {
        StreamRoute.MARKET: tuple(
            template.format(symbol=symbol)
            for symbol in lowered
            for template in MARKET_STREAM_TEMPLATES
        ),
        StreamRoute.PUBLIC: tuple(
            template.format(symbol=symbol)
            for symbol in lowered
            for template in PUBLIC_STREAM_TEMPLATES
        ),
    }


def combined_stream_url(base_url: str, route: StreamRoute, streams: tuple[str, ...]) -> str:
    """Return a routed combined-stream URL documented by Binance."""
    if not streams:
        raise ValueError("At least one stream is required")
    return f"{base_url.rstrip('/')}/{route.value}/stream?streams={'/'.join(streams)}"
