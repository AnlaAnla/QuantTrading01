from binance_momentum_lab.market_data.routes import (
    StreamRoute,
    combined_stream_url,
    streams_for_symbols,
)


def test_streams_are_partitioned_to_official_routes() -> None:
    streams = streams_for_symbols({"BTCUSDT"})

    assert streams[StreamRoute.MARKET] == (
        "btcusdt@aggTrade",
        "btcusdt@markPrice@1s",
        "btcusdt@forceOrder",
        "btcusdt@kline_1m",
    )
    assert streams[StreamRoute.PUBLIC] == (
        "btcusdt@bookTicker",
        "btcusdt@depth@100ms",
    )


def test_combined_url_keeps_market_and_public_separate() -> None:
    streams = streams_for_symbols({"BTCUSDT"})
    market = combined_stream_url(
        "wss://fstream.binance.com", StreamRoute.MARKET, streams[StreamRoute.MARKET]
    )
    public = combined_stream_url(
        "wss://fstream.binance.com", StreamRoute.PUBLIC, streams[StreamRoute.PUBLIC]
    )

    assert market.startswith("wss://fstream.binance.com/market/stream?streams=")
    assert "@aggTrade" in market and "@forceOrder" in market
    assert public.startswith("wss://fstream.binance.com/public/stream?streams=")
    assert "@depth@100ms" in public
    assert "@aggTrade" not in public
