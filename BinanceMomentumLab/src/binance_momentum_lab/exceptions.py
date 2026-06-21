"""Domain-specific exceptions."""


class MomentumLabError(Exception):
    """Base exception for expected application failures."""


class UnsafeConfigurationError(MomentumLabError):
    """Raised when startup configuration violates a safety invariant."""


class LiveTradingDisabledError(UnsafeConfigurationError):
    """Raised whenever LIVE mode is requested in this release."""


class DemoTradingUnavailableError(UnsafeConfigurationError):
    """Raised when DEMO mode is requested before its execution adapter exists."""


class BinanceAPIError(MomentumLabError):
    """Raised for a non-success Binance API response."""


class BinanceRateLimitError(BinanceAPIError):
    """Raised after Binance returns HTTP 429 or 418."""


class WebSocketProtocolError(MomentumLabError):
    """Raised when a market stream message violates its documented shape."""


class OrderBookNotSynchronizedError(MomentumLabError):
    """Raised when an order book is queried before snapshot alignment."""


class OrderBookSequenceGapError(MomentumLabError):
    """Raised when depth update IDs are discontinuous and resync is required."""


class NonMonotonicMarketDataError(MomentumLabError):
    """Raised when paper execution receives market data from the future's past."""
