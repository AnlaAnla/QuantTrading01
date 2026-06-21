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
