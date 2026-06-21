"""Small, testable signing primitive reserved for future authenticated adapters."""

import hashlib
import hmac


def hmac_sha256(secret: str, payload: str) -> str:
    """Return Binance's lowercase hexadecimal HMAC-SHA256 signature."""
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
