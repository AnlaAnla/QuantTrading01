from binance_momentum_lab.binance.signing import hmac_sha256


def test_hmac_sha256_known_vector() -> None:
    assert hmac_sha256("key", "The quick brown fox jumps over the lazy dog") == (
        "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
    )
