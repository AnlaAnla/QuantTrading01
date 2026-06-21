"""Compatibility entrypoint for tooling that targets the conventional ``app`` path."""

from binance_momentum_lab.api.app import create_app

__all__ = ["create_app"]
