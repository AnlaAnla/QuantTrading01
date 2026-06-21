from decimal import Decimal

import pytest
from pydantic import ValidationError

from binance_momentum_lab.config import AppMode, Settings, mask_api_key
from binance_momentum_lab.exceptions import (
    DemoCredentialsMissingError,
    DemoTradingUnavailableError,
    LiveTradingDisabledError,
)


def test_default_configuration_is_safe_paper_mode() -> None:
    settings = Settings(_env_file=None)

    settings.startup_safety_check()

    assert settings.app_mode is AppMode.PAPER
    assert settings.paper_initial_balance == Decimal("10000")
    assert "secret" not in settings.public_config


def test_live_mode_is_always_rejected_even_with_flag() -> None:
    settings = Settings(
        _env_file=None,
        app_mode="LIVE",
        live_trading_enabled=True,
    )

    with pytest.raises(LiveTradingDisabledError, match="hard-disabled"):
        settings.startup_safety_check()


def test_demo_mode_requires_explicit_enablement_and_environment_credentials() -> None:
    settings = Settings(_env_file=None, app_mode="DEMO")

    with pytest.raises(DemoTradingUnavailableError, match="explicit"):
        settings.startup_safety_check()

    enabled = Settings(_env_file=None, app_mode="DEMO", demo_trading_enabled=True)
    with pytest.raises(DemoCredentialsMissingError, match="BINANCE_API_KEY"):
        enabled.startup_safety_check()

    configured = Settings(
        _env_file=None,
        app_mode="DEMO",
        demo_trading_enabled=True,
        binance_api_key="demo-key",
        binance_api_secret="demo-secret",
    )
    configured.startup_safety_check()


def test_demo_endpoints_are_immutable() -> None:
    with pytest.raises(ValidationError, match=r"demo-fapi\.binance\.com"):
        Settings(_env_file=None, binance_demo_rest_url="https://fapi.binance.com")
    with pytest.raises(ValidationError, match=r"fstream\.binancefuture\.com"):
        Settings(_env_file=None, binance_demo_ws_url="wss://fstream.binance.com")


def test_api_key_masking() -> None:
    assert mask_api_key("") == ""
    assert mask_api_key("12345678") == "****"
    assert mask_api_key("abcd12345678wxyz") == "abcd...wxyz"


def test_websocket_url_must_use_tls() -> None:
    with pytest.raises(ValidationError, match="wss://"):
        Settings(_env_file=None, binance_mainnet_ws_url="ws://example.test")
