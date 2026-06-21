from datetime import UTC, datetime
from decimal import Decimal

from binance_momentum_lab.scanner import Candidate
from binance_momentum_lab.storage import DuckDBStore


def test_initializes_all_required_tables_and_replaces_candidates() -> None:
    store = DuckDBStore(":memory:")
    store.initialize()
    required = {
        "symbols",
        "market_events",
        "feature_snapshots",
        "signals",
        "paper_orders",
        "paper_fills",
        "positions",
        "account_snapshots",
        "strategy_state",
        "system_health",
    }
    assert required <= store.table_names()

    candidate = Candidate(
        symbol="ALPHAUSDT",
        last_price=Decimal("1.23"),
        price_change_24h_percent=Decimal("6"),
        quote_volume_24h=Decimal("200000000"),
        price_change_5m_percent=Decimal("3"),
        quote_volume_5m=Decimal("500000"),
        volume_zscore=Decimal("4.2"),
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    store.replace_candidates([candidate])

    assert store.list_candidates()[0]["symbol"] == "ALPHAUSDT"
    store.close()
