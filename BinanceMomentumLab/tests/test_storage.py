from datetime import UTC, datetime
from decimal import Decimal

from binance_momentum_lab.config import Settings
from binance_momentum_lab.paper.broker import PaperBroker
from binance_momentum_lab.paper.risk import RiskDecision, RiskManager
from binance_momentum_lab.scanner import Candidate
from binance_momentum_lab.storage import DuckDBStore
from binance_momentum_lab.strategy.replay import HistoricalFeatureReplay
from binance_momentum_lab.strategy.state_machine import StrategyStateMachine
from tests.test_paper_broker import market
from tests.test_risk_manager import long_signal
from tests.test_strategy_state_machine import replay_events


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
        "demo_positions",
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


def test_persists_feature_snapshots_signals_and_strategy_state() -> None:
    store = DuckDBStore(":memory:")
    store.initialize()
    results, signals = HistoricalFeatureReplay(StrategyStateMachine(Settings(_env_file=None))).run(
        replay_events()
    )
    signal = signals[0]

    store.save_feature_snapshot(signal.feature_snapshot)
    store.save_signal(signal)
    store.save_signal(signal)
    store.save_strategy_state(signal.symbol, results[-1].state, signal.timestamp)

    saved = store.list_signals()
    assert len(saved) == 1
    assert saved[0]["signal_id"] == signal.signal_id
    assert saved[0]["reason_codes"]
    store.close()


def test_persists_paper_orders_fills_positions_and_equity_curve() -> None:
    store = DuckDBStore(":memory:")
    store.initialize()
    settings = Settings(_env_file=None, paper_network_latency_ms=0)
    broker = PaperBroker(settings, RiskManager(settings), store)
    signal = long_signal()
    broker.submit_entry(signal, RiskDecision(True, Decimal("2")), signal.timestamp)
    broker.on_market(market(signal, 0))

    assert len(store.list_paper_orders()) == 3
    assert len(store.list_paper_fills()) == 1
    assert len(store.list_positions()) == 1
    assert len(store.list_account_snapshots()) == 1

    broker.on_market(market(signal, 100, bid="103", ask="103.02"))
    assert len(store.list_paper_fills()) == 2
    assert store.list_positions() == []
    assert len(store.list_account_snapshots()) == 2
    store.close()
