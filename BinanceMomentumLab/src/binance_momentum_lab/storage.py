"""DuckDB schema initialization and small phase-one repositories."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

import duckdb

from .paper.models import AccountSnapshot, PaperFill, PaperOrder, PaperPosition
from .scanner import Candidate
from .strategy.models import FeatureSnapshot, StrategySignal, StrategyState

SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS symbols (
        symbol VARCHAR PRIMARY KEY,
        status VARCHAR NOT NULL,
        contract_type VARCHAR NOT NULL,
        quote_asset VARCHAR NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_events (
        event_id VARCHAR PRIMARY KEY,
        symbol VARCHAR NOT NULL,
        event_type VARCHAR NOT NULL,
        event_time TIMESTAMPTZ NOT NULL,
        payload_json JSON NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS feature_snapshots (
        symbol VARCHAR NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        features_json JSON NOT NULL,
        PRIMARY KEY (symbol, observed_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS signals (
        signal_id VARCHAR PRIMARY KEY,
        symbol VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        signal_type VARCHAR NOT NULL,
        payload_json JSON NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_orders (
        order_id VARCHAR PRIMARY KEY,
        symbol VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        payload_json JSON NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_fills (
        fill_id VARCHAR PRIMARY KEY,
        order_id VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        filled_at TIMESTAMPTZ NOT NULL,
        payload_json JSON NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        symbol VARCHAR PRIMARY KEY,
        updated_at TIMESTAMPTZ NOT NULL,
        payload_json JSON NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS account_snapshots (
        observed_at TIMESTAMPTZ PRIMARY KEY,
        payload_json JSON NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_state (
        symbol VARCHAR PRIMARY KEY,
        state VARCHAR NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        payload_json JSON NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS system_health (
        component VARCHAR PRIMARY KEY,
        status VARCHAR NOT NULL,
        checked_at TIMESTAMPTZ NOT NULL,
        detail VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scanner_candidates (
        symbol VARCHAR PRIMARY KEY,
        last_price DECIMAL(38, 18) NOT NULL,
        price_change_24h_percent DECIMAL(38, 18) NOT NULL,
        quote_volume_24h DECIMAL(38, 18) NOT NULL,
        price_change_5m_percent DECIMAL(38, 18) NOT NULL,
        quote_volume_5m DECIMAL(38, 18) NOT NULL,
        volume_zscore DECIMAL(38, 18) NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL
    )
    """,
)


class DuckDBStore:
    """Synchronous DuckDB store guarded for async task/thread handoff."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        if str(path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(path))
        self._lock = Lock()

    def initialize(self) -> None:
        """Create all documented tables idempotently."""
        with self._lock:
            self._connection.execute("BEGIN TRANSACTION")
            try:
                for statement in SCHEMA_STATEMENTS:
                    self._connection.execute(statement)
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise

    def replace_candidates(self, candidates: Iterable[Candidate]) -> None:
        """Atomically replace the latest scanner candidate snapshot."""
        rows = [
            (
                item.symbol,
                item.last_price,
                item.price_change_24h_percent,
                item.quote_volume_24h,
                item.price_change_5m_percent,
                item.quote_volume_5m,
                item.volume_zscore,
                item.observed_at,
            )
            for item in candidates
        ]
        with self._lock:
            self._connection.execute("BEGIN TRANSACTION")
            try:
                self._connection.execute("DELETE FROM scanner_candidates")
                if rows:
                    self._connection.executemany(
                        """
                        INSERT INTO scanner_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise

    def list_candidates(self) -> list[dict[str, Any]]:
        """Return the latest candidates in deterministic rank order."""
        with self._lock:
            cursor = self._connection.execute(
                """
                SELECT * FROM scanner_candidates
                ORDER BY volume_zscore DESC, price_change_5m_percent DESC
                """
            )
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def record_health(self, component: str, status: str, detail: str | None = None) -> None:
        """Upsert one component health observation."""
        now = datetime.now(UTC)
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO system_health VALUES (?, ?, ?, ?)
                ON CONFLICT (component) DO UPDATE SET
                    status = excluded.status,
                    checked_at = excluded.checked_at,
                    detail = excluded.detail
                """,
                [component, status, now, detail],
            )

    def save_feature_snapshot(self, snapshot: FeatureSnapshot) -> None:
        payload = snapshot.model_dump_json()
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO feature_snapshots VALUES (?, ?, ?)
                ON CONFLICT (symbol, observed_at) DO UPDATE SET
                    features_json = excluded.features_json
                """,
                [snapshot.symbol, snapshot.timestamp, payload],
            )

    def save_signal(self, signal: StrategySignal) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO signals VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (signal_id) DO NOTHING
                """,
                [
                    signal.signal_id,
                    signal.symbol,
                    signal.timestamp,
                    signal.side.value,
                    signal.model_dump_json(),
                ],
            )

    def save_strategy_state(self, symbol: str, state: StrategyState, timestamp: datetime) -> None:
        payload = json.dumps({"state": state.value})
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO strategy_state VALUES (?, ?, ?, ?)
                ON CONFLICT (symbol) DO UPDATE SET
                    state = excluded.state,
                    updated_at = excluded.updated_at,
                    payload_json = excluded.payload_json
                """,
                [symbol, state.value, timestamp, payload],
            )

    def list_signals(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT payload_json FROM signals
                ORDER BY created_at DESC LIMIT ?
                """,
                [limit],
            ).fetchall()
        return [json.loads(str(row[0])) for row in rows]

    def save_paper_order(self, order: PaperOrder) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO paper_orders VALUES (?, ?, ?, ?)
                ON CONFLICT (order_id) DO UPDATE SET payload_json = excluded.payload_json
                """,
                [order.order_id, order.symbol, order.created_at, order.model_dump_json()],
            )

    def save_paper_fill(self, fill: PaperFill) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO paper_fills VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (fill_id) DO NOTHING
                """,
                [
                    fill.fill_id,
                    fill.order_id,
                    fill.symbol,
                    fill.timestamp,
                    fill.model_dump_json(),
                ],
            )

    def save_paper_position(self, position: PaperPosition | None, symbol: str) -> None:
        with self._lock:
            if position is None:
                self._connection.execute("DELETE FROM positions WHERE symbol = ?", [symbol])
                return
            payload = json.dumps(asdict(position), default=str)
            self._connection.execute(
                """
                INSERT INTO positions VALUES (?, ?, ?)
                ON CONFLICT (symbol) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    payload_json = excluded.payload_json
                """,
                [position.symbol, position.opened_at, payload],
            )

    def save_account_snapshot(self, snapshot: AccountSnapshot) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO account_snapshots VALUES (?, ?)
                ON CONFLICT (observed_at) DO UPDATE SET payload_json = excluded.payload_json
                """,
                [snapshot.timestamp, snapshot.model_dump_json()],
            )

    def list_paper_orders(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._list_json("paper_orders", "created_at", limit)

    def list_paper_fills(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._list_json("paper_fills", "filled_at", limit)

    def list_positions(self) -> list[dict[str, Any]]:
        return self._list_json("positions", "updated_at", 100)

    def list_account_snapshots(self, limit: int = 1000) -> list[dict[str, Any]]:
        return self._list_json("account_snapshots", "observed_at", limit)

    def reset_paper(self) -> None:
        with self._lock:
            self._connection.execute("BEGIN TRANSACTION")
            try:
                for table in ("paper_orders", "paper_fills", "positions", "account_snapshots"):
                    self._connection.execute(f"DELETE FROM {table}")
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise

    def _list_json(self, table: str, order_column: str, limit: int) -> list[dict[str, Any]]:
        allowed = {
            ("paper_orders", "created_at"),
            ("paper_fills", "filled_at"),
            ("positions", "updated_at"),
            ("account_snapshots", "observed_at"),
        }
        if (table, order_column) not in allowed:
            raise ValueError("Unsupported repository query")
        with self._lock:
            rows = self._connection.execute(
                f"SELECT payload_json FROM {table} ORDER BY {order_column} DESC LIMIT ?",
                [limit],
            ).fetchall()
        return [json.loads(str(row[0])) for row in rows]

    def table_names(self) -> set[str]:
        """Expose initialized table names for diagnostics and tests."""
        with self._lock:
            rows = self._connection.execute("SHOW TABLES").fetchall()
            return {str(row[0]) for row in rows}

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            self._connection.close()
