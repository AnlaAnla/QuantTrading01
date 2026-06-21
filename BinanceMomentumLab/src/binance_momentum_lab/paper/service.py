"""Bridge current public quotes and research signals into local paper execution."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from ..config import Settings
from ..market_data.events import BookTickerEvent, MarkPriceEvent, ParsedEnvelope
from ..market_data.health import MarketDataHealth
from ..market_data.order_book import LocalOrderBook
from ..strategy.models import StrategySignal
from ..strategy.service import StrategyFeatureService
from .broker import PaperBroker
from .models import MarketSnapshot
from .risk import RiskEnvironment


class PaperExecutionService:
    """Paper-only adapter; consumes public market data and never calls Binance trading APIs."""

    def __init__(
        self,
        settings: Settings,
        broker: PaperBroker,
        strategy: StrategyFeatureService,
        health: MarketDataHealth,
        order_books: dict[str, LocalOrderBook],
    ) -> None:
        self._settings = settings
        self.broker = broker
        self._strategy = strategy
        self._health = health
        self._order_books = order_books
        self._latest_markets: dict[str, MarketSnapshot] = {}
        self._next_funding_time: dict[str, int] = {}
        self._pending_funding_id: dict[str, str] = {}

    async def on_event(self, envelope: ParsedEnvelope) -> None:
        event = envelope.event
        if isinstance(event, MarkPriceEvent):
            previous = self._next_funding_time.get(event.symbol)
            if (
                previous is not None
                and event.next_funding_time_ms != previous
                and event.event_time_ms >= previous
            ):
                self._pending_funding_id[event.symbol] = str(previous)
            self._next_funding_time[event.symbol] = event.next_funding_time_ms
            return
        if not isinstance(event, BookTickerEvent):
            return
        state = self._strategy.engine.state_for(event.symbol)
        volatility = ZERO
        try:
            features = self._strategy.engine.snapshot(
                event.symbol,
                timestamp=event.event_time,
                order_book=self._order_books.get(event.symbol),
            )
            volatility = abs(features.return_1m_percent or ZERO)
        except ValueError:
            pass
        funding_id = self._pending_funding_id.pop(event.symbol, None)
        market = MarketSnapshot(
            symbol=event.symbol,
            timestamp=event.event_time,
            bid_price=event.best_bid_price,
            bid_quantity=event.best_bid_quantity,
            ask_price=event.best_ask_price,
            ask_quantity=event.best_ask_quantity,
            order_book=self._order_books.get(event.symbol),
            volatility_percent=volatility,
            funding_rate=state.funding_rate or ZERO,
            funding_event_id=funding_id,
        )
        self._latest_markets[event.symbol] = market
        self.broker.on_market(market)

    async def on_signal(self, signal: StrategySignal) -> None:
        market = self._latest_markets.get(signal.symbol)
        if market is None:
            return
        health = self._health.snapshot()
        routes = health.get("routes")
        websocket_healthy = False
        if isinstance(routes, dict):
            websocket_healthy = all(
                isinstance(value, dict) and value.get("status") == "healthy"
                for value in routes.values()
            )
        book = self._order_books.get(signal.symbol)
        environment = RiskEnvironment(
            timestamp=signal.timestamp,
            market_data_timestamp=market.timestamp,
            websocket_healthy=websocket_healthy,
            order_book_synchronized=book is not None and book.synchronized,
            spread_bps=market.spread_bps,
        )
        decision = self.broker.risk_manager.evaluate_entry(
            signal,
            environment,
            equity=self.broker.equity(),
            open_symbols=set(self.broker.positions),
        )
        self.broker.submit_entry(signal, decision, signal.timestamp)

    def emergency_stop(self, timestamp: datetime | None = None) -> None:
        self.broker.emergency_close_all(timestamp or datetime.now(UTC))


ZERO = Decimal(0)
