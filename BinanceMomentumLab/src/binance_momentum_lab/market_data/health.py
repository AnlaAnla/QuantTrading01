"""In-memory WebSocket latency, ordering, and connectivity health."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from .routes import StreamRoute


@dataclass(slots=True)
class ComponentHealth:
    status: str = "disconnected"
    connected_at: datetime | None = None
    last_event_at: datetime | None = None
    latency_ms: float | None = None
    detail: str | None = None


class MarketDataHealth:
    """Track route health and degrade on stale or out-of-order data."""

    def __init__(self, stale_after_seconds: float) -> None:
        self._stale_after_ms = stale_after_seconds * 1000
        self._routes = {route: ComponentHealth() for route in StreamRoute}
        self._last_event_ms: dict[tuple[str, str], int] = {}
        self.order_books: dict[str, ComponentHealth] = {}

    def connected(self, route: StreamRoute) -> None:
        self._routes[route] = ComponentHealth(status="connected", connected_at=datetime.now(UTC))

    def disconnected(self, route: StreamRoute, detail: str) -> None:
        self._routes[route].status = "disconnected"
        self._routes[route].detail = detail

    def observe(
        self,
        route: StreamRoute,
        stream: str,
        event_time_ms: int,
        received_at: datetime | None = None,
    ) -> None:
        received = received_at or datetime.now(UTC)
        latency_ms = received.timestamp() * 1000 - event_time_ms
        component = self._routes[route]
        component.last_event_at = received
        component.latency_ms = latency_ms
        key = (route.value, stream)
        previous = self._last_event_ms.get(key)
        if previous is not None and event_time_ms < previous:
            component.status = "degraded"
            component.detail = f"out_of_order event={event_time_ms} previous={previous}"
            return
        self._last_event_ms[key] = event_time_ms
        if latency_ms > self._stale_after_ms:
            component.status = "degraded"
            component.detail = f"stale latency_ms={latency_ms:.0f}"
        else:
            component.status = "healthy"
            component.detail = None

    def order_book_synced(self, symbol: str) -> None:
        self.order_books[symbol] = ComponentHealth(status="healthy")

    def order_book_gap(self, symbol: str, detail: str) -> None:
        self.order_books[symbol] = ComponentHealth(status="degraded", detail=detail)

    def evaluate_staleness(self, now: datetime | None = None) -> None:
        """Degrade connected routes that remain silent past the configured threshold."""
        observed_at = now or datetime.now(UTC)
        for component in self._routes.values():
            reference = component.last_event_at or component.connected_at
            if component.status == "disconnected" or reference is None:
                continue
            silence_ms = (observed_at - reference).total_seconds() * 1000
            if silence_ms > self._stale_after_ms:
                component.status = "degraded"
                component.detail = f"silent latency_ms={silence_ms:.0f}"

    def snapshot(self) -> dict[str, object]:
        return {
            "routes": {route.value: asdict(value) for route, value in self._routes.items()},
            "order_books": {symbol: asdict(value) for symbol, value in self.order_books.items()},
        }
