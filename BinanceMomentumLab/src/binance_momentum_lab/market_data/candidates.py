"""Dynamic candidate symbol set with idempotent async notifications."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable

CandidateListener = Callable[[frozenset[str]], Awaitable[None]]


class DynamicCandidateManager:
    """Own the normalized candidate set and notify listeners only on changes."""

    def __init__(self) -> None:
        self._symbols: frozenset[str] = frozenset()
        self._listeners: list[CandidateListener] = []

    @property
    def symbols(self) -> frozenset[str]:
        return self._symbols

    def subscribe(self, listener: CandidateListener) -> None:
        self._listeners.append(listener)

    async def update(self, symbols: Iterable[str]) -> bool:
        normalized = frozenset(symbol.upper() for symbol in symbols)
        if normalized == self._symbols:
            return False
        self._symbols = normalized
        for listener in self._listeners:
            await listener(normalized)
        return True
