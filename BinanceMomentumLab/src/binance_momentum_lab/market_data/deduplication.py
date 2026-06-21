"""Bounded duplicate suppression for reconnect overlap and repeated events."""

from collections import OrderedDict


class BoundedDeduplicator:
    """Remember a fixed number of event identities without unbounded growth."""

    def __init__(self, capacity: int = 100_000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._seen: OrderedDict[str, None] = OrderedDict()

    def accept(self, identity: str) -> bool:
        if identity in self._seen:
            self._seen.move_to_end(identity)
            return False
        self._seen[identity] = None
        if len(self._seen) > self._capacity:
            self._seen.popitem(last=False)
        return True
