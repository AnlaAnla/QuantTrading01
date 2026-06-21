"""Buffered raw-event writer using date/symbol partitioned Parquet files."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import duckdb

from .events import ParsedEnvelope


@dataclass(frozen=True, slots=True)
class RawEvent:
    event_time: datetime
    received_at: datetime
    symbol: str
    stream: str
    route: str
    payload_json: str

    @classmethod
    def from_envelope(cls, envelope: ParsedEnvelope, received_at: datetime) -> RawEvent:
        return cls(
            event_time=envelope.event.event_time,
            received_at=received_at,
            symbol=envelope.event.symbol,
            stream=envelope.stream,
            route=envelope.route.value,
            payload_json=json.dumps(envelope.raw_data, separators=(",", ":")),
        )


class ParquetEventWriter:
    """Flush bounded event batches to independent partition files."""

    def __init__(self, root: Path, batch_size: int, flush_seconds: float) -> None:
        self.root = root
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self._queue: asyncio.Queue[RawEvent | None] = asyncio.Queue(maxsize=batch_size * 2)
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="parquet-event-writer")

    async def write(self, event: RawEvent) -> None:
        await self._queue.put(event)

    async def stop(self) -> None:
        if self._task is not None:
            await self._queue.put(None)
            await self._task

    async def _run(self) -> None:
        batch: list[RawEvent] = []
        while True:
            stopping = False
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=self.flush_seconds)
            except TimeoutError:
                item = None
            else:
                stopping = item is None
            if item is not None:
                batch.append(item)
            if batch and (item is None or len(batch) >= self.batch_size):
                await asyncio.to_thread(self._flush, batch)
                batch = []
            if stopping:
                return

    def _flush(self, batch: list[RawEvent]) -> None:
        grouped: dict[tuple[str, str], list[RawEvent]] = {}
        for event in batch:
            key = (event.event_time.date().isoformat(), event.symbol)
            grouped.setdefault(key, []).append(event)
        for (date, symbol), events in grouped.items():
            directory = self.root / f"date={date}" / f"symbol={symbol}"
            directory.mkdir(parents=True, exist_ok=True)
            target = directory / f"part-{uuid4().hex}.parquet"
            connection = duckdb.connect(":memory:")
            try:
                connection.execute(
                    """
                    CREATE TABLE raw_events (
                        event_time TIMESTAMPTZ, received_at TIMESTAMPTZ,
                        symbol VARCHAR, stream VARCHAR, route VARCHAR, payload_json JSON
                    )
                    """
                )
                connection.executemany(
                    "INSERT INTO raw_events VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        (
                            event.event_time,
                            event.received_at,
                            event.symbol,
                            event.stream,
                            event.route,
                            event.payload_json,
                        )
                        for event in events
                    ],
                )
                escaped = str(target).replace("'", "''")
                connection.execute(f"COPY raw_events TO '{escaped}' (FORMAT PARQUET)")
            finally:
                connection.close()
