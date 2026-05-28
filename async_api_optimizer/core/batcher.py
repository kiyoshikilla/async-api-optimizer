from __future__ import annotations

import asyncio
import logging
from typing import Generic, TypeVar

import aiohttp

from ..config import BatcherConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


class Batcher(Generic[T]):
    def __init__(self, config: BatcherConfig) -> None:
        self._config = config
        self._queue: asyncio.Queue[T] = asyncio.Queue()
        self._closed = False

    async def __aenter__(self) -> "Batcher[T]":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def add(self, item: T) -> None:
        try:
            if self._closed:
                raise RuntimeError("Batcher is closed")
            await self._queue.put(item)
        except asyncio.CancelledError:
            logger.warning("Batch add cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("Batch add failed", exc_info=True)
            raise

    async def next_batch(self) -> list[T]:
        try:
            while True:
                if self._closed and self._queue.empty():
                    return []

                try:
                    first = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=self._config.max_delay,
                    )
                except asyncio.TimeoutError:
                    if self._closed and self._queue.empty():
                        return []
                    continue

                batch = [first]
                loop = asyncio.get_running_loop()
                deadline = loop.time() + self._config.max_delay
                while len(batch) < self._config.max_size:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        item = await asyncio.wait_for(
                            self._queue.get(),
                            timeout=remaining,
                        )
                        batch.append(item)
                    except asyncio.TimeoutError:
                        break
                return batch
        except asyncio.CancelledError:
            logger.warning("Batch wait cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("Batch wait failed", exc_info=True)
            raise

    async def flush(self) -> list[T]:
        try:
            batch: list[T] = []
            while True:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            return batch
        except asyncio.CancelledError:
            logger.warning("Batch flush cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("Batch flush failed", exc_info=True)
            raise

    async def close(self) -> None:
        try:
            self._closed = True
        except asyncio.CancelledError:
            logger.warning("Batch close cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("Batch close failed", exc_info=True)
            raise

    @property
    def is_closed(self) -> bool:
        return self._closed
