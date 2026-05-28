from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import TypeVar

import aiohttp

from ..config import DispatcherConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BoundedDispatcher:
    def __init__(self, config: DispatcherConfig) -> None:
        self._config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrency)

    async def dispatch(self, coro_factory: Callable[[], Awaitable[T]]) -> T:
        loop = asyncio.get_running_loop()
        start_wait = loop.time()
        acquired = False
        try:
            await self._semaphore.acquire()
            acquired = True
            waited = loop.time() - start_wait
            if waited > self._config.warn_after:
                logger.warning("Dispatch waited %.3fs", waited)
            return await coro_factory()
        except asyncio.CancelledError:
            logger.warning("Dispatch cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("Dispatch failed", exc_info=True)
            raise
        finally:
            if acquired:
                self._semaphore.release()

    async def dispatch_many(self, coro_factories: Iterable[Callable[[], Awaitable[T]]]) -> list[T]:
        tasks = [asyncio.create_task(self.dispatch(factory)) for factory in coro_factories]
        try:
            return await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.warning("Dispatch many cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("Dispatch many failed", exc_info=True)
            raise

    @property
    def max_concurrency(self) -> int:
        return self._config.max_concurrency
