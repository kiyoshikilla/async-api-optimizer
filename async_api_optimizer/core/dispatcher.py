from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import TypeVar

import aiohttp

from ..config import AdaptiveDispatcherConfig, DispatcherConfig

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


class AdaptiveDispatcher:
    def __init__(self, config: AdaptiveDispatcherConfig) -> None:
        self._config = config
        self._current_limit = config.min_concurrency
        self._in_flight = 0
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)

    async def dispatch(self, coro_factory: Callable[[], Awaitable[T]]) -> T:
        loop = asyncio.get_running_loop()
        wait_start = loop.time()
        waited = 0.0
        acquired = False
        try:
            async with self._condition:
                while self._in_flight >= self._current_limit:
                    await self._condition.wait()
                waited = loop.time() - wait_start
                self._in_flight += 1
                acquired = True
            if waited > self._config.warn_after:
                logger.warning("Dispatch waited %.3fs", waited)

            start = loop.time()
            result = await coro_factory()
            elapsed = loop.time() - start
            await self._adjust_on_success(elapsed)
            return result
        except asyncio.CancelledError:
            logger.warning("Adaptive dispatch cancelled")
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await self._adjust_on_error()
            logger.error("Adaptive dispatch failed", exc_info=True)
            raise
        except Exception:
            await self._adjust_on_error()
            logger.error("Adaptive dispatch error", exc_info=True)
            raise
        finally:
            async with self._condition:
                if acquired:
                    self._in_flight -= 1
                    self._condition.notify_all()

    async def dispatch_many(self, coro_factories: Iterable[Callable[[], Awaitable[T]]]) -> list[T]:
        tasks = [asyncio.create_task(self.dispatch(factory)) for factory in coro_factories]
        try:
            return await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.warning("Adaptive dispatch many cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("Adaptive dispatch many failed", exc_info=True)
            raise

    async def _adjust_on_success(self, elapsed: float) -> None:
        if elapsed < self._config.target_latency:
            async with self._condition:
                if self._current_limit < self._config.max_concurrency:
                    self._current_limit += 1
                    self._condition.notify_all()
        elif elapsed > self._config.critical_latency:
            await self._adjust_on_error()

    async def _adjust_on_error(self) -> None:
        async with self._condition:
            new_limit = max(self._config.min_concurrency, self._current_limit // 2)
            if new_limit < self._current_limit:
                self._current_limit = new_limit
                self._condition.notify_all()

    @property
    def current_limit(self) -> int:
        return self._current_limit

    @property
    def max_concurrency(self) -> int:
        return self._config.max_concurrency
