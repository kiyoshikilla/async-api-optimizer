from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TypeVar

import aiohttp

from ..config import CircuitBreakerConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(RuntimeError):
    pass


class CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig) -> None:
        self._config = config
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()
        self._half_open_lock = asyncio.Lock()

    async def __aenter__(self) -> "CircuitBreaker":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def call(self, coro_factory: Callable[[], Awaitable[T]]) -> T:
        try:
            await self._check_state()
            if self._state is CircuitBreakerState.HALF_OPEN:
                async with self._half_open_lock:
                    return await self._execute(coro_factory, half_open=True)
            return await self._execute(coro_factory, half_open=False)
        except asyncio.CancelledError:
            logger.warning("Circuit breaker call cancelled")
            raise

    async def _check_state(self) -> None:
        async with self._lock:
            if self._state is CircuitBreakerState.OPEN:
                if self._opened_at is None:
                    raise CircuitBreakerOpenError("Circuit breaker is open")
                now = asyncio.get_running_loop().time()
                if now - self._opened_at >= self._config.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                else:
                    raise CircuitBreakerOpenError("Circuit breaker is open")

    async def _execute(self, coro_factory: Callable[[], Awaitable[T]], *, half_open: bool) -> T:
        try:
            result = await coro_factory()
            await self._record_success(half_open)
            return result
        except asyncio.CancelledError:
            logger.warning("Circuit breaker execution cancelled")
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await self._record_failure(half_open)
            raise

    async def _record_success(self, half_open: bool) -> None:
        async with self._lock:
            self._failure_count = 0
            if half_open:
                self._state = CircuitBreakerState.CLOSED
                self._opened_at = None

    async def _record_failure(self, half_open: bool) -> None:
        async with self._lock:
            if half_open:
                self._state = CircuitBreakerState.OPEN
                self._opened_at = asyncio.get_running_loop().time()
                self._failure_count = self._config.failure_threshold
                return

            self._failure_count += 1
            if self._failure_count >= self._config.failure_threshold:
                self._state = CircuitBreakerState.OPEN
                self._opened_at = asyncio.get_running_loop().time()

    @property
    def state(self) -> CircuitBreakerState:
        return self._state
