import asyncio

import aiohttp
import pytest

from async_api_optimizer.config import CircuitBreakerConfig
from async_api_optimizer.core.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitBreakerState,
)


@pytest.mark.asyncio
async def test_circuit_breaker_allows_success() -> None:
    breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.05))

    async def ok() -> int:
        return 1

    result = await breaker.call(ok)
    assert result == 1
    assert breaker.state is CircuitBreakerState.CLOSED


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_failures() -> None:
    breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.05))

    async def fail() -> int:
        raise aiohttp.ClientError("boom")

    with pytest.raises(aiohttp.ClientError):
        await breaker.call(fail)
    with pytest.raises(aiohttp.ClientError):
        await breaker.call(fail)

    with pytest.raises(CircuitBreakerOpenError):
        await breaker.call(fail)
    assert breaker.state is CircuitBreakerState.OPEN


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_recovers() -> None:
    breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.01))

    async def fail() -> int:
        raise aiohttp.ClientError("boom")

    async def ok() -> int:
        return 2

    with pytest.raises(aiohttp.ClientError):
        await breaker.call(fail)

    await asyncio.sleep(0.02)
    result = await breaker.call(ok)
    assert result == 2
    assert breaker.state is CircuitBreakerState.CLOSED
