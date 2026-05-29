import aiohttp
import pytest

from async_api_optimizer.config import RetryConfig
from async_api_optimizer.core.retry import with_retry


@pytest.mark.asyncio
async def test_with_retry_succeeds_after_retries() -> None:
    attempts = 0

    async def flaky() -> int:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise aiohttp.ClientError("boom")
        return 42

    config = RetryConfig(max_attempts=3, base_delay=0.001, jitter=0.0)
    result = await with_retry(flaky, config)
    assert result == 42
    assert attempts == 3


@pytest.mark.asyncio
async def test_with_retry_exhausted() -> None:
    attempts = 0

    async def always_fail() -> int:
        nonlocal attempts
        attempts += 1
        raise aiohttp.ClientError("boom")

    config = RetryConfig(max_attempts=2, base_delay=0.001, jitter=0.0)
    with pytest.raises(aiohttp.ClientError):
        await with_retry(always_fail, config)
    assert attempts == 2


@pytest.mark.asyncio
async def test_with_retry_non_retry_error() -> None:
    attempts = 0

    async def fail() -> int:
        nonlocal attempts
        attempts += 1
        raise ValueError("no retry")

    config = RetryConfig(max_attempts=3, base_delay=0.001, jitter=0.0)
    with pytest.raises(ValueError):
        await with_retry(fail, config)
    assert attempts == 1
