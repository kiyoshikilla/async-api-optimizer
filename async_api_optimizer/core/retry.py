from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

import aiohttp

from ..config import RetryConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def with_retry(
    coro_factory: Callable[[], Awaitable[T]],
    config: RetryConfig,
) -> T:
    attempt = 0
    while True:
        try:
            return await coro_factory()
        except asyncio.CancelledError:
            logger.warning("Retry cancelled")
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            attempt += 1
            if attempt >= config.max_attempts:
                logger.error("Retry exhausted", exc_info=True)
                raise exc
            delay = config.base_delay * (2 ** (attempt - 1))
            delay += random.uniform(0.0, config.jitter)
            logger.warning("Retrying after %.3fs", delay)
            await asyncio.sleep(delay)
