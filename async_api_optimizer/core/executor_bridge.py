from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

import aiohttp

from ..config import ExecutorConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ExecutorBridge:
    def __init__(self, config: ExecutorConfig) -> None:
        self._config = config
        self._executor: ThreadPoolExecutor | None = None

    async def __aenter__(self) -> "ExecutorBridge":
        try:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(max_workers=self._config.max_workers)
            return self
        except asyncio.CancelledError:
            logger.warning("ExecutorBridge enter cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("ExecutorBridge enter failed", exc_info=True)
            raise

    async def __aexit__(self, *exc: object) -> None:
        try:
            if self._executor is not None:
                self._executor.shutdown(wait=True, cancel_futures=True)
        except asyncio.CancelledError:
            logger.warning("ExecutorBridge exit cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("ExecutorBridge exit failed", exc_info=True)
            raise
        finally:
            self._executor = None

    async def run(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        if self._executor is None:
            raise RuntimeError("ExecutorBridge is not initialized")
        loop = asyncio.get_running_loop()
        try:
            bound = functools.partial(func, *args, **kwargs)
            return await loop.run_in_executor(self._executor, bound)
        except asyncio.CancelledError:
            logger.warning("ExecutorBridge run cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("ExecutorBridge run failed", exc_info=True)
            raise
