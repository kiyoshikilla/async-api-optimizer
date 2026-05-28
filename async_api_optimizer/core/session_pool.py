from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from ..config import PoolConfig

logger = logging.getLogger(__name__)


class SessionPool:
    """
    Manages a single shared aiohttp.ClientSession with a
    configured TCPConnector. Must be used as an async
    context manager. Thread-unsafe by design - one pool
    per event loop.
    """

    def __init__(self, config: PoolConfig) -> None:
        self._config = config
        self._connector = aiohttp.TCPConnector(
            limit=config.limit,
            limit_per_host=config.limit_per_host,
            ttl_dns_cache=config.ttl_dns_cache,
            keepalive_timeout=config.keepalive_timeout,
        )
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "SessionPool":
        try:
            if self._session is not None and not self._session.closed:
                return self
            timeout = aiohttp.ClientTimeout(
                total=None,
                sock_connect=self._config.connect_timeout,
                sock_read=self._config.read_timeout,
            )
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=timeout,
            )
            return self
        except asyncio.CancelledError:
            logger.warning("SessionPool enter cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("SessionPool enter failed", exc_info=True)
            raise

    async def __aexit__(self, *exc: object) -> None:
        try:
            if self._session is not None and not self._session.closed:
                await self._session.close()
        except asyncio.CancelledError:
            logger.warning("SessionPool exit cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("SessionPool exit failed", exc_info=True)
            raise
        finally:
            try:
                await self._connector.close()
            except asyncio.CancelledError:
                logger.warning("SessionPool connector close cancelled")
                raise
            except aiohttp.ClientError:
                logger.error("SessionPool connector close failed", exc_info=True)
                raise

    async def get(
        self,
        url: str,
        *,
        timeout: float | None = None,
    ) -> aiohttp.ClientResponse:
        if self._session is None or self._session.closed:
            raise RuntimeError("SessionPool is not initialized")
        try:
            request_timeout = (
                aiohttp.ClientTimeout(total=timeout) if timeout is not None else None
            )
            logger.debug("GET %s", url)
            return await self._session.get(url, timeout=request_timeout)
        except asyncio.CancelledError:
            logger.warning("GET cancelled: %s", url)
            raise
        except aiohttp.ClientError:
            logger.error("GET failed: %s", url, exc_info=True)
            raise

    async def post(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> aiohttp.ClientResponse:
        if self._session is None or self._session.closed:
            raise RuntimeError("SessionPool is not initialized")
        try:
            request_timeout = (
                aiohttp.ClientTimeout(total=timeout) if timeout is not None else None
            )
            logger.debug("POST %s", url)
            return await self._session.post(
                url,
                json=payload,
                timeout=request_timeout,
            )
        except asyncio.CancelledError:
            logger.warning("POST cancelled: %s", url)
            raise
        except aiohttp.ClientError:
            logger.error("POST failed: %s", url, exc_info=True)
            raise

    @property
    def active_connections(self) -> int:
        acquired = getattr(self._connector, "_acquired", None)
        if acquired is None:
            return 0
        return len(acquired)

    @property
    def is_closed(self) -> bool:
        return self._session is None or self._session.closed
