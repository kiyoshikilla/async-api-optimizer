from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp
from aiohttp import web

from ..config import MockServerConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MockServerHandle:
    base_url: str
    close: Callable[[], Awaitable[None]]


def create_app(config: MockServerConfig) -> web.Application:
    try:
        app = web.Application()

        async def work_handler(request: web.Request) -> web.Response:
            delay_ms = random.randint(config.min_delay_ms, config.max_delay_ms)
            await asyncio.sleep(delay_ms / 1000.0)
            logger.debug("Mock work delay %sms", delay_ms)
            return web.json_response({"ok": True, "delay_ms": delay_ms})

        async def batch_handler(request: web.Request) -> web.Response:
            payload: Any = await request.json()
            if isinstance(payload, dict) and "items" in payload:
                count = len(payload["items"])
            elif isinstance(payload, list):
                count = len(payload)
            else:
                count = 1
            delay_ms = random.randint(config.min_delay_ms, config.max_delay_ms)
            await asyncio.sleep(delay_ms / 1000.0)
            logger.debug("Mock batch delay %sms", delay_ms)
            return web.json_response({"ok": True, "count": count, "delay_ms": delay_ms})

        app.router.add_get("/work", work_handler)
        app.router.add_post("/batch", batch_handler)
        return app
    except asyncio.CancelledError:
        logger.warning("Mock server create_app cancelled")
        raise
    except aiohttp.ClientError:
        logger.error("Mock server create_app failed", exc_info=True)
        raise


async def start_mock_server(config: MockServerConfig) -> MockServerHandle:
    try:
        app = create_app(config)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        addresses = runner.addresses
        port = addresses[0][1]

        async def _close() -> None:
            await runner.cleanup()

        return MockServerHandle(base_url=f"http://127.0.0.1:{port}", close=_close)
    except asyncio.CancelledError:
        logger.warning("Mock server start cancelled")
        raise
    except aiohttp.ClientError:
        logger.error("Mock server start failed", exc_info=True)
        raise
