import asyncio

import pytest

from async_api_optimizer.benchmark.mock_server import start_mock_server
from async_api_optimizer.config import MockServerConfig, PoolConfig
from async_api_optimizer.core.session_pool import SessionPool


@pytest.mark.asyncio
async def test_session_pool_get_happy_path() -> None:
    server = await start_mock_server(MockServerConfig())
    try:
        async with SessionPool(PoolConfig()) as pool:
            response = await pool.get(f"{server.base_url}/work")
            assert response.status == 200
            await response.read()
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_session_pool_without_enter_raises() -> None:
    pool = SessionPool(PoolConfig())
    with pytest.raises(RuntimeError):
        await pool.get("http://example.com")


@pytest.mark.asyncio
async def test_session_pool_cleanup_after_cancel() -> None:
    server = await start_mock_server(MockServerConfig())
    try:
        async with SessionPool(PoolConfig()) as pool:
            task = asyncio.create_task(pool.get(f"{server.base_url}/work"))
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert pool.is_closed
    finally:
        await server.close()
