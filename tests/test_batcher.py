import asyncio

import pytest

from async_api_optimizer.config import BatcherConfig
from async_api_optimizer.core.batcher import Batcher


@pytest.mark.asyncio
async def test_batcher_size_trigger() -> None:
    config = BatcherConfig(max_size=2, max_delay=1.0)
    async with Batcher(config) as batcher:
        await batcher.add(1)
        await batcher.add(2)
        batch = await batcher.next_batch()
    assert batch == [1, 2]


@pytest.mark.asyncio
async def test_batcher_flush_returns_pending() -> None:
    config = BatcherConfig(max_size=3, max_delay=1.0)
    async with Batcher(config) as batcher:
        await batcher.add(1)
        batch = await batcher.flush()
    assert batch == [1]


@pytest.mark.asyncio
async def test_batcher_cancellation_cleanup() -> None:
    config = BatcherConfig(max_size=2, max_delay=1.0)
    async with Batcher(config) as batcher:
        task = asyncio.create_task(batcher.next_batch())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await batcher.add(1)
        await batcher.add(2)
        batch = await batcher.next_batch()
    assert batch == [1, 2]
