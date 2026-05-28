import asyncio
import time

import pytest

from async_api_optimizer.config import DispatcherConfig, ExecutorConfig
from async_api_optimizer.core.dispatcher import BoundedDispatcher
from async_api_optimizer.core.executor_bridge import ExecutorBridge


@pytest.mark.asyncio
async def test_dispatcher_happy_path() -> None:
    dispatcher = BoundedDispatcher(DispatcherConfig(max_concurrency=2))
    result = await dispatcher.dispatch(lambda: asyncio.sleep(0.01, result=1))
    assert result == 1


@pytest.mark.asyncio
async def test_dispatcher_concurrency_limit() -> None:
    dispatcher = BoundedDispatcher(DispatcherConfig(max_concurrency=2))
    gate = asyncio.Event()
    lock = asyncio.Lock()
    current = 0
    max_seen = 0

    async def work() -> int:
        nonlocal current, max_seen
        async with lock:
            current += 1
            max_seen = max(max_seen, current)
        await gate.wait()
        async with lock:
            current -= 1
        return 1

    tasks = [asyncio.create_task(dispatcher.dispatch(work)) for _ in range(3)]
    await asyncio.sleep(0.01)
    gate.set()
    results = await asyncio.gather(*tasks)
    assert results == [1, 1, 1]
    assert max_seen <= 2


@pytest.mark.asyncio
async def test_dispatcher_cancellation_releases() -> None:
    dispatcher = BoundedDispatcher(DispatcherConfig(max_concurrency=1))
    gate = asyncio.Event()

    async def work() -> int:
        await gate.wait()
        return 1

    task = asyncio.create_task(dispatcher.dispatch(work))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    gate.set()
    result = await dispatcher.dispatch(lambda: asyncio.sleep(0.01, result=2))
    assert result == 2


@pytest.mark.asyncio
async def test_executor_bridge_happy_path() -> None:
    async with ExecutorBridge(ExecutorConfig(max_workers=2)) as bridge:
        result = await bridge.run(lambda x, y: x + y, 1, 2)
    assert result == 3


@pytest.mark.asyncio
async def test_executor_bridge_requires_init() -> None:
    bridge = ExecutorBridge(ExecutorConfig(max_workers=1))
    with pytest.raises(RuntimeError):
        await bridge.run(lambda: 1)


@pytest.mark.asyncio
async def test_executor_bridge_cancellation() -> None:
    async with ExecutorBridge(ExecutorConfig(max_workers=1)) as bridge:
        task = asyncio.create_task(bridge.run(time.sleep, 0.05))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
