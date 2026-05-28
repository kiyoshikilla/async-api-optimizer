from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

from ..config import (
    BatcherConfig,
    DispatcherConfig,
    ExecutorConfig,
    MockServerConfig,
    PoolConfig,
    ScenarioConfig,
    DEFAULT_SCENARIOS,
)
from ..core.batcher import Batcher
from ..core.dispatcher import BoundedDispatcher
from ..core.executor_bridge import ExecutorBridge
from ..core.session_pool import SessionPool
from .metrics import LatencyMetrics
from .mock_server import start_mock_server

logger = logging.getLogger(__name__)


def _consume_bytes(data: bytes) -> int:
    return sum(data)


async def _naive_request(url: str, pool_config: PoolConfig) -> float:
    start = asyncio.get_running_loop().time()
    timeout = aiohttp.ClientTimeout(
        total=None,
        sock_connect=pool_config.connect_timeout,
        sock_read=pool_config.read_timeout,
    )
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            await response.read()
    return asyncio.get_running_loop().time() - start


async def _pooled_request(
    pool: SessionPool,
    url: str,
    executor: ExecutorBridge | None,
) -> float:
    start = asyncio.get_running_loop().time()
    response = await pool.get(url)
    data = await response.read()
    if executor is not None:
        await executor.run(_consume_bytes, data)
    return asyncio.get_running_loop().time() - start


async def _batched_request(
    pool: SessionPool,
    url: str,
    batch: list[int],
    executor: ExecutorBridge | None,
) -> tuple[float, int]:
    start = asyncio.get_running_loop().time()
    response = await pool.post(url, {"items": batch})
    data = await response.read()
    if executor is not None:
        await executor.run(_consume_bytes, data)
    return asyncio.get_running_loop().time() - start, len(batch)


async def _run_with_limit(
    factories: list[Callable[[], Awaitable[Any]]],
    limit: int,
) -> list[Any]:
    semaphore = asyncio.Semaphore(limit)

    async def run_one(factory: Callable[[], Awaitable[Any]]) -> Any:
        async with semaphore:
            return await factory()

    tasks = [asyncio.create_task(run_one(factory)) for factory in factories]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            raise result
    return list(results)


async def _run_with_dispatcher(
    dispatcher: BoundedDispatcher | None,
    factories: list[Callable[[], Awaitable[Any]]],
    limit: int,
) -> list[Any]:
    if dispatcher is None:
        return await _run_with_limit(factories, limit)
    return await dispatcher.dispatch_many(factories)


async def _run_scenario(
    base_url: str,
    scenario: ScenarioConfig,
    pool_config: PoolConfig,
    batcher_config: BatcherConfig,
    executor_config: ExecutorConfig,
) -> dict[str, float]:
    metrics = LatencyMetrics()
    url = f"{base_url}/work"
    batch_url = f"{base_url}/batch"

    if not scenario.use_pool:
        factories = [lambda: _naive_request(url, pool_config) for _ in range(scenario.run.n)]
        durations = await _run_with_dispatcher(None, factories, scenario.run.concurrency)
        for duration in durations:
            metrics.record(duration)
        return metrics.summary()

    dispatcher = (
        BoundedDispatcher(
            DispatcherConfig(
                max_concurrency=scenario.run.concurrency,
                warn_after=2.0,
            )
        )
        if scenario.use_dispatcher
        else None
    )

    async with SessionPool(pool_config) as pool:
        if scenario.use_batcher:
            async with Batcher(batcher_config) as batcher:
                executor: ExecutorBridge | None = None
                if scenario.use_executor:
                    executor = ExecutorBridge(executor_config)
                    await executor.__aenter__()
                try:
                    tasks: list[asyncio.Task[tuple[float, int]]] = []

                    async def producer() -> None:
                        for i in range(scenario.run.n):
                            await batcher.add(i)
                        await batcher.close()

                    async def consume_batches() -> None:
                        while True:
                            batch = await batcher.next_batch()
                            if not batch and batcher.is_closed:
                                return
                            if not batch:
                                continue

                            async def send_batch(batch_items: list[int] = batch) -> tuple[float, int]:
                                return await _batched_request(pool, batch_url, batch_items, executor)

                            if dispatcher is None:
                                tasks.append(asyncio.create_task(send_batch()))
                            else:
                                tasks.append(asyncio.create_task(dispatcher.dispatch(send_batch)))

                    await asyncio.gather(producer(), consume_batches())
                    results = await asyncio.gather(*tasks)
                    for duration, count in results:
                        for _ in range(count):
                            metrics.record(duration)
                finally:
                    if executor is not None:
                        await executor.__aexit__(None, None, None)
        else:
            executor = None
            if scenario.use_executor:
                executor = ExecutorBridge(executor_config)
                await executor.__aenter__()
            try:
                factories = [
                    lambda: _pooled_request(pool, url, executor)
                    for _ in range(scenario.run.n)
                ]
                durations = await _run_with_dispatcher(
                    dispatcher,
                    factories,
                    scenario.run.concurrency,
                )
                for duration in durations:
                    metrics.record(duration)
            finally:
                if executor is not None:
                    await executor.__aexit__(None, None, None)

    return metrics.summary()


def _average_summaries(summaries: list[dict[str, float]]) -> dict[str, float]:
    merged: dict[str, float] = {"count": 0.0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
    if not summaries:
        return merged
    for key in merged:
        merged[key] = sum(summary[key] for summary in summaries) / len(summaries)
    return merged


def _render_table(results: dict[str, dict[str, float]]) -> str:
    headers = ["scenario", "count", "avg", "p50", "p95", "p99"]
    rows = [headers]
    for name, metrics in results.items():
        rows.append([
            name,
            f"{metrics['count']:.0f}",
            f"{metrics['avg']*1000:.2f}ms",
            f"{metrics['p50']*1000:.2f}ms",
            f"{metrics['p95']*1000:.2f}ms",
            f"{metrics['p99']*1000:.2f}ms",
        ])
    widths = [max(len(row[i]) for row in rows) for i in range(len(headers))]
    lines = []
    for row in rows:
        line = " | ".join(value.ljust(widths[i]) for i, value in enumerate(row))
        lines.append(line)
    return "\n".join(lines)


async def run_benchmark(
    scenarios: tuple[ScenarioConfig, ...] = DEFAULT_SCENARIOS,
    pool_config: PoolConfig | None = None,
    batcher_config: BatcherConfig | None = None,
    executor_config: ExecutorConfig | None = None,
    mock_config: MockServerConfig | None = None,
) -> dict[str, dict[str, float]]:
    try:
        pool_config = pool_config or PoolConfig()
        batcher_config = batcher_config or BatcherConfig()
        executor_config = executor_config or ExecutorConfig()
        mock_config = mock_config or MockServerConfig()

        server = await start_mock_server(mock_config)
        try:
            results: dict[str, dict[str, float]] = {}
            for scenario in scenarios:
                logger.info("Running scenario %s", scenario.name)
                summaries = []
                for _ in range(scenario.run.repeats):
                    summary = await _run_scenario(
                        server.base_url,
                        scenario,
                        pool_config,
                        batcher_config,
                        executor_config,
                    )
                    summaries.append(summary)
                results[scenario.name] = _average_summaries(summaries)
            return results
        finally:
            await server.close()
    except asyncio.CancelledError:
        logger.warning("Benchmark cancelled")
        raise
    except aiohttp.ClientError:
        logger.error("Benchmark failed", exc_info=True)
        raise


def main() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    results = asyncio.run(run_benchmark())
    print(_render_table(results))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
