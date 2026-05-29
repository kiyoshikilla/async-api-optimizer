# Async API Optimizer

Async API Optimizer provides a small async toolkit for measuring latency improvements from connection pooling,
bounded dispatch, request batching, and resilience mechanisms (circuit breaker, adaptive backpressure, retry).
The project targets Python 3.11+ and uses only stdlib, aiohttp, and pytest-asyncio.

## Benchmark

Run the benchmark harness (starts a local mock server):

```bash
python -m async_api_optimizer.benchmark.harness
```

The harness prints a text table and JSON output with p50/p95/p99 latency metrics.
It also reports success and failure counts for each scenario, including the resilience chaos run.

## Tests

Install dev dependencies and run pytest:

```bash
python -m pip install -e .[dev]
pytest
```
