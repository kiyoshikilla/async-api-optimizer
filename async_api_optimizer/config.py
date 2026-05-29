from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PoolConfig:
    limit: int = 100
    limit_per_host: int = 30
    ttl_dns_cache: int = 300
    keepalive_timeout: int = 30
    connect_timeout: float = 5.0
    read_timeout: float = 30.0


@dataclass(frozen=True)
class DispatcherConfig:
    max_concurrency: int = 50
    warn_after: float = 0.5
    target_latency: float = 0.1
    critical_latency: float = 0.5
    min_concurrency: int = 1


@dataclass(frozen=True)
class AdaptiveDispatcherConfig:
    max_concurrency: int = 50
    min_concurrency: int = 1
    warn_after: float = 0.5
    target_latency: float = 0.1
    critical_latency: float = 0.5


@dataclass(frozen=True)
class BatcherConfig:
    max_size: int = 20
    max_delay: float = 0.02


@dataclass(frozen=True)
class ExecutorConfig:
    max_workers: int = 8


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 2.0


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 0.05
    jitter: float = 0.02


@dataclass(frozen=True)
class BenchmarkRunConfig:
    concurrency: int
    n: int
    repeats: int = 3


@dataclass(frozen=True)
class MockServerConfig:
    min_delay_ms: int = 10
    max_delay_ms: int = 50
    error_rate: float = 0.0
    spike_probability: float = 0.0
    spike_delay_ms: int = 2000


@dataclass(frozen=True)
class ScenarioConfig:
    name: str
    run: BenchmarkRunConfig
    use_pool: bool
    use_dispatcher: bool
    use_batcher: bool
    use_executor: bool
    use_adaptive_dispatcher: bool = False
    use_circuit_breaker: bool = False
    use_retry: bool = False
    error_rate: float = 0.0
    spike_probability: float = 0.0


DEFAULT_SCENARIOS: tuple[ScenarioConfig, ...] = (
    ScenarioConfig(
        name="baseline",
        run=BenchmarkRunConfig(concurrency=100, n=1000, repeats=3),
        use_pool=False,
        use_dispatcher=False,
        use_batcher=False,
        use_executor=False,
        use_adaptive_dispatcher=False,
        use_circuit_breaker=False,
        use_retry=False,
        error_rate=0.0,
        spike_probability=0.0,
    ),
    ScenarioConfig(
        name="pool_only",
        run=BenchmarkRunConfig(concurrency=100, n=1000, repeats=3),
        use_pool=True,
        use_dispatcher=False,
        use_batcher=False,
        use_executor=False,
        use_adaptive_dispatcher=False,
        use_circuit_breaker=False,
        use_retry=False,
        error_rate=0.0,
        spike_probability=0.0,
    ),
    ScenarioConfig(
        name="bounded",
        run=BenchmarkRunConfig(concurrency=50, n=1000, repeats=3),
        use_pool=True,
        use_dispatcher=True,
        use_batcher=False,
        use_executor=False,
        use_adaptive_dispatcher=False,
        use_circuit_breaker=False,
        use_retry=False,
        error_rate=0.0,
        spike_probability=0.0,
    ),
    ScenarioConfig(
        name="batched",
        run=BenchmarkRunConfig(concurrency=50, n=1000, repeats=3),
        use_pool=True,
        use_dispatcher=True,
        use_batcher=True,
        use_executor=False,
        use_adaptive_dispatcher=False,
        use_circuit_breaker=False,
        use_retry=False,
        error_rate=0.0,
        spike_probability=0.0,
    ),
    ScenarioConfig(
        name="full_stack",
        run=BenchmarkRunConfig(concurrency=50, n=1000, repeats=3),
        use_pool=True,
        use_dispatcher=True,
        use_batcher=True,
        use_executor=True,
        use_adaptive_dispatcher=False,
        use_circuit_breaker=False,
        use_retry=False,
        error_rate=0.0,
        spike_probability=0.0,
    ),
    ScenarioConfig(
        name="resilience_chaos",
        run=BenchmarkRunConfig(concurrency=50, n=1000, repeats=3),
        use_pool=True,
        use_dispatcher=False,
        use_batcher=False,
        use_executor=False,
        use_adaptive_dispatcher=True,
        use_circuit_breaker=True,
        use_retry=True,
        error_rate=0.2,
        spike_probability=0.1,
    ),
)
