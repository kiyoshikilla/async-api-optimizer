from __future__ import annotations

import asyncio
import logging
import math
from statistics import mean

import aiohttp

logger = logging.getLogger(__name__)


class LatencyMetrics:
    def __init__(self) -> None:
        self._values: list[float] = []

    def record(self, duration: float) -> None:
        try:
            if duration < 0:
                raise ValueError("Duration must be non-negative")
            self._values.append(duration)
        except asyncio.CancelledError:
            logger.warning("LatencyMetrics record cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("LatencyMetrics record failed", exc_info=True)
            raise

    def summary(self) -> dict[str, float]:
        try:
            if not self._values:
                return {"count": 0.0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
            values = sorted(self._values)
            return {
                "count": float(len(values)),
                "avg": mean(values),
                "p50": self._percentile(values, 0.50),
                "p95": self._percentile(values, 0.95),
                "p99": self._percentile(values, 0.99),
            }
        except asyncio.CancelledError:
            logger.warning("LatencyMetrics summary cancelled")
            raise
        except aiohttp.ClientError:
            logger.error("LatencyMetrics summary failed", exc_info=True)
            raise

    def _percentile(self, sorted_values: list[float], pct: float) -> float:
        if not sorted_values:
            return 0.0
        if pct <= 0:
            return sorted_values[0]
        if pct >= 1:
            return sorted_values[-1]
        k = (len(sorted_values) - 1) * pct
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_values[int(k)]
        lower = sorted_values[int(f)]
        upper = sorted_values[int(c)]
        return lower + (upper - lower) * (k - f)
