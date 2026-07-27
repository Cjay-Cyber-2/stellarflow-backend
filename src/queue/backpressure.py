"""src/queue/backpressure.py – Queue backpressure control and sliding window rate limiters."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import psutil


# ---------------------------------------------------------------------------
# Issue #649 — Sliding Window Rate Limiters for Regional Node Polling
# ---------------------------------------------------------------------------
# Implements a sliding window rate-limiting guard that tracks per-key request
# timestamps within a configurable time window.  Outbound API request
# frequencies stay strictly within configured per-second windows.
#
# The algorithm uses a deque-based sliding window that evicts expired entries
# on each check, allowing burst capacity up to the full window limit while
# enforcing the average rate over the window duration.


@dataclass(frozen=True)
class SlidingWindowConfig:
    window_size_s: float = 1.0
    max_requests: int = 100
    region_prefix: str = "rl:"


@dataclass(frozen=True)
class SlidingWindowResult:
    allowed: bool
    remaining: int
    limit: int
    window_size_s: float
    retry_after_s: float


class SlidingWindowRateLimiter:
    """Sliding window rate limiter that enforces per-key request limits within
    configured time windows.

    Uses a sorted-timestamp deque internally to track request timestamps.
    On each check, expired entries (older than window_size_s) are evicted,
    and the remaining count is compared against max_requests.

    Thread-safe via a ``threading.Lock``.

    Example::

        limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=10)

        # Check before sending:
        result = limiter.check("us-east-1")
        if result.allowed:
            await send_request()
        else:
            await asyncio.sleep(result.retry_after_s)
    """

    __slots__ = ("_window_size_s", "_max_requests", "_stores", "_lock")

    def __init__(self, window_size_s: float = 1.0, max_requests: int = 100) -> None:
        if window_size_s <= 0:
            raise ValueError("window_size_s must be positive")
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")

        self._window_size_s: float = window_size_s
        self._max_requests: int = max_requests
        self._stores: Dict[str, Tuple[float, ...]] = {}
        self._lock = threading.Lock()

    @property
    def config(self) -> SlidingWindowConfig:
        return SlidingWindowConfig(
            window_size_s=self._window_size_s,
            max_requests=self._max_requests,
        )

    def allow(self, key: str) -> SlidingWindowResult:
        """Check if *key* is allowed to send a request now.

        If allowed, the current timestamp is recorded.  Returns a
        :class:`SlidingWindowResult` describing the decision.
        """
        now = time.monotonic()
        cutoff = now - self._window_size_s

        with self._lock:
            timestamps = self._stores.get(key)
            if timestamps is None:
                self._stores[key] = (now,)
                return SlidingWindowResult(
                    allowed=True,
                    remaining=self._max_requests - 1,
                    limit=self._max_requests,
                    window_size_s=self._window_size_s,
                    retry_after_s=0.0,
                )

            # Evict expired timestamps
            valid = tuple(t for t in timestamps if t > cutoff)

            if len(valid) < self._max_requests:
                self._stores[key] = valid + (now,)
                remaining = max(0, self._max_requests - len(valid) - 1)
                return SlidingWindowResult(
                    allowed=True,
                    remaining=remaining,
                    limit=self._max_requests,
                    window_size_s=self._window_size_s,
                    retry_after_s=0.0,
                )

            # Rate limited
            self._stores[key] = valid
            earliest = valid[0] if valid else now
            retry_after_s = max(0.0, self._window_size_s - (now - earliest))
            return SlidingWindowResult(
                allowed=False,
                remaining=0,
                limit=self._max_requests,
                window_size_s=self._window_size_s,
                retry_after_s=math.ceil(retry_after_s * 100) / 100,
            )

    def check(self, key: str) -> SlidingWindowResult:
        """Check if *key* is allowed to send a request **without** recording it.

        This is a read-only check — it does not consume a slot.
        """
        now = time.monotonic()
        cutoff = now - self._window_size_s

        with self._lock:
            timestamps = self._stores.get(key)

            if timestamps is None:
                return SlidingWindowResult(
                    allowed=True,
                    remaining=self._max_requests,
                    limit=self._max_requests,
                    window_size_s=self._window_size_s,
                    retry_after_s=0.0,
                )

            valid = tuple(t for t in timestamps if t > cutoff)
            self._stores[key] = valid

            if not valid:
                remaining = self._max_requests
                retry_after_s = 0.0
            else:
                remaining = max(0, self._max_requests - len(valid))
                earliest = valid[0]
                retry_after_s = max(0.0, self._window_size_s - (now - earliest))

            return SlidingWindowResult(
                allowed=remaining > 0,
                remaining=remaining,
                limit=self._max_requests,
                window_size_s=self._window_size_s,
                retry_after_s=math.ceil(retry_after_s * 100) / 100,
            )

    def remaining(self, key: str) -> int:
        """Return the number of remaining request slots for *key*."""
        now = time.monotonic()
        cutoff = now - self._window_size_s

        with self._lock:
            timestamps = self._stores.get(key)
            if timestamps is None:
                return self._max_requests

            valid = tuple(t for t in timestamps if t > cutoff)
            self._stores[key] = valid
            return max(0, self._max_requests - len(valid))

    def reset(self, key: str) -> None:
        """Clear all tracked timestamps for *key*."""
        with self._lock:
            self._stores.pop(key, None)

    def reset_all(self) -> None:
        """Clear all tracked timestamps."""
        with self._lock:
            self._stores.clear()


# ---------------------------------------------------------------------------
# QueueCapacityController (existing)
# ---------------------------------------------------------------------------


class QueueCapacityController:
    def __init__(self, initial_capacity: int, min_capacity: int = 100):
        self.initial_capacity = initial_capacity
        self.current_capacity = initial_capacity
        self.min_capacity = min_capacity
        self.memory_threshold = 85.0

    def adjust_capacity(self) -> int:
        memory_usage = psutil.virtual_memory().percent
        if memory_usage > self.memory_threshold:
            # Contract capacity when memory pressure is high
            self.current_capacity = max(self.min_capacity, int(self.current_capacity * 0.5))
        else:
            # Slowly recover capacity when memory pressure is normal
            if self.current_capacity < self.initial_capacity:
                self.current_capacity = min(self.initial_capacity, int(self.current_capacity * 1.2) + 1)

        return self.current_capacity

    def get_capacity(self) -> int:
        return self.adjust_capacity()


__all__ = [
    "SlidingWindowConfig",
    "SlidingWindowResult",
    "SlidingWindowRateLimiter",
    "QueueCapacityController",
]