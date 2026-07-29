"""
Circuit Breaker implementation for RPC endpoint failure protection.

This module implements a circuit breaker pattern with three states:
- CLOSED: Normal operation, requests pass through
- OPEN: Failure threshold exceeded, requests fail fast
- HALF_OPEN: Testing recovery, limited requests allowed

The circuit breaker prevents cascading failures by stopping requests
to failing endpoints and allowing graceful recovery.
"""

from __future__ import annotations

import logging
import math
import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "CLOSED"  # Normal operation, requests pass through
    OPEN = "OPEN"  # Failure threshold exceeded, fail fast
    HALF_OPEN = "HALF_OPEN"  # Testing recovery, limited requests


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""

    failure_threshold: int = 5  # Failures before opening circuit
    success_threshold: int = 2  # Successes in HALF_OPEN to close circuit
    timeout_seconds: float = 60.0  # Time to wait before HALF_OPEN attempt
    error_threshold_percentage: float = 50.0  # Error rate % to open circuit
    window_size: int = 10  # Rolling window size for error calculation


@dataclass
class CircuitBreakerMetrics:
    """Metrics tracked by circuit breaker."""

    state: CircuitState
    failure_count: int = 0
    success_count: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_failure_time: Optional[float] = None
    last_state_change: float = field(default_factory=time.monotonic)
    total_requests: int = 0
    rejected_requests: int = 0
    recent_results: list[bool] = field(default_factory=list)  # True=success, False=failure


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""

    pass


class CircuitBreaker:
    """
    Circuit breaker for protecting against failing endpoints.

    Implements the circuit breaker pattern with automatic state transitions
    based on failure rates and recovery attempts.
    """

    __slots__ = (
        "_config",
        "_state",
        "_metrics",
        "_lock",
        "_name",
    )

    def __init__(
        self,
        config: Optional[CircuitBreakerConfig] = None,
        name: str = "default",
    ) -> None:
        """
        Initialize circuit breaker.

        Args:
            config: Configuration for circuit breaker behavior
            name: Name identifier for logging and tracking
        """
        self._config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._metrics = CircuitBreakerMetrics(state=CircuitState.CLOSED)
        self._lock = threading.Lock()
        self._name = name

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery."""
        if self._metrics.last_failure_time is None:
            return False
        elapsed = time.monotonic() - self._metrics.last_failure_time
        return elapsed >= self._config.timeout_seconds

    def _calculate_error_rate(self) -> float:
        """Calculate current error rate from recent results."""
        if not self._metrics.recent_results:
            return 0.0
        failures = sum(1 for success in self._metrics.recent_results if not success)
        return (failures / len(self._metrics.recent_results)) * 100.0

    def _transition_to_open(self) -> None:
        """Transition circuit to OPEN state."""
        old_state = self._state
        self._state = CircuitState.OPEN
        self._metrics.state = CircuitState.OPEN
        self._metrics.last_state_change = time.monotonic()
        logger.warning(
            f"Circuit breaker '{self._name}' transitioned {old_state.value} -> OPEN. "
            f"Consecutive failures: {self._metrics.consecutive_failures}, "
            f"Error rate: {self._calculate_error_rate():.2f}%"
        )

    def _transition_to_half_open(self) -> None:
        """Transition circuit to HALF_OPEN state."""
        old_state = self._state
        self._state = CircuitState.HALF_OPEN
        self._metrics.state = CircuitState.HALF_OPEN
        self._metrics.consecutive_successes = 0
        self._metrics.last_state_change = time.monotonic()
        logger.info(
            f"Circuit breaker '{self._name}' transitioned {old_state.value} -> HALF_OPEN. "
            "Testing recovery..."
        )

    def _transition_to_closed(self) -> None:
        """Transition circuit to CLOSED state."""
        old_state = self._state
        self._state = CircuitState.CLOSED
        self._metrics.state = CircuitState.CLOSED
        self._metrics.consecutive_failures = 0
        self._metrics.last_state_change = time.monotonic()
        logger.info(
            f"Circuit breaker '{self._name}' transitioned {old_state.value} -> CLOSED. "
            "Circuit recovered."
        )

    def _record_success(self) -> None:
        """Record a successful request."""
        self._metrics.success_count += 1
        self._metrics.consecutive_successes += 1
        self._metrics.consecutive_failures = 0
        self._metrics.recent_results.append(True)

        # Maintain window size
        if len(self._metrics.recent_results) > self._config.window_size:
            self._metrics.recent_results.pop(0)

        # State-specific handling
        if self._state == CircuitState.HALF_OPEN:
            if self._metrics.consecutive_successes >= self._config.success_threshold:
                self._transition_to_closed()

    def _record_failure(self) -> None:
        """Record a failed request."""
        self._metrics.failure_count += 1
        self._metrics.consecutive_failures += 1
        self._metrics.consecutive_successes = 0
        self._metrics.last_failure_time = time.monotonic()
        self._metrics.recent_results.append(False)

        # Maintain window size
        if len(self._metrics.recent_results) > self._config.window_size:
            self._metrics.recent_results.pop(0)

        # Check if we should open the circuit
        if self._state == CircuitState.CLOSED:
            error_rate = self._calculate_error_rate()
            if (
                self._metrics.consecutive_failures >= self._config.failure_threshold
                or error_rate >= self._config.error_threshold_percentage
            ):
                self._transition_to_open()
        elif self._state == CircuitState.HALF_OPEN:
            # Any failure in HALF_OPEN returns to OPEN
            self._transition_to_open()

    def call(self, func: Callable[[], T]) -> T:
        """
        Execute a function call through the circuit breaker.

        Args:
            func: Function to execute

        Returns:
            Result from the function

        Raises:
            CircuitBreakerError: If circuit is open
            Exception: Any exception raised by func
        """
        with self._lock:
            self._metrics.total_requests += 1

            # Check if circuit is OPEN and can transition to HALF_OPEN
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    self._metrics.rejected_requests += 1
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self._name}' is OPEN. "
                        f"Requests blocked until timeout expires."
                    )

        # Execute the function (outside lock to avoid holding during I/O)
        try:
            result = func()
            with self._lock:
                self._record_success()
            return result
        except Exception as error:
            with self._lock:
                self._record_failure()
            raise error

    async def call_async(self, func: Callable[[], T]) -> T:
        """
        Execute an async function call through the circuit breaker.

        Args:
            func: Async function to execute

        Returns:
            Result from the function

        Raises:
            CircuitBreakerError: If circuit is open
            Exception: Any exception raised by func
        """
        with self._lock:
            self._metrics.total_requests += 1

            # Check if circuit is OPEN and can transition to HALF_OPEN
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    self._metrics.rejected_requests += 1
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self._name}' is OPEN. "
                        f"Requests blocked until timeout expires."
                    )

        # Execute the function (outside lock to avoid holding during I/O)
        try:
            result = await func()
            with self._lock:
                self._record_success()
            return result
        except Exception as error:
            with self._lock:
                self._record_failure()
            raise error

    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            return self._state

    def get_metrics(self) -> CircuitBreakerMetrics:
        """Get current metrics snapshot."""
        with self._lock:
            # Create a copy to avoid external mutation
            return CircuitBreakerMetrics(
                state=self._metrics.state,
                failure_count=self._metrics.failure_count,
                success_count=self._metrics.success_count,
                consecutive_failures=self._metrics.consecutive_failures,
                consecutive_successes=self._metrics.consecutive_successes,
                last_failure_time=self._metrics.last_failure_time,
                last_state_change=self._metrics.last_state_change,
                total_requests=self._metrics.total_requests,
                rejected_requests=self._metrics.rejected_requests,
                recent_results=self._metrics.recent_results.copy(),
            )

    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._metrics = CircuitBreakerMetrics(state=CircuitState.CLOSED)
            logger.info(f"Circuit breaker '{self._name}' manually reset to CLOSED.")

    def force_open(self) -> None:
        """Force circuit breaker to OPEN state (for testing/maintenance)."""
        with self._lock:
            if self._state != CircuitState.OPEN:
                self._transition_to_open()


class CircuitBreakerRegistry:
    """
    Registry for managing multiple circuit breakers by endpoint.

    Thread-safe registry that creates and tracks circuit breakers
    for different RPC endpoints or services.
    """

    __slots__ = ("_breakers", "_lock", "_default_config")

    def __init__(
        self, default_config: Optional[CircuitBreakerConfig] = None
    ) -> None:
        """
        Initialize circuit breaker registry.

        Args:
            default_config: Default configuration for new circuit breakers
        """
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()
        self._default_config = default_config or CircuitBreakerConfig()

    def get_breaker(self, name: str) -> CircuitBreaker:
        """
        Get or create a circuit breaker by name.

        Args:
            name: Unique identifier for the circuit breaker

        Returns:
            Circuit breaker instance
        """
        breaker = self._breakers.get(name)
        if breaker is None:
            with self._lock:
                breaker = self._breakers.get(name)
                if breaker is None:
                    breaker = CircuitBreaker(self._default_config, name)
                    self._breakers[name] = breaker
        return breaker

    def get_all_states(self) -> Dict[str, CircuitState]:
        """Get states of all registered circuit breakers."""
        with self._lock:
            return {name: breaker.get_state() for name, breaker in self._breakers.items()}

    def get_all_metrics(self) -> Dict[str, CircuitBreakerMetrics]:
        """Get metrics for all registered circuit breakers."""
        with self._lock:
            return {name: breaker.get_metrics() for name, breaker in self._breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        with self._lock:
            for breaker in self._breakers.values():
                breaker.reset()


# Global registry instance
circuit_breaker_registry = CircuitBreakerRegistry()


__all__ = [
    "CircuitState",
    "CircuitBreakerConfig",
    "CircuitBreakerMetrics",
    "CircuitBreakerError",
    "CircuitBreaker",
    "CircuitBreakerRegistry",
    "circuit_breaker_registry",
]
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


# ---------------------------------------------------------------------------
# Issue #629 — Adaptive Weighted Random Early Detection (WRED) Drops
# ---------------------------------------------------------------------------
# Tail-drop policies discard packets indiscriminately once a queue saturates,
# which can silently shed critical validator heartbeat/consensus packets
# during traffic surges. WRED instead derives a per-priority drop
# probability from queue saturation: below a priority's min_threshold
# nothing is dropped, the probability ramps linearly up to
# max_drop_probability by max_threshold, and stays there beyond it. Low
# priority telemetry ramps up early and can be shed with certainty, while
# consensus traffic keeps a zero drop probability so it always passes
# through.


class PacketPriority(Enum):
    """Priority flag carried by a packet entering a WRED-managed queue."""

    CONSENSUS = "CONSENSUS"  # Validator heartbeats / consensus traffic
    STANDARD = "STANDARD"
    TELEMETRY = "TELEMETRY"  # Low-priority observability/telemetry traffic


@dataclass(frozen=True)
class WREDThreshold:
    """Drop curve for a single priority class.

    Below ``min_threshold`` saturation, packets of this priority are never
    dropped. Between ``min_threshold`` and ``max_threshold`` the drop
    probability ramps up linearly to ``max_drop_probability``. At or above
    ``max_threshold``, packets are dropped with ``max_drop_probability``.
    """

    min_threshold: float
    max_threshold: float
    max_drop_probability: float


@dataclass
class WREDMetrics:
    """Metrics tracked by a :class:`WREDQueue`."""

    total_packets: int = 0
    admitted_packets: int = 0
    dropped_packets: int = 0
    dropped_by_priority: Dict[str, int] = field(default_factory=dict)


# Default drop curves: consensus never drops, standard ramps in the upper
# half of the queue, telemetry starts shedding earliest and hardest.
_DEFAULT_WRED_THRESHOLDS: Dict[PacketPriority, WREDThreshold] = {
    PacketPriority.CONSENSUS: WREDThreshold(
        min_threshold=1.0, max_threshold=1.0, max_drop_probability=0.0
    ),
    PacketPriority.STANDARD: WREDThreshold(
        min_threshold=0.6, max_threshold=0.9, max_drop_probability=0.5
    ),
    PacketPriority.TELEMETRY: WREDThreshold(
        min_threshold=0.3, max_threshold=0.8, max_drop_probability=1.0
    ),
}


class WREDQueue:
    """Bounded FIFO queue that applies Weighted Random Early Detection.

    Unlike a plain tail-drop queue — which discards indiscriminately once
    full — WRED computes a per-priority drop probability from queue
    saturation, so low-priority traffic is shed progressively as the queue
    fills while high-priority traffic keeps flowing.
    """

    __slots__ = ("_capacity", "_thresholds", "_queue", "_metrics", "_lock", "_rand")

    def __init__(
        self,
        capacity: int,
        thresholds: Optional[Dict[PacketPriority, WREDThreshold]] = None,
        rng: Optional[random.Random] = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")

        self._capacity = capacity
        self._thresholds = dict(thresholds) if thresholds else dict(_DEFAULT_WRED_THRESHOLDS)
        self._queue: List[object] = []
        self._metrics = WREDMetrics()
        self._lock = threading.Lock()
        self._rand = rng or random.Random()

    def _threshold_for(self, priority: PacketPriority) -> WREDThreshold:
        return self._thresholds.get(priority, _DEFAULT_WRED_THRESHOLDS[PacketPriority.STANDARD])

    def _drop_probability_at(self, priority: PacketPriority, saturation: float) -> float:
        threshold = self._threshold_for(priority)

        if saturation <= threshold.min_threshold:
            return 0.0
        if saturation >= threshold.max_threshold:
            return threshold.max_drop_probability

        span = threshold.max_threshold - threshold.min_threshold
        if span <= 0:
            return threshold.max_drop_probability

        ramp_ratio = (saturation - threshold.min_threshold) / span
        return threshold.max_drop_probability * ramp_ratio

    def drop_probability(self, priority: PacketPriority) -> float:
        """Return the current WRED drop probability for *priority* at the
        queue's present saturation level."""
        with self._lock:
            saturation = len(self._queue) / self._capacity
            return self._drop_probability_at(priority, saturation)

    def _record_drop(self, priority: PacketPriority) -> None:
        self._metrics.dropped_packets += 1
        self._metrics.dropped_by_priority[priority.value] = (
            self._metrics.dropped_by_priority.get(priority.value, 0) + 1
        )

    def offer(self, packet: object, priority: PacketPriority = PacketPriority.STANDARD) -> bool:
        """Attempt to enqueue *packet* under the given *priority*.

        Returns ``True`` if the packet was admitted, ``False`` if WRED (or
        the hard capacity limit) shed it.
        """
        with self._lock:
            self._metrics.total_packets += 1

            if len(self._queue) >= self._capacity:
                self._record_drop(priority)
                return False

            saturation = len(self._queue) / self._capacity
            drop_probability = self._drop_probability_at(priority, saturation)

            if drop_probability > 0.0 and self._rand.random() < drop_probability:
                self._record_drop(priority)
                return False

            self._queue.append(packet)
            self._metrics.admitted_packets += 1
            return True

    def poll(self) -> Optional[object]:
        """Dequeue and return the next packet in FIFO order, or ``None`` if empty."""
        with self._lock:
            if not self._queue:
                return None
            return self._queue.pop(0)

    def __len__(self) -> int:
        with self._lock:
            return len(self._queue)

    @property
    def saturation(self) -> float:
        with self._lock:
            return len(self._queue) / self._capacity

    def get_metrics(self) -> WREDMetrics:
        with self._lock:
            return WREDMetrics(
                total_packets=self._metrics.total_packets,
                admitted_packets=self._metrics.admitted_packets,
                dropped_packets=self._metrics.dropped_packets,
                dropped_by_priority=dict(self._metrics.dropped_by_priority),
            )


__all__ = [
    "SlidingWindowConfig",
    "SlidingWindowResult",
    "SlidingWindowRateLimiter",
    "QueueCapacityController",
    "PacketPriority",
    "WREDThreshold",
    "WREDMetrics",
    "WREDQueue",
]