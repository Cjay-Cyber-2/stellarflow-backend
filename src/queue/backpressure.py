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
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, Optional, TypeVar

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
