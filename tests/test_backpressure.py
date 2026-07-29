"""
Tests for circuit breaker state machine implementation.

This test suite verifies that the circuit breaker correctly transitions
between CLOSED, OPEN, and HALF_OPEN states based on failure thresholds
and error rates.
"""

import time
from typing import Callable
import pytest
from src.queue.backpressure import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerError,
    CircuitState,
    CircuitBreakerRegistry,
)


class TestCircuitBreakerStates:
    """Test circuit breaker state transitions and behavior."""

    def test_circuit_breaker_initial_state(self) -> None:
        """Test that circuit breaker starts in CLOSED state."""
        breaker = CircuitBreaker(name="test_initial")
        assert breaker.get_state() == CircuitState.CLOSED

    def test_circuit_breaker_closed_to_open_by_consecutive_failures(self) -> None:
        """Test transition from CLOSED to OPEN after consecutive failures."""
        config = CircuitBreakerConfig(
            failure_threshold=3,
            timeout_seconds=1.0,
            error_threshold_percentage=100.0,  # Disable error rate threshold
        )
        breaker = CircuitBreaker(config, name="test_consecutive_failures")

        # Simulate failures
        for i in range(3):
            with pytest.raises(Exception):
                breaker.call(lambda: self._failing_function())

        # Circuit should be OPEN after threshold reached
        assert breaker.get_state() == CircuitState.OPEN

    def test_circuit_breaker_closed_to_open_by_error_rate(self) -> None:
        """Test transition from CLOSED to OPEN based on error rate percentage."""
        config = CircuitBreakerConfig(
            failure_threshold=100,  # High threshold to test error rate
            timeout_seconds=1.0,
            error_threshold_percentage=50.0,  # 50% error rate triggers OPEN
            window_size=10,
        )
        breaker = CircuitBreaker(config, name="test_error_rate")

        # Add mixed results: 6 failures out of 10 = 60% error rate
        for i in range(10):
            if i < 6:
                with pytest.raises(Exception):
                    breaker.call(lambda: self._failing_function())
            else:
                breaker.call(lambda: self._successful_function())

        # Circuit should be OPEN due to error rate > 50%
        assert breaker.get_state() == CircuitState.OPEN

    def test_circuit_breaker_open_rejects_requests(self) -> None:
        """Test that OPEN circuit breaker rejects requests immediately."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            timeout_seconds=10.0,  # Long timeout to keep it OPEN
        )
        breaker = CircuitBreaker(config, name="test_reject")

        # Trigger failures to open circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(lambda: self._failing_function())

        assert breaker.get_state() == CircuitState.OPEN

        # Attempt a request while OPEN - should be rejected instantly
        with pytest.raises(CircuitBreakerError) as exc_info:
            breaker.call(lambda: self._successful_function())

        assert "is OPEN" in str(exc_info.value)

        # Verify the function wasn't actually called (rejected before execution)
        metrics = breaker.get_metrics()
        assert metrics.rejected_requests > 0

    def test_circuit_breaker_open_to_half_open_after_timeout(self) -> None:
        """Test transition from OPEN to HALF_OPEN after timeout period."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            timeout_seconds=0.1,  # Short timeout for testing
        )
        breaker = CircuitBreaker(config, name="test_timeout")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(lambda: self._failing_function())

        assert breaker.get_state() == CircuitState.OPEN

        # Wait for timeout
        time.sleep(0.15)

        # Next call should transition to HALF_OPEN
        result = breaker.call(lambda: self._successful_function())
        assert result == "success"
        assert breaker.get_state() == CircuitState.HALF_OPEN

    def test_circuit_breaker_half_open_to_closed_on_success(self) -> None:
        """Test transition from HALF_OPEN to CLOSED after successful requests."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            success_threshold=2,  # Need 2 successes to close
            timeout_seconds=0.1,
        )
        breaker = CircuitBreaker(config, name="test_recovery")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(lambda: self._failing_function())

        assert breaker.get_state() == CircuitState.OPEN

        # Wait for timeout and transition to HALF_OPEN
        time.sleep(0.15)

        # First success - should stay in HALF_OPEN
        breaker.call(lambda: self._successful_function())
        assert breaker.get_state() == CircuitState.HALF_OPEN

        # Second success - should transition to CLOSED
        breaker.call(lambda: self._successful_function())
        assert breaker.get_state() == CircuitState.CLOSED

    def test_circuit_breaker_half_open_to_open_on_failure(self) -> None:
        """Test transition from HALF_OPEN back to OPEN on any failure."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            success_threshold=2,
            timeout_seconds=0.1,
        )
        breaker = CircuitBreaker(config, name="test_half_open_failure")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(lambda: self._failing_function())

        # Wait and transition to HALF_OPEN
        time.sleep(0.15)
        breaker.call(lambda: self._successful_function())
        assert breaker.get_state() == CircuitState.HALF_OPEN

        # Failure in HALF_OPEN should immediately return to OPEN
        with pytest.raises(Exception):
            breaker.call(lambda: self._failing_function())

        assert breaker.get_state() == CircuitState.OPEN

    def test_circuit_breaker_metrics_tracking(self) -> None:
        """Test that circuit breaker tracks metrics correctly."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker(config, name="test_metrics")

        # Execute some successful calls
        for _ in range(5):
            breaker.call(lambda: self._successful_function())

        # Execute some failures
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(lambda: self._failing_function())

        metrics = breaker.get_metrics()
        assert metrics.success_count == 5
        assert metrics.failure_count == 2
        assert metrics.consecutive_failures == 2
        assert metrics.total_requests == 7
        assert len(metrics.recent_results) == 7

    def test_circuit_breaker_reset(self) -> None:
        """Test manual reset of circuit breaker."""
        config = CircuitBreakerConfig(failure_threshold=2)
        breaker = CircuitBreaker(config, name="test_reset")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(lambda: self._failing_function())

        assert breaker.get_state() == CircuitState.OPEN

        # Reset the circuit
        breaker.reset()

        assert breaker.get_state() == CircuitState.CLOSED
        metrics = breaker.get_metrics()
        assert metrics.failure_count == 0
        assert metrics.success_count == 0

    def test_circuit_breaker_force_open(self) -> None:
        """Test forcing circuit breaker to OPEN state."""
        breaker = CircuitBreaker(name="test_force_open")
        assert breaker.get_state() == CircuitState.CLOSED

        # Force open
        breaker.force_open()
        assert breaker.get_state() == CircuitState.OPEN

        # Requests should be rejected
        with pytest.raises(CircuitBreakerError):
            breaker.call(lambda: self._successful_function())

    def test_circuit_breaker_concurrent_access(self) -> None:
        """Test thread-safe operation of circuit breaker."""
        import concurrent.futures

        config = CircuitBreakerConfig(failure_threshold=10)
        breaker = CircuitBreaker(config, name="test_concurrent")

        def make_request(success: bool) -> bool:
            try:
                if success:
                    breaker.call(lambda: self._successful_function())
                else:
                    breaker.call(lambda: self._failing_function())
                return True
            except Exception:
                return False

        # Execute concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(make_request, i % 2 == 0) for i in range(100)
            ]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        # Should have mix of successes and failures
        assert sum(results) > 0
        assert sum(results) < 100

        metrics = breaker.get_metrics()
        assert metrics.total_requests == 100

    @pytest.mark.asyncio
    async def test_circuit_breaker_async_calls(self) -> None:
        """Test circuit breaker with async functions."""
        config = CircuitBreakerConfig(failure_threshold=2)
        breaker = CircuitBreaker(config, name="test_async")

        # Successful async call
        result = await breaker.call_async(lambda: self._async_successful_function())
        assert result == "async_success"
        assert breaker.get_state() == CircuitState.CLOSED

        # Failing async calls
        for _ in range(2):
            with pytest.raises(Exception):
                await breaker.call_async(lambda: self._async_failing_function())

        assert breaker.get_state() == CircuitState.OPEN

    # Helper functions for testing

    def _successful_function(self) -> str:
        """Simulate a successful function call."""
        return "success"

    def _failing_function(self) -> None:
        """Simulate a failing function call."""
        raise Exception("Simulated failure")

    async def _async_successful_function(self) -> str:
        """Simulate a successful async function call."""
        return "async_success"

    async def _async_failing_function(self) -> None:
        """Simulate a failing async function call."""
        raise Exception("Async simulated failure")


class TestCircuitBreakerRegistry:
    """Test circuit breaker registry functionality."""

    def test_registry_get_or_create_breaker(self) -> None:
        """Test getting or creating circuit breakers by name."""
        registry = CircuitBreakerRegistry()

        breaker1 = registry.get_breaker("endpoint1")
        breaker2 = registry.get_breaker("endpoint1")
        breaker3 = registry.get_breaker("endpoint2")

        # Same name should return same instance
        assert breaker1 is breaker2
        # Different name should return different instance
        assert breaker1 is not breaker3

    def test_registry_get_all_states(self) -> None:
        """Test getting states of all registered breakers."""
        registry = CircuitBreakerRegistry()

        registry.get_breaker("endpoint1")
        registry.get_breaker("endpoint2")

        states = registry.get_all_states()
        assert len(states) == 2
        assert states["endpoint1"] == CircuitState.CLOSED
        assert states["endpoint2"] == CircuitState.CLOSED

    def test_registry_reset_all(self) -> None:
        """Test resetting all circuit breakers."""
        config = CircuitBreakerConfig(failure_threshold=1)
        registry = CircuitBreakerRegistry(default_config=config)

        # Get breakers and open them
        breaker1 = registry.get_breaker("endpoint1")
        breaker2 = registry.get_breaker("endpoint2")

        with pytest.raises(Exception):
            breaker1.call(lambda: self._failing_function())
        with pytest.raises(Exception):
            breaker2.call(lambda: self._failing_function())

        assert breaker1.get_state() == CircuitState.OPEN
        assert breaker2.get_state() == CircuitState.OPEN

        # Reset all
        registry.reset_all()

        assert breaker1.get_state() == CircuitState.CLOSED
        assert breaker2.get_state() == CircuitState.CLOSED

    def _failing_function(self) -> None:
        """Simulate a failing function call."""
        raise Exception("Simulated failure")


class TestCircuitBreakerIntegration:
    """Integration tests for circuit breaker with RPC endpoints."""

    def test_circuit_breaker_prevents_cascading_failures(self) -> None:
        """Test that circuit breaker prevents cascading failures to downstream services."""
        config = CircuitBreakerConfig(
            failure_threshold=3,
            timeout_seconds=0.5,
            error_threshold_percentage=60.0,
        )
        breaker = CircuitBreaker(config, name="rpc_endpoint")

        # Simulate a failing RPC endpoint
        failure_count = 0
        rejection_count = 0

        for i in range(10):
            try:
                breaker.call(lambda: self._simulate_rpc_call(should_fail=True))
            except CircuitBreakerError:
                rejection_count += 1
            except Exception:
                failure_count += 1

        # After threshold, most requests should be rejected (not attempted)
        assert breaker.get_state() == CircuitState.OPEN
        assert rejection_count > 0
        assert rejection_count + failure_count == 10

        # Verify that outbound requests pause instantly
        metrics = breaker.get_metrics()
        assert metrics.rejected_requests > 0

    def test_circuit_breaker_recovery_flow(self) -> None:
        """Test complete recovery flow: CLOSED -> OPEN -> HALF_OPEN -> CLOSED."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            success_threshold=2,
            timeout_seconds=0.2,
        )
        breaker = CircuitBreaker(config, name="recovery_test")

        # Step 1: Start CLOSED
        assert breaker.get_state() == CircuitState.CLOSED

        # Step 2: Trigger failures to open circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(lambda: self._simulate_rpc_call(should_fail=True))

        assert breaker.get_state() == CircuitState.OPEN

        # Step 3: Verify requests are rejected while OPEN
        with pytest.raises(CircuitBreakerError):
            breaker.call(lambda: self._simulate_rpc_call(should_fail=False))

        # Step 4: Wait for timeout and transition to HALF_OPEN
        time.sleep(0.25)

        # Step 5: Successful requests in HALF_OPEN
        breaker.call(lambda: self._simulate_rpc_call(should_fail=False))
        assert breaker.get_state() == CircuitState.HALF_OPEN

        breaker.call(lambda: self._simulate_rpc_call(should_fail=False))
        assert breaker.get_state() == CircuitState.CLOSED

        # Step 6: Verify normal operation resumed
        result = breaker.call(lambda: self._simulate_rpc_call(should_fail=False))
        assert result == "rpc_success"

    def test_circuit_breaker_instant_pause_on_threshold(self) -> None:
        """Test that outbound requests pause instantly upon crossing error threshold."""
        config = CircuitBreakerConfig(
            failure_threshold=5,
            timeout_seconds=10.0,
        )
        breaker = CircuitBreaker(config, name="instant_pause_test")

        start_time = time.monotonic()

        # Trigger failures to reach threshold
        for _ in range(5):
            with pytest.raises(Exception):
                breaker.call(lambda: self._simulate_rpc_call(should_fail=True))

        threshold_reached_time = time.monotonic()

        # Circuit should be OPEN
        assert breaker.get_state() == CircuitState.OPEN

        # Next request should be rejected instantly (not attempted)
        rejection_start = time.monotonic()
        with pytest.raises(CircuitBreakerError):
            breaker.call(lambda: self._simulate_rpc_call(should_fail=False))
        rejection_time = time.monotonic() - rejection_start

        # Rejection should be nearly instant (< 10ms)
        assert rejection_time < 0.01

        # Verify that the function was not called
        metrics = breaker.get_metrics()
        assert metrics.rejected_requests > 0

    def _simulate_rpc_call(self, should_fail: bool) -> str:
        """Simulate an RPC endpoint call."""
        if should_fail:
            raise Exception("RPC endpoint failure")
        return "rpc_success"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
import pytest
from unittest.mock import patch, MagicMock
from src.queue.backpressure import (
    QueueCapacityController,
    SlidingWindowRateLimiter,
    SlidingWindowResult,
)


@patch('psutil.virtual_memory')
def test_dynamic_memory_resizing(mock_virtual_memory):
    # Setup mock to return > 85% memory usage
    mock_mem = MagicMock()
    mock_mem.percent = 86.0
    mock_virtual_memory.return_value = mock_mem

    controller = QueueCapacityController(initial_capacity=1000, min_capacity=100)

    # Check that capacity contracts when memory > 85%
    new_capacity = controller.get_capacity()
    assert new_capacity == 500  # 1000 * 0.5

    # Check it contracts again
    new_capacity = controller.get_capacity()
    assert new_capacity == 250

    # Test normal memory condition (< 85%)
    mock_mem.percent = 50.0
    new_capacity_normal = controller.get_capacity()
    assert new_capacity_normal > 250  # Should start recovering


# ---------------------------------------------------------------------------
# Issue #649 — Sliding Window Rate Limiter
# ---------------------------------------------------------------------------


def test_sliding_window_limiter_allows_within_limit():
    """Requests within the configured limit are allowed."""
    limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=5)
    results = [limiter.allow("region-1") for _ in range(5)]
    assert all(r.allowed for r in results)
    assert results[-1].remaining == 0


def test_sliding_window_limiter_blocks_over_limit():
    """Requests exceeding the configured limit are blocked."""
    limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=3)
    for _ in range(3):
        limiter.allow("region-1")
    blocked = limiter.allow("region-1")
    assert not blocked.allowed
    assert blocked.remaining == 0
    assert blocked.retry_after_s > 0


def test_sliding_window_limiter_per_key_isolation():
    """Rate limits are tracked independently per key."""
    limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=2)
    r1 = limiter.allow("key-a")
    r2 = limiter.allow("key-b")
    assert r1.allowed and r2.allowed


def test_sliding_window_limiter_check_is_readonly():
    """check() does not consume a slot."""
    limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=2)
    limiter.allow("key")
    result = limiter.check("key")
    assert result.allowed
    assert result.remaining == 1
    # allow() should still work since check didn't consume
    result2 = limiter.allow("key")
    assert result2.allowed


def test_sliding_window_limiter_window_eviction():
    """Expired timestamps are evicted from the window."""
    import time
    limiter = SlidingWindowRateLimiter(window_size_s=0.05, max_requests=2)
    limiter.allow("key")
    limiter.allow("key")
    blocked = limiter.allow("key")
    assert not blocked.allowed
    # Wait for window to expire
    time.sleep(0.06)
    allowed = limiter.allow("key")
    assert allowed.allowed


def test_sliding_window_limiter_remaining():
    """remaining() returns correct slot count."""
    limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=5)
    assert limiter.remaining("key") == 5
    limiter.allow("key")
    assert limiter.remaining("key") == 4


def test_sliding_window_limiter_reset():
    """reset() clears tracked timestamps for a key."""
    limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=2)
    limiter.allow("key")
    limiter.allow("key")
    blocked = limiter.allow("key")
    assert not blocked.allowed
    limiter.reset("key")
    allowed = limiter.allow("key")
    assert allowed.allowed


def test_sliding_window_limiter_reset_all():
    """reset_all() clears all tracked timestamps."""
    limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=1)
    limiter.allow("key-a")
    limiter.allow("key-b")
    limiter.reset_all()
    assert limiter.remaining("key-a") == 1
    assert limiter.remaining("key-b") == 1


# ---------------------------------------------------------------------------
# Issue #629 — Adaptive Weighted Random Early Detection (WRED) Drops
# ---------------------------------------------------------------------------

from src.queue.backpressure import PacketPriority, WREDQueue, WREDThreshold


class TestWREDQueue:
    """Tests for the Weighted Random Early Detection (WRED) drop policy."""

    @staticmethod
    def _fill_with_consensus(queue: WREDQueue, count: int) -> None:
        """Fill *queue* with CONSENSUS packets, which never trigger WRED."""
        for i in range(count):
            assert queue.offer(f"consensus-filler-{i}", PacketPriority.CONSENSUS)

    def test_wred_drop_policy(self) -> None:
        """CONSENSUS packets pass through at any saturation level while
        TELEMETRY packets are progressively shed as the queue saturates."""
        queue = WREDQueue(capacity=100)

        # Below every priority's min_threshold: nothing is shed yet.
        self._fill_with_consensus(queue, 20)  # saturation = 0.20
        assert queue.drop_probability(PacketPriority.TELEMETRY) == 0.0
        assert queue.drop_probability(PacketPriority.STANDARD) == 0.0
        assert queue.offer("telemetry-early", PacketPriority.TELEMETRY)
        assert queue.offer("standard-early", PacketPriority.STANDARD)

        # Saturation enters TELEMETRY's drop ramp (0.3-0.8) but stays below
        # STANDARD's ramp (0.6-0.9): only telemetry starts being shed.
        self._fill_with_consensus(queue, 30)  # saturation = 0.52
        telemetry_probability = queue.drop_probability(PacketPriority.TELEMETRY)
        assert 0.0 < telemetry_probability < 1.0
        assert queue.drop_probability(PacketPriority.STANDARD) == 0.0
        assert queue.drop_probability(PacketPriority.CONSENSUS) == 0.0

        # Saturation clears every non-CONSENSUS max_threshold: TELEMETRY is
        # now shed with certainty while CONSENSUS keeps passing through.
        self._fill_with_consensus(queue, 33)  # saturation = 0.85
        assert queue.drop_probability(PacketPriority.TELEMETRY) == 1.0
        assert not queue.offer("telemetry-shed", PacketPriority.TELEMETRY)
        assert queue.offer("consensus-passthrough", PacketPriority.CONSENSUS)

        metrics = queue.get_metrics()
        assert metrics.dropped_by_priority[PacketPriority.TELEMETRY.value] == 1
        assert PacketPriority.CONSENSUS.value not in metrics.dropped_by_priority

    def test_wred_queue_rejects_non_positive_capacity(self) -> None:
        """Capacity must be a positive integer."""
        with pytest.raises(ValueError):
            WREDQueue(capacity=0)

    def test_wred_queue_hard_capacity_drops_regardless_of_priority(self) -> None:
        """Once the queue is physically full, admission fails for any
        priority (including CONSENSUS), since there is no room left."""
        queue = WREDQueue(capacity=2)
        assert queue.offer("a", PacketPriority.CONSENSUS)
        assert queue.offer("b", PacketPriority.CONSENSUS)

        assert not queue.offer("c", PacketPriority.CONSENSUS)

        metrics = queue.get_metrics()
        assert metrics.dropped_packets == 1
        assert metrics.admitted_packets == 2

    def test_wred_queue_fifo_dequeue_order(self) -> None:
        """Admitted packets are dequeued in FIFO order."""
        queue = WREDQueue(capacity=10)
        queue.offer("first", PacketPriority.CONSENSUS)
        queue.offer("second", PacketPriority.CONSENSUS)
        queue.offer("third", PacketPriority.CONSENSUS)

        assert queue.poll() == "first"
        assert queue.poll() == "second"
        assert queue.poll() == "third"
        assert queue.poll() is None

    def test_wred_queue_custom_thresholds(self) -> None:
        """Custom per-priority thresholds override the defaults."""
        thresholds = {
            PacketPriority.TELEMETRY: WREDThreshold(
                min_threshold=0.0, max_threshold=0.1, max_drop_probability=1.0
            ),
        }
        queue = WREDQueue(capacity=10, thresholds=thresholds)

        # Saturation is already >= max_threshold (0.1) at 0 items only once
        # something occupies the queue; add one filler item first.
        queue.offer("filler", PacketPriority.CONSENSUS)  # saturation = 0.1
        assert queue.drop_probability(PacketPriority.TELEMETRY) == 1.0
        assert not queue.offer("telemetry", PacketPriority.TELEMETRY)