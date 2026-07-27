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