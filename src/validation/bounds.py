from __future__ import annotations

import collections
import threading
from statistics import median
from typing import Any, Deque, Dict, List, Optional

DEFAULT_WINDOW_SIZE: int = 100
DEFAULT_VARIANCE_THRESHOLD: float = 0.15

_price_buffer: Deque[float] = collections.deque(maxlen=DEFAULT_WINDOW_SIZE)
_buffer_lock = threading.Lock()


def _current_median() -> Optional[float]:
    if not _price_buffer:
        return None
    return median(_price_buffer)


def is_price_acceptable(
    price: float,
    window: int = DEFAULT_WINDOW_SIZE,
    variance_threshold: float = DEFAULT_VARIANCE_THRESHOLD,
) -> bool:
    """Return True if price is within variance_threshold of the rolling median."""
    if price is None:
        return False
    global _price_buffer
    with _buffer_lock:
        if _price_buffer.maxlen != window:
            old = list(_price_buffer)
            _price_buffer = collections.deque(old[-window:], maxlen=window)
        median_value = _current_median()
        if median_value is None:
            _price_buffer.append(price)
            return True
        deviation = abs(price - median_value) / median_value
        if deviation > variance_threshold:
            return False
        _price_buffer.append(price)
        return True


def filter_record(
    record: Dict[str, Any],
    price_key: str = "price",
    window: int = DEFAULT_WINDOW_SIZE,
    variance_threshold: float = DEFAULT_VARIANCE_THRESHOLD,
) -> Optional[Dict[str, Any]]:
    """Return record if its price passes the median-variance check, else None."""
    try:
        price_val = float(record.get(price_key))  # type: ignore[arg-type]
    except Exception:
        return None
    return record if is_price_acceptable(price_val, window, variance_threshold) else None


def filter_records(
    records: List[Dict[str, Any]],
    price_key: str = "price",
    window: int = DEFAULT_WINDOW_SIZE,
    variance_threshold: float = DEFAULT_VARIANCE_THRESHOLD,
) -> List[Dict[str, Any]]:
    """Filter a batch of records, dropping spiked outliers beyond the threshold."""
    return [r for r in (filter_record(rec, price_key, window, variance_threshold) for rec in records) if r is not None]


__all__ = ["is_price_acceptable", "filter_record", "filter_records"]