from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Any, Optional, Union


MAX_AGE_SECONDS = 45


def _parse_timestamp(value: Union[str, int, float, datetime]) -> Optional[datetime]:
    """Parse a timestamp value into a timezone-aware UTC datetime.

    Accepts:
    - datetime objects (naive assumed UTC)
    - ISO-8601 strings (with or without trailing Z)
    - numeric epoch seconds or milliseconds
    Returns None if the value cannot be parsed.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            # assume UTC for naive datetimes coming from external sources
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        # Heuristic: if value looks like milliseconds (>= 1e12), treat as ms
        try:
            v = float(value)
            if v > 1e12:
                return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        # Normalize trailing Z to +00:00 so fromisoformat can parse it
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        try:
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            # Try parsing as a float string
            try:
                num = float(s)
                # same heuristic as above
                if num > 1e12:
                    return datetime.fromtimestamp(num / 1000.0, tz=timezone.utc)
                return datetime.fromtimestamp(num, tz=timezone.utc)
            except Exception:
                return None

    return None


def is_fresh(timestamp: Union[str, int, float, datetime], max_age_seconds: int = MAX_AGE_SECONDS) -> bool:
    """Return True if the provided timestamp is within `max_age_seconds` of now (UTC)."""
    dt = _parse_timestamp(timestamp)
    if dt is None:
        # Treat unparsable timestamps as not fresh
        return False

    now = datetime.now(timezone.utc)
    age = now - dt
    return age <= timedelta(seconds=max_age_seconds)


def filter_fresh_records(
    records: Iterable[Dict[str, Any]],
    timestamp_key: str = "timestamp",
    max_age_seconds: int = MAX_AGE_SECONDS,
) -> List[Dict[str, Any]]:
    """Return a list containing only records whose `timestamp_key` is fresh.

    Records with missing or unparsable timestamps are dropped.
    """
    out: List[Dict[str, Any]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue

        ts = rec.get(timestamp_key)
        if ts is None:
            # drop records without timestamp
            continue

        if is_fresh(ts, max_age_seconds=max_age_seconds):
            out.append(rec)

    return out


if __name__ == "__main__":
    # Simple CLI demonstration. Not intended as a full test harness.
    import json
    import sys

    try:
        payload = json.load(sys.stdin)
    except Exception:
        print("Provide a JSON array of records on stdin", file=sys.stderr)
        sys.exit(2)

    fresh = filter_fresh_records(payload)
    print(json.dumps(fresh, default=str))
