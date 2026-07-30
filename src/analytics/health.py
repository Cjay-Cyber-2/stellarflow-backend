from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Rolling window for score evaluation.
WINDOW_SECONDS: int = 3600

# A node whose hit-rate falls below this percentage is flagged as degraded.
DEGRADED_THRESHOLD: float = 85.0

# Internal event type: (utc_timestamp, hit)
_Event = Tuple[datetime, bool]


@dataclass(frozen=True)
class NodeStatus:
    """Immutable point-in-time health snapshot for a single validator node."""

    node_id: str
    score: float             # 0.0 – 100.0 hit-rate over the rolling window
    degraded: bool           # True when score < DEGRADED_THRESHOLD
    consecutive_misses: int  # current unbroken run of missed updates
    window_events: int       # total events counted inside the rolling window


class _NodeState:
    """Per-node mutable tracking data.

    All methods assume the caller holds *lock*. Locking is the responsibility
    of HealthMonitor so that _NodeState stays a pure data container.
    """

    __slots__ = ("events", "consecutive_misses", "lock")

    def __init__(self) -> None:
        self.events: Deque[_Event] = deque()
        self.consecutive_misses: int = 0
        self.lock: threading.Lock = threading.Lock()

    def _prune(self, cutoff: datetime) -> None:
        """Drop events older than *cutoff* from the front of the deque.

        Time: O(k) where k = number of expired events — amortised O(1) per
        event appended, since each event is pruned at most once.
        """
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def record(self, hit: bool) -> None:
        """Append a hit or miss event and maintain the rolling window.

        Resets consecutive_misses to zero on a hit; increments it on a miss.
        Called under self.lock.
        """
        now = datetime.now(timezone.utc)
        self._prune(now - timedelta(seconds=WINDOW_SECONDS))
        self.events.append((now, hit))
        if hit:
            self.consecutive_misses = 0
        else:
            self.consecutive_misses += 1

    def compute_score(self) -> float:
        """Return the telemetry hit-rate as a percentage over the rolling window.

        Prunes stale events before computing so the score always reflects the
        current window, even when no new events have arrived recently.

        Returns 100.0 when the window is empty (node assumed healthy until
        proven otherwise). Callers can inspect window_events == 0 via
        build_status() to distinguish "no data" from a perfect record.

        Time: O(n) where n = events in the window.
        Called under self.lock.
        """
        now = datetime.now(timezone.utc)
        self._prune(now - timedelta(seconds=WINDOW_SECONDS))
        total = len(self.events)
        if total == 0:
            return 100.0
        hits = sum(1 for _, h in self.events if h)
        return (hits / total) * 100.0

    def build_status(self, node_id: str) -> NodeStatus:
        """Construct a NodeStatus snapshot from current state.
        Called under self.lock.
        """
        sc = self.compute_score()
        return NodeStatus(
            node_id=node_id,
            score=round(sc, 2),
            degraded=sc < DEGRADED_THRESHOLD,
            consecutive_misses=self.consecutive_misses,
            window_events=len(self.events),
        )


class HealthMonitor:
    """Automated monitoring tracker for validator node telemetry health.

    Records hit and miss events per node within a 1-hour rolling window.
    Flags a node as locally degraded when its telemetry score drops below
    DEGRADED_THRESHOLD (85%) — without requiring any on-chain broadcast.

    Each node owns an independent threading.Lock so concurrent recordings
    across different nodes proceed without contention.

    Singleton: obtain via the module-level ``health_monitor`` object or
    ``HealthMonitor()`` — both return the same instance.

    Complexity
    ----------
    Time  : O(1) amortised — record_hit, record_miss, health_score,
                             is_degraded, consecutive_misses, node_status.
            O(N * n)        — snapshot, N nodes × n window events each.
    Space : O(N * E)        — N tracked nodes, E events per node per hour.
    """

    _instance: Optional[HealthMonitor] = None
    _init_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> HealthMonitor:
        # Double-checked locking: fast path skips _init_lock once the singleton
        # is constructed, keeping repeated access essentially free.
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance.__states: Dict[str, _NodeState] = {}
                    instance.__map_lock = threading.Lock()
                    cls._instance = instance
        return cls._instance

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_state(self, node_id: str) -> _NodeState:
        """Return the per-node state object, creating it lazily on first access.

        Double-checked locking ensures __map_lock is acquired only for initial
        creation; the common path (state already exists) is contention-free.
        """
        state = self.__states.get(node_id)
        if state is None:
            with self.__map_lock:
                state = self.__states.get(node_id)
                if state is None:
                    state = _NodeState()
                    self.__states[node_id] = state
        return state

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_hit(self, node_id: str) -> None:
        """Register a successful telemetry update for *node_id*.

        Resets the consecutive-miss counter and appends a hit event to the
        rolling window.

        Time: O(1) amortised.
        """
        state = self._get_state(node_id)
        with state.lock:
            state.record(hit=True)

    def record_miss(self, node_id: str) -> None:
        """Register a missed telemetry update for *node_id*.

        Increments the consecutive-miss counter and appends a miss event.
        Emits a WARNING log when the resulting score crosses below
        DEGRADED_THRESHOLD so operators have a local signal without
        requiring an on-chain query.

        Time: O(1) amortised.
        """
        state = self._get_state(node_id)
        with state.lock:
            state.record(hit=False)
            sc = state.compute_score()
            if sc < DEGRADED_THRESHOLD:
                logger.warning(
                    "[HealthMonitor] Node degraded: %s "
                    "(score=%.2f%%, consecutive_misses=%d)",
                    node_id,
                    sc,
                    state.consecutive_misses,
                )

    def health_score(self, node_id: str) -> float:
        """Return the telemetry hit-rate percentage over the rolling window.

        Returns 100.0 if no events have been recorded yet.

        Time: O(n) where n = events in the window.
        """
        state = self._get_state(node_id)
        with state.lock:
            return state.compute_score()

    def is_degraded(self, node_id: str) -> bool:
        """Return True if *node_id*'s score is below DEGRADED_THRESHOLD.

        Time: O(n) where n = events in the window.
        """
        return self.health_score(node_id) < DEGRADED_THRESHOLD

    def consecutive_misses(self, node_id: str) -> int:
        """Return the current unbroken run of missed updates for *node_id*.

        Time: O(1).
        """
        state = self._get_state(node_id)
        with state.lock:
            return state.consecutive_misses

    def node_status(self, node_id: str) -> NodeStatus:
        """Return a full NodeStatus snapshot for *node_id*.

        Time: O(n) where n = events in the window.
        """
        state = self._get_state(node_id)
        with state.lock:
            return state.build_status(node_id)

    def snapshot(self) -> List[NodeStatus]:
        """Return a NodeStatus snapshot for every tracked node.

        Takes a copy of the node-ID set under __map_lock, then releases it
        before acquiring individual node locks. This prevents the deadlock that
        would arise if __map_lock were held while waiting on per-node locks
        already owned by other threads.

        Time: O(N * n) where N = number of nodes, n = average window events.
        """
        with self.__map_lock:
            items = list(self.__states.items())

        result: List[NodeStatus] = []
        for node_id, state in items:
            with state.lock:
                result.append(state.build_status(node_id))
        return result

    def reset(self, node_id: str) -> None:
        """Clear all recorded events and counters for *node_id*.

        Time: O(1).
        """
        state = self._get_state(node_id)
        with state.lock:
            state.events.clear()
            state.consecutive_misses = 0
        logger.info("[HealthMonitor] State reset for node: %s", node_id)


# Module-level singleton — import and use directly.
health_monitor = HealthMonitor()
