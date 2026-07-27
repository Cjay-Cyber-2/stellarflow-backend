#!/usr/bin/env python3
"""
Database Connection Keep-Alive and Adaptive Timeout Controller
==============================================================
Two complementary components for robust DB write paths under load.

ConnectionKeepAlive
-------------------
Maintains long-lived relational connections during quiet, low-volume market
windows.

Serverless / autoscaled Postgres sinks (and intermediary poolers such as
PgBouncer) frequently drop idle TCP connections after a short timeout. When the
next price record arrives, the first write then stalls waiting for a fresh TCP
handshake and re-authentication. This module runs a low-overhead background
heartbeat (``SELECT 1;``) on a fixed interval to keep the channel warm so the
write path never pays the reconnect cost.

The keep-alive is connection-agnostic: it accepts any DB-API 2.0 connection
exposing ``cursor()``. It is meaningful for networked backends (e.g. PostgreSQL
via ``psycopg2``); for a local ``sqlite3`` connection there is no socket to keep
open, so the ping is harmless but inert.

AdaptiveTimeoutController
--------------------------
Dynamically calculates per-operation query timeout boundaries based on two
real-time engine signals:

  1. **Active connection count** — a pool under heavy concurrency needs wider
     timeouts so queued writers are not rejected before they even start.
  2. **Engine response latency (ms)** — a slow Postgres instance warrants more
     headroom; a fast one can be held to a tighter budget.

The formula is intentionally transparent and deterministic so operators can
reason about it without black-box tuning:

    timeout = BASE_TIMEOUT
              + LATENCY_COEFFICIENT  × latency_ms
              + CONNECTION_COEFFICIENT × active_connections

Both coefficients and the hard floor/ceiling are configurable at construction
time. The controller is thread-safe: the same instance can be shared across the
BatchSink flush thread, the HTTP handler pool, and any background worker.

Usage::

    controller = AdaptiveTimeoutController()
    timeout_s = controller.calculate_timeout(
        active_connections=pool.checked_out(),
        latency_ms=probe.last_rtt_ms(),
    )
    with db_op_with_timeout(timeout_s):
        ...

    # Optionally record observed latency samples so the controller can expose
    # a rolling average for monitoring:
    controller.record_latency(latency_ms=probe.last_rtt_ms())
    avg = controller.average_latency_ms()
"""

import logging
import threading
import time
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, Type
from collections import deque

try:  # psycopg2 is optional: the module must import under sqlite/test setups too.
    import psycopg2
except ImportError:  # pragma: no cover - exercised only where psycopg2 is absent
    psycopg2 = None

logger = logging.getLogger(__name__)
 
# ---------------------------------------------------------------------------
# ConnectionKeepAlive constants
# ---------------------------------------------------------------------------

# Default heartbeat cadence in seconds. Idle-connection timeouts on serverless
# Postgres / PgBouncer are commonly 60-300s, so a 30s ping keeps the channel
# warm with comfortable margin.
DEFAULT_PING_INTERVAL: float = 30.0
HEARTBEAT_QUERY: str = "SELECT 1;"

# ---------------------------------------------------------------------------
# ConnectionPoolHealthMonitor constants
# ---------------------------------------------------------------------------

# How often the background loop validates a pooled connection. Shorter than the
# keep-alive cadence so a stale socket left by a DB restart is caught and the
# pool rebuilt before too many write attempts hit the dead connection.
DEFAULT_HEALTH_CHECK_INTERVAL: float = 15.0

# Number of consecutive failed probes that escalate from discarding individual
# connections to rebuilding the whole pool. A single blip discards one socket;
# a true outage (every pooled socket dead after a restart) trips the rebuild.
DEFAULT_FAILURE_THRESHOLD: int = 2

# ---------------------------------------------------------------------------
# AdaptiveTimeoutController constants
# ---------------------------------------------------------------------------

# Baseline timeout in seconds applied before any adjustment factors.
DEFAULT_BASE_TIMEOUT_S: float = 5.0

# Seconds of extra headroom added per millisecond of observed engine latency.
# At 100 ms RTT this adds 0.5 s; at 500 ms it adds 2.5 s.
DEFAULT_LATENCY_COEFFICIENT: float = 0.005

# Seconds of extra headroom added per active connection in the pool.
# 50 checked-out connections adds 2.5 s at the default coefficient.
DEFAULT_CONNECTION_COEFFICIENT: float = 0.05

# Hard lower bound: never issue a timeout shorter than this, regardless of
# how healthy the engine looks. Protects against accidental zero-timeout bugs.
DEFAULT_MIN_TIMEOUT_S: float = 2.0

# Hard upper bound: cap the timeout so a pathologically slow engine cannot
# stall a write thread indefinitely.
DEFAULT_MAX_TIMEOUT_S: float = 60.0

# Rolling window size for the internal latency sample buffer used by
# record_latency() / average_latency_ms().
DEFAULT_LATENCY_WINDOW: int = 100

# ---------------------------------------------------------------------------
# Index Usage Telemetry constants
# ---------------------------------------------------------------------------

# Threshold for considering an index as under-utilized (percentage of scans)
DEFAULT_INDEX_USAGE_THRESHOLD: float = 0.05  # 5% of total scans

# Minimum number of index scans before considering usage statistics valid
DEFAULT_MIN_INDEX_SCANS: int = 100

# Alert emission interval in seconds (prevent spam)
DEFAULT_ALERT_INTERVAL_S: float = 3600.0  # 1 hour


class IndexUsageTelemetry:
    """Tracks database index usage and generates diagnostic alerts for under-utilized indexes.
    
    This class monitors index usage statistics from PostgreSQL's pg_stat_user_indexes
    view and emits alerts when indexes are not being used effectively. Under-utilized
    indexes consume valuable storage I/O bandwidth without providing query performance
    benefits.
    
    The telemetry is designed to work with PostgreSQL databases via psycopg2. For other
    database backends, the tracking is a no-op but will not raise errors.
    
    Usage::
    
        telemetry = IndexUsageTelemetry(connection)
        telemetry.record_index_usage()  # Called periodically
        alerts = telemetry.get_underutilized_alerts()
    """
    
    def __init__(
        self,
        connection: Any,
        usage_threshold: float = DEFAULT_INDEX_USAGE_THRESHOLD,
        min_scans: int = DEFAULT_MIN_INDEX_SCANS,
        alert_interval: float = DEFAULT_ALERT_INTERVAL_S,
    ) -> None:
        """Initialize index usage telemetry tracker.
        
        Parameters
        ----------
        connection:
            Database connection object exposing cursor() method (DB-API 2.0).
        usage_threshold:
            Minimum usage ratio (0.0-1.0) below which an index is considered
            under-utilized. Default is 0.05 (5%).
        min_scans:
            Minimum number of index scans required before usage statistics
            are considered valid. Default is 100.
        alert_interval:
            Minimum seconds between repeated alerts for the same index to
            prevent alert spam. Default is 3600 (1 hour).
        """
        if connection is None:
            raise ValueError("connection must not be None")
        if not (0.0 <= usage_threshold <= 1.0):
            raise ValueError("usage_threshold must be between 0.0 and 1.0")
        if min_scans < 0:
            raise ValueError("min_scans must be non-negative")
        if alert_interval <= 0:
            raise ValueError("alert_interval must be positive")
        
        self._conn = connection
        self._usage_threshold = usage_threshold
        self._min_scans = min_scans
        self._alert_interval = alert_interval
        
        self._index_stats: Dict[str, Dict[str, Any]] = {}
        self._last_alert_time: Dict[str, float] = {}
        self._lock = threading.Lock()
        
        logger.info(
            "IndexUsageTelemetry initialised: threshold=%.2f min_scans=%d alert_interval=%.1fs",
            usage_threshold,
            min_scans,
            alert_interval,
        )
    
    def record_index_usage(self) -> None:
        """Query and record current index usage statistics from the database.
        
        This method executes a query against PostgreSQL's pg_stat_user_indexes
        view to collect index scan counts and updates the internal tracking state.
        
        For non-PostgreSQL databases or when the query fails, this method logs
        a debug message and returns without raising an exception.
        """
        if psycopg2 is None:
            logger.debug("IndexUsageTelemetry: psycopg2 not available, skipping index stats")
            return
        
        try:
            with self._lock:
                cursor = self._conn.cursor()
                try:
                    # Query PostgreSQL's index statistics
                    query = """
                        SELECT 
                            schemaname,
                            relname as table_name,
                            indexrelname as index_name,
                            idx_scan as index_scans,
                            idx_tup_read as tuples_read,
                            idx_tup_fetch as tuples_fetched
                        FROM pg_stat_user_indexes
                    """
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    
                    # Update internal statistics
                    current_time = time.time()
                    for row in rows:
                        schemaname, table_name, index_name, index_scans, tuples_read, tuples_fetched = row
                        key = f"{schemaname}.{table_name}.{index_name}"
                        
                        self._index_stats[key] = {
                            "schemaname": schemaname,
                            "table_name": table_name,
                            "index_name": index_name,
                            "index_scans": index_scans,
                            "tuples_read": tuples_read,
                            "tuples_fetched": tuples_fetched,
                            "last_updated": current_time,
                        }
                    
                    logger.debug(
                        "IndexUsageTelemetry: recorded stats for %d indexes",
                        len(rows)
                    )
                finally:
                    close = getattr(cursor, "close", None)
                    if callable(close):
                        close()
        except Exception as exc:
            logger.debug(
                "IndexUsageTelemetry: failed to record index usage (optional): %s",
                exc
            )
    
    def get_underutilized_alerts(self) -> List[Dict[str, Any]]:
        """Generate diagnostic alerts for under-utilized database indexes.
        
        An index is considered under-utilized if:
        1. It has been scanned at least min_scans times
        2. Its usage ratio is below the usage_threshold
        3. Sufficient time has passed since the last alert for this index
        
        Returns
        -------
        List[Dict[str, Any]]
            List of alert dictionaries, each containing:
            - index_name: Name of the under-utilized index
            - table_name: Table the index belongs to
            - schemaname: Schema name
            - index_scans: Total number of index scans
            - usage_ratio: Calculated usage ratio
            - timestamp: When the alert was generated
        """
        alerts = []
        current_time = time.time()
        
        if not self._index_stats:
            return alerts
        
        # Calculate total scans across all indexes for ratio calculation
        total_scans = sum(
            stats["index_scans"] for stats in self._index_stats.values()
        )
        
        if total_scans == 0:
            return alerts
        
        with self._lock:
            for key, stats in self._index_stats.items():
                index_scans = stats["index_scans"]
                
                # Skip if not enough scans to make a valid assessment
                if index_scans < self._min_scans:
                    continue
                
                # Calculate usage ratio
                usage_ratio = index_scans / total_scans
                
                # Check if under-utilized
                if usage_ratio < self._usage_threshold:
                    # Check alert interval to prevent spam
                    last_alert = self._last_alert_time.get(key, 0)
                    if current_time - last_alert >= self._alert_interval:
                        alert = {
                            "index_name": stats["index_name"],
                            "table_name": stats["table_name"],
                            "schemaname": stats["schemaname"],
                            "index_scans": index_scans,
                            "usage_ratio": usage_ratio,
                            "timestamp": current_time,
                        }
                        alerts.append(alert)
                        self._last_alert_time[key] = current_time
                        
                        logger.warning(
                            "[IndexUsageTelemetry] Under-utilized index detected: "
                            "%s on %s.%s (scans=%d, ratio=%.4f)",
                            stats["index_name"],
                            stats["schemaname"],
                            stats["table_name"],
                            index_scans,
                            usage_ratio,
                        )
        
        return alerts
    
    def get_index_stats(self) -> Dict[str, Dict[str, Any]]:
        """Return the current index usage statistics.
        
        Returns
        -------
        Dict[str, Dict[str, Any]]
            Dictionary mapping index keys to their statistics.
        """
        with self._lock:
            return dict(self._index_stats)


class ConnectionKeepAlive:
    """Background heartbeat that keeps a relational connection channel alive.
 
    A daemon thread wakes every ``interval`` seconds and issues a lightweight
    ``SELECT 1;`` against the supplied connection. The thread is interruptible:
    ``stop()`` signals it via an :class:`threading.Event`, so shutdown does not
    wait out the full interval.
    """
 
    def __init__(
        self,
        connection: Any,
        interval: float = DEFAULT_PING_INTERVAL,
        query: str = HEARTBEAT_QUERY,
    ) -> None:
        if connection is None:
            raise ValueError("connection must not be None")
        if interval <= 0:
            raise ValueError("interval must be a positive number of seconds")
 
        self._conn = connection
        self._interval = interval
        self._query = query
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
 
    @property
    def is_running(self) -> bool:
        """True while the background heartbeat thread is alive."""
        return self._thread is not None and self._thread.is_alive()
 
    def start(self) -> None:
        """Start the background heartbeat thread.
 
        Calling ``start`` on an already-running keep-alive is a no-op.
        """
        if self.is_running:
            logger.debug("ConnectionKeepAlive already running; start() ignored")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="ConnectionKeepAlive",
        )
        self._thread.start()
        logger.info(
            "ConnectionKeepAlive started; pinging every %.1f seconds", self._interval
        )
 
    def ping(self) -> bool:
        """Issue a single heartbeat query.
 
        Returns ``True`` if the ping succeeded, ``False`` if it raised. Failures
        are logged and swallowed so a transient drop never takes down the
        background loop; the next tick simply tries again.
        """
        try:
            with self._lock:
                cursor = self._conn.cursor()
                try:
                    cursor.execute(self._query)
                    cursor.fetchone()
                finally:
                    close = getattr(cursor, "close", None)
                    if callable(close):
                        close()
            logger.debug("Heartbeat ping succeeded")
            return True
        except Exception:
            logger.warning("Heartbeat ping failed; will retry next interval", exc_info=True)
            return False
 
    def _run(self) -> None:
        """Background worker loop.
 
        ``Event.wait`` returns ``True`` when ``stop()`` has been signalled and
        ``False`` on timeout, so the loop ticks once per interval and exits
        promptly on shutdown.
        """
        while not self._stop_event.wait(self._interval):
            self.ping()
 
    def stop(self, timeout: Optional[float] = 5.0) -> None:
        """Signal the background thread to stop and wait for it to exit."""
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        self._thread = None
        logger.info("ConnectionKeepAlive stopped")


def _default_broken_exceptions() -> Tuple[Type[BaseException], ...]:
    """Exception types that signal a stale / dead pooled socket.

    ``OSError`` is always included (it is the base of the stdlib ``socket.error``
    and covers raw connection-reset / broken-pipe failures). When ``psycopg2``
    is importable its ``OperationalError`` (server gone, connection closed) and
    ``InterfaceError`` (connection already closed by the client) are added — the
    two driver errors raised when a pooled connection's socket has died.
    """
    exceptions: Tuple[Type[BaseException], ...] = (OSError,)
    if psycopg2 is not None:
        exceptions = exceptions + (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
        )
    return exceptions


class ConnectionPoolHealthMonitor:
    """Validates pooled connections and rebuilds broken paths automatically.

    A sudden database restart or a transient network drop leaves a connection
    pool holding connections whose underlying TCP socket is dead. The next write
    that checks one of those connections out fails with a stale-socket error,
    and keeps failing until the process is restarted. This monitor closes that
    gap *without* a restart:

    1. **Probe** — every ``interval`` seconds (or on demand via
       :meth:`check_health`) it checks a connection out of the pool, runs a
       lightweight ``validation_query`` (``SELECT 1;``) and returns it.
    2. **Discard** — if the probe raises a stale-socket exception the connection
       is handed back with ``close=True`` so the pool drops the dead socket and
       lazily opens a fresh one on the next ``getconn``.
    3. **Rebuild** — after ``failure_threshold`` *consecutive* failed probes the
       monitor assumes the whole pool is poisoned (the common case after a full
       engine restart), calls ``pool_factory`` to build a replacement, swaps it
       in atomically and closes the old pool. A subsequent healthy probe resets
       the counter.

    The monitor is pool-agnostic: ``pool`` need only expose ``getconn()`` /
    ``putconn(conn)`` and, for the rebuild path, ``closeall()`` — the interface
    of :class:`psycopg2.pool.ThreadedConnectionPool` used by
    :class:`database.hub.ConnectionHub`. ``putconn(conn, close=True)`` is used
    when supported and degrades to a plain ``putconn(conn)`` otherwise.

    Because the pool reference is swapped on recovery, callers that want to keep
    acquiring from the healed pool should read it back through the :attr:`pool`
    property rather than caching the original object.

    The background thread mirrors :class:`ConnectionKeepAlive`: a daemon thread
    driven by a :class:`threading.Event`, so ``stop()`` is prompt and a double
    ``start()`` is a no-op.
    """

    def __init__(
        self,
        pool: Any,
        pool_factory: Callable[[], Any],
        interval: float = DEFAULT_HEALTH_CHECK_INTERVAL,
        validation_query: str = HEARTBEAT_QUERY,
        failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
        broken_exceptions: Optional[Tuple[Type[BaseException], ...]] = None,
    ) -> None:
        if pool is None:
            raise ValueError("pool must not be None")
        if pool_factory is None or not callable(pool_factory):
            raise ValueError("pool_factory must be a callable returning a pool")
        if interval <= 0:
            raise ValueError("interval must be a positive number of seconds")
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")

        self._pool = pool
        self._pool_factory = pool_factory
        self._interval = interval
        self._query = validation_query
        self._failure_threshold = failure_threshold
        self._broken = broken_exceptions or _default_broken_exceptions()

        self._consecutive_failures = 0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def pool(self) -> Any:
        """The current live pool — the rebuilt one after any recovery swap."""
        with self._lock:
            return self._pool

    @property
    def is_running(self) -> bool:
        """True while the background health-check thread is alive."""
        return self._thread is not None and self._thread.is_alive()

    @property
    def consecutive_failures(self) -> int:
        """Number of consecutive failed probes since the last healthy one."""
        with self._lock:
            return self._consecutive_failures

    def start(self) -> None:
        """Start the background health-check loop.

        Calling ``start`` on an already-running monitor is a no-op.
        """
        if self.is_running:
            logger.debug("ConnectionPoolHealthMonitor already running; start() ignored")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="ConnectionPoolHealthMonitor",
        )
        self._thread.start()
        logger.info(
            "ConnectionPoolHealthMonitor started; validating every %.1f seconds "
            "(rebuild after %d consecutive failures)",
            self._interval,
            self._failure_threshold,
        )

    def check_health(self) -> bool:
        """Run a single probe / discard / rebuild cycle.

        Returns ``True`` if the pool is healthy — either the probe succeeded or
        a rebuild produced a working pool — and ``False`` if the probe failed
        and the failure count has not yet reached the rebuild threshold.

        Never raises: any exception from the probe is classified and handled so
        the background loop can survive an indefinite outage and keep retrying.
        """
        pool = self.pool
        conn = None
        try:
            conn = pool.getconn()
            cursor = conn.cursor()
            try:
                cursor.execute(self._query)
                cursor.fetchone()
            finally:
                close = getattr(cursor, "close", None)
                if callable(close):
                    close()
        except Exception as exc:
            stale = isinstance(exc, self._broken)
            self._return_broken(pool, conn)
            return self._on_failure(exc, stale)

        # Healthy probe: return the connection and reset the failure streak.
        self._return_healthy(pool, conn)
        with self._lock:
            self._consecutive_failures = 0
        logger.debug("Pool health probe succeeded")
        return True

    def recover(self) -> bool:
        """Rebuild the pool via ``pool_factory`` and swap it in atomically.

        The old pool is closed (best-effort ``closeall()``) after the swap so
        in-flight callers holding the old reference are not yanked mid-query.
        Returns ``True`` on a successful rebuild, ``False`` if the factory
        raised (the existing pool is kept so the next tick can retry).
        """
        try:
            new_pool = self._pool_factory()
        except Exception:
            logger.exception("Pool rebuild failed; keeping existing pool for retry")
            return False

        with self._lock:
            old_pool = self._pool
            self._pool = new_pool
            self._consecutive_failures = 0

        if old_pool is not None and old_pool is not new_pool:
            closeall = getattr(old_pool, "closeall", None)
            if callable(closeall):
                try:
                    closeall()
                except Exception:
                    logger.warning("Error closing the old pool after rebuild", exc_info=True)
        logger.info("ConnectionPoolHealthMonitor rebuilt the connection pool")
        return True

    def stop(self, timeout: Optional[float] = 5.0) -> None:
        """Signal the background thread to stop and wait for it to exit."""
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        self._thread = None
        logger.info("ConnectionPoolHealthMonitor stopped")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Background worker loop; ticks once per interval, exits promptly on stop."""
        while not self._stop_event.wait(self._interval):
            self.check_health()

    def _on_failure(self, exc: BaseException, stale: bool) -> bool:
        """Record a failed probe and escalate to a rebuild past the threshold."""
        with self._lock:
            self._consecutive_failures += 1
            failures = self._consecutive_failures
        if stale:
            logger.warning(
                "Pool health probe hit a stale-socket error (%d/%d): %s",
                failures,
                self._failure_threshold,
                exc,
            )
        else:
            logger.warning(
                "Pool health probe failed (%d/%d)",
                failures,
                self._failure_threshold,
                exc_info=True,
            )
        if failures >= self._failure_threshold:
            return self.recover()
        return False

    def _return_healthy(self, pool: Any, conn: Any) -> None:
        """Return a validated connection to the pool, swallowing return errors."""
        if conn is None:
            return
        try:
            pool.putconn(conn)
        except Exception:
            logger.warning("Failed to return a healthy connection to the pool", exc_info=True)

    def _return_broken(self, pool: Any, conn: Any) -> None:
        """Discard a broken connection so the pool drops its dead socket.

        Prefers ``putconn(conn, close=True)`` (psycopg2 pools) so the pool opens
        a fresh connection next time; falls back to a plain ``putconn`` for pools
        that do not accept the keyword.
        """
        if conn is None:
            return
        try:
            pool.putconn(conn, close=True)
        except TypeError:
            # Pool's putconn does not support the close kwarg.
            try:
                pool.putconn(conn)
            except Exception:
                logger.warning("Failed to discard a broken connection", exc_info=True)
        except Exception:
            logger.warning("Failed to discard a broken connection", exc_info=True)


class AdaptiveTimeoutController:
    """Dynamically calculates database query timeout boundaries at runtime.

    The timeout for any single write operation is composed of three additive
    terms:

        timeout_s = base_timeout_s
                  + latency_coefficient   × latency_ms
                  + connection_coefficient × active_connections

    The result is then clamped to ``[min_timeout_s, max_timeout_s]``.

    Rationale
    ---------
    Hard-coded timeouts are optimised for a single point on the
    load/latency curve.  Under heavy analytical batch writes the engine
    response time rises and the connection pool fills up, making a static
    budget too tight and causing valid telemetry updates to be dropped.
    This controller adjusts the budget proportionally so writes succeed
    when they legitimately need more time, while still bounding the wait
    during a genuine outage.

    Thread safety
    -------------
    All public methods are thread-safe. ``record_latency`` and
    ``average_latency_ms`` share a ``threading.Lock`` protecting the
    internal sample deque. ``calculate_timeout`` is stateless with
    respect to the deque and is therefore lock-free.

    Parameters
    ----------
    base_timeout_s:
        Baseline query timeout in seconds.
    latency_coefficient:
        Seconds added per ms of observed engine response latency.
    connection_coefficient:
        Seconds added per active (checked-out) connection.
    min_timeout_s:
        Hard floor; the returned value is never less than this.
    max_timeout_s:
        Hard ceiling; the returned value is never greater than this.
    latency_window:
        Number of recent latency samples retained for ``average_latency_ms``.

    Raises
    ------
    ValueError
        If any boundary argument violates basic sanity (e.g. min > max,
        non-positive base, negative coefficients).
    """

    def __init__(
        self,
        base_timeout_s: float = DEFAULT_BASE_TIMEOUT_S,
        latency_coefficient: float = DEFAULT_LATENCY_COEFFICIENT,
        connection_coefficient: float = DEFAULT_CONNECTION_COEFFICIENT,
        min_timeout_s: float = DEFAULT_MIN_TIMEOUT_S,
        max_timeout_s: float = DEFAULT_MAX_TIMEOUT_S,
        latency_window: int = DEFAULT_LATENCY_WINDOW,
    ) -> None:
        if base_timeout_s <= 0:
            raise ValueError("base_timeout_s must be positive")
        if latency_coefficient < 0:
            raise ValueError("latency_coefficient must be non-negative")
        if connection_coefficient < 0:
            raise ValueError("connection_coefficient must be non-negative")
        if min_timeout_s <= 0:
            raise ValueError("min_timeout_s must be positive")
        if max_timeout_s <= 0:
            raise ValueError("max_timeout_s must be positive")
        if min_timeout_s > max_timeout_s:
            raise ValueError(
                f"min_timeout_s ({min_timeout_s}) must not exceed max_timeout_s ({max_timeout_s})"
            )
        if latency_window < 1:
            raise ValueError("latency_window must be at least 1")

        self._base = base_timeout_s
        self._latency_coeff = latency_coefficient
        self._conn_coeff = connection_coefficient
        self._min = min_timeout_s
        self._max = max_timeout_s

        self._samples: Deque[float] = deque(maxlen=latency_window)
        self._lock = threading.Lock()

        logger.info(
            "AdaptiveTimeoutController initialised: base=%.1fs "
            "latency_coeff=%.4f conn_coeff=%.4f "
            "bounds=[%.1f, %.1f]s window=%d",
            self._base,
            self._latency_coeff,
            self._conn_coeff,
            self._min,
            self._max,
            latency_window,
        )

    # ------------------------------------------------------------------
    # Core calculation
    # ------------------------------------------------------------------

    def calculate_timeout(
        self,
        active_connections: int,
        latency_ms: float,
    ) -> float:
        """Return the adaptive timeout in seconds for the current engine state.

        Parameters
        ----------
        active_connections:
            Number of connections currently checked out from the pool (or
            the total open connection count if the driver does not distinguish
            checked-out vs idle).  Must be >= 0.
        latency_ms:
            Most recent round-trip engine latency in milliseconds (e.g. from a
            ``SELECT 1`` probe or the last successful write duration).
            Must be >= 0.

        Returns
        -------
        float
            Adaptive timeout in seconds, clamped to [min_timeout_s, max_timeout_s].

        Raises
        ------
        ValueError
            If ``active_connections`` or ``latency_ms`` is negative.
        """
        if active_connections < 0:
            raise ValueError("active_connections must be >= 0")
        if latency_ms < 0:
            raise ValueError("latency_ms must be >= 0")

        raw = (
            self._base
            + self._latency_coeff * latency_ms
            + self._conn_coeff * active_connections
        )
        timeout = max(self._min, min(self._max, raw))

        logger.debug(
            "AdaptiveTimeoutController: conns=%d latency_ms=%.1f "
            "raw=%.3fs → timeout=%.3fs",
            active_connections,
            latency_ms,
            raw,
            timeout,
        )
        return timeout

    # ------------------------------------------------------------------
    # Rolling latency tracking
    # ------------------------------------------------------------------

    def record_latency(self, latency_ms: float) -> None:
        """Record an observed engine latency sample.

        Samples are kept in a bounded rolling window so callers can pass the
        result of ``average_latency_ms()`` into ``calculate_timeout`` without
        needing to maintain their own running average.

        Parameters
        ----------
        latency_ms:
            Observed round-trip latency in milliseconds.  Must be >= 0.

        Raises
        ------
        ValueError
            If ``latency_ms`` is negative.
        """
        if latency_ms < 0:
            raise ValueError("latency_ms must be >= 0")
        with self._lock:
            self._samples.append(latency_ms)
        logger.debug("AdaptiveTimeoutController: recorded latency sample %.1f ms", latency_ms)

    def average_latency_ms(self) -> float:
        """Return the rolling average of recorded latency samples.

        Returns 0.0 if no samples have been recorded yet, so callers can
        pass the result directly into ``calculate_timeout`` without a
        special-case check.

        Returns
        -------
        float
            Average latency in milliseconds over the current window.
        """
        with self._lock:
            if not self._samples:
                return 0.0
            return sum(self._samples) / len(self._samples)

    def sample_count(self) -> int:
        """Return the number of latency samples currently in the rolling window."""
        with self._lock:
            return len(self._samples)

    # ------------------------------------------------------------------
    # Convenience: timeout from internally tracked average
    # ------------------------------------------------------------------

    def timeout_from_average(self, active_connections: int) -> float:
        """Calculate timeout using the internally tracked average latency.

        Combines ``average_latency_ms()`` and ``calculate_timeout()`` in one
        call for callers that continuously record samples and want to derive
        a timeout without separately querying the average.

        Parameters
        ----------
        active_connections:
            Number of connections currently checked out from the pool.

        Returns
        -------
        float
            Adaptive timeout in seconds.
        """
        return self.calculate_timeout(
            active_connections=active_connections,
            latency_ms=self.average_latency_ms(),
        )
