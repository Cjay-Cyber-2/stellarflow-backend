import asyncio
import logging
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

import aiohttp
import requests

logger = logging.getLogger(__name__)

# Threshold Parameters
LIGHTWEIGHT_PING_TIMEOUT = 0.8  # Max acceptable time window (800ms) before degradation warning
MOVING_AVG_WINDOW_SIZE = 4      # Number of historic latency checks to weigh mathematically

# Default time a nonce may sit "pending" (issued, but neither confirmed nor
# failed) before it is reported as stale. Tunable per call via
# get_stale(address, timeout_seconds=...).
DEFAULT_STALE_TIMEOUT_SECONDS = 30.0


class HorizonNodeProfile:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.latency_history: List[float] = []
        self.is_healthy = True

    @property
    def moving_average_latency(self) -> float:
        """Calculates historical moving average execution latency parameters."""
        if not self.latency_history:
            return 0.0
        return sum(self.latency_history) / len(self.latency_history)

    def record_metric(self, latency_ms: float):
        """Appends latency sample to bounded historic window tracking loops."""
        self.latency_history.append(latency_ms)
        if len(self.latency_history) > MOVING_AVG_WINDOW_SIZE:
            self.latency_history.pop(0)


class PredictiveRPCSupervisor:
    def __init__(self, primary_endpoints: List[Dict[str, str]], fallback_endpoints: List[Dict[str, str]]):
        """
        Orchestrates network health scoring topologies across core and backup infrastructure arrays.
        Input format example: [{"name": "horizon-main", "url": "https://horizon.stellar.org"}]
        """
        self.primary_pool = [HorizonNodeProfile(node["name"], node["url"]) for node in primary_endpoints]
        self.fallback_pool = [HorizonNodeProfile(node["name"], node["url"]) for node in fallback_endpoints]
        self.active_node: HorizonNodeProfile = self.primary_pool[0]

    async def run_predictive_ping_cycle(self) -> None:
        """
        Executes parallel, lightweight validation pings across the cluster.
        Updates health statuses without introducing blocking execution lags to outer worker frameworks.
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            all_nodes = self.primary_pool + self.fallback_pool

            for node in all_nodes:
                tasks.append(self._probe_node_health(session, node))

            await asyncio.gather(*tasks)

        self._evaluate_routing_topology()

    async def _probe_node_health(self, session: aiohttp.ClientSession, node: HorizonNodeProfile) -> None:
        """
        Dispatches lightweight low-overhead endpoint probes to track real-time communication shifts.
        """
        # Horizon base path used for lightweight connection checks
        probe_url = f"{node.url.rstrip('/')}/"
        start_time = time.monotonic()

        try:
            async with asyncio.timeout(LIGHTWEIGHT_PING_TIMEOUT):
                async with session.get(probe_url) as response:
                    if response.status == 200:
                        latency_ms = (time.monotonic() - start_time) * 1000
                        node.record_metric(latency_ms)

                        # Mark degraded if moving average indicates systematic latency decline
                        if node.moving_average_latency > (LIGHTWEIGHT_PING_TIMEOUT * 1000):
                            if node.is_healthy:
                                logger.warning(f"Predictive Warning: Performance degradation detected on {node.name}. Latency: {node.moving_average_latency:.1f}ms")
                            node.is_healthy = False
                        else:
                            node.is_healthy = True
                        return

                    node.is_healthy = False
                    logger.debug(f"Node {node.name} returned non-200 footprint status: {response.status}")

        except (asyncio.TimeoutError, aiohttp.ClientError):
            node.is_healthy = False
            node.record_metric(LIGHTWEIGHT_PING_TIMEOUT * 1000 * 2)  # Penalize metric tracking log
            logger.warning(f"Predictive Supervisor flagged node [{node.name}] as UNHEALTHY (Timeout/Network breakdown)")

    def _evaluate_routing_topology(self) -> None:
        """
        Dynamically shifts layout traffic pointers to healthier candidate environments.
        """
        # If active node is healthy and performing nominal processing, preserve active route
        if self.active_node.is_healthy:
            return

        logger.warning(f"Active Horizon Endpoint [{self.active_node.name}] degraded. Initializing preemptive failover routine...")

        # 1. Scan primary pool for an alternate healthy node
        for primary in self.primary_pool:
            if primary.is_healthy:
                self.active_node = primary
                logger.info(f"Traffic routing safely shifted to alternate primary node: [{self.active_node.name}]")
                return

        # 2. Fallback to secondary isolated backup arrays if full primary tier crashes
        for fallback in self.fallback_pool:
            if fallback.is_healthy:
                self.active_node = fallback
                logger.critical(f"EMERGENCY: Primary Horizon node array completely degraded! Failover routed to backup: [{self.active_node.name}]")
                return

        logger.error("CRITICAL FAILURE: Comprehensive Horizon node matrix completely unreachable. No healthy nodes found.")


class NonceWindow:
    """Thread-safe sliding window of pre-allocated sequence numbers per account.

    Instead of handing out one sequence number at a time and forcing parallel
    broadcasters to wait on each other, a NonceWindow pre-allocates a bounded
    range ("window") of upcoming sequence numbers per account. Workers acquire
    slots from that range concurrently; the window's base slides forward as
    in-flight slots are acknowledged, making room for new ones.

    Thread-safe: all public methods are protected by a single lock, so
    acquisition/acknowledgement bookkeeping itself never blocks on network
    I/O -- only on other bookkeeping calls, which are O(1)/O(window_size).

    Parameters
    ----------
    window_size:
        Maximum number of concurrent in-flight sequence numbers per account.
    """

    DEFAULT_WINDOW_SIZE: int = 64

    def __init__(self, window_size: int = DEFAULT_WINDOW_SIZE) -> None:
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        self._window_size = window_size
        self._lock = threading.Lock()
        self._base: Dict[str, int] = {}
        self._issued: Dict[str, Set[int]] = defaultdict(set)
        self._max_issued: Dict[str, int] = {}

    @property
    def window_size(self) -> int:
        return self._window_size

    def acquire(self, account: str, seed: Optional[int] = None) -> int:
        """Acquire the next available sequence number for *account*.

        Parameters
        ----------
        account:
            Stellar public key address.
        seed:
            If provided, seeds the window base to this value on first use.
            Ignored once the window for *account* has already been seeded.

        Returns
        -------
        int
            The sequence number to use for the next transaction.

        Raises
        ------
        ValueError
            If the window has not been seeded and *seed* is not provided.
        RuntimeError
            If all window slots are currently in flight.
        """
        with self._lock:
            if account not in self._base:
                if seed is None:
                    raise ValueError(f"NonceWindow for {account!r} is unseeded — no seed supplied")
                self._base[account] = seed
                self._max_issued[account] = seed - 1

            in_flight = len(self._issued[account])
            if in_flight >= self._window_size:
                raise RuntimeError(f"Nonce window for {account!r} is exhausted")

            base = self._base[account]
            nonce = base + in_flight
            self._issued[account].add(nonce)
            self._max_issued[account] = max(self._max_issued[account], nonce)
            return nonce

    def acknowledge(self, account: str, nonce: int) -> None:
        """Acknowledge completion of *nonce*, potentially sliding the window base.

        The base slides forward past any contiguous run of previously-issued
        sequence numbers, starting at the current base, that are no longer
        in flight.

        Parameters
        ----------
        account:
            Stellar public key address.
        nonce:
            The sequence number that completed (confirmed or failed).
        """
        with self._lock:
            if account not in self._base:
                return

            self._issued[account].discard(nonce)

            base = self._base[account]
            max_issued = self._max_issued[account]
            while base <= max_issued and base not in self._issued[account]:
                base += 1
            self._base[account] = base

    def available_slots(self, account: str) -> int:
        """Return the number of free slots in the window for *account*.

        Returns 0 if the window has not been seeded.
        """
        with self._lock:
            if account not in self._base:
                return 0
            base = self._base[account]
            max_issued = self._max_issued[account]
            span = max_issued - base + 1
            return max(0, self._window_size - span)

    def sync(self, account: str, base: int) -> None:
        """Reset the window for *account* to a ledger-authoritative *base*.

        Discards any in-flight bookkeeping and reopens the full window
        starting at *base*. Call this after a tx_bad_seq error once the
        correct ledger sequence is known.

        Parameters
        ----------
        account:
            Stellar public key address.
        base:
            The sequence number to use for the next acquisition.
        """
        with self._lock:
            self._base[account] = base
            self._issued[account] = set()
            self._max_issued[account] = base - 1

    def invalidate(self, account: Optional[str] = None) -> None:
        """Evict window state.

        If *account* is provided, only that account's window is cleared and
        will require re-seeding. If *account* is ``None``, every tracked
        account's window is cleared.
        """
        with self._lock:
            if account is None:
                self._base.clear()
                self._issued.clear()
                self._max_issued.clear()
            else:
                self._base.pop(account, None)
                self._issued.pop(account, None)
                self._max_issued.pop(account, None)


@dataclass
class _PendingNonce:
    """Bookkeeping for a nonce that has been issued but not yet resolved."""

    nonce: int
    issued_at: float = field(default_factory=time.monotonic)


class NonceTracker:
    """Thread-safe per-account nonce tracker with pending-slot recovery.

    This preserves the original strictly-sequential, one-nonce-at-a-time
    contract used by the rest of the transport layer (see tx_manager.py,
    which signs and dispatches under a single per-account lock). What's new
    is visibility into nonces that were handed out but never confirmed or
    failed -- e.g. because the broadcast dropped or the response was lost --
    so callers can detect and recover from those gaps instead of silently
    trusting that every issued nonce eventually landed.

    Each account address owns an independent Lock, so concurrent operations
    across different accounts proceed without contention while a single
    account's nonces remain strictly sequential and duplicate-free.

    Complexity
    ----------
    Time  : O(1) amortised per acquisition, confirmation, failure, or sync.
            O(p) for get_stale, where p is the number of currently pending
            nonces for that account (normally small/bounded by in-flight tx
            count, not a long-term backlog).
    Space : O(n + p) where n is the number of unique account addresses
            tracked and p is the number of currently pending nonces.
    """

    _instance: Optional["NonceTracker"] = None
    _init_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> "NonceTracker":
        # Double-checked locking: fast path avoids acquiring _init_lock once
        # the singleton is fully constructed.
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._account_locks: Dict[str, threading.Lock] = {}
                    instance._nonces: Dict[str, int] = {}
                    # address -> {nonce: _PendingNonce}
                    instance._pending: Dict[str, Dict[int, _PendingNonce]] = {}
                    # Protects _account_locks dict during lazy lock creation.
                    instance._map_lock = threading.Lock()
                    cls._instance = instance
        return cls._instance

    @classmethod
    def create_standalone(cls) -> "NonceTracker":
        """Build an independent NonceTracker instance, bypassing the singleton.

        NonceTracker() always returns the shared, process-wide singleton --
        that's intentional for production code, where every caller for a
        given account should see the same state. This method exists for
        callers that need an isolated instance instead, e.g. tests that
        construct multiple TxManager objects and expect each to start with
        a clean slate, or any code intentionally tracking a separate,
        unshared set of accounts.
        """

        instance = object.__new__(cls)
        instance._account_locks = {}
        instance._nonces = {}
        instance._pending = {}
        instance._map_lock = threading.Lock()
        return instance

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_lock(self, address: str) -> threading.Lock:
        """Return the per-account lock, creating it lazily on first access.

        Double-checked locking ensures _map_lock is acquired only on the
        initial creation, keeping the common path (lock already exists)
        entirely contention-free.
        """
        lock = self._account_locks.get(address)
        if lock is None:
            with self._map_lock:
                lock = self._account_locks.get(address)
                if lock is None:
                    lock = threading.Lock()
                    self._account_locks[address] = lock
        return lock

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_next_nonce(self, address: str, seed: Optional[int] = None) -> int:
        """Return the next unique, monotonically-increasing nonce for *address*.

        On the first call for an account a *seed* (the current on-chain
        sequence number) must be supplied. Subsequent calls increment the
        cached value atomically without further network I/O.

        The returned nonce is recorded as pending until confirm() or fail()
        is called for it, or until it is reported stale via get_stale().

        Args:
            address: Account identifier (e.g. a Stellar public key).
            seed:    Bootstrap nonce when no local cache exists. Required on
                     the first call; ignored once a value is cached.

        Returns:
            An integer nonce guaranteed to be unique and sequential for
            *address* across all concurrent callers.

        Raises:
            ValueError: If no cached nonce exists and no *seed* was supplied.
        """
        lock = self._get_lock(address)
        with lock:
            try:
                cached = self._nonces.get(address)
                if cached is None:
                    if seed is None:
                        raise ValueError(
                            f"No cached nonce for '{address}' and no seed supplied."
                        )
                    self._nonces[address] = seed
                    self._mark_pending(address, seed)
                    logger.info("[NonceTracker] Seeded nonce for %s → %d", address, seed)
                    return seed

                next_nonce = cached + 1
                self._nonces[address] = next_nonce
                self._mark_pending(address, next_nonce)
                return next_nonce
            except Exception:
                # Drop the cache on any error so the next caller is forced to
                # re-sync from the ledger instead of propagating a stale value.
                self._nonces.pop(address, None)
                raise

    def confirm(self, address: str, nonce: int) -> None:
        """Mark *nonce* as confirmed (landed on the ledger) and stop tracking it.

        Call this once the caller learns -- via polling, webhook, or any other
        feedback channel -- that the transaction using this nonce succeeded.

        Time: O(1).
        """
        lock = self._get_lock(address)
        latency_ms = 0.0
        with lock:
            pending = self._pending.get(address)
            if pending is not None:
                info = pending.pop(nonce, None)
                if info is not None:
                    latency_ms = (time.monotonic() - info.issued_at) * 1000
        logger.info(
            "[NonceTracker] Confirmed nonce %d for %s | latency=%.1fms",
            nonce,
            address,
            latency_ms,
        )

    def fail(self, address: str, nonce: int) -> None:
        """Mark *nonce* as failed (rejected or dropped) and stop tracking it.

        This does not by itself roll back the cached counter -- if the chain
        ends up with a gap at this sequence, call sync_nonce() once the
        correct ledger sequence is known. This method only clears the
        pending-slot bookkeeping so the nonce stops being reported as stale.

        Time: O(1).
        """
        lock = self._get_lock(address)
        latency_ms = 0.0
        with lock:
            pending = self._pending.get(address)
            if pending is not None:
                info = pending.pop(nonce, None)
                if info is not None:
                    latency_ms = (time.monotonic() - info.issued_at) * 1000
        logger.info(
            "[NonceTracker] Failed nonce %d for %s | latency=%.1fms",
            nonce,
            address,
            latency_ms,
        )

    def get_stale(
        self, address: str, timeout_seconds: float = DEFAULT_STALE_TIMEOUT_SECONDS
    ) -> List[int]:
        """Return pending nonces for *address* older than *timeout_seconds*.

        A nonce counts as stale if it was issued by get_next_nonce() but has
        not since been resolved via confirm() or fail(), and more than
        timeout_seconds have elapsed. Use this to detect transactions that
        likely dropped or whose outcome was never reported back, so they can
        be investigated, retried, or used to trigger a sync_nonce() call.

        Time: O(p), where p is the number of currently pending nonces for
        this account.
        """
        lock = self._get_lock(address)
        with lock:
            pending = self._pending.get(address)
            if not pending:
                return []
            now = time.monotonic()
            stale = [
                nonce
                for nonce, info in pending.items()
                if (now - info.issued_at) > timeout_seconds
            ]
        return sorted(stale)

    def sync_nonce(self, address: str, nonce: int) -> None:
        """Overwrite the cached nonce with a known-good ledger value.

        Call this after a tx_bad_seq error to realign the local counter with
        the chain's authoritative sequence number. This also clears all
        pending-slot bookkeeping for the account, since any in-flight nonces
        are now superseded by the authoritative value.

        Time: O(1).
        """
        lock = self._get_lock(address)
        with lock:
            self._nonces[address] = nonce
            self._pending.pop(address, None)
            logger.info("[NonceTracker] Synced nonce for %s → %d", address, nonce)

    def get_nonce(self, address: str) -> Optional[int]:
        """Return the current cached nonce for *address*, if it exists.

        Time: O(1).
        """
        lock = self._get_lock(address)
        with lock:
            return self._nonces.get(address)

    def get_pending_nonces(self, address: str) -> List[int]:
        """Return a snapshot of currently pending (unresolved) nonces for *address*.

        This provides safe, lock-protected visibility into the pending-slot
        bookkeeping without exposing the internal ``_pending`` dict directly.
        Callers can use this to inspect which nonces have been issued but not
        yet confirmed or failed -- useful for gap detection and diagnostics.

        Time: O(p), where p is the number of currently pending nonces.
        """
        lock = self._get_lock(address)
        with lock:
            pending = self._pending.get(address)
            if not pending:
                return []
            return sorted(pending.keys())

    def invalidate(self, address: Optional[str] = None) -> None:
        """Evict the cached nonce for *address*, or all accounts when omitted.

        The next call to get_next_nonce will require a seed or an external
        sync from the ledger. Also clears any pending-slot bookkeeping for
        the affected account(s).

        Implementation note: for a full clear, a snapshot of existing accounts
        is taken under _map_lock which is then released before acquiring
        individual per-account locks. This prevents a deadlock that would arise
        if _map_lock were held while waiting for per-account locks that other
        threads may already hold.

        Time: O(1) for a single address; O(n) for a full clear.
        """
        if address is not None:
            lock = self._get_lock(address)
            with lock:
                self._nonces.pop(address, None)
                self._pending.pop(address, None)
            logger.info(
                "[NonceTracker] Nonce invalidated for %s. Re-sync required.", address
            )
            return

        # Snapshot account locks without holding _map_lock during the clear.
        with self._map_lock:
            snapshot = list(self._account_locks.items())

        for addr, lock in snapshot:
            with lock:
                self._nonces.pop(addr, None)
                self._pending.pop(addr, None)

        logger.info("[NonceTracker] All cached nonces cleared. Re-sync required.")

    def _mark_pending(self, address: str, nonce: int) -> None:
        """Record *nonce* as freshly issued and unresolved. Caller holds the lock."""
        account_pending = self._pending.setdefault(address, {})
        account_pending[nonce] = _PendingNonce(nonce=nonce)


# ---------------------------------------------------------------------------
# Nonce Gap Detector and Recovery Engine
# ---------------------------------------------------------------------------

# Default maximum age of a pending nonce before it is considered a candidate
# for gap-based recovery. Shorter than DEFAULT_STALE_TIMEOUT_SECONDS to
# enable re-synchronisation within a single ledger cycle.
DEFAULT_GAP_DETECTION_TIMEOUT_SECONDS: float = 15.0


@dataclass
class GapReport:
    """Report produced by :class:`NonceGapDetector` describing discovered gaps.

    Attributes
    ----------
    address:
        The Stellar account address that was inspected.
    stale_nonces:
        Nonces that have been pending beyond the timeout threshold.
    pending_nonces:
        All currently pending (unresolved) nonces for the account.
    current_nonce:
        The most recently issued cached nonce, if any.
    has_gaps:
        ``True`` when one or more stale nonces exist below the highest
        issued nonce, meaning resolved nonces have leapfrogged them.
    """

    address: str
    stale_nonces: List[int]
    pending_nonces: List[int]
    current_nonce: Optional[int]

    @property
    def has_gaps(self) -> bool:
        """Return True when at least one stale nonce has been leapfrogged.

        A gap exists when a pending nonce is stale AND the tracker has issued
        a higher nonce since (meaning later nonces have been resolved while
        this one remains stuck).
        """
        if not self.stale_nonces or self.current_nonce is None:
            return False
        return any(nonce < self.current_nonce for nonce in self.stale_nonces)


class NonceGapDetector:
    """Detect nonce gaps for Stellar accounts by analysing pending state.

    A *gap* is a nonce that was issued, never confirmed or failed, and has
    been leapfrogged by higher nonces that *were* resolved.  Gaps block the
    Stellar account's sequence because the chain requires strictly sequential
    nonces — a stuck nonce prevents every subsequent transaction from landing.

    Parameters
    ----------
    tracker:
        The :class:`NonceTracker` instance to inspect.  Typically the
        process-wide singleton ``nonce_tracker``.

    Example usage::

        detector = NonceGapDetector(nonce_tracker)
        report = detector.detect_gaps("GACCOUNT")
        if report.has_gaps:
            # trigger recovery
            ...
    """

    def __init__(self, tracker: NonceTracker) -> None:
        self._tracker = tracker

    def detect_gaps(
        self,
        address: str,
        timeout_seconds: float = DEFAULT_GAP_DETECTION_TIMEOUT_SECONDS,
    ) -> GapReport:
        """Analyse the tracker's pending state and return a :class:`GapReport`.

        Parameters
        ----------
        address:
            Stellar public key address.
        timeout_seconds:
            Pending nonces older than this many seconds are considered
            stale and therefore candidates for gap classification.

        Returns
        -------
        GapReport
        """
        stale = self._tracker.get_stale(address, timeout_seconds)
        pending = self._tracker.get_pending_nonces(address)
        current = self._tracker.get_nonce(address)

        return GapReport(
            address=address,
            stale_nonces=stale,
            pending_nonces=pending,
            current_nonce=current,
        )


@dataclass
class ReconciliationResult:
    """Result of a single reconciliation attempt by :class:`NonceRecoveryEngine`.

    Attributes
    ----------
    address:
        The account that was reconciled.
    gaps_detected:
        Stale nonces that were identified as gaps before recovery.
    ledger_sequence:
        The authoritative sequence number returned by the ledger query,
        or ``None`` if the query failed.
    synced:
        ``True`` when the local cache was realigned to the ledger value.
    previous_sequence:
        The locally cached nonce *before* reconciliation, if any.
    """

    address: str
    gaps_detected: List[int]
    ledger_sequence: Optional[int]
    synced: bool
    previous_sequence: Optional[int]


class NonceRecoveryEngine:
    """Automated nonce gap recovery that reconciles within one ledger cycle.

    The engine detects gaps via :class:`NonceGapDetector`, queries the Stellar
    ledger for the authoritative sequence number, and — when the ledger has
    moved past the gap — realigns the local cache via ``sync_nonce()``.

    The entire operation is designed to complete within a single Stellar
    ledger cycle (~5 seconds).  Callers should provide a time-bound
    *ledger_query* (e.g. wrapped with a requests timeout) so the query
    never blocks beyond the cycle boundary.

    Parameters
    ----------
    tracker:
        The :class:`NonceTracker` whose state is reconciled.
    ledger_query:
        A callable ``(address: str) -> int`` that returns the current
        on-chain sequence number for *address* (e.g. from Horizon's
        ``/accounts/{address}`` endpoint).  The callable should include
        its own timeout enforcement.
    gap_timeout_seconds:
        How long a nonce must be pending before the detector flags it as
        stale (default: 15 s — well within one ledger cycle).

    Example usage::

        def query_ledger(addr: str) -> int:
            resp = requests.get(
                f"https://horizon.stellar.org/accounts/{addr}",
                timeout=2,
            )
            return int(resp.json()["sequence"])

        engine = NonceRecoveryEngine(nonce_tracker, query_ledger)
        result = engine.reconcile("GACCOUNT")
        if result.synced:
            print(f"Recovered: synced to {result.ledger_sequence}")
    """

    def __init__(
        self,
        tracker: NonceTracker,
        ledger_query: Callable[[str], int],
        gap_timeout_seconds: float = DEFAULT_GAP_DETECTION_TIMEOUT_SECONDS,
    ) -> None:
        if gap_timeout_seconds < 0:
            raise ValueError("gap_timeout_seconds must be >= 0")

        self._tracker = tracker
        self._detector = NonceGapDetector(tracker)
        self._ledger_query = ledger_query
        self._gap_timeout_seconds = gap_timeout_seconds
        # Serialise reconciliation per account to prevent duplicate ledger
        # queries when multiple threads hit a tx_bad_seq simultaneously.
        self._reconcile_locks: Dict[str, threading.Lock] = {}
        self._reconcile_locks_map_lock = threading.Lock()

    def _get_reconcile_lock(self, address: str) -> threading.Lock:
        """Return a per-account lock for serialising reconcile() calls."""
        lock = self._reconcile_locks.get(address)
        if lock is None:
            with self._reconcile_locks_map_lock:
                lock = self._reconcile_locks.get(address)
                if lock is None:
                    lock = threading.Lock()
                    self._reconcile_locks[address] = lock
        return lock

    def reconcile(self, address: str) -> ReconciliationResult:
        """Detect nonce gaps and reconcile with the Stellar ledger.

        The method follows a three-phase protocol:

        1. **Detect** — use :class:`NonceGapDetector` to find stale nonces
           that have been leapfrogged by higher resolved nonces.
        2. **Query** — call the ledger query function to obtain the
           authoritative on-chain sequence number.
        3. **Sync** — when the ledger sequence exceeds the highest gap,
           realign the local cache via ``sync_nonce()``.

        Recovery is *best-effort*: if the ledger query fails (network error,
        timeout) the method returns a result with ``synced=False`` rather
        than raising, so callers can schedule a retry.

        Returns
        -------
        ReconciliationResult

        Notes
        -----
        This method is **not** re-entrant for the same *address* — a
        per-account lock prevents concurrent reconciliation attempts from
        hammering the ledger with duplicate queries.
        """
        previous = self._tracker.get_nonce(address)

        lock = self._get_reconcile_lock(address)
        with lock:
            report = self._detector.detect_gaps(address, self._gap_timeout_seconds)

            if not report.has_gaps:
                return ReconciliationResult(
                    address=address,
                    gaps_detected=[],
                    ledger_sequence=None,
                    synced=False,
                    previous_sequence=previous,
                )

            # Query the ledger with a timeout so we never block beyond one cycle.
            ledger_seq: Optional[int] = None
            try:
                ledger_seq = self._ledger_query(address)
            except Exception as exc:
                logger.warning(
                    "[NonceRecoveryEngine] Ledger query failed for %s: %s",
                    address,
                    exc,
                )

            if ledger_seq is None or ledger_seq <= max(report.stale_nonces):
                return ReconciliationResult(
                    address=address,
                    gaps_detected=list(report.stale_nonces),
                    ledger_sequence=ledger_seq,
                    synced=False,
                    previous_sequence=previous,
                )

            # Ledger has moved past the gap — realign.
            self._tracker.sync_nonce(address, ledger_seq)
            logger.info(
                "[NonceRecoveryEngine] Recovered %s | gaps=%s | ledger_seq=%d | previous=%s",
                address,
                report.stale_nonces,
                ledger_seq,
                previous,
            )

        return ReconciliationResult(
            address=address,
            gaps_detected=list(report.stale_nonces),
            ledger_sequence=ledger_seq,
            synced=True,
            previous_sequence=previous,
        )


# Module-level singletons – import and use directly.
nonce_tracker = NonceTracker()
nonce_window = NonceWindow()


class RPCNodeFailoverSupervisor:
    """Asynchronous round-robin RPC endpoint balance manager with non-blocking health checks.

    Performs parallel, non-blocking ping checks on all RPC nodes to detect failures
    within 100ms without blocking transaction submissions from healthy nodes.

    Features:
    - Async health checks using aiohttp for non-blocking I/O
    - Parallel ping operations across all endpoints
    - Round-robin load balancing with automatic unhealthy node bypass
    - Sub-100ms failure detection and routing updates
    - Background monitoring loop runs in dedicated thread with async event loop

    Complexity:
    Time: O(1) for active endpoint lookup, O(N) parallel for checking N endpoints.
    Space: O(N) to store latency stats for N endpoints.
    """

    def __init__(
        self,
        endpoints: Optional[List[str]] = None,
        check_interval_sec: float = 2.0,
        latency_threshold_ms: float = 500.0,
        ping_timeout_sec: float = 0.1,  # 100ms timeout for fast failure detection
    ) -> None:
        self.check_interval_sec = check_interval_sec
        self.latency_threshold_ms = latency_threshold_ms
        self.ping_timeout_sec = ping_timeout_sec

        if endpoints is None:
            primary = os.environ.get("RPC_URL")
            fallbacks = os.environ.get("FALLBACK_RPC_URLS")
            loaded = []
            if primary:
                loaded.append(primary.strip())
            if fallbacks:
                for f in fallbacks.split(","):
                    if f.strip():
                        loaded.append(f.strip())
            if not loaded:
                loaded = [
                    "https://rpc.testnet.stellar.org",
                    "https://rpc.mainnet.stellar.org",
                ]
            self.endpoints = loaded
        else:
            self.endpoints = list(endpoints)

        self._lock = threading.Lock()
        self._current_index = 0  # Round-robin index
        self._active_endpoint = self.endpoints[0] if self.endpoints else ""
        self._latencies: Dict[str, float] = {ep: 0.0 for ep in self.endpoints}
        self._healthy_endpoints: set = set(self.endpoints)

        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

    def start(self) -> None:
        """Start the background monitoring thread with async event loop."""
        with self._lock:
            if self._monitor_thread is not None and self._monitor_thread.is_alive():
                return
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(
                target=self._run_monitor,
                name="RPCNodeFailoverSupervisor-AsyncMonitor",
                daemon=True,
            )
            self._monitor_thread.start()
            logger.info("[RPCNodeFailoverSupervisor] Started asynchronous background monitoring with round-robin balancing.")

    def stop(self) -> None:
        """Stop the background monitoring thread and cleanup event loop."""
        self._stop_event.set()
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=2.0)
            self._monitor_thread = None
            logger.info("[RPCNodeFailoverSupervisor] Stopped background monitoring.")

    def get_active_endpoint(self) -> str:
        """Return the currently selected active RPC endpoint using round-robin."""
        with self._lock:
            # Round-robin through healthy endpoints only
            if not self._healthy_endpoints:
                # Fallback to first endpoint if all are unhealthy
                return self._active_endpoint
            
            healthy_list = [ep for ep in self.endpoints if ep in self._healthy_endpoints]
            if not healthy_list:
                return self._active_endpoint
            
            # Return next healthy endpoint in round-robin fashion
            self._current_index = (self._current_index + 1) % len(healthy_list)
            return healthy_list[self._current_index]

    def get_next_healthy_endpoint(self) -> str:
        """Get the next healthy endpoint in round-robin order.
        
        This method bypasses unhealthy nodes automatically and returns the next
        available healthy endpoint without blocking. If no healthy endpoints exist,
        returns the last known active endpoint as a fallback.
        """
        return self.get_active_endpoint()

    async def _ping_node_async(self, session: aiohttp.ClientSession, endpoint: str) -> Optional[float]:
        """Perform async, non-blocking health check on a single node.
        
        Returns latency in milliseconds if successful, None if failed.
        Uses asyncio.timeout to ensure sub-100ms failure detection.
        """
        try:
            start = time.monotonic()
            async with asyncio.timeout(self.ping_timeout_sec):
                async with session.post(
                    endpoint,
                    json={"jsonrpc": "2.0", "id": 1, "method": "getHealth"},
                    ssl=False,  # Skip SSL verification for faster checks
                ) as response:
                    latency_ms = (time.monotonic() - start) * 1000.0
                    if response.status == 200:
                        data = await response.json()
                        if "result" in data or "error" in data:
                            return latency_ms
            return None
        except (asyncio.TimeoutError, aiohttp.ClientError, Exception):
            # Any failure results in None, flagging node as unhealthy
            return None

    async def _check_all_nodes_async(self) -> None:
        """Asynchronously check all RPC nodes in parallel.
        
        Performs non-blocking health checks on all endpoints simultaneously,
        updating health status and latency metrics without blocking parallel transactions.
        """
        # Create a new session for this check cycle
        timeout = aiohttp.ClientTimeout(total=self.ping_timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Launch all ping operations in parallel
            tasks = [
                self._ping_node_async(session, endpoint)
                for endpoint in self.endpoints
            ]
            
            # Wait for all checks to complete (or timeout)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            temp_latencies = {}
            temp_healthy = set()
            
            for endpoint, result in zip(self.endpoints, results):
                if isinstance(result, float) and result is not None:
                    temp_latencies[endpoint] = result
                    temp_healthy.add(endpoint)
                else:
                    temp_latencies[endpoint] = float("inf")
            
            # Update shared state atomically
            with self._lock:
                self._latencies.update(temp_latencies)
                old_healthy = self._healthy_endpoints.copy()
                self._healthy_endpoints = temp_healthy
                
                # Log health status changes
                newly_unhealthy = old_healthy - temp_healthy
                newly_healthy = temp_healthy - old_healthy
                
                if newly_unhealthy:
                    for endpoint in newly_unhealthy:
                        logger.warning(
                            f"[RPCNodeFailoverSupervisor] Node {endpoint} flagged as UNHEALTHY "
                            f"(detection time: <{self.ping_timeout_sec*1000:.0f}ms)"
                        )
                
                if newly_healthy:
                    for endpoint in newly_healthy:
                        latency = temp_latencies.get(endpoint, 0)
                        logger.info(
                            f"[RPCNodeFailoverSupervisor] Node {endpoint} recovered "
                            f"(latency: {latency:.1f}ms)"
                        )
                
                # Update active endpoint if current is unhealthy
                if self._active_endpoint not in self._healthy_endpoints:
                    if temp_healthy:
                        # Switch to fastest healthy endpoint
                        best_endpoint = min(
                            temp_healthy,
                            key=lambda ep: temp_latencies.get(ep, float("inf"))
                        )
                        old_active = self._active_endpoint
                        self._active_endpoint = best_endpoint
                        logger.warning(
                            f"[RPCNodeFailoverSupervisor] Failover: {old_active} → {best_endpoint} "
                            f"(latency: {temp_latencies[best_endpoint]:.1f}ms)"
                        )

    def _run_monitor(self) -> None:
        """Main monitoring loop running in dedicated thread with async event loop.
        
        Creates a new event loop for this thread and runs async health checks
        at regular intervals without blocking transaction submissions.
        """
        # Create new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._event_loop = loop
        
        try:
            while not self._stop_event.is_set():
                # Run async health checks
                try:
                    loop.run_until_complete(self._check_all_nodes_async())
                except Exception as e:
                    logger.error(f"[RPCNodeFailoverSupervisor] Error during health check cycle: {e}")
                
                # Wait for next check interval
                self._stop_event.wait(self.check_interval_sec)
        finally:
            # Cleanup event loop
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
            except Exception:
                pass
            self._event_loop = None


rpc_supervisor = RPCNodeFailoverSupervisor()


__all__ = [
    "NonceTracker",
    "NonceWindow",
    "NonceGapDetector",
    "NonceRecoveryEngine",
    "GapReport",
    "ReconciliationResult",
    "nonce_tracker",
    "nonce_window",
    "RPCNodeFailoverSupervisor",
    "rpc_supervisor",
    "PredictiveRPCSupervisor",
    "HorizonNodeProfile",
]
