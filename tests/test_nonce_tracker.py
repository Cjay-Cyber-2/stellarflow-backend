from __future__ import annotations

import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from network.nonce_tracker import (
    GapReport,
    NonceGapDetector,
    NonceRecoveryEngine,
    NonceTracker,
    NonceWindow,
    ReconciliationResult,
    TransactionResubmitter,
    PendingSubmission,
)


# ---------------------------------------------------------------------------
# Basic acquire / seed behaviour
# ---------------------------------------------------------------------------


def test_acquire_requires_seed_on_first_call() -> None:
    window = NonceWindow()
    with pytest.raises(ValueError, match="unseeded"):
        window.acquire("GACC")


def test_acquire_returns_seed_then_increments() -> None:
    window = NonceWindow()
    assert window.acquire("GACC", seed=100) == 100
    assert window.acquire("GACC") == 101
    assert window.acquire("GACC") == 102


def test_seed_ignored_after_window_is_seeded() -> None:
    window = NonceWindow()
    window.acquire("GACC", seed=50)
    # A second seed value supplied here must be ignored.
    assert window.acquire("GACC", seed=999) == 51


# ---------------------------------------------------------------------------
# Window exhaustion and acknowledge-driven sliding
# ---------------------------------------------------------------------------


def test_exhaustion_raises_when_all_slots_in_flight() -> None:
    window = NonceWindow(window_size=4)
    for i in range(4):
        window.acquire("GACC", seed=0 if i == 0 else None)

    with pytest.raises(RuntimeError, match="exhausted"):
        window.acquire("GACC")


def test_acknowledge_opens_slot_after_exhaustion() -> None:
    window = NonceWindow(window_size=2)
    s0 = window.acquire("GACC", seed=10)
    s1 = window.acquire("GACC")

    with pytest.raises(RuntimeError):
        window.acquire("GACC")

    window.acknowledge("GACC", s0)
    # Window should have slid; one new slot is available.
    s2 = window.acquire("GACC")
    assert s2 == 12  # base slid to 11 then 11 was issued next


def test_window_slides_past_consecutive_leading_acknowledged_slots() -> None:
    window = NonceWindow(window_size=4)
    seqs = [window.acquire("GACC", seed=100 if i == 0 else None) for i in range(4)]

    # Acknowledge out of order: 101 first, then 100.
    window.acknowledge("GACC", seqs[1])  # 101 done; base stays at 100
    assert window.available_slots("GACC") == 0  # window still full

    window.acknowledge("GACC", seqs[0])  # 100 done; base slides past 100 and 101
    assert window.available_slots("GACC") == 2  # two slots freed (100 and 101)

    # The next sequences issued must continue from 104.
    assert window.acquire("GACC") == 104
    assert window.acquire("GACC") == 105


def test_full_window_drains_and_resets_base() -> None:
    window = NonceWindow(window_size=3)
    seqs = [window.acquire("GACC", seed=7 if i == 0 else None) for i in range(3)]

    for s in seqs:
        window.acknowledge("GACC", s)

    # All three acknowledged; base should now be at 10 with zero in-flight.
    assert window.available_slots("GACC") == 3
    assert window.acquire("GACC") == 10


# ---------------------------------------------------------------------------
# Available slots reporting
# ---------------------------------------------------------------------------


def test_available_slots_tracks_issued_count() -> None:
    window = NonceWindow(window_size=8)
    assert window.available_slots("GACC") == 0  # unseeded

    window.acquire("GACC", seed=1)
    assert window.available_slots("GACC") == 7

    window.acquire("GACC")
    assert window.available_slots("GACC") == 6


# ---------------------------------------------------------------------------
# Sync resets the window to a ledger-authoritative value
# ---------------------------------------------------------------------------


def test_sync_discards_pending_and_resets_base() -> None:
    window = NonceWindow(window_size=4)
    window.acquire("GACC", seed=50)
    window.acquire("GACC")

    window.sync("GACC", 200)

    assert window.available_slots("GACC") == 4
    assert window.acquire("GACC") == 200


# ---------------------------------------------------------------------------
# Invalidate clears one or all windows
# ---------------------------------------------------------------------------


def test_invalidate_single_account_requires_reseed() -> None:
    window = NonceWindow()
    window.acquire("GA", seed=10)
    window.acquire("GB", seed=20)

    window.invalidate("GA")

    with pytest.raises(ValueError, match="unseeded"):
        window.acquire("GA")

    # GB must be unaffected.
    assert window.acquire("GB") == 21


def test_invalidate_all_clears_every_account() -> None:
    window = NonceWindow()
    window.acquire("GA", seed=1)
    window.acquire("GB", seed=2)

    window.invalidate()

    for addr in ("GA", "GB"):
        with pytest.raises(ValueError, match="unseeded"):
            window.acquire(addr)


# ---------------------------------------------------------------------------
# Account isolation
# ---------------------------------------------------------------------------


def test_independent_accounts_do_not_share_window_state() -> None:
    window = NonceWindow(window_size=4)
    window.acquire("GA", seed=100)  # GA → 100
    window.acquire("GB", seed=200)  # GB → 200
    window.acquire("GA")            # GA → 101

    assert window.acquire("GA") == 102  # GA: third call
    assert window.acquire("GB") == 201  # GB: second call


# ---------------------------------------------------------------------------
# Acknowledge edge cases
# ---------------------------------------------------------------------------


def test_acknowledge_unknown_sequence_is_a_no_op() -> None:
    window = NonceWindow()
    window.acquire("GACC", seed=5)
    # Should not raise; just logs a warning.
    window.acknowledge("GACC", 9999)
    assert window.available_slots("GACC") == NonceWindow.DEFAULT_WINDOW_SIZE - 1


# ---------------------------------------------------------------------------
# Thread-safety: concurrent acquire across workers
# ---------------------------------------------------------------------------


def test_concurrent_acquire_produces_unique_sequences() -> None:
    """All sequences issued under parallel pressure must be unique."""
    window = NonceWindow(window_size=64)
    window.acquire("GACC", seed=0)  # seed

    results: list[int] = []
    lock = threading.Lock()

    def worker(_: int) -> None:
        seq = window.acquire("GACC")
        with lock:
            results.append(seq)

    with ThreadPoolExecutor(max_workers=16) as executor:
        list(executor.map(worker, range(63)))  # 63 + 1 seed = 64 total

    assert len(results) == 63
    all_seqs = [0] + results
    assert len(set(all_seqs)) == 64, "Duplicate sequences detected"
    assert sorted(all_seqs) == list(range(64))


def test_concurrent_acquire_across_different_accounts_no_contention() -> None:
    """Workers on distinct accounts must never interfere."""
    window = NonceWindow(window_size=32)
    accounts = [f"G{i:04d}" for i in range(8)]

    # Seed each account deterministically before releasing workers.
    for i, acct in enumerate(accounts):
        window.acquire(acct, seed=1000 * (i + 1))

    collected: dict[str, list[int]] = {a: [] for a in accounts}
    collected_lock = threading.Lock()

    def worker(account: str) -> None:
        seq = window.acquire(account)
        with collected_lock:
            collected[account].append(seq)

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(worker, a) for a in accounts for _ in range(7)]
        for f in as_completed(futures):
            f.result()

    for acct, seqs in collected.items():
        assert len(set(seqs)) == len(seqs), f"Duplicates for {acct}"
        # Each account's sequences must form a contiguous range.
        assert set(seqs) == set(range(min(seqs), min(seqs) + len(seqs)))


def test_acknowledge_and_acquire_concurrent_no_data_race() -> None:
    """Mixed acquire/acknowledge calls under thread pressure must not corrupt state."""
    window = NonceWindow(window_size=8)
    issued: list[int] = []
    issued_lock = threading.Lock()
    errors: list[Exception] = []

    # Seed the window.
    window.acquire("GACC", seed=0)

    def acquirer(_: int) -> None:
        try:
            seq = window.acquire("GACC")
            with issued_lock:
                issued.append(seq)
        except RuntimeError:
            pass  # window full; acceptable under high concurrency

    def acknowledger(_: int) -> None:
        try:
            with issued_lock:
                if issued:
                    seq = issued[0]
            window.acknowledge("GACC", seq)
        except Exception as exc:
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [
            executor.submit(acquirer if i % 2 == 0 else acknowledger, i)
            for i in range(32)
        ]
        for f in as_completed(futures):
            f.result()

    assert not errors, f"Unexpected errors during concurrent test: {errors}"


# ---------------------------------------------------------------------------
# window_size property
# ---------------------------------------------------------------------------


def test_window_size_property_reflects_constructor_argument() -> None:
    assert NonceWindow(window_size=8).window_size == 8
    assert NonceWindow().window_size == NonceWindow.DEFAULT_WINDOW_SIZE


def test_invalid_window_size_raises() -> None:
    with pytest.raises(ValueError):
        NonceWindow(window_size=0)
    with pytest.raises(ValueError):
        NonceWindow(window_size=-1)


def test_rpc_node_failover_supervisor_basic(monkeypatch) -> None:
    import time
    from network.nonce_tracker import RPCNodeFailoverSupervisor
    import requests

    endpoints = [
        "https://rpc-primary.stellar.org",
        "https://rpc-secondary.stellar.org",
    ]

    class MockResponse:
        status_code = 200

        def json(self):
            return {"result": {"network": "testnet"}}

    mock_calls = []

    def mock_post(url, json=None, timeout=None):
        mock_calls.append(url)
        return MockResponse()

    monkeypatch.setattr(requests, "post", mock_post)

    supervisor = RPCNodeFailoverSupervisor(
        endpoints=endpoints,
        check_interval_sec=0.1,
        latency_threshold_ms=100.0,
        ping_timeout_sec=0.5,
    )

    assert supervisor.get_active_endpoint() == endpoints[0]

    supervisor.start()
    time.sleep(0.3)
    supervisor.stop()

    assert len(mock_calls) > 0
    assert endpoints[0] in mock_calls


def test_rpc_node_failover_supervisor_latency_failover(monkeypatch) -> None:
    import time
    from network.nonce_tracker import RPCNodeFailoverSupervisor
    import requests

    endpoints = [
        "https://rpc-primary.stellar.org",
        "https://rpc-secondary.stellar.org",
    ]

    def mock_post(url, json=None, timeout=None):
        class MockResponse:
            status_code = 200

            def json(self):
                return {"result": {"network": "testnet"}}

        if "primary" in url:
            time.sleep(0.15)
        return MockResponse()

    monkeypatch.setattr(requests, "post", mock_post)

    supervisor = RPCNodeFailoverSupervisor(
        endpoints=endpoints,
        check_interval_sec=0.1,
        latency_threshold_ms=100.0,
        ping_timeout_sec=0.5,
    )

    assert supervisor.get_active_endpoint() == endpoints[0]

    supervisor.start()
    time.sleep(0.3)
    supervisor.stop()

    assert supervisor.get_active_endpoint() == endpoints[1]


def test_rpc_node_failover_supervisor_failure_failover(monkeypatch) -> None:
    import time
    from network.nonce_tracker import RPCNodeFailoverSupervisor
    import requests

    endpoints = [
        "https://rpc-primary.stellar.org",
        "https://rpc-secondary.stellar.org",
    ]

    def mock_post(url, json=None, timeout=None):
        class MockResponse:
            status_code = 200

            def json(self):
                return {"result": {"network": "testnet"}}

        if "primary" in url:
            raise requests.exceptions.ConnectionError("Connection refused")
        return MockResponse()

    monkeypatch.setattr(requests, "post", mock_post)

    supervisor = RPCNodeFailoverSupervisor(
        endpoints=endpoints,
        check_interval_sec=0.1,
        latency_threshold_ms=100.0,
        ping_timeout_sec=0.5,
    )

    assert supervisor.get_active_endpoint() == endpoints[0]

    supervisor.start()
    time.sleep(0.3)
    supervisor.stop()

    assert supervisor.get_active_endpoint() == endpoints[1]


# ===========================================================================
# NonceGapDetector & NonceRecoveryEngine  —  Issue #641
# ===========================================================================


# ---------------------------------------------------------------------------
# get_pending_nonces
# ---------------------------------------------------------------------------


def test_get_pending_nonces_returns_issued_but_unresolved_nonces() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.get_next_nonce("GA")
    assert tracker.get_pending_nonces("GA") == [100, 101, 102]


def test_get_pending_nonces_removes_confirmed_nonces_from_list() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 100)
    assert tracker.get_pending_nonces("GA") == [101, 102]


def test_get_pending_nonces_returns_empty_for_unknown_account() -> None:
    tracker = NonceTracker.create_standalone()
    assert tracker.get_pending_nonces("UNKNOWN") == []


def test_get_pending_nonces_returns_empty_after_full_confirm() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=1)
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 1)
    tracker.confirm("GA", 2)
    assert tracker.get_pending_nonces("GA") == []


# ---------------------------------------------------------------------------
# GapReport.has_gaps
# ---------------------------------------------------------------------------


def test_gap_report_no_stale_nonces_means_no_gaps() -> None:
    report = GapReport(
        address="GA",
        stale_nonces=[],
        pending_nonces=[100, 101],
        current_nonce=101,
    )
    assert not report.has_gaps


def test_gap_report_stale_nonce_below_current_is_a_gap() -> None:
    report = GapReport(
        address="GA",
        stale_nonces=[101],
        pending_nonces=[101, 103],
        current_nonce=104,
    )
    assert report.has_gaps


def test_gap_report_all_stale_above_current_is_not_a_gap() -> None:
    # All stale nonces are >= current — nothing has leapfrogged them.
    report = GapReport(
        address="GA",
        stale_nonces=[105, 106],
        pending_nonces=[105, 106],
        current_nonce=104,
    )
    assert not report.has_gaps


def test_gap_report_no_current_nonce_means_no_gaps() -> None:
    report = GapReport(
        address="GA",
        stale_nonces=[100],
        pending_nonces=[100],
        current_nonce=None,
    )
    assert not report.has_gaps


# ---------------------------------------------------------------------------
# NonceGapDetector.detect_gaps
# ---------------------------------------------------------------------------


def test_detect_gaps_no_pending_nonces_returns_empty() -> None:
    tracker = NonceTracker.create_standalone()
    detector = NonceGapDetector(tracker)
    report = detector.detect_gaps("GA", timeout_seconds=0.0)
    assert report.stale_nonces == []
    assert report.pending_nonces == []
    assert report.current_nonce is None
    assert not report.has_gaps


def test_detect_gaps_fresh_nonces_are_not_stale() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    detector = NonceGapDetector(tracker)
    # With a large timeout, nothing is stale yet.
    report = detector.detect_gaps("GA", timeout_seconds=999.0)
    assert report.stale_nonces == []
    assert report.pending_nonces == [100, 101]
    assert not report.has_gaps


def test_detect_gaps_identifies_stale_nonces() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.get_next_nonce("GA")
    # Confirm nonce 101 so it is no longer pending — 100 remains stuck.
    tracker.confirm("GA", 101)
    tracker.confirm("GA", 102)

    detector = NonceGapDetector(tracker)
    # With zero timeout, any pending nonce is immediately stale.
    report = detector.detect_gaps("GA", timeout_seconds=0.0)
    assert report.stale_nonces == [100]
    # 100 < current_nonce (102) → it has been leapfrogged → gap!
    assert report.has_gaps


def test_detect_gaps_multiple_pending_nonces_with_mixed_state() -> None:
    tracker = NonceTracker.create_standalone()
    for _ in range(6):
        tracker.get_next_nonce("GA", seed=100)
    # Confirm some, leave others pending.
    tracker.confirm("GA", 100)
    tracker.confirm("GA", 102)
    tracker.confirm("GA", 104)
    # Pending: 101, 103, 105

    detector = NonceGapDetector(tracker)
    report = detector.detect_gaps("GA", timeout_seconds=0.0)
    assert set(report.stale_nonces) == {101, 103, 105}
    # 101 and 103 are below current_nonce (105) → gaps
    assert report.has_gaps


def test_detect_gaps_respects_per_account_isolation() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GB", seed=200)
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 100)
    tracker.confirm("GA", 101)

    detector = NonceGapDetector(tracker)
    # GA has no stale nonces.
    report_a = detector.detect_gaps("GA", timeout_seconds=0.0)
    assert not report_a.has_gaps
    assert report_a.stale_nonces == []

    # GB still has nonce 200 pending (and it was the first/only nonce).
    report_b = detector.detect_gaps("GB", timeout_seconds=0.0)
    assert report_b.stale_nonces == [200]
    # 200 is NOT below current nonce (also 200), so no gap.
    assert not report_b.has_gaps


# ---------------------------------------------------------------------------
# NonceRecoveryEngine — reconcile
# ---------------------------------------------------------------------------


def test_nonce_gap_reconciliation() -> None:
    """End-to-end reconciliation: detect gaps, query ledger, sync.

    This is the acceptance test referenced in Issue #641.
    """
    tracker = NonceTracker.create_standalone()

    # Issue a batch of nonces.
    tracker.get_next_nonce("GA", seed=100)
    for _ in range(5):
        tracker.get_next_nonce("GA")
    # Issued: 100-105; all pending.

    # Confirm most; leave 101 and 103 as stuck gaps.
    tracker.confirm("GA", 100)
    tracker.confirm("GA", 102)
    tracker.confirm("GA", 104)
    tracker.confirm("GA", 105)
    # Pending: 101, 103

    # The ledger has moved past both gaps.
    def ledger_query(addr: str) -> int:
        return 106

    engine = NonceRecoveryEngine(
        tracker,
        ledger_query,
        gap_timeout_seconds=0.0,  # immediate staleness for test
    )

    result = engine.reconcile("GA")

    assert result.address == "GA"
    assert sorted(result.gaps_detected) == [101, 103]
    assert result.ledger_sequence == 106
    assert result.synced is True
    assert result.previous_sequence == 105

    # After reconciliation, the tracker must have synced to the ledger.
    assert tracker.get_nonce("GA") == 106
    # All pending slots must be cleared.
    assert tracker.get_pending_nonces("GA") == []


def test_reconcile_no_gaps_returns_early() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.confirm("GA", 100)

    call_count = 0

    def ledger_query(addr: str) -> int:
        nonlocal call_count
        call_count += 1
        return 105

    engine = NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=0.0)
    result = engine.reconcile("GA")

    assert not result.synced
    assert result.gaps_detected == []
    # Ledger must NOT be queried when there are no gaps.
    assert call_count == 0


def test_reconcile_ledger_query_failure_is_handled_gracefully() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 101)
    tracker.confirm("GA", 102)
    # 100 is stale; gap exists.

    def failing_query(addr: str) -> int:
        raise ConnectionError("ledger unreachable")

    engine = NonceRecoveryEngine(
        tracker,
        failing_query,
        gap_timeout_seconds=0.0,
    )

    # Must NOT raise.
    result = engine.reconcile("GA")
    assert result.gaps_detected == [100]
    assert result.ledger_sequence is None
    assert not result.synced
    # Tracker state must be unchanged.
    assert tracker.get_nonce("GA") == 102
    assert tracker.get_pending_nonces("GA") == [100]


def test_reconcile_ledger_not_past_gap_no_sync() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 100)
    tracker.confirm("GA", 102)
    # 101 is stuck in a gap.

    def ledger_query(addr: str) -> int:
        # Ledger is still at 100 — hasn't passed the gap.
        return 100

    engine = NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=0.0)
    result = engine.reconcile("GA")

    assert result.gaps_detected == [101]
    assert result.ledger_sequence == 100
    assert not result.synced
    # Tracker must remain unchanged.
    assert tracker.get_nonce("GA") == 102
    assert 101 in tracker.get_pending_nonces("GA")


def test_reconcile_is_best_effort_on_ledger_none() -> None:
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 101)
    # 100 is stale.

    def ledger_query(addr: str) -> int:
        return None  # type: ignore — simulate a broken query

    engine = NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=0.0)
    result = engine.reconcile("GA")

    assert result.gaps_detected == [100]
    assert not result.synced


def test_reconcile_sync_clears_all_pending() -> None:
    tracker = NonceTracker.create_standalone()
    for _ in range(5):
        tracker.get_next_nonce("GA", seed=200)
    # 200-204 all pending.

    tracker.confirm("GA", 200)
    # 201-204 are stuck/stale.

    def ledger_query(addr: str) -> int:
        return 210

    engine = NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=0.0)
    result = engine.reconcile("GA")

    assert result.synced
    assert result.ledger_sequence == 210
    # sync_nonce clears ALL pending, not just stale ones.
    assert tracker.get_pending_nonces("GA") == []
    assert tracker.get_nonce("GA") == 210


def test_reconcile_does_not_sync_when_ledger_equals_max_gap() -> None:
    """When ledger equals the highest gap, we must not sync — we'd be at the same spot."""
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 102)
    # 100 and 101 are stale gaps.

    def ledger_query(addr: str) -> int:
        return 101  # equals max stale

    engine = NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=0.0)
    result = engine.reconcile("GA")

    # Must not sync because ledger (101) is not strictly greater than max gap (101).
    assert not result.synced
    assert tracker.get_nonce("GA") == 102


# ---------------------------------------------------------------------------
# NonceRecoveryEngine — construction & configuration
# ---------------------------------------------------------------------------


def test_recovery_engine_rejects_invalid_gap_timeout() -> None:
    tracker = NonceTracker.create_standalone()

    def ledger_query(addr: str) -> int:
        return 0

    # Zero is valid (immediate staleness for tests).
    NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=0.0)

    with pytest.raises(ValueError, match="gap_timeout_seconds"):
        NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=-1)





# ---------------------------------------------------------------------------
# NonceRecoveryEngine — multi-account isolation
# ---------------------------------------------------------------------------


def test_reconcile_multiple_accounts_independently() -> None:
    tracker = NonceTracker.create_standalone()

    # GA: gap at 101
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 100)
    tracker.confirm("GA", 102)

    # GB: no gaps
    tracker.get_next_nonce("GB", seed=200)
    tracker.confirm("GB", 200)

    def ledger_query(addr: str) -> int:
        return {"GA": 105, "GB": 201}[addr]

    engine = NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=0.0)

    result_ga = engine.reconcile("GA")
    assert result_ga.synced
    assert result_ga.gaps_detected == [101]

    result_gb = engine.reconcile("GB")
    assert not result_gb.synced  # no gaps, nothing to do


# ---------------------------------------------------------------------------
# NonceRecoveryEngine — concurrent reconcile serialisation
# ---------------------------------------------------------------------------


def test_reconcile_serialises_concurrent_calls_for_same_account() -> None:
    """When multiple callers reconcile simultaneously, only one ledger query
    should win; the second should see the already-reconciled state."""
    tracker = NonceTracker.create_standalone()
    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 100)
    tracker.confirm("GA", 102)
    # 101 is stuck.

    call_count = 0
    call_lock = threading.Lock()

    def ledger_query(addr: str) -> int:
        nonlocal call_count
        with call_lock:
            call_count += 1
        return 105

    engine = NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=0.0)

    results: list[ReconciliationResult] = []

    def runner() -> None:
        results.append(engine.reconcile("GA"))

    threads = [threading.Thread(target=runner) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # At least one result must have synced.
    synced_count = sum(1 for r in results if r.synced)
    assert synced_count >= 1
    # The per-account lock ensures the ledger query is only called by the
    # first thread to acquire the lock; subsequent threads see the synced
    # state and return early.
    assert call_count == 1


# ---------------------------------------------------------------------------
# Integration: NonceRecoveryEngine + NonceTracker + NonceWindow
# ---------------------------------------------------------------------------


def test_recovery_engine_preserves_existing_behavior_after_reconcile() -> None:
    """After reconciliation, the tracker must continue normal operation."""
    tracker = NonceTracker.create_standalone()

    tracker.get_next_nonce("GA", seed=100)
    tracker.get_next_nonce("GA")
    tracker.get_next_nonce("GA")
    tracker.confirm("GA", 100)
    tracker.confirm("GA", 102)

    def ledger_query(addr: str) -> int:
        return 105

    engine = NonceRecoveryEngine(tracker, ledger_query, gap_timeout_seconds=0.0)
    result = engine.reconcile("GA")
    assert result.synced

    # After sync, issuing new nonces must continue from the synced value
    # (sync_nonce sets the last-used value; get_next_nonce returns next).
    assert tracker.get_next_nonce("GA") == 106
    assert tracker.get_next_nonce("GA") == 107
    tracker.confirm("GA", 106)
    tracker.confirm("GA", 107)
    assert tracker.get_pending_nonces("GA") == []


# ---------------------------------------------------------------------------
# GapReport edge cases
# ---------------------------------------------------------------------------


def test_gap_report_with_mixed_stale_and_fresh_pending() -> None:
    """Only stale nonces contribute to has_gaps check; fresh pending ones don't."""
    report = GapReport(
        address="GA",
        stale_nonces=[100],  # stale and below current
        pending_nonces=[100, 101, 102],
        current_nonce=102,
    )
    assert report.has_gaps  # 100 is stale AND below 102


def test_gap_report_stale_equals_current_is_not_a_gap() -> None:
    """If the only stale nonce equals current_nonce, nothing has leapfrogged it."""
    report = GapReport(
        address="GA",
        stale_nonces=[105],
        pending_nonces=[105],
        current_nonce=105,
    )
    assert not report.has_gaps


# =========================================================================
# Gas Escalation Resubmission Tests
# =========================================================================


def test_gas_escalation_resubmission_construction_defaults() -> None:
    """TransactionResubmitter constructs with default parameters."""
    submitter = TransactionResubmitter()
    assert submitter._initial_fee == 100
    assert submitter._escalation_factor == 1.5
    assert submitter._max_fee == 10_000
    assert submitter._resubmit_timeout == 30.0
    assert submitter.get_pending_count() == 0


def test_gas_escalation_resubmission_custom_params() -> None:
    """TransactionResubmitter accepts custom parameters."""
    submitter = TransactionResubmitter(
        initial_fee=200,
        escalation_factor=2.0,
        max_fee=5_000,
        resubmit_timeout=10.0,
    )
    assert submitter._initial_fee == 200
    assert submitter._escalation_factor == 2.0
    assert submitter._max_fee == 5_000
    assert submitter._resubmit_timeout == 10.0


def test_gas_escalation_resubmission_rejects_invalid_params() -> None:
    """Invalid constructor parameters raise ValueError."""
    with pytest.raises(ValueError, match="initial_fee"):
        TransactionResubmitter(initial_fee=50)
    with pytest.raises(ValueError, match="escalation_factor"):
        TransactionResubmitter(escalation_factor=1.0)
    with pytest.raises(ValueError, match="max_fee"):
        TransactionResubmitter(initial_fee=500, max_fee=300)
    with pytest.raises(ValueError, match="resubmit_timeout"):
        TransactionResubmitter(resubmit_timeout=0)


def test_gas_escalation_resubmission_track_and_untrack() -> None:
    """Track adds and untrack removes from pending set."""
    submitter = TransactionResubmitter()
    submitter.track("tx-1")
    assert submitter.get_pending_count() == 1
    assert "tx-1" in submitter.get_pending_ids()

    submitter.track("tx-2", base_fee=500)
    assert submitter.get_pending_count() == 2

    submitter.untrack("tx-1")
    assert submitter.get_pending_count() == 1
    assert "tx-1" not in submitter.get_pending_ids()


def test_gas_escalation_resubmission_skips_duplicate() -> None:
    """Tracking the same tx_id twice does not create duplicates."""
    submitter = TransactionResubmitter()
    submitter.track("tx-1")
    submitter.track("tx-1")
    assert submitter.get_pending_count() == 1


def test_gas_escalation_resubmission_computes_escalated_fee() -> None:
    """_compute_escalated_fee multiplies base fee by factor, capped at max."""
    submitter = TransactionResubmitter(initial_fee=100, escalation_factor=2.0, max_fee=1000)
    from network.nonce_tracker import PendingSubmission

    sub = PendingSubmission(tx_id="tx-1", base_fee=100)
    assert submitter._compute_escalated_fee(sub) == 200

    sub.base_fee = 600
    assert submitter._compute_escalated_fee(sub) == 1000  # capped at max_fee

    sub.base_fee = 1000
    assert submitter._compute_escalated_fee(sub) == 1000  # already at cap


def test_gas_escalation_resubmission_escalate_pending_timeout_not_reached() -> None:
    """No escalation when txs have not yet exceeded the timeout."""
    submitter = TransactionResubmitter(resubmit_timeout=60.0)
    submitter.track("tx-1", base_fee=100)

    import asyncio
    escalated = asyncio.run(submitter.escalate_pending())
    assert escalated == []


def test_gas_escalation_resubmission_escalate_pending_calls_callback() -> None:
    """Callback is invoked for pending tx exceeding timeout."""
    callback_log: list = []

    def fake_resubmit(tx_id: str, new_fee: int) -> bool:
        callback_log.append((tx_id, new_fee))
        return True

    submitter = TransactionResubmitter(
        resubmit_timeout=0.001,
        initial_fee=100,
        escalation_factor=2.0,
        max_fee=10_000,
        resubmit_fn=fake_resubmit,
    )

    # Manually set submitted_at far in the past to trigger escalation
    from network.nonce_tracker import PendingSubmission
    import time

    submitter._pending["tx-1"] = PendingSubmission(
        tx_id="tx-1", base_fee=100, submitted_at=time.monotonic() - 60,
    )

    import asyncio
    escalated = asyncio.run(submitter.escalate_pending())
    assert "tx-1" in escalated
    assert len(callback_log) == 1
    assert callback_log[0] == ("tx-1", 200)  # 100 * 2.0


def test_gas_escalation_resubmission_escalate_multiple() -> None:
    """Multiple pending txs are escalated correctly."""
    callback_log: list = []

    def fake_resubmit(tx_id: str, new_fee: int) -> bool:
        callback_log.append((tx_id, new_fee))
        return True

    submitter = TransactionResubmitter(
        resubmit_timeout=0.001,
        escalation_factor=1.5,
        max_fee=10_000,
        resubmit_fn=fake_resubmit,
    )

    import time
    from network.nonce_tracker import PendingSubmission

    for i in range(3):
        submitter._pending[f"tx-{i}"] = PendingSubmission(
            tx_id=f"tx-{i}", base_fee=100, submitted_at=time.monotonic() - 60,
        )

    import asyncio
    escalated = asyncio.run(submitter.escalate_pending())
    assert len(escalated) == 3
    assert len(callback_log) == 3
    for tx_id, new_fee in callback_log:
        assert new_fee == 150  # 100 * 1.5


def test_gas_escalation_resubmission_callback_failure_does_not_crash() -> None:
    """A failing callback is caught and does not prevent other escalations."""
    call_count = 0

    def fake_resubmit(tx_id: str, new_fee: int) -> bool:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Network error")
        return True

    submitter = TransactionResubmitter(
        resubmit_timeout=0.001,
        resubmit_fn=fake_resubmit,
    )

    import time
    from network.nonce_tracker import PendingSubmission

    submitter._pending["tx-1"] = PendingSubmission(
        tx_id="tx-1", base_fee=100, submitted_at=time.monotonic() - 60,
    )
    submitter._pending["tx-2"] = PendingSubmission(
        tx_id="tx-2", base_fee=100, submitted_at=time.monotonic() - 60,
    )

    import asyncio
    escalated = asyncio.run(submitter.escalate_pending())
    assert len(escalated) == 1
    assert "tx-2" in escalated
    assert call_count == 2


def test_gas_escalation_resubmission_set_resubmit_fn() -> None:
    """set_resubmit_fn replaces the callback after construction."""
    submitter = TransactionResubmitter()
    assert submitter._resubmit_fn is None

    def fn(tx_id: str, fee: int) -> bool:
        return True

    submitter.set_resubmit_fn(fn)
    assert submitter._resubmit_fn is fn

