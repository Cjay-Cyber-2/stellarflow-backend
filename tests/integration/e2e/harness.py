"""End-to-end system harness for the StellarFlow backend integration suite.

The harness wires the five microservice layers into a single in-process
system-under-test so integration tests can drive real data through the whole
stack without standing up Postgres / Redis / Stellar:

    Ingestion  → parser flattens raw ticker frames into normalised tuples
    Event Proc → async pipeline + sliding-window rate limiter process tuples
    Database   → sqlite-backed BatchSink + PartitionedTelemetryWriter
    API        → tamper-evident AdminAuditLog
    Keeper     → KeyKeeper (secure secret store + HMAC signing + zeroisation)

It also provides a :class:`MetricsCollector` that captures the three
cross-cutting robustness signals required for release verification:

    * unhandled exceptions (sys / threading / asyncio hooks)
    * database lock contention (instrumented sqlite connection)
    * memory leak (tracemalloc current-vs-baseline delta)

And a :class:`LoadRunner` that hammers every layer concurrently for a fixed
duration so those signals can be asserted under simulated load.
"""

from __future__ import annotations

import asyncio
import gc
import importlib.util
import os
import re
import sqlite3
import sys
import threading
import time
import tracemalloc
from pathlib import Path
from typing import Any, Dict, List, Optional

# ``src`` is on sys.path via the repo conftest, but the ``queue`` package
# collides with the stdlib ``queue`` module (already cached in sys.modules by
# pytest).  Load the src submodules that do NOT collide by name, and load the
# ``queue.*`` modules via a file-based alias loader to avoid the clash.
_SRC = Path(__file__).resolve().parents[3] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def _load_src(module_path: str):
    """Load a src submodule by file path under a collision-free alias."""
    rel = module_path.replace(".", os.sep) + ".py"
    full = _SRC / rel
    alias = "sf_" + module_path.replace(".", "_")
    spec = importlib.util.spec_from_file_location(alias, full)
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


# Non-colliding modules import by their real names (src is on sys.path).
from ingestion.parser import build_telemetry_segments, iter_flat_ticker_tuples  # type: ignore
from database.writer import PartitionedTelemetryWriter  # type: ignore
from database.batch_sink import BatchSink  # type: ignore
from api.admin_audit import (  # type: ignore
    AdminAuditLog,
    AdminActor,
    ClientInfo,
)
from state.keeper import KeyKeeper  # type: ignore

# Collision-free aliases for the ``queue`` package.
# ``queue.backpressure`` imports ``psutil`` at module level; inject a minimal
# stub so the module loads without the optional dependency.
import types

if "psutil" not in sys.modules:
    _psutil = types.ModuleType("psutil")

    class _VirtualMemory:
        percent = 0.0

    def _virtual_memory():
        return _VirtualMemory()

    _psutil.virtual_memory = _virtual_memory
    sys.modules["psutil"] = _psutil

_queue_pipeline = _load_src("queue.pipeline")
_queue_bp = _load_src("queue.backpressure")
run_pipeline = _queue_pipeline.run_pipeline
SlidingWindowRateLimiter = _queue_bp.SlidingWindowRateLimiter
CircuitBreaker = _queue_bp.CircuitBreaker




# ---------------------------------------------------------------------------
# Database layer shim: make the real writer run on sqlite
# ---------------------------------------------------------------------------
# The production ``DatabaseWriter`` bulk-inserts via psycopg2's
# ``execute_values``.  Under the harness we redirect that single helper to a
# sqlite-compatible ``executemany`` so the *real* BatchSink /
# PartitionedTelemetryWriter code paths execute unchanged.
_SQL_RE = re.compile(r"INSERT INTO\s+([\"\w]+)\s*\(([^)]*)\)\s*VALUES\s*%s", re.IGNORECASE)


def _sqlite_execute_values(cursor: Any, sql: str, values: List[tuple], page_size: int) -> None:
    match = _SQL_RE.match(sql)
    if not match:
        raise RuntimeError(f"unexpected bulk SQL shape: {sql!r}")
    table, cols = match.group(1), match.group(2)
    col_list = [c.strip() for c in cols.split(",")]
    # PartitionedTelemetryWriter injects a ``__partition_table`` routing tag into
    # every record.  The base table has no such column, so drop it before the
    # real columns are persisted (partition DDL is still created separately).
    if "__partition_table" in col_list:
        idx = col_list.index("__partition_table")
        col_list.pop(idx)
        values = [tuple(v[:idx] + v[idx + 1 :]) for v in values]
    placeholders = ", ".join(["?"] * len(col_list))
    stmt = f'INSERT INTO {table} ({", ".join(col_list)}) VALUES ({placeholders})'
    cursor.executemany(stmt, values)


def _install_sqlite_writer_shim() -> None:
    import database.writer as writer_mod  # type: ignore

    writer_mod._execute_values_bulk = _sqlite_execute_values
    # The same file may also be imported as ``src.database.writer`` (the
    # ``src`` prefix is on sys.path).  Patch whichever module object the
    # ``DatabaseWriter`` class actually resolves its globals from.
    src_mod = sys.modules.get("src.database.writer")
    if src_mod is not None and src_mod is not writer_mod:
        src_mod._execute_values_bulk = _sqlite_execute_values


# ---------------------------------------------------------------------------
# Instrumented sqlite connection — counts lock contention
# ---------------------------------------------------------------------------
class InstrumentedConnection(sqlite3.Connection):
    """sqlite connection that records lock/busy contention events."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._lock_errors = 0
        # Retry briefly on contention instead of failing outright so the
        # pipeline stays healthy; genuine contention still increments the
        # counter so the release gate can fail if it ever occurs.
        try:
            self.execute("PRAGMA busy_timeout=4000")
            self.execute("PRAGMA journal_mode=WAL")
            self.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.Error:
            pass

    def instrumented_cursor(self) -> sqlite3.Cursor:
        cur = self.cursor()
        return cur

    def execute(self, sql: str, parameters: Any = ...) -> sqlite3.Cursor:  # type: ignore[override]
        if parameters is ...:
            parameters = ()
        try:
            return super().execute(sql, parameters)
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "locked" in msg or "busy" in msg:
                self._lock_errors += 1
            raise

    @property
    def lock_errors(self) -> int:
        return self._lock_errors


def connect_instrumented(path: str) -> InstrumentedConnection:
    conn = sqlite3.connect(
        path,
        factory=InstrumentedConnection,
        check_same_thread=False,
    )
    assert isinstance(conn, InstrumentedConnection)
    return conn


# ---------------------------------------------------------------------------
# Metrics collector
# ---------------------------------------------------------------------------
class MetricsCollector:
    """Captures unhandled exceptions, lock contention and memory growth."""

    def __init__(self) -> None:
        self.unhandled: List[Dict[str, str]] = []
        self._prev_sys_excepthook = None
        self._prev_threading_excepthook = None
        self._prev_asyncio_handler = None
        self._tracemalloc_started = False
        self.baseline_rss_kb = 0.0
        self.baseline_trace_bytes = 0
        self.peak_trace_bytes = 0
        self.final_trace_bytes = 0

    # -- exception hooks ---------------------------------------------------
    def _sys_hook(self, exc_type: Any, exc_value: Any, exc_tb: Any) -> None:
        self.unhandled.append(
            {
                "origin": "sys.excepthook",
                "type": str(exc_type),
                "message": str(exc_value),
            }
        )
        if callable(self._prev_sys_excepthook):
            self._prev_sys_excepthook(exc_type, exc_value, exc_tb)

    def _threading_hook(self, args: Any) -> None:
        exc = getattr(args, "exc_value", None)
        self.unhandled.append(
            {
                "origin": "threading.excepthook",
                "type": str(getattr(args, "exc_type", "?")),
                "message": str(exc),
            }
        )

    def _asyncio_hook(self, loop: Any, context: Dict[str, Any]) -> None:
        self.unhandled.append(
            {
                "origin": "asyncio",
                "type": "asyncio.exception",
                "message": str(context.get("message"))
                + " "
                + str(context.get("exception")),
            }
        )

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        self._prev_sys_excepthook = sys.excepthook
        sys.excepthook = self._sys_hook
        self._prev_threading_excepthook = threading.excepthook
        threading.excepthook = self._threading_hook
        if not tracemalloc.is_tracing():
            tracemalloc.start()
            self._tracemalloc_started = True
        gc.collect()
        self.baseline_trace_bytes = tracemalloc.get_traced_memory()[0]

        # Best-effort asyncio exception capture (only if a loop is running).
        try:
            import asyncio

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is not None:
                self._prev_asyncio_handler = loop.get_exception_handler()
                loop.set_exception_handler(self._asyncio_hook)
        except Exception:
            pass

    def sample_peak(self) -> None:
        _, peak = tracemalloc.get_traced_memory()
        self.peak_trace_bytes = max(self.peak_trace_bytes, peak)

    def reset_baseline(self) -> None:
        """Re-anchor the memory baseline after a warmup cycle so one-time
        allocations are not mistaken for leaks."""
        gc.collect()
        self.baseline_trace_bytes = tracemalloc.get_traced_memory()[0]

    def stop(self) -> None:
        gc.collect()
        self.final_trace_bytes = tracemalloc.get_traced_memory()[0]
        if self._prev_sys_excepthook is not None:
            sys.excepthook = self._prev_sys_excepthook
        if self._prev_threading_excepthook is not None:
            threading.excepthook = self._prev_threading_excepthook
        try:
            import asyncio

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is not None:
                loop.set_exception_handler(self._prev_asyncio_handler)
        except Exception:
            pass
        if self._tracemalloc_started:
            tracemalloc.stop()

    @property
    def unhandled_count(self) -> int:
        return len(self.unhandled)

    @property
    def leaked_bytes(self) -> int:
        return max(0, self.final_trace_bytes - self.baseline_trace_bytes)


# ---------------------------------------------------------------------------
# System under test
# ---------------------------------------------------------------------------
TELEMETRY_SCHEMA = {
    "asset_id": "TEXT",
    "price": "REAL",
    "source": "TEXT",
    "ts": "INTEGER",
}


class SystemUnderTest:
    """Wires all five layers into one in-process backend."""

    def __init__(self, workdir: Path) -> None:
        _install_sqlite_writer_shim()
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)

        # Database layer
        self.db_path = str(self.workdir / "telemetry.db")
        self.conn = connect_instrumented(self.db_path)
        # The production sink assumes the target table already exists; create
        # the canonical telemetry shape so the real writer code paths run.
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS telemetry "
            "(asset_id TEXT, price REAL, source TEXT, ts INTEGER)"
        )
        self.sink = BatchSink(self.conn, table_name="telemetry", flush_interval=0.2)
        self.writer = PartitionedTelemetryWriter(
            self.sink,
            base_table="telemetry",
            timestamp_field="ts",
            schema_source=TELEMETRY_SCHEMA,
        )

        # API layer
        self.audit = AdminAuditLog(
            log_path=self.workdir / "admin-audit.jsonl",
            secret_key=b"test-audit-secret",
        )

        # Keeper layer
        self.keeper = KeyKeeper(
            root_key=b"test-root-key",
            state_path=self.workdir / "keeper-state.json",
        )

        # Event processing layer
        self.rate_limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=100_000)

        self._actor = AdminActor(user_id="e2e", user_name="e2e-bot", user_role="ci")
        self._client = ClientInfo(ip_address="127.0.0.1", user_agent="e2e")

    # -- Ingestion ---------------------------------------------------------
    def ingest(self, raw_frames: List[dict]) -> List[tuple]:
        """Run raw frames through the ingestion parser."""
        segments = build_telemetry_segments(raw_frames, drop_invalid=True)
        tuples: List[tuple] = []
        for seg in segments:
            tuples.extend(seg)
        return tuples

    # -- Event processing + Database --------------------------------------
    async def process(self, tuples: List[tuple], rate_limit_key: str = "global") -> int:
        """Push tuples through the async pipeline; each is persisted to the DB."""
        processed = 0

        async def _processor(item: tuple) -> None:
            nonlocal processed
            self.rate_limiter.allow(rate_limit_key)
            asset_id, price, ts, *_ = item
            record = {
                "asset_id": str(asset_id),
                "price": float(price),
                "source": "e2e",
                "ts": int(ts) if ts else int(time.time()),
            }
            self.writer.save(record)
            processed += 1

        async def _stream():
            for item in tuples:
                yield item

        await run_pipeline(_stream(), _processor, max_concurrent=64)
        return processed

    # -- API ---------------------------------------------------------------
    def record_admin_action(self, command: str, before: dict, after: dict) -> None:
        self.audit.record(
            command=command,
            actor=self._actor,
            client=self._client,
            before=before,
            after=after,
            params={"via": "e2e"},
        )

    def verify_audit_chain(self) -> bool:
        return self.audit.verify_chain().valid

    # -- counts ------------------------------------------------------------
    def db_count(self) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM telemetry")
            return int(cur.fetchone()[0])
        except sqlite3.Error:
            return 0
        finally:
            cur.close()

    @property
    def lock_errors(self) -> int:
        return self.conn.lock_errors

    def shutdown(self) -> None:
        try:
            self.writer.shutdown()
        except Exception:
            pass
        try:
            self.keeper.secure_wipe()
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Load runner
# ---------------------------------------------------------------------------
def make_raw_frames(n: int, base_ts: int) -> List[dict]:
    assets = ["NGN/XLM", "KES/XLM", "GHS/XLM", "USD/XLM"]
    frames = []
    for i in range(n):
        frames.append(
            {
                "asset_id": assets[i % len(assets)],
                "price": 1000.0 + (i % 50),
                "timestamp": base_ts + i,
                "sequence": i,
            }
        )
    return frames


class LoadRunner:
    """Drives all layers concurrently for ``duration_s`` seconds."""

    def __init__(self, sut: SystemUnderTest, metrics: MetricsCollector) -> None:
        self.sut = sut
        self.metrics = metrics
        self.received = 0
        self.db_written = 0
        self.api_records = 0
        self.sign_ops = 0
        self._stop = threading.Event()

    async def _produce(self, burst: int) -> None:
        frames = make_raw_frames(burst, int(time.time()))
        tuples = self.sut.ingest(frames)
        self.received += len(tuples)
        written = await self.sut.process(tuples)
        self.db_written += written

    def _api_and_keeper_loop(self) -> None:
        while not self._stop.is_set():
            self.sut.record_admin_action(
                "config.update",
                before={"v": 1},
                after={"v": 2},
            )
            self.api_records += 1
            name = f"signer-{self.api_records % 8}"
            if not self.sut.keeper.has(name):
                self.sut.keeper.put(name, f"secret-{name}".encode())
            sig = self.sut.keeper.sign(name, b"payload")
            assert self.sut.keeper.verify(name, b"payload", sig)
            self.sign_ops += 1
            time.sleep(0.001)

    async def run(self, duration_s: float = 2.0, burst: int = 200) -> Dict[str, int]:
        api_thread = threading.Thread(target=self._api_and_keeper_loop, daemon=True)
        api_thread.start()
        try:
            deadline = time.monotonic() + duration_s
            while time.monotonic() < deadline:
                await self._produce(burst)
                self.metrics.sample_peak()
                await asyncio.sleep(0.0)
        finally:
            self._stop.set()
            api_thread.join(timeout=2.0)
        return {
            "received": self.received,
            "db_written": self.db_written,
            "api_records": self.api_records,
            "sign_ops": self.sign_ops,
        }


__all__ = [
    "SystemUnderTest",
    "MetricsCollector",
    "LoadRunner",
    "InstrumentedConnection",
    "connect_instrumented",
    "make_raw_frames",
]
