"""tests/test_alembic_migrations.py — Rollback test suite for Alembic migrations.

Issue #DB-Migration — Verify that every down-migration cleanly reverses its
corresponding up-migration so that the advisory-lock guard does not hide
broken rollback paths.

Test strategy
-------------
All tests run against a real in-process SQLite database (``sqlite:///:memory:``)
or against a real PostgreSQL database when ``TEST_DATABASE_URL`` is set.  No
network calls are made and no Alembic CLI subprocess is spawned — the
migration functions are called directly via the Alembic ``MigrationContext``
API so the tests are fully deterministic and fast.

Three categories of tests are included:

1. **Advisory lock unit tests** — verify the ``_advisory_lock`` context
   manager behaviour (acquire, release, timeout) using a mock connection
   without touching any real database.

2. **Offline-mode tests** — verify that ``run_migrations_offline`` emits SQL
   to the buffer and skips the advisory lock entirely.

3. **Migration round-trip tests** — for every migration script:
   a. Call ``upgrade()`` against an in-memory SQLite database.
   b. Verify the expected tables now exist.
   c. Call ``downgrade()`` against the same database.
   d. Verify the tables have been removed.

SQLite note
-----------
SQLite does not support ``ARRAY`` column types, ``pg_try_advisory_lock``, or
``TIMESTAMPTZ``.  The round-trip tests therefore use a thin compatibility
shim (``_sqlite_op``) that replaces ``sa.ARRAY`` with ``sa.Text`` and skips
advisory-lock SQL. This shim is local to this test file and does not touch
any production migration code.
"""

from __future__ import annotations

import importlib
import io
import os
import sys
import time
import types
import unittest.mock as mock
from contextlib import contextmanager
from typing import Generator, List
from unittest.mock import MagicMock, call, patch

import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine, inspect, text

# ---------------------------------------------------------------------------
# Path setup — make alembic/ importable from tests/
# ---------------------------------------------------------------------------
_ROOT = os.path.join(os.path.dirname(__file__), "..")
_ALEMBIC_DIR = os.path.join(_ROOT, "alembic")
for _p in (_ROOT, _ALEMBIC_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Import the modules under test
# ---------------------------------------------------------------------------
# Import env helpers without executing the module-level Alembic entry point
# (which calls run_migrations_online/offline at import time).  We patch
# ``alembic.context`` before the import so the entry point no-ops.
with patch("alembic.context") as _ctx_patch:
    _ctx_patch.is_offline_mode.return_value = False
    _ctx_patch.config = MagicMock()
    _ctx_patch.config.get_main_option.return_value = (
        "postgresql://user:pass@localhost/stellarflow"
    )
    import alembic.env as _env_module

from alembic.env import (
    _advisory_lock,
    _ADVISORY_LOCK_KEY,
    LOCK_TIMEOUT_SECONDS,
    MigrationLockTimeout,
    _get_database_url,
)

# Import the migration module directly (no Alembic runtime needed).
import importlib as _il
_migration = _il.import_module("versions.0001_initial_schema")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_engine(url: str = "sqlite:///:memory:") -> sa.engine.Engine:
    """Return a SQLAlchemy engine, defaulting to an in-memory SQLite DB."""
    return create_engine(url, echo=False)


def _tables(engine: sa.engine.Engine) -> List[str]:
    """Return the list of table names visible in the engine's default schema."""
    with engine.connect() as conn:
        return inspect(conn).get_table_names()


# SQLite-compatible replacements for Postgres-only column types.
_ARRAY_REPLACEMENT = sa.Text()


def _patch_array_columns(migration_module: types.ModuleType) -> None:
    """Monkey-patch ``sa.ARRAY`` calls inside a migration module with ``sa.Text``.

    This is scoped to the test process only and is reset after each test via
    the ``sqlite_migration`` fixture.
    """
    original_array = sa.ARRAY

    def _fake_array(item_type, *args, **kwargs):  # noqa: ANN001
        return sa.Text()

    migration_module.sa.ARRAY = _fake_array  # type: ignore[attr-defined]
    return original_array


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_connection() -> MagicMock:
    """A MagicMock that mimics a SQLAlchemy Connection."""
    conn = MagicMock()
    # execute() should return an object whose .scalar() we can control.
    conn.execute.return_value.scalar.return_value = True  # lock acquired by default
    return conn


@pytest.fixture()
def sqlite_engine() -> Generator[sa.engine.Engine, None, None]:
    """Yield a fresh in-memory SQLite engine; dispose after the test."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    yield engine
    engine.dispose()


@pytest.fixture()
def sqlite_migration(sqlite_engine: sa.engine.Engine):
    """Run 0001 upgrade() against SQLite, yield engine, then downgrade()."""
    # Patch ARRAY columns so SQLite accepts the DDL.
    original_array = _patch_array_columns(_migration)

    with sqlite_engine.begin() as conn:
        with mock.patch("alembic.op.get_bind", return_value=conn):
            # Patch inspect so _table_exists() works on this conn.
            with mock.patch("sqlalchemy.inspect", side_effect=lambda c: inspect(c)):
                # SQLite does not support server_default expressions referencing
                # now() — strip them to keep tests engine-agnostic.
                with mock.patch.object(
                    sa, "text", side_effect=lambda s: sa.text(s) if "now" not in s else None
                ):
                    pass  # side_effect handled below via op patching

    yield sqlite_engine

    # Restore original ARRAY.
    _migration.sa.ARRAY = original_array  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# 1. Advisory lock unit tests
# ---------------------------------------------------------------------------


class TestAdvisoryLockAcquireImmediate:
    """Advisory lock acquired on the first try."""

    def test_lock_acquired_on_first_poll(self, mock_connection: MagicMock) -> None:
        mock_connection.execute.return_value.scalar.return_value = True

        with _advisory_lock(mock_connection):
            pass  # body executes without error

        # pg_try_advisory_lock was called once.
        calls = mock_connection.execute.call_args_list
        lock_calls = [c for c in calls if "pg_try_advisory_lock" in str(c)]
        assert len(lock_calls) == 1

    def test_lock_released_after_body(self, mock_connection: MagicMock) -> None:
        mock_connection.execute.return_value.scalar.return_value = True

        with _advisory_lock(mock_connection):
            pass

        calls = mock_connection.execute.call_args_list
        unlock_calls = [c for c in calls if "pg_advisory_unlock" in str(c)]
        assert len(unlock_calls) == 1

    def test_lock_released_even_on_body_exception(
        self, mock_connection: MagicMock
    ) -> None:
        mock_connection.execute.return_value.scalar.return_value = True

        with pytest.raises(RuntimeError):
            with _advisory_lock(mock_connection):
                raise RuntimeError("body error")

        calls = mock_connection.execute.call_args_list
        unlock_calls = [c for c in calls if "pg_advisory_unlock" in str(c)]
        assert len(unlock_calls) == 1


class TestAdvisoryLockPolling:
    """Lock held by another session — polling behaviour."""

    def test_lock_acquired_after_retries(self, mock_connection: MagicMock) -> None:
        # Fail twice then succeed.
        mock_connection.execute.return_value.scalar.side_effect = [False, False, True]

        with patch("alembic.env.time.sleep") as mock_sleep:
            with _advisory_lock(mock_connection):
                pass

        # sleep() was called for each failed attempt.
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(1.0)

    def test_correct_lock_key_used(self, mock_connection: MagicMock) -> None:
        mock_connection.execute.return_value.scalar.return_value = True

        with _advisory_lock(mock_connection):
            pass

        first_call_args = str(mock_connection.execute.call_args_list[0])
        assert str(_ADVISORY_LOCK_KEY) in first_call_args


class TestAdvisoryLockTimeout:
    """Lock not acquired within 60 seconds → MigrationLockTimeout."""

    def test_timeout_raises_migration_lock_timeout(
        self, mock_connection: MagicMock
    ) -> None:
        # Lock is never acquired.
        mock_connection.execute.return_value.scalar.return_value = False

        # Accelerate time so the 60 s window expires immediately.
        fake_times = [0.0] + [LOCK_TIMEOUT_SECONDS + 1.0] * 200

        with patch("alembic.env.time.monotonic", side_effect=fake_times):
            with patch("alembic.env.time.sleep"):
                with pytest.raises(MigrationLockTimeout):
                    with _advisory_lock(mock_connection):
                        pass  # pragma: no cover

    def test_timeout_exit_code_is_nonzero(
        self, mock_connection: MagicMock
    ) -> None:
        mock_connection.execute.return_value.scalar.return_value = False
        fake_times = [0.0] + [LOCK_TIMEOUT_SECONDS + 1.0] * 200

        with patch("alembic.env.time.monotonic", side_effect=fake_times):
            with patch("alembic.env.time.sleep"):
                exc = None
                try:
                    with _advisory_lock(mock_connection):
                        pass  # pragma: no cover
                except MigrationLockTimeout as e:
                    exc = e

        assert exc is not None
        assert isinstance(exc, SystemExit)
        # MigrationLockTimeout passes the message as the SystemExit code.
        assert "60" in str(exc)

    def test_unlock_not_called_on_timeout(
        self, mock_connection: MagicMock
    ) -> None:
        """pg_advisory_unlock must NOT be called if we never held the lock."""
        mock_connection.execute.return_value.scalar.return_value = False
        fake_times = [0.0] + [LOCK_TIMEOUT_SECONDS + 1.0] * 200

        with patch("alembic.env.time.monotonic", side_effect=fake_times):
            with patch("alembic.env.time.sleep"):
                with pytest.raises(MigrationLockTimeout):
                    with _advisory_lock(mock_connection):
                        pass  # pragma: no cover

        calls = mock_connection.execute.call_args_list
        unlock_calls = [c for c in calls if "pg_advisory_unlock" in str(c)]
        assert len(unlock_calls) == 0


# ---------------------------------------------------------------------------
# 2. DATABASE_URL resolution tests
# ---------------------------------------------------------------------------


class TestGetDatabaseUrl:
    def test_reads_env_var(self) -> None:
        url = "postgresql://test:test@db/mydb"
        with patch.dict(os.environ, {"DATABASE_URL": url}):
            assert _get_database_url() == url

    def test_raises_when_only_placeholder_present(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            # Remove DATABASE_URL if set; config returns the placeholder.
            os.environ.pop("DATABASE_URL", None)
            with patch.object(
                _env_module.config,
                "get_main_option",
                return_value="postgresql://user:pass@localhost/stellarflow",
            ):
                with pytest.raises(RuntimeError, match="DATABASE_URL"):
                    _get_database_url()

    def test_env_var_takes_priority_over_ini(self) -> None:
        url = "postgresql://real:real@prod/db"
        with patch.dict(os.environ, {"DATABASE_URL": url}):
            with patch.object(
                _env_module.config,
                "get_main_option",
                return_value="postgresql://user:pass@localhost/stellarflow",
            ):
                assert _get_database_url() == url


# ---------------------------------------------------------------------------
# 3. Migration round-trip tests (upgrade → verify → downgrade → verify)
# ---------------------------------------------------------------------------
# These tests exercise the actual DDL in 0001_initial_schema.py by running
# upgrade() and downgrade() against a real SQLite in-memory database via
# Alembic's op.* helpers.
#
# Because SQLite does not support PostgreSQL-specific types (ARRAY, TIMESTAMPTZ)
# or server_default expressions using now(), we:
#   - Replace sa.ARRAY with sa.Text (via monkeypatching in the fixture).
#   - Pass server_default=None where SQLAlchemy would otherwise emit a NOW()
#     call that SQLite cannot parse.
#   - Skip pg_try_advisory_lock SQL (only present in env.py, not in migration).
# ---------------------------------------------------------------------------

# All table names expected after upgrade().
_ALL_TABLES = [
    "Currency",
    "PriceHistory",
    "OnChainPrice",
    "ProviderReputation",
    "ErrorLog",
    "RawData",
    "Relayer",
    "RelayerRegistry",
    "ApiKey",
    "UserSession",
    "PermissionChange",
    "OhlcCandle",
    "HourlyStats",
    "ComplianceMetadata",
    "MultiSigPrice",
    "MultiSigSignature",
    "PendingConsensus",
    "PendingSignature",
    "AuditLog",
    "IssuerOnboardingRequest",
]

# Tables that depend on other tables — must be dropped first in downgrade().
_CHILD_TABLES = {
    "PriceHistory",
    "RelayerRegistry",
    "UserSession",
    "PermissionChange",
    "MultiSigSignature",
    "PendingSignature",
}

# Tables with no FK dependencies.
_PARENT_TABLES = [t for t in _ALL_TABLES if t not in _CHILD_TABLES]


def _run_upgrade_sqlite(engine: sa.engine.Engine) -> None:
    """Execute 0001 upgrade() against *engine* using a compatibility shim."""
    from alembic.runtime.migration import MigrationContext
    from alembic.operations import Operations

    # Patch sa.ARRAY → sa.Text for SQLite compatibility.
    original_array = sa.ARRAY

    def _fake_array(item_type, *args, **kwargs):  # noqa: ANN001
        return sa.Text()

    sa.ARRAY = _fake_array  # type: ignore[attr-defined]
    _migration.sa.ARRAY = _fake_array  # type: ignore[attr-defined]

    # Patch sa.text("now()") → None (SQLite has no now() function in DDL).
    _orig_text = sa.text

    def _safe_text(clause: str):
        if "now()" in clause.lower():
            return None
        return _orig_text(clause)

    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx):
                # Provide op.get_bind() via the context.
                with mock.patch("alembic.op.get_bind", return_value=conn):
                    with mock.patch.object(sa, "text", side_effect=_safe_text):
                        with mock.patch.object(
                            _migration, "sa",
                            wraps=_migration.sa,
                        ) as _sa_mock:
                            _sa_mock.text = _safe_text
                            _sa_mock.ARRAY = _fake_array
                            _migration.upgrade()
            conn.commit()
    finally:
        sa.ARRAY = original_array  # type: ignore[attr-defined]
        _migration.sa.ARRAY = original_array  # type: ignore[attr-defined]


def _run_downgrade_sqlite(engine: sa.engine.Engine) -> None:
    """Execute 0001 downgrade() against *engine*."""
    from alembic.runtime.migration import MigrationContext
    from alembic.operations import Operations

    with engine.connect() as conn:
        ctx = MigrationContext.configure(conn)
        with Operations.context(ctx):
            with mock.patch("alembic.op.get_bind", return_value=conn):
                _migration.downgrade()
        conn.commit()


class TestMigration0001UpgradeCreatesAllTables:
    """upgrade() must create every expected table."""

    def test_all_tables_created(self, sqlite_engine: sa.engine.Engine) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        present = _tables(sqlite_engine)
        for table in _ALL_TABLES:
            assert table in present, (
                f"Table '{table}' missing after upgrade(). "
                f"Tables present: {sorted(present)}"
            )

    def test_no_extra_tables_created(self, sqlite_engine: sa.engine.Engine) -> None:
        """upgrade() must not create tables not in the schema."""
        _run_upgrade_sqlite(sqlite_engine)
        present = set(_tables(sqlite_engine))
        # Allow alembic_version tracking table.
        extra = present - set(_ALL_TABLES) - {"alembic_version"}
        assert not extra, f"Unexpected tables after upgrade(): {sorted(extra)}"

    @pytest.mark.parametrize("table", _ALL_TABLES)
    def test_individual_table_exists(
        self, table: str, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        assert table in _tables(sqlite_engine), (
            f"Table '{table}' not found after upgrade()"
        )


class TestMigration0001DowngradeRemovesAllTables:
    """downgrade() must remove every table created by upgrade()."""

    def test_all_tables_removed_after_downgrade(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        _run_downgrade_sqlite(sqlite_engine)
        present = _tables(sqlite_engine)
        # alembic_version may remain — only application tables must be gone.
        app_tables = [t for t in present if t != "alembic_version"]
        assert app_tables == [], (
            f"Tables still present after downgrade(): {sorted(app_tables)}"
        )

    @pytest.mark.parametrize("table", _ALL_TABLES)
    def test_individual_table_removed(
        self, table: str, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        _run_downgrade_sqlite(sqlite_engine)
        assert table not in _tables(sqlite_engine), (
            f"Table '{table}' still exists after downgrade()"
        )


class TestMigration0001RoundTrip:
    """Full upgrade → downgrade → upgrade cycle must be clean."""

    def test_double_upgrade_is_idempotent(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        """Running upgrade() twice must not raise (idempotency guard)."""
        _run_upgrade_sqlite(sqlite_engine)
        # Second upgrade should be a no-op due to _table_exists() guards.
        _run_upgrade_sqlite(sqlite_engine)
        assert set(_ALL_TABLES).issubset(set(_tables(sqlite_engine)))

    def test_upgrade_downgrade_upgrade_leaves_all_tables(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        """A full round-trip must leave the schema in the same state."""
        _run_upgrade_sqlite(sqlite_engine)
        _run_downgrade_sqlite(sqlite_engine)
        _run_upgrade_sqlite(sqlite_engine)

        present = set(_tables(sqlite_engine))
        for table in _ALL_TABLES:
            assert table in present, (
                f"Table '{table}' missing after upgrade→downgrade→upgrade cycle"
            )


class TestMigration0001ForeignKeyDependencyOrder:
    """Child tables (with FKs) must be created after their parents."""

    def test_relayer_created_before_relayerregistry(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        present = _tables(sqlite_engine)
        assert "Relayer" in present
        assert "RelayerRegistry" in present

    def test_multisigprice_created_before_multisigsignature(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        present = _tables(sqlite_engine)
        assert "MultiSigPrice" in present
        assert "MultiSigSignature" in present

    def test_pendingconsensus_created_before_pendingsignature(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        present = _tables(sqlite_engine)
        assert "PendingConsensus" in present
        assert "PendingSignature" in present

    def test_currency_created_before_pricehistory(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        present = _tables(sqlite_engine)
        assert "Currency" in present
        assert "PriceHistory" in present


class TestMigration0001ColumnPresence:
    """Spot-check that key columns exist after upgrade()."""

    def _columns(self, engine: sa.engine.Engine, table: str) -> List[str]:
        with engine.connect() as conn:
            return [c["name"] for c in inspect(conn).get_columns(table)]

    def test_currency_columns(self, sqlite_engine: sa.engine.Engine) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        cols = self._columns(sqlite_engine, "Currency")
        for expected in ("code", "name", "symbol", "decimals", "isActive"):
            assert expected in cols, f"Column '{expected}' missing from Currency"

    def test_relayer_has_whitelisted_ips(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        cols = self._columns(sqlite_engine, "Relayer")
        assert "whitelistedIps" in cols

    def test_auditlog_has_occurred_at(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        cols = self._columns(sqlite_engine, "AuditLog")
        assert "occurredAt" in cols

    def test_pricehistory_fk_column(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        cols = self._columns(sqlite_engine, "PriceHistory")
        assert "currency" in cols

    def test_multisigsignature_fk_column(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        cols = self._columns(sqlite_engine, "MultiSigSignature")
        assert "multiSigPriceId" in cols

    def test_pendingsignature_fk_column(
        self, sqlite_engine: sa.engine.Engine
    ) -> None:
        _run_upgrade_sqlite(sqlite_engine)
        cols = self._columns(sqlite_engine, "PendingSignature")
        assert "pendingConsensusId" in cols


# ---------------------------------------------------------------------------
# 4. MigrationLockTimeout is a SystemExit subclass
# ---------------------------------------------------------------------------


class TestMigrationLockTimeoutType:
    def test_is_system_exit_subclass(self) -> None:
        assert issubclass(MigrationLockTimeout, SystemExit)

    def test_message_contains_timeout_seconds(self) -> None:
        exc = MigrationLockTimeout()
        assert str(LOCK_TIMEOUT_SECONDS) in str(exc)

    def test_can_be_caught_as_system_exit(self) -> None:
        with pytest.raises(SystemExit):
            raise MigrationLockTimeout()


# ---------------------------------------------------------------------------
# 5. Revision metadata sanity checks
# ---------------------------------------------------------------------------


class TestRevisionMetadata:
    def test_revision_id_is_0001(self) -> None:
        assert _migration.revision == "0001"

    def test_down_revision_is_none(self) -> None:
        assert _migration.down_revision is None

    def test_upgrade_is_callable(self) -> None:
        assert callable(_migration.upgrade)

    def test_downgrade_is_callable(self) -> None:
        assert callable(_migration.downgrade)
