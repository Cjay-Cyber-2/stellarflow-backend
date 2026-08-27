from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.admin_audit import (
    AdminActor,
    AdminAuditLog,
    ClientInfo,
    configure_audit_log,
    get_audit_log,
)


@pytest.fixture
def log_path(tmp_path):
    return tmp_path / "admin-audit.jsonl"


@pytest.fixture
def actor():
    return AdminActor(user_id="admin-1", user_name="Alice", user_role="SUPER_ADMIN")


@pytest.fixture
def client():
    return ClientInfo(ip_address="192.168.1.1", user_agent="pytest")


@pytest.fixture
def audit_log(log_path):
    return AdminAuditLog(log_path=log_path, secret_key=b"test-secret-key")


# ------------------------------------------------------------------
# Record
# ------------------------------------------------------------------


def test_record_creates_file(audit_log, log_path, actor, client):
    entry = audit_log.record(command="reload-secret", actor=actor, client=client)
    assert log_path.exists()
    assert entry.seq == 1
    assert entry.command == "reload-secret"
    assert entry.actor == actor
    assert entry.client == client


def test_record_returns_signed_entry(audit_log, actor, client):
    entry = audit_log.record(command="reload-secret", actor=actor, client=client)
    assert entry.entry_hash is not None
    assert len(entry.entry_hash) == 64  # SHA-256 hex = 64 chars
    assert entry.prev_hash != entry.entry_hash
    assert entry.prev_hash is not None


def test_record_increments_seq(audit_log, actor, client):
    e1 = audit_log.record(command="cmd-a", actor=actor, client=client)
    e2 = audit_log.record(command="cmd-b", actor=actor, client=client)
    assert e2.seq == e1.seq + 1


def test_record_chains_entries(audit_log, actor, client):
    e1 = audit_log.record(command="cmd-a", actor=actor, client=client)
    e2 = audit_log.record(command="cmd-b", actor=actor, client=client)
    assert e2.prev_hash == e1.entry_hash


def test_record_with_before_after(audit_log, actor, client):
    before = {"maxRequests": 100}
    after = {"maxRequests": 200}
    entry = audit_log.record(
        command="update-rate-limit",
        actor=actor,
        client=client,
        before=before,
        after=after,
    )
    assert entry.before == before
    assert entry.after == after


def test_record_with_params(audit_log, actor, client):
    params = {"trigger": "admin-endpoint"}
    entry = audit_log.record(
        command="reload-secret",
        actor=actor,
        client=client,
        params=params,
    )
    assert entry.params == params


def test_record_sets_timestamp(audit_log, actor, client):
    entry = audit_log.record(command="test", actor=actor, client=client)
    assert entry.timestamp is not None
    assert entry.timestamp.endswith(("+00:00", "Z")) or "+" in entry.timestamp


# ------------------------------------------------------------------
# Verify chain
# ------------------------------------------------------------------


def test_verify_chain_empty(audit_log):
    result = audit_log.verify_chain()
    assert result.valid
    assert result.entries_checked == 0


def test_verify_chain_valid(audit_log, actor, client):
    audit_log.record(command="cmd-a", actor=actor, client=client)
    audit_log.record(command="cmd-b", actor=actor, client=client)
    result = audit_log.verify_chain()
    assert result.valid
    assert result.entries_checked == 2


def test_verify_chain_fails_on_tamper(audit_log, log_path, actor, client):
    audit_log.record(command="cmd-a", actor=actor, client=client)
    audit_log.record(command="cmd-b", actor=actor, client=client)

    raw = log_path.read_text("utf-8")
    corrupted = raw.replace("cmd-a", "cmd-x")
    log_path.write_text(corrupted, "utf-8")

    result = audit_log.verify_chain()
    assert not result.valid
    assert "HMAC mismatch" in (result.reason or "")


def test_verify_chain_fails_on_broken_chain(audit_log, log_path, actor, client):
    audit_log.record(command="cmd-a", actor=actor, client=client)
    audit_log.record(command="cmd-b", actor=actor, client=client)

    # Remove the first line (breaks chain)
    raw = log_path.read_text("utf-8")
    lines = raw.strip().splitlines()
    log_path.write_text(lines[1] + "\n", "utf-8")

    result = audit_log.verify_chain()
    assert not result.valid
    # prev_hash of the remaining entry won't match sha256("") (empty chain start)
    assert "prev_hash chain broken" in (result.reason or "")


def test_verify_chain_fails_on_missing_hash(audit_log, log_path, actor, client):
    audit_log.record(command="cmd-a", actor=actor, client=client)

    raw = log_path.read_text("utf-8")
    data = json.loads(raw.strip())
    data.pop("entry_hash", None)
    log_path.write_text(json.dumps(data, ensure_ascii=False) + "\n", "utf-8")

    result = audit_log.verify_chain()
    assert not result.valid
    assert "missing entry_hash" in (result.reason or "")


# ------------------------------------------------------------------
# Tail
# ------------------------------------------------------------------


def test_tail_returns_last_n(audit_log, actor, client):
    entries = [
        audit_log.record(command=f"cmd-{i}", actor=actor, client=client)
        for i in range(5)
    ]
    tail = audit_log.tail(2)
    assert len(tail) == 2
    assert tail[0].seq == entries[-2].seq
    assert tail[1].seq == entries[-1].seq


def test_tail_empty(audit_log):
    assert audit_log.tail() == []


def test_tail_more_than_available(audit_log, actor, client):
    audit_log.record(command="cmd-a", actor=actor, client=client)
    tail = audit_log.tail(10)
    assert len(tail) == 1


# ------------------------------------------------------------------
# Restore chain from existing file
# ------------------------------------------------------------------


def test_restores_chain_from_existing_file(log_path, actor, client):
    log1 = AdminAuditLog(log_path=log_path, secret_key=b"test-secret-key")
    log1.record(command="cmd-a", actor=actor, client=client)
    e2 = log1.record(command="cmd-b", actor=actor, client=client)

    # Fresh instance reading the same file should continue the chain
    log2 = AdminAuditLog(log_path=log_path, secret_key=b"test-secret-key")
    e3 = log2.record(command="cmd-c", actor=actor, client=client)

    assert e3.seq == 3
    assert e3.prev_hash == e2.entry_hash

    result = log2.verify_chain()
    assert result.valid
    assert result.entries_checked == 3


# ------------------------------------------------------------------
# Module-level singleton
# ------------------------------------------------------------------


def test_get_audit_log_returns_singleton():
    log = get_audit_log()
    assert isinstance(log, AdminAuditLog)
    assert log is get_audit_log()


def test_configure_audit_log(tmp_path, actor, client):
    path = tmp_path / "test.jsonl"
    log = configure_audit_log(log_path=path, secret_key=b"cfg-secret")
    assert isinstance(log, AdminAuditLog)
    assert log.log_path == path

    # Calling get_audit_log returns the configured instance
    same = get_audit_log()
    assert same is log
