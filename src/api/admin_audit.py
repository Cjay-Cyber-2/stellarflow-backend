from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

__all__ = [
    "AdminActor",
    "ClientInfo",
    "AuditEntry",
    "VerificationResult",
    "AdminAuditLog",
    "configure_audit_log",
    "get_audit_log",
]


@dataclass(frozen=True)
class AdminActor:
    user_id: str
    user_name: str
    user_role: str


@dataclass(frozen=True)
class ClientInfo:
    ip_address: str
    user_agent: str | None = None


@dataclass(frozen=True)
class AuditEntry:
    seq: int
    timestamp: str
    command: str
    actor: AdminActor
    client: ClientInfo
    params: dict
    before: dict | None
    after: dict | None
    prev_hash: str
    entry_hash: str


@dataclass(frozen=True)
class VerificationResult:
    valid: bool
    entries_checked: int
    first_bad_seq: int | None = None
    reason: str | None = None


_DEFAULT_LOG_PATH = Path("logs") / "admin-audit.jsonl"

# Module-level singleton behind a lock so tests can replace it.
_lock = threading.Lock()
_instance: AdminAuditLog | None = None


class AdminAuditLog:
    """Append-only audit trail for admin configuration changes.

    Each entry is HMAC-signed and chained to its predecessor so that the
    resulting log file is tamper-evident.  Records are flushed to a JSONL
    file immediately on every call to *record*.
    """

    def __init__(
        self,
        log_path: Path | str,
        secret_key: bytes | None = None,
    ) -> None:
        self._log_path = Path(log_path)
        self._lock = threading.Lock()

        if secret_key is not None:
            self._secret = secret_key
        else:
            raw = os.environ.get("STELLARFLOW_AUDIT_SECRET")
            if raw is None:
                raw = ""
            self._secret = raw.encode("utf-8")

        self._log_path.parent.mkdir(parents=True, exist_ok=True)

        self._seq: int = 0
        self._prev_hash: str = hashlib.sha256(b"").hexdigest()
        # Prime from existing file so appending processes keep the chain
        # intact even when the module is reloaded.
        self._load_previous_tail()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(
        self,
        *,
        command: str,
        actor: AdminActor,
        client: ClientInfo,
        before: dict | None = None,
        after: dict | None = None,
        params: dict | None = None,
    ) -> AuditEntry:
        """Append one signed audit entry and return it."""
        with self._lock:
            self._seq += 1
            seq = self._seq
            prev_hash = self._prev_hash

        entry = self._build_entry(
            seq=seq,
            command=command,
            actor=actor,
            client=client,
            before=before,
            after=after,
            params=params,
            prev_hash=prev_hash,
        )
        signed = self._sign(entry)

        with self._lock:
            self._append(signed)
            self._prev_hash = signed.entry_hash

        return signed

    def verify_chain(self) -> VerificationResult:
        """Re-read the log and verify every entry's HMAC and hash chain."""
        if not self._log_path.exists():
            return VerificationResult(valid=True, entries_checked=0)

        with self._lock:
            try:
                raw = self._log_path.read_text("utf-8")
            except Exception as exc:
                return VerificationResult(
                    valid=False,
                    entries_checked=0,
                    reason=f"cannot read log: {exc}",
                )

        prev_hash = hashlib.sha256(b"").hexdigest()
        checked = 0
        for i, line in enumerate(raw.strip().splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                return VerificationResult(
                    valid=False,
                    entries_checked=checked,
                    first_bad_seq=i,
                    reason=f"line {i}: invalid JSON: {exc}",
                )
            stored_hash = data.pop("entry_hash", None)
            if stored_hash is None:
                return VerificationResult(
                    valid=False,
                    entries_checked=checked,
                    first_bad_seq=i,
                    reason=f"line {i}: missing entry_hash",
                )
            expected_hash = hmac.new(
                self._secret,
                json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8"),
                "sha256",
            ).hexdigest()
            if not hmac.compare_digest(expected_hash, stored_hash):
                return VerificationResult(
                    valid=False,
                    entries_checked=checked,
                    first_bad_seq=i,
                    reason=f"line {i}: HMAC mismatch",
                )
            actual_prev = data.get("prev_hash", "")
            if actual_prev != prev_hash:
                return VerificationResult(
                    valid=False,
                    entries_checked=checked,
                    first_bad_seq=i,
                    reason=f"line {i}: prev_hash chain broken",
                )
            prev_hash = stored_hash
            checked += 1

        return VerificationResult(valid=True, entries_checked=checked)

    def tail(self, n: int = 10) -> list[AuditEntry]:
        """Return the last *n* entries from the log file."""
        if not self._log_path.exists():
            return []
        raw = self._log_path.read_text("utf-8")
        lines = [ln for ln in raw.strip().splitlines() if ln.strip()]
        selected = lines[-n:]
        return [AuditEntry(**json.loads(ln)) for ln in selected]

    @property
    def log_path(self) -> Path:
        return self._log_path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_previous_tail(self) -> None:
        if not self._log_path.exists():
            return
        try:
            raw = self._log_path.read_text("utf-8").strip()
            if not raw:
                return
            last_line = raw.splitlines()[-1].strip()
            if not last_line:
                return
            data = json.loads(last_line)
            self._seq = data.get("seq", 0) or 0
            stored = data.get("entry_hash", "")
            if stored:
                self._prev_hash = stored
        except (OSError, json.JSONDecodeError, KeyError):
            pass

    def _build_entry(
        self,
        *,
        seq: int,
        command: str,
        actor: AdminActor,
        client: ClientInfo,
        before: dict | None,
        after: dict | None,
        params: dict | None,
        prev_hash: str,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "seq": seq,
            "timestamp": now,
            "command": command,
            "actor": actor,
            "client": client,
            "params": params or {},
            "before": before,
            "after": after,
            "prev_hash": prev_hash,
        }
        return payload

    def _sign(self, payload: dict) -> AuditEntry:
        canonical = json.dumps(
            payload,
            sort_keys=True,
            ensure_ascii=False,
            default=self._serialize,
        )
        entry_hash = hmac.new(
            self._secret,
            canonical.encode("utf-8"),
            "sha256",
        ).hexdigest()
        payload["entry_hash"] = entry_hash
        return AuditEntry(**payload)

    @staticmethod
    def _serialize(obj):
        if isinstance(obj, (AdminActor, ClientInfo)):
            return asdict(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    def _append(self, entry: AuditEntry) -> None:
        line = json.dumps(asdict(entry), ensure_ascii=False, default=str) + "\n"
        with self._log_path.open("a") as f:
            f.write(line)
            f.flush()


# ------------------------------------------------------------------
# Convenience helpers (module-level access)
# ------------------------------------------------------------------


def configure_audit_log(
    log_path: Path | str | None = None,
    secret_key: bytes | None = None,
) -> AdminAuditLog:
    """Create or reconfigure the module-level singleton.

    Calling this again with different arguments raises RuntimeError if the
    singleton was already used (i.e. *get_audit_log* was already called).
    """
    global _instance
    instance = AdminAuditLog(
        log_path=Path(log_path) if log_path else _DEFAULT_LOG_PATH,
        secret_key=secret_key,
    )
    with _lock:
        _instance = instance
    return _instance


def get_audit_log() -> AdminAuditLog:
    """Return the module-level singleton, creating it with defaults if needed."""
    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                _instance = AdminAuditLog(
                    log_path=_DEFAULT_LOG_PATH,
                )
    return _instance
