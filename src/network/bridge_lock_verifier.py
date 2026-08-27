from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional
import requests

logger = logging.getLogger(__name__)


class ChainType(str, Enum):
    EVM = "evm"
    SOLANA = "solana"


class VerificationStatus(str, Enum):
    PENDING = "PENDING"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"
    WAITING_CONFIRMATIONS = "WAITING_CONFIRMATIONS"


@dataclass(frozen=True)
class LockProof:
    job_id: str
    chain_type: ChainType
    tx_hash: str
    expected_amount: int
    expected_recipient: str
    rpc_url: str
    required_confirmations: int = 12


class BridgeLockVerificationWorker:
    """Worker verifying cross-chain lock proofs against EVM and Solana RPCs."""

    def __init__(self, timeout_sec: float = 10.0, session: Optional[requests.Session] = None) -> None:
        self.timeout_sec = timeout_sec
        self.session = session or requests.Session()

    def verify_evm_tx(self, proof: LockProof) -> Dict[str, Any]:
        """Queries EVM JSON-RPC for receipt execution status and confirmation depth."""
        # 1. Fetch transaction receipt
        receipt_payload = {
            "jsonrpc": "2.0",
            "method": "eth_getTransactionReceipt",
            "params": [proof.tx_hash],
            "id": 1,
        }
        try:
            resp = self.session.post(proof.rpc_url, json=receipt_payload, timeout=self.timeout_sec)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error("EVM RPC receipt query error for %s: %s", proof.tx_hash, exc)
            return {"valid": False, "status": VerificationStatus.FAILED, "error": str(exc)}

        receipt = data.get("result")
        if not receipt:
            return {"valid": False, "status": VerificationStatus.FAILED, "reason": "Receipt not found"}

        if receipt.get("status") != "0x1":
            return {"valid": False, "status": VerificationStatus.FAILED, "reason": "Transaction reverted"}

        tx_block_hex = receipt.get("blockNumber")
        if not tx_block_hex:
            return {"valid": False, "status": VerificationStatus.FAILED, "reason": "No block number in receipt"}

        tx_block = int(tx_block_hex, 16)

        # 2. Fetch current block height
        block_payload = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 2,
        }
        try:
            resp = self.session.post(proof.rpc_url, json=block_payload, timeout=self.timeout_sec)
            resp.raise_for_status()
            current_block_hex = resp.json().get("result", "0x0")
            current_block = int(current_block_hex, 16)
        except Exception as exc:
            logger.error("EVM RPC block query error: %s", exc)
            return {"valid": False, "status": VerificationStatus.FAILED, "error": str(exc)}

        confirmations = max(0, current_block - tx_block)
        if confirmations < proof.required_confirmations:
            return {
                "valid": False,
                "status": VerificationStatus.WAITING_CONFIRMATIONS,
                "confirmations": confirmations,
                "required": proof.required_confirmations,
                "tx_block": tx_block,
                "current_block": current_block,
            }

        return {
            "valid": True,
            "status": VerificationStatus.VERIFIED,
            "confirmations": confirmations,
            "block_number": tx_block,
            "logs": receipt.get("logs", []),
        }

    def verify_solana_tx(self, proof: LockProof) -> Dict[str, Any]:
        """Queries Solana RPC getSignatureStatuses with finalized commitment."""
        payload = {
            "jsonrpc": "2.0",
            "method": "getSignatureStatuses",
            "params": [[proof.tx_hash], {"searchTransactionHistory": True}],
            "id": 1,
        }
        try:
            resp = self.session.post(proof.rpc_url, json=payload, timeout=self.timeout_sec)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error("Solana RPC query error for %s: %s", proof.tx_hash, exc)
            return {"valid": False, "status": VerificationStatus.FAILED, "error": str(exc)}

        statuses = data.get("result", {}).get("value", [])
        if not statuses or statuses[0] is None:
            return {"valid": False, "status": VerificationStatus.FAILED, "reason": "Signature not found"}

        status_info = statuses[0]
        if status_info.get("err") is not None:
            return {
                "valid": False,
                "status": VerificationStatus.FAILED,
                "reason": f"Solana tx execution error: {status_info['err']}",
            }

        confirmation_status = status_info.get("confirmationStatus")
        if confirmation_status != "finalized":
            return {
                "valid": False,
                "status": VerificationStatus.WAITING_CONFIRMATIONS,
                "confirmation_status": confirmation_status,
                "slot": status_info.get("slot"),
            }

        return {
            "valid": True,
            "status": VerificationStatus.VERIFIED,
            "slot": status_info.get("slot"),
            "confirmations": status_info.get("confirmations"),
        }

    def verify_proof(self, proof: LockProof) -> Dict[str, Any]:
        """Verifies lock proof according to chain type."""
        if proof.chain_type == ChainType.EVM:
            return self.verify_evm_tx(proof)
        elif proof.chain_type == ChainType.SOLANA:
            return self.verify_solana_tx(proof)
        else:
            raise ValueError(f"Unsupported chain: {proof.chain_type}")

    def log_verified_job(
        self,
        db_connection: sqlite3.Connection,
        job_id: str,
        status: VerificationStatus,
        details: Optional[str] = None,
    ) -> None:
        """Logs/persists verified bridge jobs prior to relayer dispatch."""
        epoch_ms = int(time.time() * 1000)
        db_connection.execute(
            """
            CREATE TABLE IF NOT EXISTS bridge_lock_verifications (
                job_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                details TEXT,
                updated_at INTEGER NOT NULL
            )
            """
        )
        db_connection.execute(
            """
            INSERT INTO bridge_lock_verifications (job_id, status, details, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                status = excluded.status,
                details = excluded.details,
                updated_at = excluded.updated_at
            """,
            (job_id, status.value, details, epoch_ms),
        )
        db_connection.commit()