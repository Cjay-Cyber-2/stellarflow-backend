import sqlite3
import pytest
from unittest.mock import MagicMock
from network.bridge_lock_verifier import (
    BridgeLockVerificationWorker,
    LockProof,
    ChainType,
    VerificationStatus,
)


@pytest.fixture
def mock_session():
    return MagicMock()


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def test_evm_verification_success(mock_session):
    worker = BridgeLockVerificationWorker(session=mock_session)
    proof = LockProof(
        job_id="job-101",
        chain_type=ChainType.EVM,
        tx_hash="0xabc123",
        expected_amount=1000,
        expected_recipient="GABC...",
        rpc_url="http://localhost:8545",
        required_confirmations=12,
    )

    receipt_mock = MagicMock()
    receipt_mock.json.return_value = {
        "result": {"status": "0x1", "blockNumber": "0x64", "logs": []}  # Block 100
    }
    receipt_mock.raise_for_status = MagicMock()

    block_mock = MagicMock()
    block_mock.json.return_value = {"result": "0x78"}  # Block 120 (20 confirmations >= 12)
    block_mock.raise_for_status = MagicMock()

    mock_session.post.side_effect = [receipt_mock, block_mock]

    result = worker.verify_proof(proof)
    assert result["valid"] is True
    assert result["status"] == VerificationStatus.VERIFIED
    assert result["confirmations"] == 20


def test_evm_verification_waiting_confirmations(mock_session):
    worker = BridgeLockVerificationWorker(session=mock_session)
    proof = LockProof(
        job_id="job-102",
        chain_type=ChainType.EVM,
        tx_hash="0xabc123",
        expected_amount=1000,
        expected_recipient="GABC...",
        rpc_url="http://localhost:8545",
        required_confirmations=12,
    )

    receipt_mock = MagicMock()
    receipt_mock.json.return_value = {
        "result": {"status": "0x1", "blockNumber": "0x64", "logs": []}  # Block 100
    }
    receipt_mock.raise_for_status = MagicMock()

    block_mock = MagicMock()
    block_mock.json.return_value = {"result": "0x68"}  # Block 104 (4 confirmations < 12)
    block_mock.raise_for_status = MagicMock()

    mock_session.post.side_effect = [receipt_mock, block_mock]

    result = worker.verify_proof(proof)
    assert result["valid"] is False
    assert result["status"] == VerificationStatus.WAITING_CONFIRMATIONS
    assert result["confirmations"] == 4


def test_solana_verification_finalized(mock_session):
    worker = BridgeLockVerificationWorker(session=mock_session)
    proof = LockProof(
        job_id="job-103",
        chain_type=ChainType.SOLANA,
        tx_hash="solana_signature_xyz",
        expected_amount=500,
        expected_recipient="GABC...",
        rpc_url="http://localhost:8899",
    )

    solana_mock = MagicMock()
    solana_mock.json.return_value = {
        "result": {
            "value": [
                {
                    "slot": 987654,
                    "confirmations": None,
                    "err": None,
                    "confirmationStatus": "finalized",
                }
            ]
        }
    }
    solana_mock.raise_for_status = MagicMock()
    mock_session.post.return_value = solana_mock

    result = worker.verify_proof(proof)
    assert result["valid"] is True
    assert result["status"] == VerificationStatus.VERIFIED
    assert result["slot"] == 987654


def test_log_verified_job_in_database(in_memory_db, mock_session):
    worker = BridgeLockVerificationWorker(session=mock_session)
    job_id = "job-sqlite-test"
    worker.log_verified_job(in_memory_db, job_id, VerificationStatus.VERIFIED, "Verified at block 120")

    cursor = in_memory_db.cursor()
    cursor.execute("SELECT status, details FROM bridge_lock_verifications WHERE job_id = ?", (job_id,))
    row = cursor.fetchone()

    assert row is not None
    assert row[0] == "VERIFIED"
    assert row[1] == "Verified at block 120"