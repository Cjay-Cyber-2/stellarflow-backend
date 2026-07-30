from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import requests


RPC_URL = "http://127.0.0.1:8000/rpc"
STELLAR_NETWORK_PASSPHRASE = "Standalone Network ; February 2017"
QUICKSTART_IMAGE = os.environ.get("STELLAR_QUICKSTART_IMAGE", "stellar/quickstart:testing")
CONTAINER_NAME = os.environ.get("STELLAR_QUICKSTART_CONTAINER", "stellarflow-test-quickstart")


def _run(command: list[str], *, env: dict[str, str] | None = None, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=merged_env,
        timeout=timeout,
    )


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _stellar_cli_available() -> bool:
    return shutil.which("stellar") is not None


def _wait_for_rpc_ready(url: str, *, timeout_sec: float = 120.0) -> dict:
    deadline = time.monotonic() + timeout_sec
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            response = requests.post(url, json=payload, timeout=2.0)
            response.raise_for_status()
            body = response.json()
            if body.get("result") == "healthy":
                return body
            last_error = RuntimeError(f"unexpected health payload: {body!r}")
        except Exception as exc:  # pragma: no cover - exercised only during startup
            last_error = exc
        time.sleep(2.0)

    raise RuntimeError("quickstart RPC did not become healthy") from last_error


@pytest.fixture(scope="session")
def stellar_quickstart() -> Iterator[dict[str, str]]:
    if not _docker_available():
        pytest.skip("docker is required for the Stellar Quickstart integration suite")

    subprocess.run(
        ["docker", "rm", "-f", CONTAINER_NAME],
        check=False,
        capture_output=True,
        text=True,
    )

    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            CONTAINER_NAME,
            "-p",
            "8000:8000",
            QUICKSTART_IMAGE,
            "--local",
            "--enable-stellar-rpc",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    )

    try:
        health = _wait_for_rpc_ready(RPC_URL)
        yield {"rpc_url": RPC_URL, "health": json.dumps(health)}
    finally:
        subprocess.run(
            ["docker", "rm", "-f", CONTAINER_NAME],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )


@pytest.fixture(scope="session")
def stellar_cli_env(stellar_quickstart: dict[str, str]) -> Iterator[dict[str, str]]:
    if not _stellar_cli_available():
        pytest.skip("stellar CLI is required to deploy a test contract instance")

    with tempfile.TemporaryDirectory(prefix="stellarflow-stellar-config-") as tmpdir:
        config_dir = Path(tmpdir)
        env = os.environ.copy()
        env["XDG_CONFIG_HOME"] = str(config_dir)
        env["STELLAR_RPC_URL"] = stellar_quickstart["rpc_url"]

        _run(
            [
                "stellar",
                "network",
                "add",
                "--global",
                "local",
                "--rpc-url",
                stellar_quickstart["rpc_url"],
                "--network-passphrase",
                STELLAR_NETWORK_PASSPHRASE,
            ],
            env=env,
        )

        _run(
            [
                "stellar",
                "keys",
                "generate",
                "--global",
                "pytest",
                "--network",
                "local",
                "--fund",
            ],
            env=env,
        )

        yield env


@pytest.fixture(scope="session")
def deployed_contract_id(stellar_cli_env: dict[str, str]) -> str:
    completed = _run(
        [
            "stellar",
            "contract",
            "asset",
            "deploy",
            "--asset",
            "native",
            "--source-account",
            "pytest",
            "--alias",
            "native-asset",
            "--network",
            "local",
        ],
        env=stellar_cli_env,
        timeout=300,
    )

    contract_id = completed.stdout.strip().splitlines()[-1].strip()
    assert contract_id.startswith("C"), completed.stdout
    return contract_id


def parse_mock_rpc_events(payload: dict[str, object]) -> list[tuple[str, int, str, str]]:
    result = payload.get("result", {})
    events = result.get("events", []) if isinstance(result, dict) else []
    parsed: list[tuple[str, int, str, str]] = []

    for event in events:
        if not isinstance(event, dict):
            continue
        contract_id = str(event.get("contractId", ""))
        ledger = int(event.get("ledger", 0))
        tx_hash = str(event.get("txHash", ""))
        event_type = str(event.get("type", ""))
        parsed.append((contract_id, ledger, tx_hash, event_type))

    return parsed


def test_quickstart_rpc_health(stellar_quickstart: dict[str, str]) -> None:
    response = requests.post(
        stellar_quickstart["rpc_url"],
        json={"jsonrpc": "2.0", "id": 1, "method": "getHealth"},
        timeout=5.0,
    )
    response.raise_for_status()
    assert response.json()["result"] == "healthy"


def test_deploys_soroban_contract_instance(deployed_contract_id: str) -> None:
    assert deployed_contract_id.startswith("C")
    assert len(deployed_contract_id) >= 10


def test_parser_accuracy_against_mock_rpc_events(deployed_contract_id: str) -> None:
    mock_payload = {
        "jsonrpc": "2.0",
        "id": 8675309,
        "result": {
            "latestLedger": 320543,
            "events": [
                {
                    "type": "contract",
                    "ledger": 320540,
                    "contractId": deployed_contract_id,
                    "id": "0000863490289963008-0000000010",
                    "txHash": "d0ee56996d4a750989c385bde0feb322825dbcf82e8053659806e79db1998828",
                    "topic": ["AAAADwAAAAh0cmFuc2Zlcg==", "*", "*", "*"],
                    "value": "AAAACgAAAAAAAAAAAAAAAAAAAAo=",
                },
                {
                    "type": "contract",
                    "ledger": 320541,
                    "contractId": deployed_contract_id,
                    "id": "0000863490289963008-0000000011",
                    "txHash": "d0ee56996d4a750989c385bde0feb322825dbcf82e8053659806e79db1998829",
                    "topic": ["AAAACQAAAAZkZXBvc2l0", "*", "*", "*"],
                    "value": "AAAACgAAAAAAAAAAAAAAAAAAABQ=",
                },
            ],
            "cursor": "0000863490289963008-0000000011",
        },
    }

    parsed = parse_mock_rpc_events(mock_payload)

    assert parsed == [
        (
            deployed_contract_id,
            320540,
            "d0ee56996d4a750989c385bde0feb322825dbcf82e8053659806e79db1998828",
            "contract",
        ),
        (
            deployed_contract_id,
            320541,
            "d0ee56996d4a750989c385bde0feb322825dbcf82e8053659806e79db1998829",
            "contract",
        ),
    ]

