from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class ProofScheme(str, Enum):
    GROTH16 = "groth16"
    PLONK = "plonk"
    MARLIN = "marlin"
    FLONK = "flonk"


class ProofPayload(BaseModel):
    proof_hex: str = Field(..., min_length=1, description="Hex-encoded zero-knowledge proof bytes")
    public_inputs: List[str] = Field(..., min_length=1, description="Public inputs for proof verification")
    contract_params: Dict[str, Any] = Field(..., description="Parameters for contract simulation")
    transaction_hash: str = Field(..., min_length=64, max_length=64, description="Hex transaction hash (32 bytes)")
    proof_scheme: ProofScheme = Field(default=ProofScheme.GROTH16, description="Proof system scheme")

    @field_validator("proof_hex")
    @classmethod
    def validate_proof_hex(cls, value: str) -> str:
        if not all(c in "0123456789abcdefABCDEF" for c in value):
            raise ValueError("proof_hex must contain only hexadecimal characters")
        if len(value) % 2 != 0:
            raise ValueError("proof_hex length must be even")
        return value.lower()

    @field_validator("transaction_hash")
    @classmethod
    def validate_transaction_hash(cls, value: str) -> str:
        if not all(c in "0123456789abcdefABCDEF" for c in value):
            raise ValueError("transaction_hash must contain only hexadecimal characters")
        return value.lower()


class ContractSimulationParams(BaseModel):
    contract_id: str = Field(..., description="Soroban contract identifier")
    function_name: str = Field(..., min_length=1, description="Contract function to simulate")
    args: List[Any] = Field(default_factory=list, description="Positional arguments for the function")
    source_account: str = Field(..., description="Source account for simulation")
    ledger_sequence: Optional[int] = Field(default=None, description="Ledger sequence for simulation")


class ProofValidationResult(BaseModel):
    valid: bool
    proof_hash: str
    verification_time_ms: float
    cached: bool
    error: Optional[str] = None
    contract_simulation_ready: bool = False
    public_inputs_count: int = 0


class ProofVerificationRequest(BaseModel):
    proof: ProofPayload
    simulate_contract: bool = Field(default=False, description="Whether to run contract simulation")


class ProofVerificationResponse(BaseModel):
    success: bool
    result: Optional[ProofValidationResult] = None
    message: Optional[str] = None


__all__ = [
    "ProofScheme",
    "ProofPayload",
    "ContractSimulationParams",
    "ProofValidationResult",
    "ProofVerificationRequest",
    "ProofVerificationResponse",
]
