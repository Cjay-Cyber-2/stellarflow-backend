"""FastAPI entrypoint for the StellarFlow Python service.

Issue #824 — Shielded Transaction Proof Verification Offloading Engine

The Dockerfile starts this module with:
    uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.models.proof import ProofVerificationRequest, ProofVerificationResponse
from app.services.auth_challenge import (
    consume_auth_challenge,
    create_auth_challenge,
)
from app.services.proof_verification_engine import (
    PROOF_CACHE_TTL_SECONDS,
    PROOF_PROCESS_POOL_WORKERS,
    get_process_pool,
    shutdown_process_pool,
    verify_proof_async,
    verify_proof_batch,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage process pool lifecycle."""
    get_process_pool()
    yield
    shutdown_process_pool()


app = FastAPI(
    title="StellarFlow Proof Verification Engine",
    description="Issue #824 — Offloaded ZK proof verification with async process pools",
    version="1.0.0",
    lifespan=lifespan,
)


class AuthChallengeConsumeRequest(BaseModel):
    nonce: str


@app.post("/api/v1/auth/challenge")
async def auth_challenge() -> JSONResponse:
    """Issue a one-time authentication challenge nonce."""
    try:
        nonce = await create_auth_challenge()
        return JSONResponse({"success": True, "data": {"nonce": nonce}})
    except Exception as exc:
        logger.exception("Auth challenge creation failed: %s", exc)
        raise HTTPException(
            status_code=503, detail="Authentication unavailable"
        ) from exc


@app.post("/api/v1/auth/challenge/consume")
async def auth_challenge_consume(
    request: AuthChallengeConsumeRequest,
) -> JSONResponse:
    """Atomically consume an authentication challenge nonce exactly once."""
    try:
        consumed = await consume_auth_challenge(request.nonce)
    except Exception as exc:
        logger.exception("Auth challenge consumption failed: %s", exc)
        raise HTTPException(
            status_code=503, detail="Authentication unavailable"
        ) from exc

    if not consumed:
        raise HTTPException(status_code=401, detail="Invalid or expired challenge")

    return JSONResponse({"success": True, "data": {"consumed": True}})


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse(
        {
            "success": True,
            "service": "proof-verification",
            "processPoolWorkers": PROOF_PROCESS_POOL_WORKERS,
            "cacheTtlSeconds": PROOF_CACHE_TTL_SECONDS,
        }
    )


@app.post("/proof/verify", response_model=ProofVerificationResponse)
async def verify_proof(request: ProofVerificationRequest) -> ProofVerificationResponse:
    """Verify a single shielded transaction proof.

    Offloads CPU-intensive ZK proof checks to a background worker process pool.
    Returns cached results within the 100ms latency budget when available.
    """
    try:
        result = await verify_proof_async(
            proof_hex=request.proof.proof_hex,
            public_inputs=request.proof.public_inputs,
            contract_params=request.proof.contract_params,
            proof_scheme=request.proof.proof_scheme.value,
            simulate_contract=request.simulate_contract,
        )
        return ProofVerificationResponse(
            success=True,
            result=result,
        )
    except Exception as exc:
        logger.exception("Proof verification endpoint error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/proof/verify-batch")
async def verify_proof_batch_endpoint(
    requests: list[ProofVerificationRequest],
) -> JSONResponse:
    """Verify multiple shielded transaction proofs concurrently."""
    if not requests:
        raise HTTPException(status_code=400, detail="requests list is empty")

    try:
        payloads = [req.model_dump() for req in requests]
        results = await verify_proof_batch(payloads)
        return JSONResponse(
            {
                "success": True,
                "results": [r.to_dict() for r in results],
            }
        )
    except Exception as exc:
        logger.exception("Batch proof verification endpoint error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/proof/pool-status")
async def pool_status() -> JSONResponse:
    """Return process pool status for observability."""
    pool = get_process_pool()
    return JSONResponse(
        {
            "success": True,
            "maxWorkers": PROOF_PROCESS_POOL_WORKERS,
        }
    )


# Include existing routers (if any)
try:
    from app.adapters.anchor import router as anchor_router

    app.include_router(anchor_router, prefix="/webhook", tags=["Webhooks"])
except ImportError:
    pass

try:
    from app.graphql import graphql_app

    app.include_router(graphql_app, prefix="/graphql", tags=["GraphQL"])
except ImportError:
    logger.warning("GraphQL dependencies are unavailable; /graphql is disabled")
