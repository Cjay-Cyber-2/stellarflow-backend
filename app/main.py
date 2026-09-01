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
from app.services.executor_pool import (
    LATENCY_BUDGET_MS,
    get_heavy_pool,
    get_latency_monitor,
    shutdown_pools,
    start_latency_monitor,
    stop_latency_monitor,
)
from app.services.proof_verification_engine import (
    PROOF_CACHE_TTL_SECONDS,
    PROOF_PROCESS_POOL_WORKERS,
    get_process_pool,
    shutdown_process_pool,
    verify_proof_async,
    verify_proof_batch,
)

try:
    from app.routers import revenue as revenue_router
    _HAS_REVENUE_ROUTER = True
except ImportError:
    _HAS_REVENUE_ROUTER = False

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage executor pools and latency monitor lifecycle."""
    get_process_pool()
    get_heavy_pool()
    await start_latency_monitor()
    yield
    await stop_latency_monitor()
    shutdown_process_pool()
    shutdown_pools()


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
    
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        status_code = getattr(e, "status_code", 500)
        if isinstance(e, HTTPException):
            status_code = e.status_code
        if status_code in (401, 404):
            raise e
        sentry_sdk.capture_exception(e)
        raise e

app.include_router(revenue.router, prefix="/api/v1")

@app.get("/health")
def health_check():
    return {"status": "ok"}
