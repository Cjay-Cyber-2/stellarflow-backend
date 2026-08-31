"""FastAPI entrypoint for the StellarFlow Python service.

Issue #824 — Shielded Transaction Proof Verification Offloading Engine
Issue #NEW — Cryptographically Signed Audit Logging System for Administrative Operations

The Dockerfile starts this module with:
    uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

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
from app.security.kms import KeyRotationHandler, LocalVaultProvider
from app.services.audit_logger import init_audit_logger

# Import routers
try:
    from app.routers import revenue as revenue_router
    _HAS_REVENUE_ROUTER = True
except ImportError:
    _HAS_REVENUE_ROUTER = False

try:
    from app.routers import audit as audit_router
    _HAS_AUDIT_ROUTER = True
except ImportError:
    _HAS_AUDIT_ROUTER = False

logger = logging.getLogger(__name__)

# Global KMS and audit logger instances
_key_handler: Optional[KeyRotationHandler] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage executor pools, KMS, latency monitor, and audit logger lifecycle."""
    global _key_handler
    
    # Initialize core processing pools
    get_process_pool()
    get_heavy_pool()
    await start_latency_monitor()
    
    # Initialize KMS and audit logging system
    try:
        # Initialize with LocalVaultProvider for development/stub mode
        # In production, this would use AwsKmsProvider with proper configuration
        provider = LocalVaultProvider()
        _key_handler = KeyRotationHandler(provider)
        await _key_handler.start()
        
        # Initialize the audit logger with the KMS key handler
        init_audit_logger(_key_handler)
        logger.info("KMS and audit logging system initialized successfully")
    except Exception as exc:
        logger.error("Failed to initialize KMS and audit logging system: %s", exc)
        # Continue running even if audit logging fails to not break other services
    
    yield
    
    # Cleanup resources
    if _key_handler:
        await _key_handler.stop()
    await stop_latency_monitor()
    shutdown_process_pool()
    shutdown_pools()


app = FastAPI(
    title="StellarFlow Backend Services",
    description="Combined service including proof verification, revenue tracking, and compliance audit logging",
    version="1.0.0",
    lifespan=lifespan,
)

# Include all available routers
if _HAS_REVENUE_ROUTER:
    app.include_router(revenue_router.router)
    logger.info("Included revenue router")

if _HAS_AUDIT_ROUTER:
    app.include_router(audit_router.router)
    logger.info("Included audit router")


@app.get("/health")
async def health() -> JSONResponse:
    health_data = {
        "success": True,
        "services": {
            "proof_verification": {
                "processPoolWorkers": PROOF_PROCESS_POOL_WORKERS,
                "cacheTtlSeconds": PROOF_CACHE_TTL_SECONDS,
                "status": "healthy"
            },
            "audit_logging": {
                "status": "healthy" if _key_handler and _key_handler.get_active_key() else "unavailable",
                "activeKeyId": _key_handler.active_key_id if _key_handler else None
            }
        }
    }
    return JSONResponse(health_data)


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


# Example administrative endpoint that logs an operation
from fastapi import Request
from app.services.audit_logger import log_administrative_operation
from app.models.audit import AdministrativeOperationType

@app.post("/admin/key-rotation", status_code=200, summary="Trigger a key rotation (demo endpoint)")
async def trigger_key_rotation(request: Request):
    """Demo endpoint that simulates a key rotation and logs it to the audit system."""
    try:
        # Log the key rotation operation
        await log_administrative_operation(
            operation_type=AdministrativeOperationType.KEY_ROTATION,
            actor="admin@stellarflow.io",
            payload={
                "reason": "Scheduled monthly key rotation",
                "old_key_id": _key_handler.active_key_id if _key_handler else None,
                "initiated_by": "scheduled_automation"
            },
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent")
        )
        
        return JSONResponse({"success": True, "message": "Key rotation logged to audit trail"})
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


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


@app.get("/proof/latency")
async def latency_status() -> JSONResponse:
    """Return event-loop latency monitor status."""
    monitor = get_latency_monitor()
    return JSONResponse(
        {
            "success": True,
            "budgetMs": LATENCY_BUDGET_MS,
            "maxLatencyMs": round(monitor.max_latency_ms, 3),
            "avgLatencyMs": round(monitor.avg_latency_ms, 3),
            "violationCount": monitor.violation_count,
            "isHealthy": monitor.is_healthy,
        }
    )


# Include existing routers (if any)
try:
    from app.adapters.anchor import router as anchor_router

    app.include_router(anchor_router, prefix="/webhook", tags=["Webhooks"])
except ImportError:
    pass

if _HAS_REVENUE_ROUTER:
    app.include_router(revenue_router.router, tags=["Analytics"])