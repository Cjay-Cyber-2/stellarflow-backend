"""FastAPI entrypoint for the StellarFlow Python service.

Issue #824 — Shielded Transaction Proof Verification Offloading Engine

The Dockerfile starts this module with:
    uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

# ---------------------------------------------------------------------------
# Logging MUST be configured before any other app imports so that every
# module that calls logging.getLogger() at import time is already wired to
# the structlog JSON pipeline.
# ---------------------------------------------------------------------------
from app.core.logging import configure_logging  # noqa: E402 — intentional first import

configure_logging()

import uuid
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.logging import bind_request_context, clear_contextvars
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

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Request-scoped logging middleware
# ---------------------------------------------------------------------------

class StructlogRequestMiddleware(BaseHTTPMiddleware):
    """Inject a per-request trace_id into the structlog context.

    For every inbound HTTP request:
    - Reads ``X-Trace-Id`` from the request headers (set by a gateway or
      load-balancer upstream), or generates a fresh UUID4 when absent.
    - Binds ``trace_id``, ``method``, and ``path`` into the context so every
      log line emitted during that request carries those fields.
    - Clears the context after the response is sent to prevent leakage.
    - Logs a single ``request.completed`` record with the HTTP status code and
      wall-clock duration (ms) at the end of each request.
    """

    async def dispatch(self, request: Request, call_next):
        trace_id = request.headers.get("x-trace-id") or str(uuid.uuid4())

        bind_request_context(trace_id=trace_id)
        # Bind method + path for the lifetime of this request
        structlog.contextvars.bind_contextvars(
            http_method=request.method,
            http_path=request.url.path,
        )

        import time
        start = time.monotonic()
        try:
            response = await call_next(request)
            elapsed_ms = round((time.monotonic() - start) * 1000, 2)
            log.info(
                "request.completed",
                status_code=response.status_code,
                duration_ms=elapsed_ms,
            )
            # Echo the trace_id back to the caller so it can be correlated
            response.headers["x-trace-id"] = trace_id
            return response
        except Exception:
            elapsed_ms = round((time.monotonic() - start) * 1000, 2)
            log.exception("request.failed", duration_ms=elapsed_ms)
            raise
        finally:
            clear_contextvars()


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage executor pools and latency monitor lifecycle."""
    log.info(
        "stellarflow.startup",
        process_pool_workers=PROOF_PROCESS_POOL_WORKERS,
        cache_ttl_seconds=PROOF_CACHE_TTL_SECONDS,
    )
    get_process_pool()
    get_heavy_pool()
    await start_latency_monitor()
    yield
    log.info("stellarflow.shutdown")
    await stop_latency_monitor()
    shutdown_process_pool()
    shutdown_pools()


# ---------------------------------------------------------------------------
# Application instance
# ---------------------------------------------------------------------------

app = FastAPI(
    title="StellarFlow Proof Verification Engine",
    description="Issue #824 — Offloaded ZK proof verification with async process pools",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(StructlogRequestMiddleware)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

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
        log.exception("proof.verify.error", error=str(exc))
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
        log.exception("proof.verify_batch.error", error=str(exc))
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


# ---------------------------------------------------------------------------
# Mount optional routers
# ---------------------------------------------------------------------------

try:
    from app.adapters.anchor import router as anchor_router

    app.include_router(anchor_router, prefix="/webhook", tags=["Webhooks"])
except ImportError:
    pass

if _HAS_REVENUE_ROUTER:
    app.include_router(revenue_router.router, tags=["Analytics"])
