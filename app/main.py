from fastapi import FastAPI, Request, HTTPException
from app.routers import revenue
from app.sentry import init_sentry, set_sentry_context
import sentry_sdk

init_sentry()

app = FastAPI(title="StellarFlow FastAPI Service")

@app.middleware("http")
async def sentry_context_middleware(request: Request, call_next):
    ledger_sequence = request.headers.get("X-Ledger-Sequence")
    user_id = request.headers.get("X-User-ID")
    trace_id = request.headers.get("X-Trace-ID") or request.headers.get("X-Correlation-ID")
    
    set_sentry_context(
        ledger_sequence=int(ledger_sequence) if ledger_sequence else None,
        user_id=user_id,
        trace_id=trace_id
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
