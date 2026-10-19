"""FastAPI application exposing the FADE ML scorer.

Phase 0 ships the liveness and metrics endpoints only; model serving lands
in later phases.
"""

from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

__version__ = "0.1.0"

app = FastAPI(
    title="FADE ML Scorer",
    version=__version__,
    description="Machine-learning scoring service for FADE (Phase 0 skeleton).",
)

REQUESTS = Counter(
    "fade_ml_scorer_requests_total",
    "Total requests handled by the ML scorer.",
    ["endpoint", "status"],
)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness probe: always 200 while the process is up."""
    REQUESTS.labels(endpoint="/healthz", status="200").inc()
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus exposition (process and request metrics)."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
