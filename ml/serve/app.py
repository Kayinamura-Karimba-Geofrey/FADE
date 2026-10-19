"""FastAPI application exposing the FADE ML scorer.

Phase 0 ships the liveness endpoint only; model serving lands in later
phases.
"""

from fastapi import FastAPI

__version__ = "0.1.0"

app = FastAPI(
    title="FADE ML Scorer",
    version=__version__,
    description="Machine-learning scoring service for FADE (Phase 0 skeleton).",
)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness probe: always 200 while the process is up."""
    return {"status": "ok"}
