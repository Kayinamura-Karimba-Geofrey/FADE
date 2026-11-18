"""FastAPI scorer service for FADE Phase 4."""

from __future__ import annotations

import os
import time
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.responses import Response

from ml.serve.model import ModelBundle, ScoreRequest

MODEL_DIR = os.environ.get("MODEL_DIR", "ml/models/lgbm-2026.11.1")
MODEL_ROOT = Path(os.environ.get("MODEL_ROOT", str(Path(MODEL_DIR).parent)))
_model_lock = Lock()
_model: ModelBundle | None = None
_model_cache: dict[str, ModelBundle] = {}
_model_cache_order: list[str] = []
_unknown_extra_total = 0

app = FastAPI(title="FADE ML Scorer")

REQUESTS = Counter("fade_ml_scorer_requests_total", "Total score requests")
ERRORS = Counter("fade_ml_scorer_errors_total", "Total score errors")
LATENCY = Histogram("fade_ml_scorer_latency_seconds", "Score latency")


def load_model(path: str | Path) -> ModelBundle:
    return ModelBundle(Path(path))


def get_model(version: str | None) -> ModelBundle:
    """Return the active model or a pinned version from MODEL_ROOT.

    Replay pins versions and the cache holds at most three bundles. An unknown
    version is a hard error: a replay must never score with a different model.
    """
    global _model, _model_cache_order
    if not version:
        if _model is None:
            raise HTTPException(status_code=503, detail="model not loaded")
        return _model
    with _model_lock:
        if version in _model_cache:
            return _model_cache[version]
        path = MODEL_ROOT / version
        if not (path / "manifest.json").is_file():
            raise HTTPException(status_code=422, detail=f"model version not available: {version}")
        bundle = load_model(path)
        _model_cache[version] = bundle
        _model_cache_order.append(version)
        while len(_model_cache_order) > 3:
            evicted = _model_cache_order.pop(0)
            _model_cache.pop(evicted, None)
        return bundle


@app.on_event("startup")
def startup() -> None:
    global _model
    try:
        _model = load_model(MODEL_DIR)
    except Exception:
        _model = None


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict[str, str]:
    if _model is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    return {"status": "ready"}


@app.post("/score/batch")
def score_batch(payload: dict) -> dict:
    global _unknown_extra_total
    items = payload.get("items") or payload.get("requests") or []
    if not isinstance(items, list):
        raise HTTPException(status_code=422, detail="items must be a list")
    out = []
    for item in items:
        features = item.get("features", item) if isinstance(item, dict) else {}
        explain = bool(item.get("explain", False)) if isinstance(item, dict) else False
        version = item.get("model_version") if isinstance(item, dict) else None
        model = get_model(version)
        missing = model.validate_features(features)
        if missing:
            raise HTTPException(status_code=422, detail={"missing": missing})
        try:
            model._assemble(features, strict=True)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        REQUESTS.inc()
        start = time.perf_counter()
        with _model_lock:
            resp = model.predict(features, explain=explain)
            _unknown_extra_total += getattr(model, "last_unknown_extra", 0)
        resp.latency_ms = (time.perf_counter() - start) * 1000.0
        out.append(resp.model_dump())
    return {"results": out}


@app.get("/model")
def model_info() -> dict:
    if _model is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    return {
        "model_version": _model.model_version,
        "feature_names": _model.feature_names,
        "feature_defaults": _model.feature_defaults,
    }


@app.post("/score")
def score(req: ScoreRequest) -> dict:
    global _model, _unknown_extra_total
    if _model is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    missing = _model.validate_features(req.features)
    if missing:
        raise HTTPException(status_code=422, detail={"missing": missing})
    # Reject unknown types before assembly by attempting assembly.
    try:
        _model._assemble(req.features, strict=True)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    REQUESTS.inc()
    start = time.perf_counter()
    try:
        with _model_lock:
            resp = _model.predict(req.features, explain=req.explain)
            _unknown_extra_total += getattr(_model, "last_unknown_extra", 0)
        resp.latency_ms = (time.perf_counter() - start) * 1000.0
        LATENCY.observe(time.perf_counter() - start)
        return resp.model_dump()
    except Exception as exc:  # noqa: BLE001
        ERRORS.inc()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/admin/reload")
def reload_model(payload: dict) -> dict[str, str]:
    global _model
    path = payload.get("model_dir") or payload.get("path")
    if not path:
        raise HTTPException(status_code=400, detail="model_dir required")
    try:
        candidate = load_model(path)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"invalid model: {exc}") from exc
    with _model_lock:
        _model = candidate
    return {"status": "reloaded", "model_dir": str(path)}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
