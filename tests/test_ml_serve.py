"""Tests for the ml.serve package (Phase 0: healthz endpoint only)."""

from fastapi.testclient import TestClient

from ml.serve.app import app

client = TestClient(app)


def test_healthz_returns_ok() -> None:
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_metrics_exposes_prometheus_families() -> None:
    client.get("/healthz")  # populate the request counter first
    response = client.get("/metrics")
    assert response.status_code == 200
    body = response.text
    assert "fade_ml_scorer_requests_total" in body
    assert "process_cpu_seconds_total" in body
