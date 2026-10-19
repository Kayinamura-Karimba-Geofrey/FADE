"""Tests for the ml.serve package (Phase 0: healthz endpoint only)."""

from fastapi.testclient import TestClient

from ml.serve.app import app

client = TestClient(app)


def test_healthz_returns_ok() -> None:
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
