"""Model loading and prediction helpers for the FADE scorer."""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
import shap
from pydantic import BaseModel, Field


class ScoreRequest(BaseModel):
    features: dict[str, Any]
    explain: bool = False


class Contribution(BaseModel):
    feature: str
    value: float
    shap: float
    direction: str


class ScoreResponse(BaseModel):
    model_version: str
    fraud_prob: float
    anomaly_score: float
    ml_score: int
    status: str
    latency_ms: float
    top_contributions: list[Contribution] = Field(default_factory=list)
    explain_skipped: bool = False


class ModelBundle:
    def __init__(self, model_dir: Path):
        self.model_dir = Path(model_dir)
        self.feature_spec = json.loads((self.model_dir / "features.json").read_text())
        self.feature_names = [f["name"] for f in self.feature_spec["features"]]
        self.feature_defaults = {f["name"]: f.get("default", 0) for f in self.feature_spec["features"]}
        self.model_version = self.model_dir.name
        self.booster = lgb.Booster(model_file=str(self.model_dir / "model.txt"))
        cal = json.loads((self.model_dir / "calibrator.json").read_text())
        self.cal_x = np.asarray(cal["x"], dtype=float)
        self.cal_y = np.asarray(cal["y"], dtype=float)
        self.iso = joblib.load(self.model_dir / "isoforest.joblib")
        self.cdf = np.asarray(json.loads((self.model_dir / "isoforest_cdf.json").read_text()), dtype=float)
        bg = pd.read_parquet(self.model_dir / "shap_background.parquet")
        self.background = bg[self.feature_names].to_numpy(dtype=np.float32)
        self.explainer = shap.TreeExplainer(
            self.booster, data=self.background, feature_perturbation="interventional"
        )
        self.golden = json.loads((self.model_dir / "golden_predictions.json").read_text())
        # Warm-up and golden verification.
        self._warmup()

    def _warmup(self) -> None:
        # 100 warm-up predictions.
        vec = np.zeros((1, len(self.feature_names)), dtype=np.float32)
        for _ in range(100):
            self._predict_array(vec, explain=False)
        # Golden verification.
        for item in self.golden:
            features = item["features"]
            arr = self._assemble(features, strict=False)
            fp, an, ms = self._predict_array(arr.reshape(1, -1), explain=False)
            expected = item["expected"]
            if abs(fp - expected["fraud_prob"]) > 1e-6:
                raise RuntimeError(f"golden fraud_prob mismatch: {fp} vs {expected['fraud_prob']}")
            if abs(an - expected["anomaly_score"]) > 1e-6:
                raise RuntimeError(f"golden anomaly mismatch: {an} vs {expected['anomaly_score']}")
            if int(ms) != int(expected["ml_score"]):
                raise RuntimeError(f"golden ml_score mismatch: {ms} vs {expected['ml_score']}")

    def _assemble(self, features: dict[str, Any], strict: bool = True) -> np.ndarray:
        missing = [name for name in self.feature_names if name not in features]
        if strict and missing:
            raise ValueError("missing features: " + ", ".join(missing))
        values = []
        unknown_extra = 0
        for name in self.feature_names:
            if name in features:
                raw = features[name]
            else:
                raw = self.feature_defaults.get(name, 0)
            if isinstance(raw, bool):
                raw = 1 if raw else 0
            try:
                val = float(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"non-numeric feature {name}") from exc
            if not math.isfinite(val):
                raise ValueError(f"non-finite feature {name}")
            values.append(val)
        # Count extras (ignored).
        for key in features:
            if key not in self.feature_names:
                unknown_extra += 1
        self.last_unknown_extra = unknown_extra
        return np.asarray(values, dtype=np.float32)

    def _predict_array(self, arr: np.ndarray, explain: bool) -> tuple[float, float, int]:
        raw_fp = float(self.booster.predict(arr)[0])
        fp = float(np.interp(raw_fp, self.cal_x, self.cal_y, left=self.cal_y[0], right=self.cal_y[-1]))
        raw_an = -self.iso.score_samples(arr)[0]
        an = float(np.interp(raw_an, self.cdf, np.linspace(0.0, 1.0, len(self.cdf)), left=0.0, right=1.0))
        ms = int(round(100.0 * (0.7 * fp + 0.3 * an)))
        return fp, an, ms

    def predict(self, features: dict[str, Any], explain: bool = False) -> ScoreResponse:
        arr = self._assemble(features, strict=True).reshape(1, -1)
        fp, an, ms = self._predict_array(arr, explain=explain)
        contributions = []
        explain_skipped = False
        if explain:
            start = time.perf_counter()
            shap_values = self.explainer.shap_values(arr)
            if isinstance(shap_values, list):
                shap_values = shap_values[1] if len(shap_values) > 1 else shap_values[0]
            if shap_values.ndim == 3:
                shap_values = shap_values[:, :, 1]
            row = shap_values[0]
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if elapsed_ms > 10.0:
                explain_skipped = True
            else:
                order = sorted(range(len(self.feature_names)), key=lambda i: abs(row[i]), reverse=True)[:5]
                for i in order:
                    contributions.append(Contribution(
                        feature=self.feature_names[i],
                        value=float(arr[0, i]),
                        shap=float(row[i]),
                        direction="increases" if row[i] >= 0 else "decreases",
                    ))
        return ScoreResponse(
            model_version=self.model_version,
            fraud_prob=fp,
            anomaly_score=an,
            ml_score=ms,
            status="OK",
            latency_ms=0.0,
            top_contributions=contributions,
            explain_skipped=explain_skipped,
        )

    def validate_features(self, features: dict[str, Any]) -> list[str]:
        missing = [name for name in self.feature_names if name not in features]
        return missing
