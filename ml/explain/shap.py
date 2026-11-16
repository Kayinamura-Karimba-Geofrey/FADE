"""SHAP computation and additivity checks.

The scorer returns raw top contributions (feature, value, shap, direction) and
does not build a human-facing explanation. This module only computes and
validates SHAP values; additivity is checked against the model output.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import shap


def top_contributions(
    feature_names: list[str],
    values: np.ndarray,
    shap_row: np.ndarray,
    k: int = 5,
) -> list[dict[str, Any]]:
    order = sorted(range(len(feature_names)), key=lambda i: abs(float(shap_row[i])), reverse=True)[:k]
    out: list[dict[str, Any]] = []
    for i in order:
        sv = float(shap_row[i])
        out.append(
            {
                "feature": feature_names[i],
                "value": float(values[i]),
                "shap": sv,
                "direction": "increases" if sv >= 0 else "decreases",
            }
        )
    return out


def additivity_holds(
    base_value: float,
    shap_row: np.ndarray,
    prediction: float,
    atol: float = 1e-4,
) -> bool:
    return bool(abs(float(base_value) + float(np.sum(shap_row)) - float(prediction)) <= atol)


def verify_additivity(
    explainer: shap.Explainer,
    X: np.ndarray,
    predictions: np.ndarray,
    atol: float = 1e-4,
) -> bool:
    shap_values = explainer.shap_values(X)
    if isinstance(shap_values, list):
        shap_values = shap_values[1] if len(shap_values) > 1 else shap_values[0]
    shap_values = np.asarray(shap_values)
    if shap_values.ndim == 3:
        shap_values = shap_values[:, :, 1]
    base = explainer.expected_value
    if isinstance(base, list | tuple | np.ndarray):
        base = base[1] if len(base) > 1 else base[0]
    base = float(base)
    for i in range(X.shape[0]):
        if not additivity_holds(base, shap_values[i], float(predictions[i]), atol=atol):
            return False
    return True

