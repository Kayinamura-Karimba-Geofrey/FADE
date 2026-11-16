"""Evaluation helpers for FADE Phase 4 models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score


def threshold_at_fpr(y: np.ndarray, scores: np.ndarray, target_fpr: float) -> tuple[float, float]:
    y = np.asarray(y)
    scores = np.asarray(scores, dtype=float)
    negatives = scores[y == 0]
    n = len(negatives)
    if n == 0:
        raise ValueError("no negative examples")
    k = max(1, int(np.floor(target_fpr * n)))
    k = min(k, n)
    ordered = np.sort(negatives)[::-1]
    threshold = float(ordered[k - 1])
    achieved = float((negatives >= threshold).mean())
    return threshold, achieved


def classification_at_threshold(y: np.ndarray, scores: np.ndarray, threshold: float) -> dict[str, float]:
    pred = (np.asarray(scores) >= threshold).astype(int)
    y = np.asarray(y).astype(int)
    tp = int(((pred == 1) & (y == 1)).sum())
    fp = int(((pred == 1) & (y == 0)).sum())
    tn = int(((pred == 0) & (y == 0)).sum())
    fn = int(((pred == 0) & (y == 1)).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "precision": precision, "recall": recall,
        "achieved_fpr": fp / (fp + tn) if fp + tn else 0.0,
    }


def ece(y: np.ndarray, prob: np.ndarray, bins: int = 10) -> tuple[float, list[dict[str, float]]]:
    y = np.asarray(y).astype(float)
    prob = np.asarray(prob).astype(float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    total = len(y)
    ece_value = 0.0
    curve = []
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        mask = (prob >= lo) & (prob < hi if i < bins - 1 else prob <= hi)
        if not mask.any():
            curve.append({"bin": i, "count": 0, "mean_pred": 0.0, "mean_obs": 0.0})
            continue
        mean_pred = float(prob[mask].mean())
        mean_obs = float(y[mask].mean())
        weight = float(mask.sum() / total)
        ece_value += weight * abs(mean_pred - mean_obs)
        curve.append({"bin": i, "count": int(mask.sum()), "mean_pred": mean_pred, "mean_obs": mean_obs})
    return float(ece_value), curve


def percentiles(values: np.ndarray) -> dict[str, float]:
    q = [1, 5, 25, 50, 75, 95, 99]
    out = np.percentile(np.asarray(values, dtype=float), q)
    return {f"p{p}": float(v) for p, v in zip(q, out, strict=False)}


def amount_weighted_recall(
    y: np.ndarray, scores: np.ndarray, amounts: np.ndarray, threshold: float
) -> float:
    y = np.asarray(y).astype(int)
    amounts = np.asarray(amounts, dtype=float)
    caught = float((((y == 1) & (np.asarray(scores) >= threshold)) * amounts).sum())
    total = float(((y == 1) * amounts).sum())
    return caught / total if total else 0.0


def per_scenario_recall(
    scenarios: np.ndarray, y: np.ndarray, scores: np.ndarray, threshold: float
) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for scenario in sorted(set(str(x) for x in scenarios)):
        if scenario in {"", "none", "LEGIT"}:
            continue
        mask = (scenarios == scenario) & (y == 1)
        if not mask.any():
            continue
        recall = float(((scores[mask] >= threshold)).mean())
        out[scenario] = {"n": int(mask.sum()), "recall": recall}
    return out


def evaluate_binary(
    y: np.ndarray,
    prob: np.ndarray,
    scores_for_threshold: np.ndarray | None = None,
    amounts: np.ndarray | None = None,
    scenarios: np.ndarray | None = None,
) -> dict[str, Any]:
    y = np.asarray(y).astype(int)
    prob = np.asarray(prob, dtype=float)
    result: dict[str, Any] = {
        "pr_auc": float(average_precision_score(y, prob)),
        "roc_auc": float(roc_auc_score(y, prob)),
        "brier": float(brier_score_loss(y, prob)),
    }
    ece_value, curve = ece(y, prob)
    result["ece"] = ece_value
    result["reliability"] = curve
    result["score_percentiles"] = {
        "fraud": percentiles(prob[y == 1]),
        "legit": percentiles(prob[y == 0]),
    }
    if scores_for_threshold is None:
        scores_for_threshold = prob
    for fpr in (0.005, 0.01, 0.02):
        threshold, achieved = threshold_at_fpr(y, scores_for_threshold, fpr)
        cls = classification_at_threshold(y, scores_for_threshold, threshold)
        key = f"fpr_{int(fpr * 10000) / 100:.1f}".replace(".0", "")
        # Use stable keys.
        key = {0.005: "fpr_0_5", 0.01: "fpr_1", 0.02: "fpr_2"}[fpr]
        result[key] = {
            "target_fpr": fpr,
            "threshold": threshold,
            "achieved_fpr": achieved,
            "precision": cls["precision"],
            "recall": cls["recall"],
            "tp": cls["tp"],
            "fp": cls["fp"],
        }
        if amounts is not None and fpr == 0.01:
            result[key]["amount_weighted_recall"] = amount_weighted_recall(
                y, scores_for_threshold, amounts, threshold
            )
        if scenarios is not None and fpr == 0.01:
            result[key]["per_scenario_recall"] = per_scenario_recall(
                scenarios, y, scores_for_threshold, threshold
            )
    return result


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
