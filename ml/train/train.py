"""Train FADE Phase 4 models and write an immutable artifact directory."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, cast

import joblib
import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd
import shap
from sklearn.ensemble import IsolationForest
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from ml.config import (
    FEATURE_NAMES,
    FEATURE_SCHEMA_VERSION,
    SEED,
    TARGET_COLUMN,
    feature_manifest,
    load_vocab,
    model_dir,
    version_name,
)
from ml.train.metrics import evaluate_binary, save_json


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _native(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def load_split(data_dir: Path) -> tuple[Any, ...]:
    frame = pd.read_parquet(data_dir / "ml_table.parquet")
    train = frame[frame["split"] == "train"].reset_index(drop=True)
    valid = frame[frame["split"] == "valid"].reset_index(drop=True)
    test = frame[frame["split"] == "test"].reset_index(drop=True)
    if train.empty or valid.empty or test.empty:
        raise ValueError("missing split")
    if train[TARGET_COLUMN].nunique() < 2 or valid[TARGET_COLUMN].nunique() < 2 or test[TARGET_COLUMN].nunique() < 2:
        raise ValueError("split lacks both classes")
    Xtr = train[list(FEATURE_NAMES)].to_numpy(dtype=np.float32)
    Xva = valid[list(FEATURE_NAMES)].to_numpy(dtype=np.float32)
    Xte = test[list(FEATURE_NAMES)].to_numpy(dtype=np.float32)
    ytr = train[TARGET_COLUMN].to_numpy(dtype=np.int8)
    yva = valid[TARGET_COLUMN].to_numpy(dtype=np.int8)
    yte = test[TARGET_COLUMN].to_numpy(dtype=np.int8)
    return frame, train, valid, test, Xtr, Xva, Xte, ytr, yva, yte


def anomaly_scores(iso: IsolationForest, X: np.ndarray, cdf: np.ndarray) -> np.ndarray:
    raw = -iso.score_samples(X)
    return np.interp(raw, cdf, np.linspace(0.0, 1.0, len(cdf)), left=0.0, right=1.0).astype(np.float64)


def ml_scores(fp: np.ndarray, an: np.ndarray, w_fp: float = 0.7, w_an: float = 0.3) -> np.ndarray:
    return np.rint(100.0 * (w_fp * fp + w_an * an)).astype(int)


def train_baseline_logistic(
    Xtr: np.ndarray,
    ytr: np.ndarray,
    Xva: np.ndarray,
    yva: np.ndarray,
    Xte: np.ndarray,
    yte: np.ndarray,
    amounts_va: np.ndarray,
    amounts_te: np.ndarray,
    scenarios_va: np.ndarray,
    scenarios_te: np.ndarray,
) -> dict[str, Any]:
    # Subsample the (huge) training set for the logistic baseline so the fit
    # stays tractable; validation/test are scored in full.
    rng = np.random.default_rng(SEED)
    cap = min(200000, len(Xtr))
    idx = rng.choice(len(Xtr), size=cap, replace=False)
    Xtr_b, ytr_b = Xtr[idx], ytr[idx]
    scaler = StandardScaler()
    Xtr_s = scaler.fit_transform(Xtr_b)
    Xva_s = scaler.transform(Xva)
    Xte_s = scaler.transform(Xte)
    model = LogisticRegression(
        max_iter=500, class_weight="balanced", random_state=SEED, n_jobs=4, solver="lbfgs"
    )
    model.fit(Xtr_s, ytr_b)
    pva = model.predict_proba(Xva_s)[:, 1]
    pte = model.predict_proba(Xte_s)[:, 1]
    return {
        "valid": evaluate_binary(yva, pva, amounts=amounts_va, scenarios=scenarios_va),
        "test": evaluate_binary(yte, pte, amounts=amounts_te, scenarios=scenarios_te),
        "coefficients": {name: float(v) for name, v in zip(FEATURE_NAMES, model.coef_[0], strict=False)},
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/run42"))
    parser.add_argument("--version", type=str, default=None)
    parser.add_argument("--time-budget", type=int, default=1200)
    parser.add_argument("--monotone-compare", action="store_true", default=True)
    args = parser.parse_args()

    if args.version is None:
        args.version = version_name("lgbm", 2026, 11, 1)

    out = model_dir(args.version)
    out.mkdir(parents=True, exist_ok=True)

    frame, train, valid, test, Xtr, Xva, Xte, ytr, yva, yte = load_split(args.data_dir)
    amounts_va = valid["amount"].to_numpy(dtype=float) if "amount" in valid else np.ones(len(valid))
    amounts_te = test["amount"].to_numpy(dtype=float) if "amount" in test else np.ones(len(test))
    scenarios_va = valid["scenario"].to_numpy() if "scenario" in valid else np.array([""] * len(valid))
    scenarios_te = test["scenario"].to_numpy() if "scenario" in test else np.array([""] * len(test))

    rng = np.random.default_rng(SEED)

    # Baseline logistic regression (skipped if BASELINE_SKIP env set for speed).
    import os as _os
    if _os.environ.get("BASELINE_SKIP") == "1":
        baseline_metrics = {}
    else:
        baseline_metrics = train_baseline_logistic(Xtr, ytr, Xva, yva, Xte, yte, amounts_va, amounts_te, scenarios_va, scenarios_te)

    # Isolation forest on LEGIT only.
    legit_mask = ytr == 0
    Xtr_legit = Xtr[legit_mask]
    iso = IsolationForest(
        n_estimators=200, max_samples=min(10000, len(Xtr_legit)), contamination="auto",
        random_state=SEED, n_jobs=4,
    )
    iso.fit(Xtr_legit)
    raw_legit = -iso.score_samples(Xtr_legit)
    cdf = np.quantile(np.sort(raw_legit), np.linspace(0.0, 1.0, 1001))
    an_tr = anomaly_scores(iso, Xtr, cdf)
    an_va = anomaly_scores(iso, Xva, cdf)
    an_te = anomaly_scores(iso, Xte, cdf)

    # Monotone constraints comparison.
    monotone_list = []
    increasing = {
        "velocity_1m", "velocity_10m", "velocity_1h", "amount_sum_10m", "amount_sum_24h",
        "near_limit_ratio", "small_txn_run_1h", "new_device", "sim_changed_72h", "night_hour",
        "cycle_len_2_4", "agent_float_delta_pct", "kyc_tier",
    }
    decreasing = {"account_age_days"}
    for name in FEATURE_NAMES:
        if name in increasing:
            monotone_list.append(1)
        elif name in decreasing:
            monotone_list.append(-1)
        else:
            monotone_list.append(0)

    def train_lgb(params: dict[str, Any], monotone: bool = False, Xtr_in: np.ndarray | None = None, ytr_in: np.ndarray | None = None, Xva_in: np.ndarray | None = None, yva_in: np.ndarray | None = None) -> Any:
        _Xtr = Xtr if Xtr_in is None else Xtr_in
        _ytr = ytr if ytr_in is None else ytr_in
        _Xva = Xva if Xva_in is None else Xva_in
        _yva = yva if yva_in is None else yva_in
        p = dict(params)
        p.update({
            "objective": "binary",
            "metric": "average_precision",
            "verbosity": -1,
            "seed": SEED,
            "num_threads": 4,
            "feature_pre_filter": False,
        })
        if monotone:
            p["monotone_constraints"] = monotone_list
        dtr = lgb.Dataset(_Xtr, label=_ytr, feature_name=list(FEATURE_NAMES))
        dva = lgb.Dataset(_Xva, label=_yva, feature_name=list(FEATURE_NAMES), reference=dtr)
        callbacks: list[Any] = [lgb.early_stopping(50, verbose=False), lgb.log_evaluation(0)]
        booster = lgb.train(
            p, dtr, num_boost_round=200, valid_sets=[dva], callbacks=callbacks,
        )
        return booster

    # Optuna.
    def objective(trial: Any) -> float:
        params = {
            "learning_rate": trial.suggest_float("learning_rate", 1e-3, 0.2, log=True),
            "num_leaves": trial.suggest_int("num_leaves", 15, 255),
            "min_child_samples": trial.suggest_int("min_child_samples", 20, 200),
            "feature_fraction": trial.suggest_float("feature_fraction", 0.6, 1.0),
            "bagging_fraction": trial.suggest_float("bagging_fraction", 0.6, 1.0),
            "bagging_freq": 1,
            "lambda_l1": trial.suggest_float("lambda_l1", 1e-8, 10.0, log=True),
            "lambda_l2": trial.suggest_float("lambda_l2", 1e-8, 10.0, log=True),
            "max_depth": trial.suggest_int("max_depth", -1, 12),
        }
        # Subsample training rows for the search to keep trials tractable.
        _rng = np.random.default_rng(SEED + trial.number)
        _cap = min(200000, len(Xtr))
        _idx = _rng.choice(len(Xtr), size=_cap, replace=False)
        booster = train_lgb(params, monotone=False, Xtr_in=Xtr[_idx], ytr_in=ytr[_idx])
        pred = booster.predict(Xva, num_iteration=booster.best_iteration)
        # PR-AUC
        from sklearn.metrics import average_precision_score
        return float(average_precision_score(yva, pred))

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=SEED))
    study.optimize(objective, timeout=args.time_budget, n_trials=8, show_progress_bar=False)
    best_params = study.best_params
    best_iteration = None

    # Monotone comparison models on validation.
    unconstrained = train_lgb(best_params, monotone=False)
    constrained = train_lgb(best_params, monotone=True)
    pu = unconstrained.predict(Xva, num_iteration=unconstrained.best_iteration)
    pc = constrained.predict(Xva, num_iteration=constrained.best_iteration)
    from sklearn.metrics import average_precision_score
    monotone_report = {
        "unconstrained_valid_pr_auc": float(average_precision_score(yva, pu)),
        "constrained_valid_pr_auc": float(average_precision_score(yva, pc)),
        "selected": "unconstrained" if average_precision_score(yva, pu) >= average_precision_score(yva, pc) else "constrained",
    }
    if monotone_report["selected"] == "constrained":
        final_booster = constrained
    else:
        final_booster = unconstrained
    best_iteration = final_booster.best_iteration

    # Calibration on valid.
    raw_va = final_booster.predict(Xva, num_iteration=best_iteration)
    calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    calibrator.fit(raw_va, yva)
    cal_va = calibrator.predict(raw_va)
    raw_te = final_booster.predict(Xte, num_iteration=best_iteration)
    cal_te = calibrator.predict(raw_te)
    raw_tr = final_booster.predict(Xtr, num_iteration=best_iteration)
    cal_tr = calibrator.predict(raw_tr)

    # Anomaly combine.
    fp_tr, fp_va, fp_te = cal_tr, cal_va, cal_te
    ms_tr = ml_scores(fp_tr, an_tr)
    ms_va = ml_scores(fp_va, an_va)
    ms_te = ml_scores(fp_te, an_te)

    # Metrics.
    metrics = {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "version": args.version,
        "seed": SEED,
        "split_sizes": {
            "train": int(len(train)), "valid": int(len(valid)), "test": int(len(test)),
            "train_fraud": int(ytr.sum()), "valid_fraud": int(yva.sum()), "test_fraud": int(yte.sum()),
        },
        "optuna": {
            "best_value": float(study.best_value),
            "best_params": best_params,
            "best_iteration": int(best_iteration),
            "n_trials": len(study.trials),
        },
        "monotone_comparison": monotone_report,
        "baseline_logistic": baseline_metrics,
        "lightgbm": {
            "valid_raw": evaluate_binary(yva, raw_va, amounts=amounts_va, scenarios=scenarios_va),
            "valid_calibrated": evaluate_binary(yva, cal_va, amounts=amounts_va, scenarios=scenarios_va),
            "test_raw": evaluate_binary(yte, raw_te, amounts=amounts_te, scenarios=scenarios_te),
            "test_calibrated": evaluate_binary(yte, cal_te, amounts=amounts_te, scenarios=scenarios_te),
            "ml_score_valid": evaluate_binary(yva, ms_va / 100.0, amounts=amounts_va, scenarios=scenarios_va),
            "ml_score_test": evaluate_binary(yte, ms_te / 100.0, amounts=amounts_te, scenarios=scenarios_te),
        },
        "anomaly": {
            "train_percentiles": {
                "p1": float(np.percentile(an_tr, 1)), "p50": float(np.percentile(an_tr, 50)), "p99": float(np.percentile(an_tr, 99)),
            }
        },
    }

    # SHAP top features.
    background_n = min(1000, len(Xtr))
    bg_idx = rng.choice(len(Xtr), size=background_n, replace=False)
    background = Xtr[bg_idx]
    explainer = shap.TreeExplainer(final_booster, data=background, feature_perturbation="interventional")
    shap_sample_n = min(2000, len(Xte))
    shap_idx = rng.choice(len(Xte), size=shap_sample_n, replace=False)
    shap_values = explainer.shap_values(Xte[shap_idx])
    if isinstance(shap_values, list):
        shap_values = shap_values[1]
    if shap_values.ndim == 3:
        shap_values = shap_values[:, :, 1]
    mean_abs = np.abs(shap_values).mean(axis=0)
    top_features = sorted(
            [{"feature": name, "mean_abs_shap": float(v)} for name, v in zip(FEATURE_NAMES, mean_abs, strict=False)],
            key=lambda x: float(cast(Any, x)["mean_abs_shap"]), reverse=True,
        )
    metrics["shap_top10"] = top_features[:10]

    # Save artifacts.
    final_booster.save_model(str(out / "model.txt"))
    joblib.dump(iso, out / "isoforest.joblib")
    (out / "isoforest_cdf.json").write_text(json.dumps([_native(x) for x in cdf.tolist()]), encoding="utf-8")
    (out / "calibrator.json").write_text(json.dumps({
        "x": [_native(x) for x in calibrator.X_thresholds_.tolist()],
        "y": [_native(x) for x in calibrator.y_thresholds_.tolist()],
    }), encoding="utf-8")
    (out / "features.json").write_text(json.dumps(feature_manifest(load_vocab()), indent=2), encoding="utf-8")

    # SHAP background parquet.
    import pyarrow as pa
    import pyarrow.parquet as pq
    bg_frame = pd.DataFrame(background, columns=list(FEATURE_NAMES))
    pq.write_table(pa.Table.from_pandas(bg_frame, preserve_index=False), out / "shap_background.parquet")

    # Training quantiles for PSI.
    train_quantiles = {
        "ml_score": np.quantile(ms_tr, np.linspace(0, 1, 21)).tolist(),
        "fraud_prob": np.quantile(fp_tr, np.linspace(0, 1, 21)).tolist(),
    }
    save_json(out / "training_quantiles.json", {k: [_native(x) for x in v] for k, v in train_quantiles.items()})

    # Golden predictions.
    golden_n = 200
    golden_idx = rng.choice(len(test), size=golden_n, replace=False)
    golden = []
    for idx in golden_idx:
        row = Xte[idx]
        fv = {name: _native(row[i]) for i, name in enumerate(FEATURE_NAMES)}
        fp_one = float(calibrator.predict(final_booster.predict(row.reshape(1, -1), num_iteration=best_iteration))[0])
        an_one = float(anomaly_scores(iso, row.reshape(1, -1), cdf)[0])
        ms_one = int(ml_scores(np.array([fp_one]), np.array([an_one]))[0])
        golden.append({
            "features": fv,
            "expected": {
                "fraud_prob": fp_one,
                "anomaly_score": an_one,
                "ml_score": ms_one,
                "model_version": args.version,
            },
        })
    save_json(out / "golden_predictions.json", golden)

    save_json(out / "metrics.json", metrics)

    # Model card.
    (out / "model_card.md").write_text(
        f"# Model card: {args.version}\n\n"
        f"Feature schema: {FEATURE_SCHEMA_VERSION}\n\n"
        f"Train/valid/test: {len(train)}/{len(valid)}/{len(test)}\n\n"
        f"Test PR-AUC: {metrics['lightgbm']['test_calibrated']['pr_auc']:.4f}\n"
        f"Test recall @1% FPR: {metrics['lightgbm']['test_calibrated']['fpr_1']['recall']:.4f}\n"
        f"ECE: {metrics['lightgbm']['test_calibrated']['ece']:.4f}\n",
        encoding="utf-8",
    )

    # Manifest hashes.
    manifest = {"version": args.version, "files": {}}
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest["files"][path.name] = _sha256(path)
    save_json(out / "manifest.json", manifest)

    print(json.dumps({"version": args.version, "test_pr_auc": metrics["lightgbm"]["test_calibrated"]["pr_auc"]}, indent=2))


if __name__ == "__main__":
    main()

