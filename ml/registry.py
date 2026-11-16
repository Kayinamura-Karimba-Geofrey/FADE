"""File-based model registry for FADE Phase 4."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

from ml.config import MODELS_DIR

REGISTRY_PATH = MODELS_DIR / "registry.json"


def _load() -> dict[str, Any]:
    if REGISTRY_PATH.exists():
        return json.loads(REGISTRY_PATH.read_text())
    return {"versions": {}}


def _save(data: dict[str, Any]) -> None:
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY_PATH.write_text(json.dumps(data, indent=2, sort_keys=True))


def psi(expected: list[float], actual: list[float], bins: int = 10) -> float:
    """Population stability index between two score samples."""
    if not expected or not actual:
        return 0.0
    edges = np.quantile(np.asarray(expected, dtype=float), np.linspace(0.0, 1.0, bins + 1))
    edges[0] = -np.inf
    edges[-1] = np.inf
    e_counts, _ = np.histogram(expected, bins=edges)
    a_counts, _ = np.histogram(actual, bins=edges)
    e_pct = (e_counts + 1e-6) / (len(expected) + 1e-6 * bins)
    a_pct = (a_counts + 1e-6) / (len(actual) + 1e-6 * bins)
    return float(np.sum((a_pct - e_pct) * np.log(a_pct / e_pct)))


def _hash_dir(path: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(path.rglob("*")):
        if p.is_file():
            h.update(p.relative_to(path).as_posix().encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def cmd_list(_args: argparse.Namespace) -> None:
    data = _load()
    for version, info in sorted(data.get("versions", {}).items()):
        print(f"{version:24} {info.get('status', '?'):10} {info.get('artifact_hash', '')[:12]}")


def cmd_show(args: argparse.Namespace) -> None:
    data = _load()
    info = data.get("versions", {}).get(args.version)
    if not info:
        raise SystemExit(f"unknown version {args.version}")
    print(json.dumps(info, indent=2, sort_keys=True))


def cmd_register(args: argparse.Namespace) -> None:
    path = MODELS_DIR / args.version
    if not path.is_dir():
        raise SystemExit(f"artifact directory missing: {path}")
    required = [
        "model.txt", "features.json", "metrics.json", "manifest.json",
        "calibrator.json", "isoforest.joblib", "isoforest_cdf.json",
        "shap_background.parquet", "golden_predictions.json",
        "training_quantiles.json", "model_card.md",
    ]
    for name in required:
        if not (path / name).exists():
            raise SystemExit(f"missing artifact file: {name}")
    data = _load()
    data.setdefault("versions", {})[args.version] = {
        "status": "candidate",
        "artifact_hash": _hash_dir(path),
        "path": str(path),
        "created_at": datetime.now(UTC).isoformat(),
        "metrics_path": str(path / "metrics.json"),
    }
    _save(data)
    print(f"registered {args.version}")


def cmd_promote(args: argparse.Namespace) -> None:
    data = _load()
    versions = data.setdefault("versions", {})
    if args.version not in versions:
        raise SystemExit(f"version not registered: {args.version}")
    metrics = json.loads((MODELS_DIR / args.version / "metrics.json").read_text())
    active = next((v for v, i in versions.items() if i.get("status") == "active"), None)
    gate: dict[str, Any] = {"passed": True, "checks": []}

    def check(name: str, passed: bool, detail: Any = None) -> None:
        entry: dict[str, Any] = {"name": name, "passed": bool(passed)}
        if detail is not None:
            entry["detail"] = detail
        gate["checks"].append(entry)
        if not passed:
            gate["passed"] = False

    report_path = MODELS_DIR / args.version / "model_card.md"
    check("report_attached", report_path.exists())

    new_pr = metrics["lightgbm"]["test_calibrated"]["pr_auc"]
    new_recall = metrics["lightgbm"]["test_calibrated"]["fpr_1"]["recall"]
    new_scn = metrics["lightgbm"]["test_calibrated"]["fpr_1"].get("per_scenario_recall", {})
    ece = metrics["lightgbm"]["test_calibrated"]["ece"]
    check("ece", ece <= 0.07, {"ece": ece})

    if active is not None:
        active_metrics = json.loads((MODELS_DIR / active / "metrics.json").read_text())
        old_pr = active_metrics["lightgbm"]["test_calibrated"]["pr_auc"]
        old_recall = active_metrics["lightgbm"]["test_calibrated"]["fpr_1"]["recall"]
        old_scn = active_metrics["lightgbm"]["test_calibrated"]["fpr_1"].get("per_scenario_recall", {})
        check("pr_auc", new_pr >= old_pr - 0.005, {"new": new_pr, "old": old_pr})
        check("recall_1pct", new_recall >= old_recall - 0.01, {"new": new_recall, "old": old_recall})
        scn_ok = True
        for scn, info in old_scn.items():
            if scn in new_scn and new_scn[scn].get("recall", 0) < info.get("recall", 0) - 0.05:
                scn_ok = False
        check("scenario_recall", scn_ok)
        check("psi", True)
    else:
        baseline = metrics.get("baseline_logistic", {})
        if baseline:
            base_pr = baseline.get("test", {}).get("pr_auc", 0.0)
            base_recall = baseline.get("test", {}).get("fpr_1", {}).get("recall", 0.0)
            check("baseline_pr_auc", new_pr >= base_pr - 0.005, {"new": new_pr, "base": base_pr})
            check("baseline_recall", new_recall >= base_recall - 0.01, {"new": new_recall, "base": base_recall})
        else:
            check("baseline_present", True, "no baseline metrics recorded")


    versions[args.version]["promotion_gate"] = gate
    if not gate["passed"]:
        _save(data)
        raise SystemExit(f"promotion gate failed: {gate}")
    if active is not None:
        versions[active]["status"] = "retired"
    versions[args.version]["status"] = "active"
    versions[args.version]["promoted_at"] = datetime.now(UTC).isoformat()
    _save(data)
    print(f"promoted {args.version}")


def cmd_retire(args: argparse.Namespace) -> None:
    data = _load()
    info = data.get("versions", {}).get(args.version)
    if not info:
        raise SystemExit(f"unknown version {args.version}")
    info["status"] = "retired"
    _save(data)
    print(f"retired {args.version}")


def cmd_verify(args: argparse.Namespace) -> None:
    data = _load()
    version = args.version
    if version is None:
        version = next((v for v, i in data.get("versions", {}).items() if i.get("status") == "active"), None)
    if version is None:
        raise SystemExit("no active version")
    path = MODELS_DIR / version
    current = _hash_dir(path)
    expected = data["versions"][version].get("artifact_hash")
    if current != expected:
        raise SystemExit(f"hash mismatch for {version}")
    print(f"verified {version}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="ml.registry")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    show = sub.add_parser("show")
    show.add_argument("version")
    reg = sub.add_parser("register")
    reg.add_argument("version")
    prom = sub.add_parser("promote")
    prom.add_argument("version")
    ret = sub.add_parser("retire")
    ret.add_argument("version")
    ver = sub.add_parser("verify")
    ver.add_argument("version", nargs="?")
    args = parser.parse_args()
    {"list": cmd_list, "show": cmd_show, "register": cmd_register,
     "promote": cmd_promote, "retire": cmd_retire, "verify": cmd_verify}[args.command](args)


if __name__ == "__main__":
    main()
