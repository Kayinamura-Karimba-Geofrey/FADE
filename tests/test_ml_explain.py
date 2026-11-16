import numpy as np

from ml.explain import additivity_holds, top_contributions


def test_top_contributions_sorted() -> None:
    names = ["a", "b", "c"]
    values = np.array([1.0, 2.0, 3.0])
    shap_row = np.array([0.1, -0.5, 0.2])
    out = top_contributions(names, values, shap_row, k=2)
    assert [c["feature"] for c in out] == ["b", "c"]
    assert out[0]["direction"] == "decreases"


def test_additivity_holds() -> None:
    assert additivity_holds(0.5, np.array([0.1, -0.2]), 0.4)
    assert not additivity_holds(0.5, np.array([0.1, -0.2]), 0.9)

