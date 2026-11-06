"""Shared helpers for the predictions stream DAGs (`predict_stream_history`,
`predict_stream_trend`).

The selector DAGs publish per-actual Hard/Soft/Adaptive decisions into
`*_SELECTION_LOG`. This module projects the *latest* such decision onto
every future CLOCK that already has per-model predictions in the active
prediction table, materializing the stream into `*_PRED_SELECTED`.

Soft-weight renormalization mirrors `select_adaptive_history.py:340-346`
exactly — any divergence would create inconsistent values at a CLOCK that
both the selector and the stream eventually write.

This file is a helper module, not a workflow definition; the
underscore-prefixed filename keeps it out of file-discovery scans.
"""
from __future__ import annotations

import json
import math


from _selector import PRED_MODELS  # noqa: F401 — re-export for back-compat


def load_latest_decision(cursor, log_table: str, itemid: int) -> dict | None:
    """Latest closed-loop decision for an itemid from ``<log_table>``.

    Returns ``{decision_clock, hard_model, soft_weights, method}`` or None
    if the itemid has no row yet (cold start).
    """
    cursor.execute(
        f"SELECT CLOCK, HARD_MODEL, SOFT_WEIGHTS, METHOD "
        f"FROM {log_table} "
        f"WHERE ITEMID=%s ORDER BY CLOCK DESC LIMIT 1",
        (itemid,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    decision_clock, hard_model, soft_weights_raw, method = row
    try:
        soft_weights = (
            json.loads(soft_weights_raw)
            if isinstance(soft_weights_raw, (str, bytes, bytearray))
            else (soft_weights_raw or {})
        )
    except (TypeError, ValueError):
        # Defensive: corrupted JSON in one row shouldn't kill the whole tick.
        print(f"  [itemid={itemid}] SOFT_WEIGHTS parse failed; treating as empty")
        soft_weights = {}
    return {
        "decision_clock": int(decision_clock),
        "hard_model": hard_model,
        "soft_weights": soft_weights,
        "method": method,
    }


def load_future_predictions(cursor, pred_table: str, itemid: int,
                            after_clock: int, value_column: str = "VALUE",
                            ) -> dict[int, dict[str, float]]:
    """All future per-model predictions for an itemid past ``after_clock``.

    Trend prediction tables store the prediction in ``VALUE_AVG`` instead
    of ``VALUE``; the caller passes ``value_column`` accordingly.

    Returns ``{clock: {model: value}}``. Missing (clock, model) pairs are
    simply absent from the inner dict; consumers handle that via
    ``project_row``.
    """
    cursor.execute(
        f"SELECT CLOCK, MODEL, {value_column} FROM {pred_table} "
        f"WHERE ITEMID=%s AND CLOCK > %s",
        (itemid, after_clock),
    )
    out: dict[int, dict[str, float]] = {}
    for clock, model, value in cursor.fetchall():
        if value is None:
            continue
        try:
            out.setdefault(int(clock), {})[model] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def project_row(decision: dict, preds: dict[str, float]
                ) -> tuple[float | None, float | None, float | None]:
    """Project a frozen selector decision onto one CLOCK's per-model preds.

    Returns ``(hard_value, soft_value, adaptive_value)``.

    - HARD_VALUE: ``preds[hard_model]`` when present and finite, else None.
    - SOFT_VALUE: renormalized weighted mean over models present in both
      ``preds`` and ``soft_weights`` with finite values. None if no such
      model exists or the surviving weights sum to zero.
    - ADAPTIVE_VALUE: HARD_VALUE when ``method=='Hard'`` else SOFT_VALUE
      (may itself be None).

    Mirrors `select_adaptive_history.py:338-357` so the stream DAG and
    SELECTION_LOG agree on values at any CLOCK they both write.
    """
    hard_model = decision["hard_model"]
    soft_weights = decision["soft_weights"] or {}
    method = decision["method"]

    hv_raw = preds.get(hard_model)
    hv: float | None
    if hv_raw is None or not math.isfinite(hv_raw):
        hv = None
    else:
        hv = float(hv_raw)

    finite_pairs: list[tuple[float, float]] = []
    for m in PRED_MODELS:
        if m not in preds or m not in soft_weights:
            continue
        try:
            w = float(soft_weights[m])
            p = float(preds[m])
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(w) and math.isfinite(p)):
            continue
        finite_pairs.append((w, p))

    sv: float | None
    if not finite_pairs:
        sv = None
    else:
        wsum = sum(w for w, _ in finite_pairs)
        sv = (sum(w * p for w, p in finite_pairs) / wsum) if wsum > 0 else None

    av = hv if method == "Hard" else sv
    return hv, sv, av


def pipeline_itemids(active_pipeline_var: str = "activePipeLine") -> list[int]:
    """Resolved active itemids; empty list if the Variable is missing."""
    from airflow.sdk import Variable

    try:
        return sorted(int(x) for x in json.loads(Variable.get(active_pipeline_var)))
    except Exception as exc:
        print(
            f"{active_pipeline_var} not set/unparseable ({exc}); "
            "stream has no itemids to process this tick."
        )
        return []
