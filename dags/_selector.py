"""Streaming port of the three-tier selector from
`run_all_variables_tuned.py:475-592`.

The reference loops a full numpy series in memory; this module exposes the
per-step body of `hard_selector` and `adaptive_selector` (plus the stateless
`_softmax_weights`) as resumable functions that mutate an externally
persisted state dict. Math is character-for-character identical to the
reference; only the scaffolding is restructured for one-step-at-a-time
invocation against `HISTORY_ERROR_EWMA` / `TRENDS_ERROR_EWMA`.

EWMA scores themselves are **not** maintained here — the message processor
DAGs already update them in `HISTORY_ERROR_EWMA.FINAL_EWMA` (mirror of the
reference's `_ewma_update` at `run_all_variables_tuned.py:475-490`). The
selector reads them.

Underscore filename keeps this out of Airflow's DAG discovery scan.
"""
from __future__ import annotations

import math
from typing import Iterable


# Canonical model list for the 5-model ensemble. Imported by every selector,
# stream, and message-processor DAG so a future model swap stays single-source.
# `*_PRED_*` and `*_ERROR_EWMA_*` tables tag rows with these exact strings.
PRED_MODELS = ["SARIMAX", "LightGBM", "LSTM", "GRU", "PatchTST"]


# Defaults from run_all_variables_tuned.py:128-139 (FIXED_PARAMS — empirical
# medians of the per-variable Adaptive-optimal configs from the offline
# sweep). Used when an itemid has no per-itemid override in the
# `ewmaTunedParams*` Airflow Variable.
DEFAULT_SELECTOR_PARAMS = {
    "alpha":               0.40,
    "alpha_slow":          0.005,
    "gamma":               0.70,
    "eta":                 4.0,
    "switch_margin_hard":  0.01,
    "patience_hard":       2,
    "switch_margin_adapt": 0.02,
    "patience_adapt":      2,
    "min_dwell":           12,
    "window":              24,
}


def _is_finite(x) -> bool:
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x)


def softmax_weights(scores: dict, model_names: Iterable[str],
                    eta: float, floor: float = 1e-6) -> dict:
    """Mirror of `_softmax_weights` at run_all_variables_tuned.py:493-504.

    Lower score → higher weight. Models with non-finite scores get weight 0;
    if every score is non-finite we fall back to uniform.
    """
    model_names = list(model_names)
    valid = {m: float(s) for m, s in scores.items() if _is_finite(s)}
    if not valid:
        u = 1.0 / len(model_names)
        return {m: u for m in model_names}

    s_min = min(valid.values())
    logits = {m: -eta * (s - s_min) for m, s in valid.items()}
    l_max = max(logits.values())
    raw = {m: math.exp(l - l_max) for m, l in logits.items()}
    total = sum(raw.values())
    floored = {m: max(v / total, floor) for m, v in raw.items()}
    s2 = sum(floored.values())
    norm = {m: v / s2 for m, v in floored.items()}
    out = {m: 0.0 for m in model_names}
    out.update(norm)
    return out


def make_hard_state(initial_active: str) -> dict:
    """Fresh state for the hard-selector machine. `last_switch_clock=0` is
    fine as a sentinel; the first switch is unblocked once min_dwell steps
    of CLOCK have elapsed since 0, which is always true for real CLOCKs."""
    return {
        "active": initial_active,
        "pending": None,
        "streak": 0,
        "last_switch_clock": 0,
    }


def hard_step(state: dict, scores: dict, clock: int,
              switch_margin: float, patience: int, min_dwell: int) -> str:
    """Advance the hard-selector state by one step. Mirror of the per-`t`
    body of `hard_selector` at run_all_variables_tuned.py:517-533.

    - `state`: mutated in place (see `make_hard_state` for shape).
    - `scores`: {model_name: FINAL_EWMA} — typically the row from
      `HISTORY_ERROR_EWMA` at this CLOCK, one entry per model.
    - `clock`: integer CLOCK of the row being evaluated. Used for the
      `(clock - last_switch_clock) >= min_dwell` cooldown. The reference
      uses step index `t`; here we use CLOCK directly. For history (1-min
      cadence), `min_dwell=12` means 12 minutes; for trend (1-hour cadence)
      it means 12 hours — caller must scale `min_dwell` to the desired
      number of steps in CLOCK units (multiply by 60 / 3600 respectively).

    Returns the active model name after the step (`state["active"]`).
    """
    active = state["active"]
    pending = state["pending"]
    streak = state["streak"]
    last_sw = state["last_switch_clock"]

    valid = {m: float(s) for m, s in scores.items() if _is_finite(s)}

    # If the currently-active model has no valid score yet, jump to the
    # best available (line 519-520 of the reference).
    if active not in valid and valid:
        active = min(valid, key=valid.get)

    best = min(valid, key=valid.get) if valid else active
    cs = valid.get(active)
    bs = valid.get(best)

    can_switch = (
        cs is not None and bs is not None
        and bs <= cs * (1.0 - switch_margin)
    )

    if best != active and can_switch:
        if pending == best:
            streak = streak + 1
        else:
            streak = 1
        pending = best
    else:
        pending = None
        streak = 0

    if pending is not None and streak >= patience and (clock - last_sw) >= min_dwell:
        active = pending
        last_sw = clock
        pending = None
        streak = 0

    state["active"] = active
    state["pending"] = pending
    state["streak"] = streak
    state["last_switch_clock"] = last_sw
    return active


def make_adaptive_state(initial_active: str = "Hard") -> dict:
    """Fresh state for the adaptive (Hard-vs-Soft) meta-selector."""
    if initial_active not in ("Hard", "Soft"):
        raise ValueError(f"initial_active must be 'Hard' or 'Soft', got {initial_active!r}")
    return {
        "active": initial_active,
        "pending": None,
        "streak": 0,
        "last_switch_clock": 0,
    }


def adaptive_step(state: dict, hard_mae: float, soft_mae: float, clock: int,
                  switch_margin: float, patience: int, min_dwell: int) -> str:
    """Advance the adaptive meta-selector by one step. Mirror of the per-`t`
    body of `adaptive_selector` at run_all_variables_tuned.py:576-590.

    `hard_mae` / `soft_mae` are the rolling MAEs over the last `window`
    CLOCKs of the Hard- vs Soft-selector predictions against the actuals
    (computed by the caller from `HISTORY_SELECTION_LOG`). Either may be
    NaN early in the stream; the meta-selector simply doesn't switch until
    both are finite.

    Returns the active method name (`"Hard"` or `"Soft"`) after the step.
    """
    active = state["active"]
    pending = state["pending"]
    streak = state["streak"]
    last_sw = state["last_switch_clock"]

    challenger = None
    if _is_finite(hard_mae) and _is_finite(soft_mae):
        if active == "Hard" and soft_mae <= hard_mae * (1.0 - switch_margin):
            challenger = "Soft"
        elif active == "Soft" and hard_mae <= soft_mae * (1.0 - switch_margin):
            challenger = "Hard"

    if challenger is not None:
        if pending == challenger:
            streak = streak + 1
        else:
            streak = 1
        pending = challenger
    else:
        pending = None
        streak = 0

    if pending is not None and streak >= patience and (clock - last_sw) >= min_dwell:
        active = pending
        last_sw = clock
        pending = None
        streak = 0

    state["active"] = active
    state["pending"] = pending
    state["streak"] = streak
    state["last_switch_clock"] = last_sw
    return active


def merge_params(itemid_overrides: dict | None) -> dict:
    """Layer per-itemid overrides on top of `DEFAULT_SELECTOR_PARAMS`.
    Unknown keys in the override are kept verbatim (forward-compat); all
    DEFAULT keys are guaranteed to be present in the result."""
    out = dict(DEFAULT_SELECTOR_PARAMS)
    if itemid_overrides:
        out.update(itemid_overrides)
    return out
