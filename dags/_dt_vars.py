"""Shared lookup helper for the `DT_Variables` Airflow Variable.

`DT_Variables` is a JSON object keyed by itemid (string), with values like::

    {"31322": {"value_type": 3, "key_": "ups.Temperature",
               "name": "UPS Temperature"}, ...}

This file is a helper module, not a workflow definition; the
underscore-prefixed filename keeps it out of file-discovery scans.
"""
from __future__ import annotations

import json


def get_dt_variables() -> dict:
    """Return the DT_Variables map. Empty dict on missing/unparseable
    Variable. Call from inside Airflow tasks (each call hits the
    metadata DB)."""
    from airflow.sdk import Variable

    try:
        return json.loads(Variable.get("DT_Variables"))
    except Exception as exc:
        print(f"DT_Variables not set/unparseable ({exc}); falling back to bare itemids")
        return {}


def itemid_label(itemid, dt_vars: dict | None = None) -> str:
    """Human-readable label for an itemid: ``"NNNN (key.name)"`` if
    DT_Variables knows it, otherwise just ``"NNNN"``.

    Pass ``dt_vars`` to amortize the lookup cost when labelling many
    itemids in a single task::

        dt = get_dt_variables()
        for iid in ids:
            print(itemid_label(iid, dt))
    """
    if dt_vars is None:
        dt_vars = get_dt_variables()
    entry = dt_vars.get(str(itemid))
    if not isinstance(entry, dict):
        return str(itemid)
    label = entry.get("key_") or entry.get("name")
    return f"{itemid} ({label})" if label else str(itemid)


def get_selector_params(itemid, variable_name: str) -> dict:
    """Resolve the per-itemid selector params from an `ewmaTunedParams*`
    Airflow Variable, falling back to `DEFAULT_SELECTOR_PARAMS` for any
    missing key.

    `variable_name` is the Airflow Variable to read — typically
    ``"ewmaTunedParamsHistory"`` or ``"ewmaTunedParamsTrend"``. The Variable
    is expected to be a JSON object keyed by itemid (string) whose values
    contain any subset of the selector param keys (alpha, alpha_slow,
    gamma, eta, switch_margin_hard, patience_hard, switch_margin_adapt,
    patience_adapt, min_dwell, window). Missing/unparseable Variable
    silently degrades to defaults.
    """
    from airflow.sdk import Variable

    from _selector import merge_params

    try:
        tuned = json.loads(Variable.get(variable_name))
    except Exception as exc:
        print(f"{variable_name} not set/unparseable ({exc}); using selector defaults")
        return merge_params(None)

    entry = tuned.get(str(itemid)) or tuned.get(itemid) or {}
    if not isinstance(entry, dict):
        return merge_params(None)
    return merge_params(entry)
