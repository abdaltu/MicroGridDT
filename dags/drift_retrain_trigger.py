"""Drift-triggered model retraining.

Every 6 hours, scans the per-mode error-EWMA tables for both pipelines
(trend + history) and looks for itemids whose prediction error has drifted
relative to their value-EWMA magnitude. For each (pipeline, mode) pair
that has drifted itemids, triggers that mode's training DAG with an
`itemids_override` conf so only those itemids are retrained.

Drift signal — for each (itemid, mode):
    worst_error = MAX over models of latest FINAL_EWMA in *_ERROR_EWMA_<MODE>
    value_mag   = abs(latest FINAL_EWMA in *_VALUE_EWMA)
    ratio       = worst_error / max(epsilon, value_mag)
    drifted     = ratio > ratio_threshold

Defaults: ratio_threshold=0.15, epsilon=1e-6. Override via Airflow Variable
`driftRetrainConfig` (JSON object).

Which modes are scanned per pipeline is governed by the same enabled-modes
Variables the tick DAGs use:
  - `enabledModesHistory` (default ["recursive","direct60"])
  - `enabledModesTrend`   (default ["recursive","direct24"])

Pipelines / triggers (per enabled mode):
  trend  : reads TRENDS_VALUE_EWMA + TRENDS_ERROR_EWMA_<MODE>;
           triggers train_models_recursive_trend or train_models_direct24_trend.
  history: reads HISTORY_VALUE_EWMA + HISTORY_ERROR_EWMA_<MODE>;
           triggers train_models_recursive_history or train_models_direct60_history.
"""
from __future__ import annotations

import json
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

from _db import get_db_config
from _dt_vars import get_dt_variables, itemid_label
from _enabled import resolve_enabled_set
from _modes import (
    DEFAULT_ENABLED_HISTORY_MODES,
    DEFAULT_ENABLED_TREND_MODES,
    HISTORY_MODES,
    TREND_MODES,
)


DRIFT_CONFIG_VAR = "driftRetrainConfig"
DEFAULT_DRIFT_CONFIG = {
    "ratio_threshold": 0.15,
    "epsilon": 1e-6,
}

# Per-pipeline drift config. Tables + trainer DAG IDs all come from the
# central `_modes` registry — this file only owns the value-EWMA table
# name (mode-independent) and the enabled-modes Variable name.
PIPELINES = (
    {
        "name":                  "trend",
        "value_ewma_table":      "TRENDS_VALUE_EWMA",
        "modes_registry":        TREND_MODES,
        "enabled_modes_var":     "enabledModesTrend",
        "default_enabled_modes": DEFAULT_ENABLED_TREND_MODES,
    },
    {
        "name":                  "history",
        "value_ewma_table":      "HISTORY_VALUE_EWMA",
        "modes_registry":        HISTORY_MODES,
        "enabled_modes_var":     "enabledModesHistory",
        "default_enabled_modes": DEFAULT_ENABLED_HISTORY_MODES,
    },
)


def _load_drift_config() -> dict:
    try:
        cfg = json.loads(Variable.get(DRIFT_CONFIG_VAR))
    except Exception as exc:
        print(f"{DRIFT_CONFIG_VAR} not set/unparseable ({exc}); using defaults {DEFAULT_DRIFT_CONFIG}")
        return DEFAULT_DRIFT_CONFIG
    out = dict(DEFAULT_DRIFT_CONFIG)
    out.update(cfg)
    return out


def _detect_drift(pipeline_cfg: dict, mode: str, drift_cfg: dict,
                  itemids: list[int], dt_vars: dict) -> list[int]:
    """Return the subset of itemids whose worst-model error EWMA for this
    `mode` has drifted relative to their value EWMA magnitude. Numeric
    value_types only (skips char). Itemids missing either EWMA table entry
    are skipped silently — drift can only be assessed once both streams
    have data."""
    import mysql.connector

    ratio_threshold = float(drift_cfg.get("ratio_threshold", DEFAULT_DRIFT_CONFIG["ratio_threshold"]))
    epsilon         = float(drift_cfg.get("epsilon",         DEFAULT_DRIFT_CONFIG["epsilon"]))
    error_table     = pipeline_cfg["modes_registry"][mode].error_ewma_table
    value_table     = pipeline_cfg["value_ewma_table"]

    drifted: list[int] = []
    conn = mysql.connector.connect(**get_db_config())
    cursor = conn.cursor()
    try:
        for itemid in itemids:
            vt = (dt_vars.get(str(itemid)) or {}).get("value_type")
            if vt not in (0, 3):
                continue   # skip char-typed and unknown

            # Latest value-EWMA (mode-independent; one table per pipeline)
            cursor.execute(
                f"SELECT FINAL_EWMA FROM {value_table} "
                f"WHERE ITEMID=%s ORDER BY CLOCK DESC LIMIT 1",
                (itemid,),
            )
            v_row = cursor.fetchone()
            if v_row is None:
                continue
            value_mag = abs(float(v_row[0]))
            denom = max(epsilon, value_mag)

            # Latest error-EWMA per model from THIS mode's error table —
            # pick worst (highest)
            cursor.execute(
                f"SELECT t.MODEL, t.FINAL_EWMA FROM ("
                f"  SELECT MODEL, FINAL_EWMA, "
                f"         ROW_NUMBER() OVER (PARTITION BY MODEL ORDER BY CLOCK DESC) AS rn "
                f"  FROM {error_table} WHERE ITEMID=%s"
                f") t WHERE t.rn = 1",
                (itemid,),
            )
            latest_per_model = {model: float(val) for model, val in cursor.fetchall()}
            if not latest_per_model:
                continue

            worst_model, worst_err = max(latest_per_model.items(), key=lambda kv: kv[1])
            ratio = worst_err / denom

            if ratio > ratio_threshold:
                drifted.append(itemid)
                print(
                    f"  [{pipeline_cfg['name']}/{mode}] {itemid_label(itemid, dt_vars)}: "
                    f"DRIFT — worst {worst_model} err_ewma={worst_err:.4f}, "
                    f"value_ewma={value_mag:.4f}, ratio={ratio:.3f} > {ratio_threshold}"
                )
            else:
                print(
                    f"  [{pipeline_cfg['name']}/{mode}] {itemid_label(itemid, dt_vars)}: "
                    f"ok (worst {worst_model} ratio={ratio:.3f})"
                )
    finally:
        cursor.close()
        conn.close()
    return drifted


def _resolve_enabled_modes(pipeline_cfg: dict) -> list[str]:
    """Thin wrapper around the shared `_enabled.resolve_enabled_set`
    that pulls the right keys out of `pipeline_cfg`."""
    return resolve_enabled_set(
        pipeline_cfg["enabled_modes_var"],
        pipeline_cfg["default_enabled_modes"],
        pipeline_cfg["modes_registry"].keys(),
    )


@dag(
    dag_id="drift_retrain_trigger",
    start_date=datetime(2024, 6, 1),
    schedule="0 */6 * * *",   # every 6 hours
    catchup=False,
    tags=["training", "drift"],
    max_active_runs=1,
    doc_md=__doc__,
)
def drift_retrain_trigger():
    @task
    def detect_all() -> list[dict]:
        """Scan both pipelines and return TriggerDagRunOperator kwargs for
        each that has drift. Empty list = nothing to retrain."""
        drift_cfg = _load_drift_config()
        try:
            itemids = sorted(int(x) for x in json.loads(Variable.get("DT_Var_Ids")))
        except Exception as exc:
            print(f"DT_Var_Ids unparseable ({exc}); aborting drift check.")
            return []

        # Restrict the drift scan to the actively-modeled subset. Itemids
        # outside `activePipeLine` aren't trained or predicted today, so
        # there's no model to retrain even if drift were detected — checking
        # them just wastes scheduler cycles. If `activePipeLine` is unset or
        # empty, fall back to the full DT_Var_Ids set (preserves the legacy
        # "scan everything" behaviour).
        try:
            active = frozenset(int(x) for x in json.loads(Variable.get("activePipeLine")))
        except Exception as exc:
            print(f"activePipeLine unset/unparseable ({exc}); scanning all DT_Var_Ids")
            active = None
        if active:
            before = len(itemids)
            itemids = [i for i in itemids if i in active]
            print(
                f"activePipeLine restricts drift scan: {before} -> {len(itemids)} itemids "
                f"({sorted(active)})"
            )

        dt_vars = get_dt_variables()
        print(f"Drift config: {drift_cfg}; checking {len(itemids)} itemids per pipeline")

        triggers: list[dict] = []
        for pipeline_cfg in PIPELINES:
            enabled_modes = _resolve_enabled_modes(pipeline_cfg)
            print(
                f"=== Pipeline: {pipeline_cfg['name']} "
                f"(enabled modes: {enabled_modes}) ==="
            )
            for mode in enabled_modes:
                trainer_dag_id = pipeline_cfg["modes_registry"][mode].trainer_dag_id
                drifted = _detect_drift(
                    pipeline_cfg, mode, drift_cfg, itemids, dt_vars,
                )
                if not drifted:
                    print(f"  [{mode}] no drift detected; not triggering {trainer_dag_id}")
                    continue
                print(
                    f"  [{mode}] triggering {trainer_dag_id} for "
                    f"{len(drifted)} drifted itemid(s): {drifted}"
                )
                triggers.append({
                    "trigger_dag_id": trainer_dag_id,
                    "conf": {"itemids_override": drifted},
                })
        return triggers

    triggers = detect_all()

    # expand_kwargs over the per-pipeline trigger entries. If `triggers` is
    # empty, no mapped tasks run (graceful no-op).
    TriggerDagRunOperator.partial(
        task_id="trigger_retrain",
    ).expand_kwargs(triggers)


drift_retrain_trigger()
