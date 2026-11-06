"""Adaptive model selector for the trend (1-hour) pipeline.

Per-mode factory: registers one `@dag` instance per mode in
`enabledModesTrend` (default ``["recursive","direct24"]``). dag_ids are
`select_adaptive_trend_recursive` and `select_adaptive_trend_direct24`.

Mirror of `select_adaptive_history` against `TRENDS_*` tables. Each
per-mode DAG runs at the top of every hour, advances the three-tier
selector per ITEMID using `TRENDS_ERROR_EWMA_<MODE>` as the score source,
and writes decisions to `TRENDS_SELECTION_LOG_<MODE>` /
`TRENDS_SELECTION_STATE_<MODE>`. Triggers `predict_stream_trend_<mode>`.

Differences vs. history selector:
  - All `HISTORY_*` table names → `TRENDS_*`.
  - Per-itemid params come from the `ewmaTunedParamsTrend` Variable
    (same Variable the trend message processor reads).
  - `STEP_SECONDS = 3600` so the reference's "min_dwell in steps" maps
    to "min_dwell in hours" against unix-second CLOCKs.
  - Predictions aligned to the hour: `clock_hr = (clock // 3600) * 3600`.

Schema for `TRENDS_SELECTION_STATE_*` and `TRENDS_SELECTION_LOG_*` mirrors
the original single-mode tables; see `migrate_dualmode.sql`.
"""
from __future__ import annotations

import json
import math
from collections import deque
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

from _db import get_db_config
from _dt_vars import get_dt_variables, get_selector_params, itemid_label
from _enabled import resolve_enabled_set
from _modes import DEFAULT_ENABLED_TREND_MODES, TREND_MODES
from _selector import (
    PRED_MODELS,
    adaptive_step,
    hard_step,
    make_adaptive_state,
    make_hard_state,
    softmax_weights,
)


INITIAL_HARD_ACTIVE = PRED_MODELS[0]

ACTIVE_PIPELINE_VAR = "activePipeLine"
SELECTOR_PARAMS_VAR = "ewmaTunedParamsTrend"

ENABLED_MODES_VAR = "enabledModesTrend"
ENABLED_MODES = resolve_enabled_set(
    ENABLED_MODES_VAR, DEFAULT_ENABLED_TREND_MODES, TREND_MODES,
)

# 1 trend step = 1 hour. CLOCK is unix seconds, so min_dwell (in steps)
# scales by 3600.
STEP_SECONDS = 3600

_VALID_VALUE_TYPES = {0, 3}


def _pipeline_itemids() -> list[int]:
    try:
        return sorted(int(x) for x in json.loads(Variable.get(ACTIVE_PIPELINE_VAR)))
    except Exception as exc:
        print(
            f"{ACTIVE_PIPELINE_VAR} not set/unparseable ({exc}); "
            "selector has no itemids to process this tick."
        )
        return []


def _ensure_state_row(cursor, state_table: str, itemid: int) -> None:
    cursor.execute(
        f"INSERT IGNORE INTO {state_table} "
        f"(ITEMID, LAST_PROCESSED_CLOCK, HARD_ACTIVE, HARD_PENDING, HARD_STREAK, "
        f" HARD_LAST_SWITCH_CLOCK, METHOD_ACTIVE, METHOD_PENDING, METHOD_STREAK, "
        f" METHOD_LAST_SWITCH_CLOCK) "
        f"VALUES (%s, 0, %s, NULL, 0, 0, 'Hard', NULL, 0, 0)",
        (itemid, INITIAL_HARD_ACTIVE),
    )


def _load_state(cursor, state_table: str, itemid: int) -> dict:
    cursor.execute(
        f"SELECT LAST_PROCESSED_CLOCK, HARD_ACTIVE, HARD_PENDING, HARD_STREAK, "
        f"       HARD_LAST_SWITCH_CLOCK, METHOD_ACTIVE, METHOD_PENDING, "
        f"       METHOD_STREAK, METHOD_LAST_SWITCH_CLOCK "
        f"FROM {state_table} WHERE ITEMID=%s",
        (itemid,),
    )
    row = cursor.fetchone()
    return {
        "last_processed_clock": int(row[0]),
        "hard": {
            "active":            row[1],
            "pending":           row[2],
            "streak":            int(row[3]),
            "last_switch_clock": int(row[4]),
        },
        "adaptive": {
            "active":            row[5],
            "pending":           row[6],
            "streak":            int(row[7]),
            "last_switch_clock": int(row[8]),
        },
    }


def _save_state(cursor, state_table: str, itemid: int, state: dict) -> None:
    cursor.execute(
        f"UPDATE {state_table} SET "
        f"  LAST_PROCESSED_CLOCK=%s, "
        f"  HARD_ACTIVE=%s, HARD_PENDING=%s, HARD_STREAK=%s, HARD_LAST_SWITCH_CLOCK=%s, "
        f"  METHOD_ACTIVE=%s, METHOD_PENDING=%s, METHOD_STREAK=%s, METHOD_LAST_SWITCH_CLOCK=%s "
        f"WHERE ITEMID=%s",
        (
            state["last_processed_clock"],
            state["hard"]["active"], state["hard"]["pending"],
            state["hard"]["streak"], state["hard"]["last_switch_clock"],
            state["adaptive"]["active"], state["adaptive"]["pending"],
            state["adaptive"]["streak"], state["adaptive"]["last_switch_clock"],
            itemid,
        ),
    )


def _load_seed_window(cursor, log_table: str, itemid: int, window: int) -> deque:
    seed = deque(maxlen=window)
    if window <= 1:
        return seed
    cursor.execute(
        f"SELECT ACTUAL, HARD_VALUE, SOFT_VALUE FROM {log_table} "
        f"WHERE ITEMID=%s ORDER BY CLOCK DESC LIMIT %s",
        (itemid, window - 1),
    )
    rows = cursor.fetchall()
    for actual, hv, sv in reversed(rows):
        seed.append((
            None if actual is None else float(actual),
            None if hv     is None else float(hv),
            None if sv     is None else float(sv),
        ))
    return seed


def _new_error_rows(cursor, error_ewma_table: str, itemid: int,
                    since_clock: int) -> list[dict]:
    cursor.execute(
        f"SELECT CLOCK, MODEL, FINAL_EWMA, ACTUAL FROM {error_ewma_table} "
        f"WHERE ITEMID=%s AND CLOCK>%s ORDER BY CLOCK ASC, MODEL ASC",
        (itemid, since_clock),
    )
    rows = cursor.fetchall()

    grouped: dict[int, dict] = {}
    for clock, model, final_ewma, actual in rows:
        clock = int(clock)
        bucket = grouped.setdefault(clock, {"clock": clock, "actual": actual, "scores": {}})
        if final_ewma is not None and math.isfinite(float(final_ewma)):
            bucket["scores"][model] = float(final_ewma)
        if bucket["actual"] is None and actual is not None:
            bucket["actual"] = actual

    return [grouped[c] for c in sorted(grouped)]


def _load_predictions(cursor, pred_table: str, itemid: int,
                      clock_hours: list[int]) -> dict[int, dict[str, float]]:
    if not clock_hours:
        return {}
    placeholders = ",".join(["%s"] * len(clock_hours))
    # TRENDS_PRED_* stores the per-model prediction in VALUE_AVG (history's
    # equivalent column is VALUE — easy copy-paste pitfall when porting from
    # select_adaptive_history.py).
    cursor.execute(
        f"SELECT CLOCK, MODEL, VALUE_AVG FROM {pred_table} "
        f"WHERE ITEMID=%s AND CLOCK IN ({placeholders})",
        (itemid, *clock_hours),
    )
    out: dict[int, dict[str, float]] = {}
    for clock, model, value in cursor.fetchall():
        out.setdefault(int(clock), {})[model] = float(value)
    return out


def _rolling_mae(buffer: deque, key_idx: int) -> float:
    total = 0.0
    n = 0
    for triple in buffer:
        actual, hv, sv = triple
        pred = hv if key_idx == 1 else sv
        if actual is None or pred is None:
            continue
        if not (math.isfinite(actual) and math.isfinite(pred)):
            continue
        total += abs(actual - pred)
        n += 1
    return float("nan") if n == 0 else total / n


def _build_dag(mode: str):
    """Factory: build one trend-selector DAG bound to a specific mode's
    tables."""
    m                = TREND_MODES[mode]
    pred_table       = m.pred_table
    error_ewma_table = m.error_ewma_table
    state_table      = m.selection_state_table
    log_table        = m.selection_log_table
    stream_dag_id    = m.stream_dag_id
    dag_id           = f"select_adaptive_trend_{mode}"

    @dag(
        dag_id=dag_id,
        start_date=datetime(2024, 6, 1),
        schedule="0 * * * *",
        catchup=False,
        max_active_runs=1,
        # Self-fail a hung run so it can't hold the single max_active_runs
        # slot indefinitely (e.g. after a long scheduler outage leaves an
        # orphaned "running" run). Hourly cadence + extra headroom to absorb
        # CPU/MySQL contention while the train_models_* DAGs are running —
        # only a genuine hang trips it.
        dagrun_timeout=timedelta(minutes=60),
        tags=["selector", "adaptive", "trend", mode],
        doc_md=__doc__,
    )
    def _dag():

        @task
        def advance_selector() -> dict:
            import mysql.connector

            itemids = _pipeline_itemids()
            if not itemids:
                return {"itemids_processed": 0, "log_rows_inserted": 0}

            dt_vars = get_dt_variables()

            conn = mysql.connector.connect(**get_db_config())
            cursor = conn.cursor()

            total_log_rows = 0
            processed = 0
            try:
                for itemid in itemids:
                    vt = (dt_vars.get(str(itemid)) or {}).get("value_type")
                    if vt not in _VALID_VALUE_TYPES:
                        print(f"Skipping {itemid_label(itemid, dt_vars)}: value_type={vt} (non-numeric)")
                        continue

                    params = get_selector_params(itemid, SELECTOR_PARAMS_VAR)
                    eta              = float(params["eta"])
                    sm_hard          = float(params["switch_margin_hard"])
                    pat_hard         = int(params["patience_hard"])
                    sm_adapt         = float(params["switch_margin_adapt"])
                    pat_adapt        = int(params["patience_adapt"])
                    window           = int(params["window"])
                    min_dwell_secs   = int(params["min_dwell"]) * STEP_SECONDS

                    _ensure_state_row(cursor, state_table, itemid)
                    state = _load_state(cursor, state_table, itemid)

                    hard_state = state["hard"] or make_hard_state(INITIAL_HARD_ACTIVE)
                    adapt_state = state["adaptive"] or make_adaptive_state("Hard")

                    new_rows = _new_error_rows(
                        cursor, error_ewma_table, itemid, state["last_processed_clock"]
                    )
                    if not new_rows:
                        continue

                    clock_hours = sorted({(r["clock"] // STEP_SECONDS) * STEP_SECONDS for r in new_rows})
                    preds_by_hour = _load_predictions(cursor, pred_table, itemid, clock_hours)

                    window_buf = _load_seed_window(cursor, log_table, itemid, window)
                    log_rows: list[tuple] = []

                    for r in new_rows:
                        clock = r["clock"]
                        actual = None if r["actual"] is None else float(r["actual"])
                        scores = r["scores"]
                        preds = preds_by_hour.get((clock // STEP_SECONDS) * STEP_SECONDS, {})

                        hard_model = hard_step(
                            hard_state, scores, clock,
                            switch_margin=sm_hard, patience=pat_hard,
                            min_dwell=min_dwell_secs,
                        )
                        weights = softmax_weights(scores, PRED_MODELS, eta=eta)

                        hv = preds.get(hard_model)
                        sv: float | None
                        finite_pairs = [(weights[m], preds[m]) for m in PRED_MODELS
                                        if m in preds and math.isfinite(preds[m])]
                        if not finite_pairs:
                            sv = None
                        else:
                            wsum = sum(w for w, _ in finite_pairs)
                            sv = (sum(w * p for w, p in finite_pairs) / wsum) if wsum > 0 else None

                        window_buf.append((actual, hv, sv))
                        hard_mae = _rolling_mae(window_buf, key_idx=1)
                        soft_mae = _rolling_mae(window_buf, key_idx=2)

                        method = adaptive_step(
                            adapt_state, hard_mae, soft_mae, clock,
                            switch_margin=sm_adapt, patience=pat_adapt,
                            min_dwell=min_dwell_secs,
                        )
                        adaptive_value = hv if method == "Hard" else sv

                        log_rows.append((
                            itemid, clock,
                            None if actual is None else float(actual),
                            hard_model,
                            None if hv is None else float(hv),
                            None if sv is None else float(sv),
                            json.dumps({m: weights[m] for m in PRED_MODELS}),
                            method,
                            None if adaptive_value is None else float(adaptive_value),
                        ))

                    cursor.executemany(
                        f"INSERT IGNORE INTO {log_table} "
                        f"(ITEMID, CLOCK, ACTUAL, HARD_MODEL, HARD_VALUE, SOFT_VALUE, "
                        f" SOFT_WEIGHTS, METHOD, ADAPTIVE_VALUE) "
                        f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        log_rows,
                    )

                    state["last_processed_clock"] = new_rows[-1]["clock"]
                    state["hard"] = hard_state
                    state["adaptive"] = adapt_state
                    _save_state(cursor, state_table, itemid, state)

                    total_log_rows += len(log_rows)
                    processed += 1
                    print(
                        f"[{mode}] {itemid_label(itemid, dt_vars)}: advanced through CLOCK="
                        f"{new_rows[-1]['clock']} ({len(log_rows)} new log rows); "
                        f"hard.active={hard_state['active']}, method={adapt_state['active']}"
                    )

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
                conn.close()

            print(
                f"[{mode}] Selector tick done: {processed}/{len(itemids)} "
                f"itemids advanced, {total_log_rows} log rows inserted, "
                f"pred_table={pred_table}, log_table={log_table}"
            )
            return {"itemids_processed": processed, "log_rows_inserted": total_log_rows}

        stream = TriggerDagRunOperator(
            task_id=f"trigger_{stream_dag_id}",
            trigger_dag_id=stream_dag_id,
            wait_for_completion=False,
            deferrable=False,
            reset_dag_run=True,
        )

        advance_selector() >> stream

    return _dag


for _mode in ENABLED_MODES:
    _build_dag(_mode)()
