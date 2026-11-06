"""Adaptive model selector for the history (1-min) pipeline.

Per-mode factory: registers one `@dag` instance per mode in
`enabledModesHistory` (default ``["recursive","direct60"]``). dag_ids are
`select_adaptive_history_recursive` and `select_adaptive_history_direct60`.
Each instance reads its own per-mode error-EWMA + selection-state +
selection-log tables and triggers its own per-mode stream DAG. The two
selectors run independently, with their own decision timelines, against
their respective prediction tables.

Each DAG runs every minute, reads new rows from
`HISTORY_ERROR_EWMA_<MODE>` since the last processed CLOCK, and advances
the three-tier selector (Hard + Soft + Adaptive meta) per ITEMID.
Decisions are persisted to:

  - `HISTORY_SELECTION_STATE_<MODE>` (one row per ITEMID, persistent state)
  - `HISTORY_SELECTION_LOG_<MODE>`   (one row per ITEMID per CLOCK; the
                                      closed-loop decision timeline)

The selector math itself lives in `_selector.py`. EWMA scores are read
straight from `HISTORY_ERROR_EWMA_<MODE>.FINAL_EWMA` (already maintained
by `message_processor_history`); we do not recompute them here.

Schema for `HISTORY_SELECTION_STATE_*` and `HISTORY_SELECTION_LOG_*`
mirrors the original single-mode tables; see `migrate_dualmode.sql`.
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
from _modes import DEFAULT_ENABLED_HISTORY_MODES, HISTORY_MODES
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
SELECTOR_PARAMS_VAR = "ewmaTunedParamsHistory"

ENABLED_MODES_VAR = "enabledModesHistory"
ENABLED_MODES = resolve_enabled_set(
    ENABLED_MODES_VAR, DEFAULT_ENABLED_HISTORY_MODES, HISTORY_MODES,
)

# Each "step" in the reference selector corresponds to one CLOCK tick.
# CLOCK is a unix timestamp in seconds; for the history pipeline a step
# is 1 minute, so min_dwell (in steps) → seconds is *60. The window for
# the rolling MAE stays in step count (we LIMIT N rows of the LOG).
STEP_SECONDS = 60

# value_type=0 → float (HISTORY), =3 → UINT (HISTORY_UINT), =1 → char (skip).
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
    """Insert a default state row for an itemid if it doesn't exist yet."""
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
    """Pre-load the last `window-1` LOG rows so the rolling MAE for the
    first new CLOCK in this tick already includes recent history. The
    deque holds (actual, hard_value, soft_value) triples; values may be
    None when the row didn't have a finite prediction."""
    seed = deque(maxlen=window)
    if window <= 1:
        return seed
    cursor.execute(
        f"SELECT ACTUAL, HARD_VALUE, SOFT_VALUE FROM {log_table} "
        f"WHERE ITEMID=%s ORDER BY CLOCK DESC LIMIT %s",
        (itemid, window - 1),
    )
    rows = cursor.fetchall()
    # Reverse so the deque ends up oldest→newest and the next append
    # pushes the oldest out at maxlen.
    for actual, hv, sv in reversed(rows):
        seed.append((
            None if actual is None else float(actual),
            None if hv     is None else float(hv),
            None if sv     is None else float(sv),
        ))
    return seed


def _new_error_rows(cursor, error_ewma_table: str, itemid: int,
                    since_clock: int) -> list[dict]:
    """Pull error-EWMA rows after `since_clock`, grouped by CLOCK.

    Returns a list of {clock, actual, scores: {model: final_ewma}} dicts,
    sorted by CLOCK ascending.
    """
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
        # ACTUAL is identical across models at the same CLOCK (it's the
        # observed value that drove the EWMA update); the first row's
        # value is fine. Take the max-of-not-null defensively.
        if bucket["actual"] is None and actual is not None:
            bucket["actual"] = actual

    out = [grouped[c] for c in sorted(grouped)]
    return out


def _load_predictions(cursor, pred_table: str, itemid: int,
                      clock_minutes: list[int]) -> dict[int, dict[str, float]]:
    """Bulk-fetch per-model predictions for a set of minute-aligned CLOCKs.

    Returns {clock_min: {model: value}}. Missing (clock, model) pairs are
    simply absent from the inner dict.
    """
    if not clock_minutes:
        return {}
    placeholders = ",".join(["%s"] * len(clock_minutes))
    cursor.execute(
        f"SELECT CLOCK, MODEL, VALUE FROM {pred_table} "
        f"WHERE ITEMID=%s AND CLOCK IN ({placeholders})",
        (itemid, *clock_minutes),
    )
    out: dict[int, dict[str, float]] = {}
    for clock, model, value in cursor.fetchall():
        out.setdefault(int(clock), {})[model] = float(value)
    return out


def _rolling_mae(buffer: deque, key_idx: int) -> float:
    """Mean of |actual - pred| across the buffer; NaN if no finite pair."""
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
    """Factory: build one selector DAG bound to a specific mode's tables.

    Tables and the downstream stream-DAG id are resolved once at parse
    time per mode and captured by closure into the task functions. Means
    no runtime mode-checking inside tasks — each registered DAG is a
    plain single-table consumer."""
    m                = HISTORY_MODES[mode]
    pred_table       = m.pred_table
    error_ewma_table = m.error_ewma_table
    state_table      = m.selection_state_table
    log_table        = m.selection_log_table
    stream_dag_id    = m.stream_dag_id
    dag_id           = f"select_adaptive_history_{mode}"

    @dag(
        dag_id=dag_id,
        start_date=datetime(2024, 6, 1),
        schedule="* * * * *",
        catchup=False,
        max_active_runs=1,
        # Self-fail a hung run so it can't hold the single max_active_runs
        # slot indefinitely (e.g. after a long scheduler outage leaves an
        # orphaned "running" run). Normal ticks finish in well under a minute,
        # but the ceiling is set high to absorb CPU/MySQL contention while the
        # train_models_* DAGs are running — only a genuine hang trips it.
        dagrun_timeout=timedelta(minutes=30),
        tags=["selector", "adaptive", "history", mode],
        doc_md=__doc__,
    )
    def _dag():

        @task
        def advance_selector() -> dict:
            """Advance the per-itemid selector state by all unprocessed CLOCKs."""
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
                    # Reference's min_dwell is in step counts; CLOCK is seconds,
                    # one history step = 60 seconds.
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

                    clock_minutes = sorted({(r["clock"] // 60) * 60 for r in new_rows})
                    preds_by_minute = _load_predictions(cursor, pred_table, itemid, clock_minutes)

                    window_buf = _load_seed_window(cursor, log_table, itemid, window)
                    log_rows: list[tuple] = []

                    for r in new_rows:
                        clock = r["clock"]
                        actual = None if r["actual"] is None else float(r["actual"])
                        scores = r["scores"]
                        preds = preds_by_minute.get((clock // 60) * 60, {})

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
