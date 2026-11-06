from __future__ import annotations

import json
import math
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.sdk import Variable

from _db import get_db_config
from _enabled import resolve_enabled_set
from _modes import DEFAULT_ENABLED_TREND_MODES, TREND_MODES
from _selector import PRED_MODELS


DEFAULT_EWMA = {"alpha": 0.20, "alpha_slow": 0.01, "gamma": 0.70, "eta": 8.0}
VALUE_EWMA_TABLE = "TRENDS_VALUE_EWMA"
STAGE_TABLE      = "TRENDS_KAFKA_STAGE"
EWMA_PARAMS_VAR  = "ewmaTunedParamsTrend"

# This DAG stays SINGLE (not per-mode) so the stage-table consumer remains
# single-reader/single-writer — two parallel processor DAGs would race on
# `DELETE FROM TRENDS_KAFKA_STAGE`. Instead, this DAG loops over
# `enabledModesTrend` and writes per-mode error-EWMA rows into the
# corresponding *_RECURSIVE / *_DIRECT24 tables (from TREND_MODES) in a
# single transaction. VALUE_EWMA is mode-independent and written once.

ENABLED_MODES_VAR = "enabledModesTrend"
ENABLED_MODES = resolve_enabled_set(
    ENABLED_MODES_VAR, DEFAULT_ENABLED_TREND_MODES, TREND_MODES,
)


def _ewma_step(x, prev_final, prev_slow, alpha, alpha_slow, gamma):
    # Mirrors _ewma_update in run_all_variables_tuned.py:475-490: the floored
    # value (max(fast, gamma*slow)) is what propagates as the next step's
    # "fast" prior.
    if prev_final is None or prev_slow is None or (
        isinstance(prev_final, float) and math.isnan(prev_final)
    ):
        fast = x
        slow = x
    else:
        fast = alpha * x + (1.0 - alpha) * prev_final
        slow = alpha_slow * x + (1.0 - alpha_slow) * prev_slow
    final = max(fast, gamma * slow)
    return fast, slow, final


@dag(
    dag_id="message_processor_trend",
    start_date=pendulum.datetime(2025, 7, 21, tz="UTC"),
    catchup=False,
    # Trend data arrives hourly (Zabbix trend buckets) and the kafka batch
    # processor is on `*/15`. A per-minute schedule wakes the scheduler
    # ~57 times per hour to scan an empty stage table. Run on the quarter
    # hour to drain whatever the producer just staged without overshooting.
    schedule="*/15 * * * *",
    max_active_runs=1,
    # Self-fail a hung run so it can't hold the single max_active_runs slot
    # indefinitely (e.g. after a long scheduler outage leaves an orphaned
    # "running" run). Drains every 15 min; ceiling raised to absorb CPU/MySQL
    # contention while the train_models_* DAGs are running — only a genuine
    # hang trips it.
    dagrun_timeout=timedelta(minutes=30),
    tags=["kafka-processor"],
)
def message_processor_dag():
    """Runs every minute. Snapshots all BATCH_IDs currently in
    TRENDS_KAFKA_STAGE, computes dual-time EWMA over their rows, and
    deletes those BATCH_IDs on success. Decoupled from
    `kafka_batch_processor_dag_trend` — the consumer just stages, this
    DAG drains on a fixed cadence regardless of how long any single
    consumer run takes.

    Prediction triggering is independent — owned by the trend prediction
    DAGs on their own cadence.

    EWMA streams per message:
      - Value EWMA on raw VALUE_AVG (mode-independent, written once into
        TRENDS_VALUE_EWMA).
      - Error EWMA per (ITEMID, MODEL) on |actual - pred|, computed once
        per enabled mode in `enabledModesTrend`. Each mode's errors go
        into TRENDS_ERROR_EWMA_<MODE>, joining against that mode's
        prediction table (TRENDS_PRED_RECURSIVE / TRENDS_PRED_DIRECT24).
    """

    @task
    def load_messages_from_stage() -> dict:
        """Snapshot pending BATCH_IDs and return them with their staged rows.

        Snapshot semantics: SELECT DISTINCT BATCH_ID at the start defines
        the work boundary. Rows from a consumer run that finishes during
        this DAG run (with a BATCH_ID not in the snapshot) stay in the
        stage table and are picked up by the next tick.
        """
        import mysql.connector

        conn = mysql.connector.connect(**get_db_config())
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT DISTINCT BATCH_ID FROM {STAGE_TABLE}")
            batch_ids = [r[0] for r in cursor.fetchall()]
            if not batch_ids:
                print("Stage table empty; nothing to do.")
                return {"batch_ids": [], "payloads": []}
        finally:
            cursor.close()

        cursor = conn.cursor(dictionary=True)
        try:
            placeholders = ",".join(["%s"] * len(batch_ids))
            cursor.execute(
                f"SELECT ITEMID, CLOCK, VALUE_AVG FROM {STAGE_TABLE} "
                f"WHERE BATCH_ID IN ({placeholders})",
                tuple(batch_ids),
            )
            rows = cursor.fetchall()
            print(
                f"Snapshot: {len(batch_ids)} batch_id(s), "
                f"{len(rows)} staged row(s) to process."
            )
            return {"batch_ids": batch_ids, "payloads": rows}
        finally:
            cursor.close()
            conn.close()

    @task
    def compute_and_store_ewma(payloads: list) -> int:
        if not payloads:
            print("No payloads received; skipping EWMA computation.")
            return 0

        import mysql.connector

        try:
            tuned = json.loads(Variable.get(EWMA_PARAMS_VAR))
        except Exception as exc:
            print(f"{EWMA_PARAMS_VAR} not set or unparseable ({exc}); using DEFAULT_EWMA")
            tuned = {}

        def params_for(itemid):
            entry = tuned.get(str(itemid)) or tuned.get(itemid) or {}
            return (
                float(entry.get("alpha",      DEFAULT_EWMA["alpha"])),
                float(entry.get("alpha_slow", DEFAULT_EWMA["alpha_slow"])),
                float(entry.get("gamma",      DEFAULT_EWMA["gamma"])),
            )

        # Sort by (ITEMID, CLOCK) so within-batch state chains correctly.
        sorted_payloads = sorted(
            (m for m in payloads
             if m.get("ITEMID") is not None and m.get("CLOCK") is not None),
            key=lambda m: (m["ITEMID"], m["CLOCK"]),
        )

        conn = mysql.connector.connect(**get_db_config())
        cursor = conn.cursor()

        # value_state: itemid -> (final_prev, slow_prev) — single per pipeline.
        # error_state: (mode, itemid, model) -> (final_prev, slow_prev) — per mode.
        value_state: dict = {}
        error_state: dict = {}
        value_rows: list = []
        error_rows_by_mode: dict[str, list] = {m: [] for m in ENABLED_MODES}

        try:
            for msg in sorted_payloads:
                itemid = msg["ITEMID"]
                clock  = msg["CLOCK"]
                raw    = msg.get("VALUE_AVG")
                if raw is None:
                    continue
                try:
                    actual = float(raw)
                except (TypeError, ValueError):
                    print(f"Non-numeric VALUE_AVG, skipping: {msg}")
                    continue

                alpha, alpha_slow, gamma = params_for(itemid)

                # ---- Value EWMA (mode-independent) ----
                if itemid not in value_state:
                    cursor.execute(
                        f"SELECT FINAL_EWMA, SLOW_EWMA FROM {VALUE_EWMA_TABLE} "
                        f"WHERE ITEMID=%s AND CLOCK<%s "
                        f"ORDER BY CLOCK DESC LIMIT 1",
                        (itemid, clock),
                    )
                    row = cursor.fetchone()
                    value_state[itemid] = (row[0], row[1]) if row else (None, None)

                prev_final, prev_slow = value_state[itemid]
                fast, slow, final = _ewma_step(
                    actual, prev_final, prev_slow, alpha, alpha_slow, gamma,
                )
                value_state[itemid] = (final, slow)
                value_rows.append((itemid, clock, actual, fast, slow, final))

                # ---- Error EWMA per enabled mode ----
                # Zabbix TREND rows are aggregated per hour but the CLOCK
                # column carries the bucket start; snap the actual's CLOCK
                # to its containing hour for the prediction lookup.
                clock_hr = (clock // 3600) * 3600
                for mode in ENABLED_MODES:
                    pred_table = TREND_MODES[mode].pred_table
                    error_ewma_table = TREND_MODES[mode].error_ewma_table

                    cursor.execute(
                        f"SELECT MODEL, VALUE_AVG FROM {pred_table} "
                        f"WHERE ITEMID=%s AND CLOCK=%s",
                        (itemid, clock_hr),
                    )
                    preds = {model: float(pred) for model, pred in cursor.fetchall()}
                    for model in PRED_MODELS:
                        if model not in preds:
                            continue
                        err = abs(actual - preds[model])
                        key = (mode, itemid, model)
                        if key not in error_state:
                            cursor.execute(
                                f"SELECT FINAL_EWMA, SLOW_EWMA FROM {error_ewma_table} "
                                f"WHERE ITEMID=%s AND MODEL=%s AND CLOCK<%s "
                                f"ORDER BY CLOCK DESC LIMIT 1",
                                (itemid, model, clock),
                            )
                            erow = cursor.fetchone()
                            error_state[key] = (erow[0], erow[1]) if erow else (None, None)

                        e_prev_final, e_prev_slow = error_state[key]
                        e_fast, e_slow, e_final = _ewma_step(
                            err, e_prev_final, e_prev_slow, alpha, alpha_slow, gamma,
                        )
                        error_state[key] = (e_final, e_slow)
                        error_rows_by_mode[mode].append(
                            (itemid, clock, model, err, actual, e_fast, e_slow, e_final)
                        )

            if value_rows:
                cursor.executemany(
                    f"INSERT IGNORE INTO {VALUE_EWMA_TABLE} "
                    f"(ITEMID, CLOCK, ACTUAL, FAST_EWMA, SLOW_EWMA, FINAL_EWMA) "
                    f"VALUES (%s, %s, %s, %s, %s, %s)",
                    value_rows,
                )
            total_error_rows = 0
            for mode, rows in error_rows_by_mode.items():
                if not rows:
                    continue
                error_ewma_table = TREND_MODES[mode].error_ewma_table
                cursor.executemany(
                    f"INSERT IGNORE INTO {error_ewma_table} "
                    f"(ITEMID, CLOCK, MODEL, ERROR, ACTUAL, FAST_EWMA, SLOW_EWMA, FINAL_EWMA) "
                    f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    rows,
                )
                total_error_rows += len(rows)
            conn.commit()
            print(
                f"Trend EWMA inserted: {len(value_rows)} value rows, "
                f"{total_error_rows} error rows across modes "
                f"({ {m: len(rs) for m, rs in error_rows_by_mode.items()} }) "
                f"from {len(sorted_payloads)} payloads"
            )
            return len(value_rows) + total_error_rows
        finally:
            try:
                cursor.close()
            finally:
                conn.close()

    @task
    def cleanup_stage(batch_ids: list) -> int:
        """Delete every row under the snapshotted BATCH_IDs."""
        if not batch_ids:
            return 0

        import mysql.connector

        conn = mysql.connector.connect(**get_db_config())
        cursor = conn.cursor()
        try:
            placeholders = ",".join(["%s"] * len(batch_ids))
            cursor.execute(
                f"DELETE FROM {STAGE_TABLE} WHERE BATCH_ID IN ({placeholders})",
                tuple(batch_ids),
            )
            deleted = cursor.rowcount
            conn.commit()
            print(f"Deleted {deleted} stage rows across {len(batch_ids)} batch_id(s)")
            return deleted
        finally:
            cursor.close()
            conn.close()

    bundle    = load_messages_from_stage()
    ewma_done = compute_and_store_ewma(bundle["payloads"])
    cleanup   = cleanup_stage(bundle["batch_ids"])
    ewma_done >> cleanup


message_processor_dag()
