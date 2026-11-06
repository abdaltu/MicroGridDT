"""Predictions stream — projects each mode's trend selector decision onto
every future-CLOCK row in that mode's prediction table.

Per-mode factory: registers `predict_stream_trend_recursive` and
`predict_stream_trend_direct24` (per `enabledModesTrend`). Each is
triggered by its sibling `select_adaptive_trend_<mode>` DAG at the end of
every selector tick.

Per-mode behavior (recursive shown, direct24 mirrors):
  - For every itemid in `activePipeLine`, look up the latest row from
    `TRENDS_SELECTION_LOG_RECURSIVE` (the frozen Hard model, Soft
    weights, Method).
  - Read every prediction row in `TRENDS_PRED_RECURSIVE` whose CLOCK is
    after that decision CLOCK.
  - Apply the frozen decision to compute HARD_VALUE / SOFT_VALUE /
    ADAPTIVE_VALUE per future CLOCK and upsert into
    `TRENDS_PRED_SELECTED_RECURSIVE`.

Skipped itemids per tick:
  - Cold start (no SELECTION_LOG row yet) — silent.
  - Stale decision (decision older than `streamStalenessSecondsTrend`,
    default 10800 s = 3 hours) — logged + skipped.

Schema for `TRENDS_PRED_SELECTED_*` mirrors the original single-mode
table; see `migrate_dualmode.sql`.
"""
from __future__ import annotations

import json
import time
from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import Variable

from _db import get_db_config
from _dt_vars import get_dt_variables, itemid_label
from _enabled import resolve_enabled_set
from _modes import DEFAULT_ENABLED_TREND_MODES, TREND_MODES
from _stream import (
    load_future_predictions,
    load_latest_decision,
    pipeline_itemids,
    project_row,
)


# Trend PRED tables store the per-model prediction in column VALUE_AVG
# (history tables use VALUE).
PRED_VALUE_COLUMN = "VALUE_AVG"

STALENESS_VAR = "streamStalenessSecondsTrend"
DEFAULT_STALENESS_SECONDS = 10800   # 3 hours; ~3 trend selector ticks.

ENABLED_MODES_VAR = "enabledModesTrend"
ENABLED_MODES = resolve_enabled_set(
    ENABLED_MODES_VAR, DEFAULT_ENABLED_TREND_MODES, TREND_MODES,
)


def _staleness_limit() -> int:
    try:
        v = int(Variable.get(STALENESS_VAR))
    except Exception:
        return DEFAULT_STALENESS_SECONDS
    return max(0, v)


def _build_dag(mode: str):
    """Factory: build one trend stream DAG bound to a specific mode's tables."""
    m            = TREND_MODES[mode]
    pred_table   = m.pred_table
    log_table    = m.selection_log_table
    stream_table = m.pred_selected_table
    dag_id       = f"predict_stream_trend_{mode}"

    upsert_sql = (
        f"INSERT INTO {stream_table} "
        f"(ITEMID, CLOCK, HARD_MODEL, HARD_VALUE, SOFT_WEIGHTS, SOFT_VALUE, "
        f" METHOD, ADAPTIVE_VALUE, DECISION_CLOCK) "
        f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        f"ON DUPLICATE KEY UPDATE "
        f"  HARD_MODEL=VALUES(HARD_MODEL), "
        f"  HARD_VALUE=VALUES(HARD_VALUE), "
        f"  SOFT_WEIGHTS=VALUES(SOFT_WEIGHTS), "
        f"  SOFT_VALUE=VALUES(SOFT_VALUE), "
        f"  METHOD=VALUES(METHOD), "
        f"  ADAPTIVE_VALUE=VALUES(ADAPTIVE_VALUE), "
        f"  DECISION_CLOCK=VALUES(DECISION_CLOCK)"
    )

    @dag(
        dag_id=dag_id,
        start_date=datetime(2024, 6, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["forecast", "stream", "trend", mode],
        doc_md=__doc__,
    )
    def _dag():

        @task
        def project_all_itemids() -> dict:
            import mysql.connector

            itemids = pipeline_itemids()
            if not itemids:
                return {"itemids_projected": 0, "rows_upserted": 0}

            dt_vars = get_dt_variables()
            staleness_limit = _staleness_limit()
            now_unix = int(time.time())

            conn = mysql.connector.connect(**get_db_config())
            cursor = conn.cursor()

            total_rows = 0
            projected = 0
            skipped_cold = 0
            skipped_stale = 0
            skipped_nofuture = 0
            try:
                for itemid in itemids:
                    decision = load_latest_decision(cursor, log_table, itemid)
                    if decision is None:
                        skipped_cold += 1
                        continue

                    age = now_unix - decision["decision_clock"]
                    if staleness_limit and age > staleness_limit:
                        print(
                            f"  [{mode}] {itemid_label(itemid, dt_vars)}: decision is "
                            f"{age}s old (> {staleness_limit}s); skipping"
                        )
                        skipped_stale += 1
                        continue

                    preds_by_clock = load_future_predictions(
                        cursor, pred_table, itemid, decision["decision_clock"],
                        value_column=PRED_VALUE_COLUMN,
                    )
                    if not preds_by_clock:
                        skipped_nofuture += 1
                        continue

                    weights_json = json.dumps(decision["soft_weights"])
                    rows = []
                    for clock in sorted(preds_by_clock):
                        hv, sv, av = project_row(decision, preds_by_clock[clock])
                        rows.append((
                            itemid, clock,
                            decision["hard_model"],
                            hv,
                            weights_json,
                            sv,
                            decision["method"],
                            av,
                            decision["decision_clock"],
                        ))

                    cursor.executemany(upsert_sql, rows)
                    total_rows += len(rows)
                    projected += 1
                    print(
                        f"  [{mode}] {itemid_label(itemid, dt_vars)}: projected "
                        f"{len(rows)} future rows from decision_clock="
                        f"{decision['decision_clock']} "
                        f"(hard={decision['hard_model']}, method={decision['method']})"
                    )

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
                conn.close()

            print(
                f"[{mode}] Stream tick done: {projected}/{len(itemids)} "
                f"itemids projected, {total_rows} rows upserted into "
                f"{stream_table}; skipped cold={skipped_cold}, "
                f"stale={skipped_stale}, no_future={skipped_nofuture}; "
                f"pred_table={pred_table}"
            )
            return {"itemids_projected": projected, "rows_upserted": total_rows}

        project_all_itemids()

    return _dag


for _mode in ENABLED_MODES:
    _build_dag(_mode)()
