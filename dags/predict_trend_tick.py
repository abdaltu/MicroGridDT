"""Scheduled prediction trigger for the trend pipeline.

Decouples trend prediction cadence from Kafka polling. While
`kafka_batch_processor_dag_trend` keeps polling on its own schedule (so
EWMA stays fresh), this DAG fires a prediction fan-out on its own clock
— default every hour at :00, configurable via the Airflow Variable
`trendPredictionIntervalHours`.

Each tick:
  1. Resolves `enabledModesTrend` (JSON array, default
     ``["recursive","direct24"]``) into the list of trigger DAGs to fire:
     `predict_hourly_trend_recursive_DT` and/or
     `predict_hourly_trend_direct24_DT`. Both modes run in parallel by
     default — set the Variable to ``["recursive"]`` (or ``["direct24"]``)
     to disable one.
  2. Iterates `activePipeLine`, skipping char-typed itemids (value_type=1)
     and unknown itemids per `DT_Variables`.
  3. Builds one trigger conf per remaining itemid: ``{"message":
     {"ITEMID": itemid, "CLOCK": now_unix}}``. The downstream prediction
     DAG fetches its own LOOKBACK window from the live TRENDS/TRENDS_UINT
     table with ``WHERE CLOCK <= clock_unix``, so passing "now" gets the
     freshest bucket.
  4. Fans out via one `TriggerDagRunOperator.partial(...).expand(conf=...)`
     per enabled mode, sharing the same `configs` XCom across expansions.

`trendPredictionIntervalHours` is clamped to ``[1, 23]``. Recommended
values that divide 24 evenly (so the cadence is uniform across the day):
1, 2, 3, 4, 6, 8, 12. Other values produce an irregular gap at the
midnight boundary (e.g. ``*/5`` fires at 00:00, 05:00, …, 20:00, then
00:00 — a 4-hour gap).

Per-message inference fan-out used to live in `message_processor_trend`;
that DAG now does only EWMA + stage cleanup.
"""
from __future__ import annotations

import json
import time
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

from _dt_vars import get_dt_variables, itemid_label
from _enabled import resolve_enabled_set
from _modes import DEFAULT_ENABLED_TREND_MODES, TREND_MODES


INTERVAL_VAR = "trendPredictionIntervalHours"
DEFAULT_INTERVAL_HOURS = 1

ENABLED_MODES_VAR = "enabledModesTrend"

ACTIVE_PIPELINE_VAR = "activePipeLine"
DEFAULT_PRED_ITEMIDS = frozenset({
    31316, 31317, 31318, 31319, 31320, 31322, 31332,
    33908, 33910, 33912, 33914, 33916,
    35911,
    36550, 36554, 36556, 36557, 36560, 36930,
})

# value_type=0 → float (TRENDS), =3 → UINT (TRENDS_UINT), =1 → char (skip).
_VALUE_TYPE_TO_LIVE_TABLE = {0: "TRENDS", 3: "TRENDS_UINT"}


def _live_table_for(itemid, dt_vars: dict) -> str | None:
    vt = (dt_vars.get(str(itemid)) or {}).get("value_type")
    return _VALUE_TYPE_TO_LIVE_TABLE.get(vt)


# ---- parse-time resolution -------------------------------------------------
try:
    _raw_interval = int(Variable.get(INTERVAL_VAR))
except Exception as _exc:
    print(
        f"{INTERVAL_VAR} not set/unparseable ({_exc}); "
        f"defaulting to {DEFAULT_INTERVAL_HOURS}"
    )
    _raw_interval = DEFAULT_INTERVAL_HOURS

INTERVAL_HOURS = max(1, min(23, _raw_interval))
if INTERVAL_HOURS != _raw_interval:
    print(
        f"{INTERVAL_VAR}={_raw_interval} outside [1, 23]; "
        f"clamped to {INTERVAL_HOURS}"
    )
SCHEDULE_CRON = f"0 */{INTERVAL_HOURS} * * *"

ENABLED_MODES = resolve_enabled_set(
    ENABLED_MODES_VAR, DEFAULT_ENABLED_TREND_MODES, TREND_MODES,
)
ENABLED_TRIGGER_DAG_IDS = [TREND_MODES[m].trigger_dag_id for m in ENABLED_MODES]

try:
    PRED_ITEMIDS = frozenset(
        int(x) for x in json.loads(Variable.get(ACTIVE_PIPELINE_VAR))
    )
except Exception as _exc:
    print(
        f"{ACTIVE_PIPELINE_VAR} not set/unparseable ({_exc}); "
        f"using defaults ({len(DEFAULT_PRED_ITEMIDS)} itemids)"
    )
    PRED_ITEMIDS = DEFAULT_PRED_ITEMIDS


@dag(
    dag_id="predict_trend_tick",
    start_date=datetime(2024, 6, 1),
    schedule=SCHEDULE_CRON,
    catchup=False,
    max_active_runs=1,
    tags=["forecast", "trend", "scheduler"],
    doc_md=__doc__,
)
def predict_trend_tick():
    @task
    def build_configs() -> list[dict]:
        """One trigger conf per enabled numeric itemid, anchored at `now`."""
        dt_vars = get_dt_variables()
        now_unix = int(time.time())

        configs: list[dict] = []
        skipped: list = []
        for itemid in sorted(PRED_ITEMIDS):
            live_table = _live_table_for(itemid, dt_vars)
            if live_table is None:
                skipped.append(itemid_label(itemid, dt_vars))
                continue
            configs.append({
                "message": {"ITEMID": itemid, "CLOCK": now_unix},
            })

        print(
            f"Triggering {ENABLED_TRIGGER_DAG_IDS} for {len(configs)} "
            f"itemid(s) at CLOCK={now_unix}; modes={ENABLED_MODES!r}, "
            f"interval={INTERVAL_HOURS}h"
        )
        if skipped:
            print(f"Skipped (non-numeric value_type): {skipped}")
        return configs

    configs = build_configs()

    # One mapped trigger task per enabled mode. The `configs` XCom is
    # reused across expansions — built once, fanned out to both modes'
    # worker DAGs.
    for _dag_id in ENABLED_TRIGGER_DAG_IDS:
        TriggerDagRunOperator.partial(
            task_id=f"trigger_{_dag_id}",
            trigger_dag_id=_dag_id,
        ).expand(conf=configs)


predict_trend_tick()
