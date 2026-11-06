"""Scheduled prediction trigger for the history pipeline.

Decouples history prediction cadence from Kafka polling. While
`kafka_batch_processor_dag_history` keeps polling every minute (so EWMA
stays per-minute), this DAG fires a prediction fan-out on its own clock
— default every 10 minutes, configurable via the Airflow Variable
`historyPredictionIntervalMinutes`.

Each tick:
  1. Resolves `enabledModesHistory` (JSON array, default
     ``["recursive","direct60"]``) into the list of trigger DAGs to fire:
     `predict_minute_history_recursive_DT` and/or
     `predict_minute_history_direct60_DT`. Both modes run in parallel by
     default — set the Variable to ``["recursive"]`` (or ``["direct60"]``)
     to disable one.
  2. Iterates `activePipeLine`, skipping char-typed itemids (value_type=1)
     and unknown itemids per `DT_Variables`.
  3. Builds one trigger conf per remaining itemid: ``{"message":
     {"ITEMID": itemid, "CLOCK": now_unix}}``. The downstream prediction
     DAG fetches its own LOOKBACK window from the live HISTORY/HISTORY_UINT
     table with ``WHERE CLOCK <= clock_unix``, so passing "now" gets the
     freshest data.
  4. Fans out via one `TriggerDagRunOperator.partial(...).expand(conf=...)`
     per enabled mode, sharing the same `configs` XCom across expansions.

`historyPredictionIntervalMinutes` is clamped to ``[1, 59]``. Recommended
values that divide 60 evenly (so the cadence is uniform across the hour):
1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30. Other values produce an irregular
gap at the hour boundary (e.g. ``*/7`` fires at :00, :07, …, :49, :56,
then :00 — a 4-minute gap).

Per-message inference fan-out used to live in `message_processor_history`;
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
from _modes import DEFAULT_ENABLED_HISTORY_MODES, HISTORY_MODES


INTERVAL_VAR = "historyPredictionIntervalMinutes"
DEFAULT_INTERVAL_MIN = 10

ENABLED_MODES_VAR = "enabledModesHistory"

ACTIVE_PIPELINE_VAR = "activePipeLine"
DEFAULT_PRED_ITEMIDS = frozenset({
    31316, 31317, 31318, 31319, 31320, 31322, 31332,
    33908, 33910, 33912, 33914, 33916,
    35911,
    36550, 36554, 36556, 36557, 36560, 36930,
})

# value_type=0 → float (HISTORY), =3 → UINT (HISTORY_UINT), =1 → char (skip).
_VALUE_TYPE_TO_LIVE_TABLE = {0: "HISTORY", 3: "HISTORY_UINT"}


def _live_table_for(itemid, dt_vars: dict) -> str | None:
    vt = (dt_vars.get(str(itemid)) or {}).get("value_type")
    return _VALUE_TYPE_TO_LIVE_TABLE.get(vt)


# ---- parse-time resolution -------------------------------------------------
try:
    _raw_interval = int(Variable.get(INTERVAL_VAR))
except Exception as _exc:
    print(
        f"{INTERVAL_VAR} not set/unparseable ({_exc}); "
        f"defaulting to {DEFAULT_INTERVAL_MIN}"
    )
    _raw_interval = DEFAULT_INTERVAL_MIN

INTERVAL_MIN = max(1, min(59, _raw_interval))
if INTERVAL_MIN != _raw_interval:
    print(
        f"{INTERVAL_VAR}={_raw_interval} outside [1, 59]; "
        f"clamped to {INTERVAL_MIN}"
    )
SCHEDULE_CRON = f"*/{INTERVAL_MIN} * * * *"

ENABLED_MODES = resolve_enabled_set(
    ENABLED_MODES_VAR, DEFAULT_ENABLED_HISTORY_MODES, HISTORY_MODES,
)
ENABLED_TRIGGER_DAG_IDS = [HISTORY_MODES[m].trigger_dag_id for m in ENABLED_MODES]

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
    dag_id="predict_history_tick",
    start_date=datetime(2024, 6, 1),
    schedule=SCHEDULE_CRON,
    catchup=False,
    max_active_runs=1,
    tags=["forecast", "history", "scheduler"],
    doc_md=__doc__,
)
def predict_history_tick():
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
            f"interval={INTERVAL_MIN}m"
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


predict_history_tick()
