# Airflow DAG Inter-Dependencies

Generated from `dags/*.py`. Two parallel pipelines — **history** (1-minute
resolution) and **trend** (hourly resolution) — each running two **modes**
(history: `recursive`, `direct60`; trend: `recursive`, `direct24`). Several
DAGs are per-mode factories, so one `.py` file registers multiple `dag_id`s.

Dependencies come in two flavours:

- **Trigger edges** — explicit `TriggerDagRunOperator` (one DAG fires another).
- **Data edges** — implicit coupling through shared MySQL tables / Kafka
  (no trigger; ordering is enforced by schedule + table state).

The mode → table/dag-id mapping that wires it all together lives in
[`_modes.py`](_modes.py) (`HISTORY_MODES` / `TREND_MODES`).

---

## 1. Trigger graph (explicit `TriggerDagRunOperator`)

```
predict_history_tick ──┬─▶ predict_minute_history_recursive_DT
 (schedule: */N min)   └─▶ predict_minute_history_direct60_DT
                                            (worker, schedule=None)

predict_trend_tick ────┬─▶ predict_hourly_trend_recursive_DT
 (schedule: */N min)   └─▶ predict_hourly_trend_direct24_DT
                                            (worker, schedule=None)

select_adaptive_history_recursive ─▶ predict_stream_history_recursive
select_adaptive_history_direct60  ─▶ predict_stream_history_direct60
 (schedule: * * * * *)                       (stream, schedule=None)

select_adaptive_trend_recursive   ─▶ predict_stream_trend_recursive
select_adaptive_trend_direct24    ─▶ predict_stream_trend_direct24
 (schedule: 0 * * * *)                       (stream, schedule=None)

drift_retrain_trigger ─┬─▶ train_models_recursive_history   (drifted itemids only)
 (schedule: 0 */6 * * *)├─▶ train_models_direct60_history
                        ├─▶ train_models_recursive_trend
                        └─▶ train_models_direct24_trend
                                            (trainer, schedule=None)
```

| Source DAG | File | Trigger target(s) | Mechanism |
|---|---|---|---|
| `predict_history_tick` | [predict_history_tick.py](predict_history_tick.py) | `predict_minute_history_{recursive,direct60}_DT` | `TriggerDagRunOperator.partial().expand(conf=…)`, one per enabled mode, fanned out per itemid |
| `predict_trend_tick` | [predict_trend_tick.py](predict_trend_tick.py) | `predict_hourly_trend_{recursive,direct24}_DT` | same fan-out pattern |
| `select_adaptive_history_<mode>` | [select_adaptive_history.py](select_adaptive_history.py) | `predict_stream_history_<mode>` | `TriggerDagRunOperator` at end of each selector tick |
| `select_adaptive_trend_<mode>` | [select_adaptive_trend.py](select_adaptive_trend.py) | `predict_stream_trend_<mode>` | `TriggerDagRunOperator` at end of each selector tick |
| `drift_retrain_trigger` | [drift_retrain_trigger.py](drift_retrain_trigger.py) | `train_models_*` (per drifted mode) | `TriggerDagRunOperator.partial().expand()` with `itemids_override` conf |

The actual target ids are resolved at parse time from `_modes.py`
(`trigger_dag_id`, `stream_dag_id`, `trainer_dag_id`).

---

## 2. Data graph (shared tables / Kafka — no trigger)

End-to-end flow for one pipeline (history shown; trend is the mirror with
`TRENDS_*` tables and `direct24`):

```
        Zabbix HISTORY / HISTORY_UINT (live source tables)
                 │                         │
   (Kafka topic) │                         │ (LOOKBACK read)
                 ▼                         ▼
 kafka_batch_processor_dag_history   predict_minute_history_<mode>_DT
        │  writes                          │  writes
        ▼                                  ▼
 HISTORY_KAFKA_STAGE                HISTORY_PRED_<MODE>
        │                                  │
        └──────────────┬───────────────────┘
                       ▼  reads both
              message_processor_history
                       │  writes
          ┌────────────┴───────────────┐
          ▼                            ▼
 HISTORY_VALUE_EWMA          HISTORY_ERROR_EWMA_<MODE>
          │                            │
          │                            ▼  reads
          │                  select_adaptive_history_<mode>
          │                            │  writes
          │                            ▼
          │            HISTORY_SELECTION_{STATE,LOG}_<MODE>
          │                            │  reads (+ HISTORY_PRED_<MODE>)
          │                            ▼
          │                  predict_stream_history_<mode>
          │                            │  writes
          │                            ▼
          │                  HISTORY_PRED_SELECTED_<MODE>  (final adaptive output)
          │
          └──▶ drift_retrain_trigger  reads HISTORY_VALUE_EWMA + HISTORY_ERROR_EWMA_<MODE>
                       │  triggers
                       ▼
              train_models_<mode>_history
                       │  writes model files (/home/ubuntu/DTmodels/…)
                       ▼
              predict_minute_history_<mode>_DT  loads models on next tick
```

| Producer DAG | Table / artifact | Consumer DAG |
|---|---|---|
| `kafka_batch_processor_dag_history` | `HISTORY_KAFKA_STAGE` | `message_processor_history` |
| `predict_minute_history_<mode>_DT` | `HISTORY_PRED_<MODE>` | `message_processor_history` (error calc), `predict_stream_history_<mode>` |
| `message_processor_history` | `HISTORY_VALUE_EWMA`, `HISTORY_ERROR_EWMA_<MODE>` | `select_adaptive_history_<mode>`, `drift_retrain_trigger` |
| `select_adaptive_history_<mode>` | `HISTORY_SELECTION_STATE/LOG_<MODE>` | `predict_stream_history_<mode>` |
| `predict_stream_history_<mode>` | `HISTORY_PRED_SELECTED_<MODE>` | downstream consumers (Grafana etc.) |
| `train_models_<mode>_history` | model files under `/home/ubuntu/DTmodels/<mode>_history/<itemid>/` | `predict_minute_history_<mode>_DT` |

The trend pipeline is identical with `TRENDS_*` tables, the trend Kafka
processor, and modes `recursive`/`direct24`.

---

## 3. Maintenance DAG (no DAG-to-DAG edges)

| DAG | File | Schedule | Role |
|---|---|---|---|
| `mysql_housekeeping` | [mysql_housekeeping.py](mysql_housekeeping.py) | `0 3 * * *` (daily) | Prunes orphaned `*_KAFKA_STAGE` rows (>24h) and time-bounds the `*_PRED_*` / `*_ERROR_EWMA_*` tables per `predictionRetentionDays`. Touches shared tables but neither triggers nor is triggered by other DAGs. |

---

## 4. Complete DAG inventory

| `dag_id` | File | Schedule | Triggered by | Triggers |
|---|---|---|---|---|
| `kafka_batch_processor_dag_history` | [kafka_batch_processor_history.py](kafka_batch_processor_history.py) | `* * * * *` | — | — (data → message_processor_history) |
| `kafka_batch_processor_dag_trend` | [kafka_batch_processor_trend.py](kafka_batch_processor_trend.py) | `*/15 * * * *` | — | — (data → message_processor_trend) |
| `message_processor_history` | [message_processor_history.py](message_processor_history.py) | `* * * * *` | — | — (data → selectors / drift) |
| `message_processor_trend` | [message_processor_trend.py](message_processor_trend.py) | `*/15 * * * *` | — | — (data → selectors / drift) |
| `predict_history_tick` | [predict_history_tick.py](predict_history_tick.py) | `*/N * * * *` | — | `predict_minute_history_{recursive,direct60}_DT` |
| `predict_trend_tick` | [predict_trend_tick.py](predict_trend_tick.py) | `*/N * * * *` | — | `predict_hourly_trend_{recursive,direct24}_DT` |
| `predict_minute_history_recursive_DT` | [predict_recursive_history.py](predict_recursive_history.py) | None | `predict_history_tick` | — (data → `HISTORY_PRED_RECURSIVE`) |
| `predict_minute_history_direct60_DT` | [predict_direct60_history.py](predict_direct60_history.py) | None | `predict_history_tick` | — (data → `HISTORY_PRED_DIRECT60`) |
| `predict_hourly_trend_recursive_DT` | [predict_recursive_trend.py](predict_recursive_trend.py) | None | `predict_trend_tick` | — (data → `TRENDS_PRED_RECURSIVE`) |
| `predict_hourly_trend_direct24_DT` | [predict_direct24_trend.py](predict_direct24_trend.py) | None | `predict_trend_tick` | — (data → `TRENDS_PRED_DIRECT24`) |
| `select_adaptive_history_recursive` | [select_adaptive_history.py](select_adaptive_history.py) | `* * * * *` | — | `predict_stream_history_recursive` |
| `select_adaptive_history_direct60` | [select_adaptive_history.py](select_adaptive_history.py) | `* * * * *` | — | `predict_stream_history_direct60` |
| `select_adaptive_trend_recursive` | [select_adaptive_trend.py](select_adaptive_trend.py) | `0 * * * *` | — | `predict_stream_trend_recursive` |
| `select_adaptive_trend_direct24` | [select_adaptive_trend.py](select_adaptive_trend.py) | `0 * * * *` | — | `predict_stream_trend_direct24` |
| `predict_stream_history_recursive` | [predict_stream_history.py](predict_stream_history.py) | None | `select_adaptive_history_recursive` | — (data → `HISTORY_PRED_SELECTED_RECURSIVE`) |
| `predict_stream_history_direct60` | [predict_stream_history.py](predict_stream_history.py) | None | `select_adaptive_history_direct60` | — (data → `HISTORY_PRED_SELECTED_DIRECT60`) |
| `predict_stream_trend_recursive` | [predict_stream_trend.py](predict_stream_trend.py) | None | `select_adaptive_trend_recursive` | — (data → `TRENDS_PRED_SELECTED_RECURSIVE`) |
| `predict_stream_trend_direct24` | [predict_stream_trend.py](predict_stream_trend.py) | None | `select_adaptive_trend_direct24` | — (data → `TRENDS_PRED_SELECTED_DIRECT24`) |
| `drift_retrain_trigger` | [drift_retrain_trigger.py](drift_retrain_trigger.py) | `0 */6 * * *` | — | `train_models_*` (per drifted mode) |
| `train_models_recursive_history` | [train_models_recursive_history.py](train_models_recursive_history.py) | None | `drift_retrain_trigger` (also manual) | — (writes models) |
| `train_models_direct60_history` | [train_models_direct60_history.py](train_models_direct60_history.py) | None | `drift_retrain_trigger` (also manual) | — (writes models) |
| `train_models_recursive_trend` | [train_models_recursive_trend.py](train_models_recursive_trend.py) | None | `drift_retrain_trigger` (also manual) | — (writes models) |
| `train_models_direct24_trend` | [train_models_direct24_trend.py](train_models_direct24_trend.py) | None | `drift_retrain_trigger` (also manual) | — (writes models) |
| `mysql_housekeeping` | [mysql_housekeeping.py](mysql_housekeeping.py) | `0 3 * * *` | — | — |

### Helper modules (not DAGs)

`_atomic.py`, `_db.py`, `_dt_vars.py`, `_enabled.py`, `_modes.py`,
`_selector.py`, `_stream.py` — underscore-prefixed so file discovery skips
them. `_modes.py` is the single source of truth for the mode → table/dag-id
mapping that defines every trigger edge above.
