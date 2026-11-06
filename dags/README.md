# Airflow DAGs

These are the DAGs of the deployment.
[`DAG_DEPENDENCIES.md`](DAG_DEPENDENCIES.md) explains how they trigger one
another and which tables they share.

Two pipelines run in parallel: **history** (readings once per minute) and
**trend** (hourly aggregates).

| Stage | Files |
|---|---|
| Ingestion (Kafka → MySQL staging) | `kafka_batch_processor_history.py`, `kafka_batch_processor_trend.py` |
| Digestion (staging → working tables, EWMA) | `message_processor_history.py`, `message_processor_trend.py` |
| Forecasting | `predict_*_tick.py`, `predict_recursive_*.py`, `predict_direct60_history.py`, `predict_direct24_trend.py`, `predict_stream_*.py` |
| Adaptive model selection | `select_adaptive_*.py`, `_selector.py` |
| Training and drift-triggered retraining | `train_models_*.py`, `drift_retrain_trigger.py` |
| Housekeeping | `mysql_housekeeping.py` |
| Shared helpers | `_db.py`, `_dt_vars.py`, `_enabled.py`, `_modes.py`, `_stream.py`, `_atomic.py` |

## Requirements

Use Airflow 3 with the providers `apache-airflow-providers-apache-kafka`,
`apache-airflow-providers-mysql`, and `apache-airflow-providers-standard`; see
[`requirements.txt`](requirements.txt).

## Airflow configuration expected by the DAGs

The code stores no credentials. Create the following in Airflow
(Admin → Connections / Variables).

Connections:
- `mysql_dt` — the MySQL connection to the `DT` database (`schema/dt_schema.sql`).
- `kafkaHistory`, `kafkaTrend` — the Kafka connections (`bootstrap.servers`,
  `group.id`) for the topics `ora-zab-history`, `ora-zab-history-uint`,
  `ora-zab-trend`, and `ora-zab-trend-uint`.

Variables (JSON or scalar values; see
[`variables.example.json`](variables.example.json) when available):
- Items and pipeline: `DT_Variables`, `DT_Var_Ids`, `activePipeLine`, `enabledModesHistory`, `enabledModesTrend`
- Models: `enabledModelsHistory`, `enabledModelsTrend`, `ewmaTunedParamsHistory`, `ewmaTunedParamsTrend`
- Scheduling: `historyPredictionIntervalMinutes`, `trendPredictionIntervalHours`,
  `streamStalenessSecondsHistory`, `streamStalenessSecondsTrend`, `trainingNiceIncrement`
- Drift and housekeeping: `driftRetrainConfig`, `predictionRetentionDays`, `optimizeTablesWeekly`
