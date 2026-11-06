# Database schema

`dt_schema.sql` holds the structure of the MySQL database `DT`
(24 tables and views, no data). The host address and the `DEFINER` account were
removed, and the views use `SQL SECURITY INVOKER`.

- `HISTORY`, `HISTORY_UINT`, `TRENDS`, `TRENDS_UINT`: a working copy of the
  Zabbix readings (raw history and hourly trends).
- `*_KAFKA_STAGE`: staging tables written by the Kafka consumer task.
- `*_PRED_*`, `*_SELECTION_*`, `*_ERROR_EWMA`, `*_VALUE_EWMA`: forecasts, model
  selection, and drift monitoring used by the forecasting and retraining DAGs.

Load it with: `mysql -u <user> -p DT < dt_schema.sql`
