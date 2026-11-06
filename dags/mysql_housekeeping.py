from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.sdk import Variable

from _db import get_db_config


STAGE_TABLES = ("TRENDS_KAFKA_STAGE", "HISTORY_KAFKA_STAGE")

# Rows older than this in either Kafka stage table are assumed orphaned —
# their owning message_processor_* run failed before reaching its
# cleanup_stage task, so the live pipeline will never delete them.
STAGE_TTL = timedelta(hours=24)

# Prediction-side tables that grow forever without pruning. At ~19 itemids
# × 5 models × 60 horizons × 6 ticks/h, recursive_history alone adds ~34k
# rows/h. Tagged by CLOCK (unix-seconds bigint or int unsigned). Retention
# window is read from the Airflow Variable `predictionRetentionDays`
# (default 30).
PRUNABLE_PREDICTION_TABLES = (
    "HISTORY_PRED_RECURSIVE",
    "HISTORY_PRED_DIRECT60",
    "TRENDS_PRED_RECURSIVE",
    "TRENDS_PRED_DIRECT24",
    "HISTORY_ERROR_EWMA_RECURSIVE",
    "HISTORY_ERROR_EWMA_DIRECT60",
    "TRENDS_ERROR_EWMA_RECURSIVE",
    "TRENDS_ERROR_EWMA_DIRECT24",
    "HISTORY_SELECTION_LOG_RECURSIVE",
    "HISTORY_SELECTION_LOG_DIRECT60",
    "TRENDS_SELECTION_LOG_RECURSIVE",
    "TRENDS_SELECTION_LOG_DIRECT24",
    "HISTORY_PRED_SELECTED_RECURSIVE",
    "HISTORY_PRED_SELECTED_DIRECT60",
    "TRENDS_PRED_SELECTED_RECURSIVE",
    "TRENDS_PRED_SELECTED_DIRECT24",
    "HISTORY_VALUE_EWMA",
    "TRENDS_VALUE_EWMA",
)
RETENTION_DAYS_VAR = "predictionRetentionDays"
DEFAULT_RETENTION_DAYS = 30

# Tables that churn heavily under DELETE (the prune set above). Weekly
# OPTIMIZE TABLE reclaims fragmented InnoDB pages so the working set stays
# resident. Skipped if `optimizeTablesWeekly` Variable is set to "false".
OPTIMIZABLE_TABLES = PRUNABLE_PREDICTION_TABLES
OPTIMIZE_FLAG_VAR = "optimizeTablesWeekly"


@dag(
    dag_id="mysql_housekeeping",
    start_date=pendulum.datetime(2025, 7, 21, tz="UTC"),
    catchup=False,
    schedule="0 3 * * *",  # daily at 03:00 UTC
    tags=["housekeeping", "maintenance"],
    max_active_runs=1,
    doc_md=(
        "Reaps orphaned rows from auxiliary MySQL tables that the live "
        "pipeline is supposed to clean up but may have failed to, prunes "
        "old prediction / selector rows beyond the retention window, and "
        "once a week OPTIMIZE TABLEs the high-churn tables to reclaim "
        "fragmented InnoDB pages."
    ),
)
def mysql_housekeeping():
    @task
    def cleanup_kafka_stage(table: str) -> int:
        """Delete rows in the given stage table older than STAGE_TTL.

        Trend and history each have their own per-batch cleanup_stage task
        that fires on success. Anything still here past the TTL belongs to
        a run that crashed mid-flight; reprocessing would be unsafe (its
        downstream tasks may already have run), so we just drop the rows.
        """
        import mysql.connector

        cutoff_seconds = int(STAGE_TTL.total_seconds())
        conn = mysql.connector.connect(**get_db_config())
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"DELETE FROM {table} "
                f"WHERE STAGED_AT < NOW() - INTERVAL %s SECOND",
                (cutoff_seconds,),
            )
            deleted = cursor.rowcount
            conn.commit()
            print(
                f"Reaped {deleted} orphan rows from {table} "
                f"(older than {STAGE_TTL})"
            )
            return deleted
        finally:
            cursor.close()
            conn.close()

    @task
    def prune_predictions(table: str) -> int:
        """Delete rows from a prediction/selector table whose CLOCK is older
        than the retention window.

        CLOCK is stored as unix seconds (bigint or int unsigned), so the
        cutoff is just `UNIX_TIMESTAMP() - N * 86400`. Logs the deleted
        count and the cutoff in human form.
        """
        import mysql.connector

        try:
            days = max(1, int(Variable.get(RETENTION_DAYS_VAR)))
        except Exception as exc:
            print(
                f"{RETENTION_DAYS_VAR} not set/unparseable ({exc}); "
                f"defaulting to {DEFAULT_RETENTION_DAYS}"
            )
            days = DEFAULT_RETENTION_DAYS

        conn = mysql.connector.connect(**get_db_config())
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"DELETE FROM {table} "
                f"WHERE CLOCK < UNIX_TIMESTAMP() - %s",
                (days * 86400,),
            )
            deleted = cursor.rowcount
            conn.commit()
            print(
                f"Pruned {deleted} rows from {table} older than {days}d "
                f"(cutoff = UNIX_TIMESTAMP() - {days * 86400})"
            )
            return deleted
        finally:
            cursor.close()
            conn.close()

    @task
    def optimize_table(table: str) -> str:
        """Run OPTIMIZE TABLE to reclaim fragmented InnoDB pages after
        prune. Only runs on Sundays (UTC) so the cost amortizes weekly,
        and can be disabled by setting `optimizeTablesWeekly=false`.

        OPTIMIZE TABLE on InnoDB rewrites the table to a fresh tablespace
        (effectively `ALTER TABLE ... FORCE`); it briefly locks the table
        but does NOT block reads thanks to MVCC. Safe to run during normal
        operation but adds I/O.
        """
        import datetime
        import mysql.connector

        if datetime.datetime.utcnow().weekday() != 6:  # Sunday
            return f"{table}: skip (not Sunday UTC)"

        try:
            flag = str(Variable.get(OPTIMIZE_FLAG_VAR)).strip().lower()
        except Exception:
            flag = "true"
        if flag == "false":
            return f"{table}: skip ({OPTIMIZE_FLAG_VAR}=false)"

        conn = mysql.connector.connect(**get_db_config())
        cursor = conn.cursor()
        try:
            cursor.execute(f"OPTIMIZE TABLE {table}")
            rows = cursor.fetchall()
            # OPTIMIZE returns a status row; surface it in the log.
            for r in rows:
                print(f"OPTIMIZE {table}: {r}")
            return f"{table}: optimized"
        finally:
            cursor.close()
            conn.close()

    cleanup_kafka_stage.expand(table=list(STAGE_TABLES))
    prune_predictions.expand(table=list(PRUNABLE_PREDICTION_TABLES))
    optimize_table.expand(table=list(OPTIMIZABLE_TABLES))


mysql_housekeeping()
