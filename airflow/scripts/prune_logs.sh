#!/bin/bash
# Prune Airflow task logs older than the retention window, then remove empty
# dirs left behind. Airflow OSS never cleans task logs itself, so this timer
# keeps /home/ubuntu/airflow/logs from growing unbounded (660k+ tiny files).
LOGDIR=/home/ubuntu/airflow/logs
RETENTION_DAYS=7
find "$LOGDIR" -type f -mtime +"$RETENTION_DAYS" -delete 2>/dev/null
find "$LOGDIR" -mindepth 1 -type d -empty -delete 2>/dev/null
exit 0
