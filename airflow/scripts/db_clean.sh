#!/bin/bash
# Weekly online prune of Airflow metadata (keeps last 7 days). Runs while
# Airflow is up (no vacuum, so no downtime); bounds row growth of the SQLite
# metadata DB driven by the minute/tick DAGs. To reclaim file size, run a
# manual stop+clean+VACUUM instead.
source /home/ubuntu/miniconda3/etc/profile.d/conda.sh
conda activate py312
export AIRFLOW_HOME=/home/ubuntu/airflow
export PYTHONPATH=/home/ubuntu/airflow/dags
CUTOFF=$(date -d "7 days ago" "+%Y-%m-%dT%H:%M:%S%:z")
echo "db clean cutoff: $CUTOFF"
airflow db clean --clean-before-timestamp "$CUTOFF" --skip-archive -y
