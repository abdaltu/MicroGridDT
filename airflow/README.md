# Airflow host scripts

In the reference deployment, Airflow runs directly on an Ubuntu VM, not in
containers. It uses the conda environment `py312`, sets
`AIRFLOW_HOME=/home/ubuntu/airflow`, and uses the LocalExecutor.

- `scripts/airflow.sh` — runs `airflow standalone` in a single process.
- `scripts/db_clean.sh` — prunes Airflow metadata once a week and keeps 7 days.
- `scripts/prune_logs.sh` — rotates the logs.

`airflow.cfg` is not included, because it holds the instance's secret key and
Fernet key. Generate a fresh one with `airflow config list --defaults`.
