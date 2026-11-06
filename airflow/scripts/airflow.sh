#!/bin/bash
source /home/ubuntu/miniconda3/etc/profile.d/conda.sh
conda activate py312 
export AIRFLOW_HOME=/home/ubuntu/airflow
export PYTHONPATH=/home/ubuntu/airflow/dags
airflow standalone
