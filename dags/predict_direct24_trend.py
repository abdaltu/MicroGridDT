"""Direct 48->24 forecasting using models trained by train_models_direct24_trend.

Mirrors MultiModel_v2_Direct48to24.ipynb cell 4 (direct multi-step):
  - LSTM / GRU / PatchTST: single forward pass on a (1, 48, 9) sequence
    produces all 24 forecasts at once.
  - LightGBM: single MultiOutputRegressor.predict() on flattened (1, 48*9)
    features produces all 24 forecasts.
  - SARIMAX: .apply() recent actuals to advance state, then a single
    get_forecast(steps=24) with future cyclical exog.

Triggered by `predict_trend_tick` when `direct24` is in `enabledModesTrend`
(default ["recursive","direct24"]). One DAG run = 24 forecast rows per
available model into TRENDS_PRED_DIRECT24.
"""
from __future__ import annotations

import json
import os
import pickle
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.sdk import Variable

import warnings

from _db import get_db_config
from _dt_vars import get_dt_variables, itemid_label
from _enabled import resolve_enabled_set


PRED_TABLE = "TRENDS_PRED_DIRECT24"
MODELS_DIR = "/home/ubuntu/DTmodels/direct24"

# Feature list this predictor knows how to produce. Must match the
# trainer's FEATURE_COLS exactly — see staleness check below.
EXPECTED_FEATURE_COLS = [
    "hour_sin", "hour_cos",
    "day_of_week_sin", "day_of_week_cos",
    "month_sin", "month_cos",
    "day_of_year_sin", "day_of_year_cos",
    "VALUE_AVG",
]

LOOKBACK = 48
HORIZON  = 24
SARIMAX_EXTEND_HOURS = 24 * 30
KERAS_MODELS = ("LSTM", "GRU", "PatchTST")

# Which models contribute to the ensemble. Configurable at runtime via the
# Airflow Variable `enabledModelsTrend` (JSON array of model names). Defaults
# to all 5; unknown names are ignored. Disabled models are skipped here even
# if their .pkl/.keras file exists on disk from a previous training run.
ALL_MODELS = ("SARIMAX", "LightGBM", "LSTM", "GRU", "PatchTST")
ENABLED_MODELS_VAR = "enabledModelsTrend"
ENABLED_MODELS = frozenset(resolve_enabled_set(
    ENABLED_MODELS_VAR, ALL_MODELS, ALL_MODELS,
))

# Zabbix's two live trend tables. value_type=0 → float, =3 → UINT, =1 → char (skip).
_VALUE_TYPE_TO_LIVE_TABLE = {0: "TRENDS", 3: "TRENDS_UINT"}


def _live_table_for(itemid, dt_vars: dict) -> str | None:
    """Resolve the live Zabbix trend table to read for this itemid based on
    its value_type from DT_Variables. Returns None for char-typed itemids
    (caller should skip)."""
    vt = (dt_vars.get(str(itemid)) or {}).get("value_type")
    return _VALUE_TYPE_TO_LIVE_TABLE.get(vt)


def _add_cyclical(df):
    import numpy as np
    df = df.copy()
    df["hour_sin"]        = np.sin(2 * np.pi * df.index.hour / 24)
    df["hour_cos"]        = np.cos(2 * np.pi * df.index.hour / 24)
    df["day_of_week_sin"] = np.sin(2 * np.pi * df.index.dayofweek / 7)
    df["day_of_week_cos"] = np.cos(2 * np.pi * df.index.dayofweek / 7)
    df["month_sin"]       = np.sin(2 * np.pi * (df.index.month - 1) / 12)
    df["month_cos"]       = np.cos(2 * np.pi * (df.index.month - 1) / 12)
    df["day_of_year_sin"] = np.sin(2 * np.pi * (df.index.dayofyear - 1) / 365.25)
    df["day_of_year_cos"] = np.cos(2 * np.pi * (df.index.dayofyear - 1) / 365.25)
    return df


def _fetch_history(itemid, clock_unix, hours, live_table: str):
    import mysql.connector
    import pandas as pd

    conn = mysql.connector.connect(**get_db_config())
    try:
        df = pd.read_sql(
            f"SELECT CLOCK, VALUE_AVG FROM {live_table} "
            f"WHERE ITEMID={itemid} AND CLOCK <= {clock_unix} "
            f"ORDER BY CLOCK DESC LIMIT {hours}",
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return None
    df["CLOCK_dt"] = pd.to_datetime(df["CLOCK"], unit="s")
    df = df.sort_values("CLOCK_dt").set_index("CLOCK_dt")
    df["VALUE_AVG"] = df["VALUE_AVG"].astype(float)
    # TRENDS' PK is on auto-incremented `rid`, not (ITEMID, CLOCK), so
    # duplicate (itemid, hour) rows can appear. `.asfreq("h")` raises on
    # duplicates; `.resample("h").mean()` collapses them defensively.
    df = df.resample("h").mean().interpolate(method="linear").ffill().bfill()
    return _add_cyclical(df)


def _insert_predictions(itemid, future_ts, preds_by_model):
    import mysql.connector

    rows = []
    for model_name, preds in preds_by_model.items():
        for ts, val in zip(future_ts, preds):
            rows.append((itemid, int(ts.timestamp()), float(val), model_name))
    if not rows:
        return 0
    conn = mysql.connector.connect(**get_db_config())
    cursor = conn.cursor()
    try:
        cursor.executemany(
            f"INSERT INTO {PRED_TABLE} "
            f"(ITEMID, CLOCK, VALUE_AVG, MODEL) VALUES (%s, %s, %s, %s) "
            f"ON DUPLICATE KEY UPDATE VALUE_AVG = VALUES(VALUE_AVG)",
            rows,
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    return len(rows)


@task
def forecast_and_insert():
    import numpy as np
    import pandas as pd
    from airflow.sdk import get_current_context

    # `message` flows in via the triggering DAG's conf=
    # ({"message": {"ITEMID": id, "CLOCK": ts}}). Factory-function args are
    # bound at DAG parse time, not at task runtime, so we read it here from
    # the current task context instead.
    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    message = (dag_run.conf or {}).get("message") if dag_run else None
    if not message:
        params = ctx.get("params") or {}
        message = params.get("message")

    if not message:
        print("Empty message; skipping.")
        return 0
    itemid = message.get("ITEMID")
    clock  = message.get("CLOCK")
    if itemid is None or clock is None:
        print(f"Missing ITEMID/CLOCK: {message}")
        return 0

    dt_vars = get_dt_variables()
    live_table = _live_table_for(itemid, dt_vars)
    if live_table is None:
        print(
            f"ITEMID {itemid_label(itemid, dt_vars)} has unsupported value_type "
            f"(non-numeric); skipping."
        )
        return 0

    model_dir = os.path.join(MODELS_DIR, str(itemid))
    if not os.path.isdir(model_dir):
        print(f"No direct24 models for ITEMID {itemid_label(itemid, dt_vars)}; skipping.")
        return 0

    with open(os.path.join(model_dir, "feature_cols.json")) as f:
        spec = json.load(f)
    feature_cols = spec["FEATURE_COLS"]
    exog_cols    = spec["EXOG_COLS"]
    target_col   = spec["TARGET_COL"]
    if feature_cols != EXPECTED_FEATURE_COLS:
        print(
            f"Stale model for ITEMID {itemid_label(itemid, dt_vars)}: saved "
            f"FEATURE_COLS={feature_cols!r} differs from predictor's "
            f"EXPECTED_FEATURE_COLS={EXPECTED_FEATURE_COLS!r}; retrain to refresh."
        )
        return 0
    with open(os.path.join(model_dir, "scaler_X.pkl"), "rb") as f:
        scaler_X = pickle.load(f)
    with open(os.path.join(model_dir, "scaler_y.pkl"), "rb") as f:
        scaler_y = pickle.load(f)

    fetch_hours = max(SARIMAX_EXTEND_HOURS, LOOKBACK) + HORIZON
    df = _fetch_history(itemid, clock, fetch_hours, live_table)
    if df is None or len(df) < LOOKBACK + 1:
        print(f"Insufficient live data for ITEMID {itemid_label(itemid, dt_vars)} in {live_table}.")
        return 0

    origin_ts = df.index[-1]
    future_ts = [origin_ts + pd.Timedelta(hours=h + 1) for h in range(HORIZON)]

    preds_by_model = {}

    # ---- SARIMAX (single get_forecast(24) on advanced state) ----
    sarimax_path = os.path.join(model_dir, "SARIMAX.pkl")
    if "SARIMAX" in ENABLED_MODELS and os.path.exists(sarimax_path):
        try:
            with open(sarimax_path, "rb") as f:
                arima = pickle.load(f)
            tail = df.iloc[-SARIMAX_EXTEND_HOURS:]
            ext = arima.apply(
                tail[target_col], exog=tail[exog_cols], refit=False,
            )
            future_idx = pd.DatetimeIndex(future_ts, freq="h")
            future_exog = pd.DataFrame(index=future_idx)
            future_exog = _add_cyclical(future_exog)[exog_cols]
            fc = ext.get_forecast(steps=HORIZON, exog=future_exog).predicted_mean
            preds_by_model["SARIMAX"] = list(map(float, fc.values))
        except Exception as exc:
            print(f"SARIMAX failed for ITEMID {itemid}: {exc}")

    # Build the (LOOKBACK, n_features) input window for keras + LightGBM.
    # `scaler_X` was fitted on a DataFrame at train time, so it warns when
    # called on an ndarray. Match the LightGBM filter pattern — Airflow's
    # task runner resets module-level filters before this executes.
    df_tail = df.iloc[-LOOKBACK:][feature_cols]
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="X does not have valid feature names",
            category=UserWarning,
        )
        seq_sc = scaler_X.transform(df_tail.values).astype(np.float32)

    # ---- LightGBM (MultiOutputRegressor) ----
    lgb_path = os.path.join(model_dir, "LightGBM.pkl")
    if "LightGBM" in ENABLED_MODELS and os.path.exists(lgb_path):
        try:
            with open(lgb_path, "rb") as f:
                lgbm = pickle.load(f)
            X_flat = seq_sc.reshape(1, -1)
            # Inner LGBMRegressor auto-assigns Column_0..N during fit on
            # ndarray, then sklearn warns at predict time when input is
            # also ndarray. Filter at call site — Airflow's task runner
            # resets module-level filters before this executes.
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="X does not have valid feature names",
                    category=UserWarning,
                )
                preds = lgbm.predict(X_flat).reshape(-1)  # (HORIZON,) raw values
            preds_by_model["LightGBM"] = list(map(float, preds))
        except Exception as exc:
            print(f"LightGBM failed for ITEMID {itemid}: {exc}")

    # ---- Keras (LSTM / GRU / PatchTST), single forward pass for HORIZON values ----
    # Filtered by ENABLED_MODELS so we don't spin up the TF runtime if no
    # enabled keras model is available.
    keras_paths = [
        (m, os.path.join(model_dir, f"{m}.keras"))
        for m in KERAS_MODELS if m in ENABLED_MODELS
    ]
    if any(os.path.exists(p) for _, p in keras_paths):
        # Cap TF threads BEFORE the Keras backend initializes — otherwise
        # each per-itemid prediction task grabs all cores and they thrash.
        import tensorflow as tf
        tf.config.threading.set_intra_op_parallelism_threads(2)
        tf.config.threading.set_inter_op_parallelism_threads(1)
        from keras.models import load_model
        for model_name, path in keras_paths:
            if not os.path.exists(path):
                continue
            try:
                model = load_model(path)
                x = seq_sc.reshape(1, LOOKBACK, -1)
                # __call__ avoids per-call dispatch overhead of .predict();
                # numerically identical in eager mode.
                pred_sc = model(x, training=False).numpy().reshape(-1, 1)  # (HORIZON, 1)
                pred = scaler_y.inverse_transform(pred_sc).reshape(-1)  # (HORIZON,)
                preds_by_model[model_name] = list(map(float, pred))
            except Exception as exc:
                print(f"{model_name} failed for ITEMID {itemid}: {exc}")

    n = _insert_predictions(itemid, future_ts, preds_by_model)
    print(
        f"ITEMID {itemid_label(itemid)}: inserted {n} prediction rows from "
        f"{len(preds_by_model)} models for horizons {future_ts[0]}..{future_ts[-1]}"
    )
    return n


@dag(
    dag_id="predict_hourly_trend_direct24_DT",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False,
    tags=["forecast", "direct24", "trend"],
    params={"message": Param(default=None, type=["object", "null"])},
    doc_md=__doc__,
)
def predict_hourly_trend_direct24_DT(message: dict | None = None):
    forecast_and_insert()


predict_hourly_trend_direct24_DT()
