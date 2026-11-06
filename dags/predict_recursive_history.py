"""Recursive 1-step x 60-minute forecasting using models trained by
train_models_recursive_history.

Mirror of predict_recursive_trend.py but at minute resolution against Zabbix
HISTORY / HISTORY_UINT live tables, with 60-step recursion (instead of 24).
TIME_STEPS=360 (6-hour input window). 11 features (8 hourly cyclical +
2 minute cyclical + target).

Triggered by `predict_history_tick` when `recursive` is in
`enabledModesHistory` (default ["recursive","direct60"]). One DAG run =
up to 60 forecast rows per available model into HISTORY_PRED_RECURSIVE.
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


PRED_TABLE = "HISTORY_PRED_RECURSIVE"
MODELS_DIR = "/home/ubuntu/DTmodels/recursive_history"

# Feature list the predictor knows how to produce in `_feature_row`. Must
# match the trainer's FEATURE_COLS exactly. Predictor short-circuits any
# itemid whose saved model has a different list — see the staleness check
# in `forecast_and_insert`. Bump the trainer's FEATURE_COLS + retrain to
# evolve the schema.
EXPECTED_FEATURE_COLS = [
    "minute_sin", "minute_cos",
    "hour_sin", "hour_cos",
    "day_of_week_sin", "day_of_week_cos",
    "month_sin", "month_cos",
    "day_of_year_sin", "day_of_year_cos",
    "VALUE",
]

TIME_STEPS = 360                # 6-hour lookback at 1-minute resolution
HORIZON    = 60                 # 1-hour forecast
SARIMAX_EXTEND_MINUTES = 60 * 24 * 3   # bring SARIMAX state up to "now" with the last 3 days of actuals
KERAS_MODELS = ("LSTM", "GRU", "PatchTST")

# Which models contribute to the ensemble. Configurable at runtime via the
# Airflow Variable `enabledModelsHistory` (JSON array of model names). Defaults
# to all 5; unknown names are ignored. Disabled models are skipped here even
# if their .pkl/.keras file exists on disk from a previous training run.
ALL_MODELS = ("SARIMAX", "LightGBM", "LSTM", "GRU", "PatchTST")
ENABLED_MODELS_VAR = "enabledModelsHistory"
ENABLED_MODELS = frozenset(resolve_enabled_set(
    ENABLED_MODELS_VAR, ALL_MODELS, ALL_MODELS,
))

# Zabbix's two live history tables. value_type=0 → float, =3 → UINT, =1 → char (skip).
_VALUE_TYPE_TO_LIVE_TABLE = {0: "HISTORY", 3: "HISTORY_UINT"}


def _live_table_for(itemid, dt_vars: dict) -> str | None:
    """Resolve the live Zabbix history table to read for this itemid based on
    its value_type from DT_Variables. Returns None for char-typed itemids
    (caller should skip)."""
    vt = (dt_vars.get(str(itemid)) or {}).get("value_type")
    return _VALUE_TYPE_TO_LIVE_TABLE.get(vt)


def _add_cyclical(df):
    import numpy as np
    df = df.copy()
    df["minute_sin"]      = np.sin(2 * np.pi * df.index.minute / 60)
    df["minute_cos"]      = np.cos(2 * np.pi * df.index.minute / 60)
    df["hour_sin"]        = np.sin(2 * np.pi * df.index.hour / 24)
    df["hour_cos"]        = np.cos(2 * np.pi * df.index.hour / 24)
    df["day_of_week_sin"] = np.sin(2 * np.pi * df.index.dayofweek / 7)
    df["day_of_week_cos"] = np.cos(2 * np.pi * df.index.dayofweek / 7)
    df["month_sin"]       = np.sin(2 * np.pi * (df.index.month - 1) / 12)
    df["month_cos"]       = np.cos(2 * np.pi * (df.index.month - 1) / 12)
    df["day_of_year_sin"] = np.sin(2 * np.pi * (df.index.dayofyear - 1) / 365.25)
    df["day_of_year_cos"] = np.cos(2 * np.pi * (df.index.dayofyear - 1) / 365.25)
    return df


def _feature_row(ts, value, feature_cols):
    import numpy as np
    cyc = {
        "minute_sin":      np.sin(2 * np.pi * ts.minute / 60),
        "minute_cos":      np.cos(2 * np.pi * ts.minute / 60),
        "hour_sin":        np.sin(2 * np.pi * ts.hour / 24),
        "hour_cos":        np.cos(2 * np.pi * ts.hour / 24),
        "day_of_week_sin": np.sin(2 * np.pi * ts.dayofweek / 7),
        "day_of_week_cos": np.cos(2 * np.pi * ts.dayofweek / 7),
        "month_sin":       np.sin(2 * np.pi * (ts.month - 1) / 12),
        "month_cos":       np.cos(2 * np.pi * (ts.month - 1) / 12),
        "day_of_year_sin": np.sin(2 * np.pi * (ts.dayofyear - 1) / 365.25),
        "day_of_year_cos": np.cos(2 * np.pi * (ts.dayofyear - 1) / 365.25),
        "VALUE":           value,
    }
    return [cyc[c] for c in feature_cols]


def _fetch_history(itemid, clock_unix, minutes, live_table: str):
    import mysql.connector
    import pandas as pd

    conn = mysql.connector.connect(**get_db_config())
    try:
        df = pd.read_sql(
            f"SELECT CLOCK, VALUE FROM {live_table} "
            f"WHERE ITEMID={itemid} AND CLOCK <= {clock_unix} "
            f"ORDER BY CLOCK DESC LIMIT {minutes}",
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return None
    df["CLOCK_dt"] = pd.to_datetime(df["CLOCK"], unit="s")
    df = df.sort_values("CLOCK_dt").set_index("CLOCK_dt")
    df["VALUE"] = df["VALUE"].astype(float)
    # Zabbix history can poll sub-minute; collapse duplicates onto the
    # 1-min grid via mean aggregation (asfreq would raise on duplicates).
    df = df[["VALUE"]].resample("1min").mean().interpolate(method="linear").ffill().bfill()
    return _add_cyclical(df)


def _recurse_keras(model, df_tail, scaler_X, scaler_y, feature_cols, future_ts):
    import numpy as np

    seq_sc = scaler_X.transform(df_tail[feature_cols].values)  # (TIME_STEPS, n_feat)
    out = []
    for ts in future_ts:
        x = seq_sc.reshape(1, seq_sc.shape[0], seq_sc.shape[1])
        # __call__ avoids the per-call dispatch overhead of .predict() on
        # tiny single-sample inputs; numerically identical in eager mode.
        pred_sc = float(model(x, training=False).numpy().reshape(-1)[0])
        pred = float(scaler_y.inverse_transform([[pred_sc]])[0, 0])
        out.append(pred)
        new_row = _feature_row(ts, pred, feature_cols)
        new_row_sc = scaler_X.transform(np.array([new_row]))[0]
        seq_sc = np.vstack([seq_sc[1:], new_row_sc])
    return out


def _recurse_lightgbm(model, origin_val, feature_cols, future_ts):
    import numpy as np

    out = []
    cur = origin_val
    for ts in future_ts:
        row = np.array(_feature_row(ts, cur, feature_cols)).reshape(1, -1)
        pred = float(model.predict(row)[0])
        out.append(pred)
        cur = pred
    return out


def _recurse_sarimax(arima, df, origin_ts, origin_val, feature_cols, future_ts):
    """1-step recursion mirroring recurse_arima in run_all_variables_tuned.py.

    Trained model expects endog = VALUE_shifted (next step) and exog at the
    same index as the current step. We extend Kalman state with the most
    recent SARIMAX_EXTEND_MINUTES of actuals, then iterate 1-step forecasts
    feeding our own predictions back as endog.
    """
    import pandas as pd

    endog_full = df["VALUE"].shift(-1).dropna()
    exog_full  = df[feature_cols].loc[endog_full.index]
    tail_endog = endog_full.iloc[-SARIMAX_EXTEND_MINUTES:]
    tail_exog  = exog_full.loc[tail_endog.index]
    ext = arima.apply(tail_endog, exog=tail_exog, refit=False)

    out = []
    cur = origin_val
    cur_ts = origin_ts
    for ts in future_ts:
        # exog row at cur_ts is used to forecast endog[cur_ts] = VALUE[ts]
        exog_row = pd.DataFrame(
            [_feature_row(cur_ts, cur, feature_cols)],
            index=pd.DatetimeIndex([cur_ts], freq="min"),
            columns=feature_cols,
        )
        fc = float(ext.get_forecast(steps=1, exog=exog_row).predicted_mean.iloc[0])
        out.append(fc)
        # endog_pd must carry the same Series name as tail_endog (the endog
        # given to arima.apply above). statsmodels labels the endog column by
        # the Series name, and .append() row-concatenates the old+new frames;
        # a name mismatch ("VALUE" vs None) raises "Columns must match to
        # concatenate along rows" and silently kills the SARIMAX forecast.
        endog_pd = pd.Series([fc], index=exog_row.index, name=tail_endog.name)
        ext = ext.append(endog=endog_pd, exog=exog_row, refit=False)
        cur = fc
        cur_ts = ts
    return out


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
            f"(ITEMID, CLOCK, VALUE, MODEL) VALUES (%s, %s, %s, %s) "
            f"ON DUPLICATE KEY UPDATE VALUE = VALUES(VALUE)",
            rows,
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    return len(rows)


@task
def forecast_and_insert():
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
        print(f"No recursive_history models for ITEMID {itemid_label(itemid, dt_vars)}; skipping.")
        return 0

    with open(os.path.join(model_dir, "feature_cols.json")) as f:
        spec = json.load(f)
    feature_cols = spec["FEATURE_COLS"]
    # Stale-model guard: skip itemids whose on-disk model was trained
    # against a different FEATURE_COLS list than this predictor knows
    # how to produce. Without this check, `arima.apply()` and
    # `scaler_X.transform()` would explode deep inside the recursion with
    # confusing pandas/sklearn errors (e.g. the "Columns must match"
    # failure seen for stale models pre-SARIMAX-optimization). Caller
    # action: retrain the itemid.
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

    fetch_minutes = max(SARIMAX_EXTEND_MINUTES, TIME_STEPS) + HORIZON
    df = _fetch_history(itemid, clock, fetch_minutes, live_table)
    if df is None or len(df) < TIME_STEPS + 1:
        print(f"Insufficient live data for ITEMID {itemid_label(itemid, dt_vars)} in {live_table}.")
        return 0

    origin_ts  = df.index[-1]
    origin_val = float(df.iloc[-1]["VALUE"])
    future_ts  = [origin_ts + pd.Timedelta(minutes=h + 1) for h in range(HORIZON)]

    preds_by_model = {}

    # SARIMAX
    sarimax_path = os.path.join(model_dir, "SARIMAX.pkl")
    if "SARIMAX" in ENABLED_MODELS and os.path.exists(sarimax_path):
        try:
            with open(sarimax_path, "rb") as f:
                arima = pickle.load(f)
            preds_by_model["SARIMAX"] = _recurse_sarimax(
                arima, df, origin_ts, origin_val, feature_cols, future_ts,
            )
        except Exception as exc:
            print(f"SARIMAX failed for ITEMID {itemid}: {exc}")

    # LightGBM
    lgb_path = os.path.join(model_dir, "LightGBM.pkl")
    if "LightGBM" in ENABLED_MODELS and os.path.exists(lgb_path):
        try:
            with open(lgb_path, "rb") as f:
                lgbm = pickle.load(f)
            # Inner LGBMRegressor auto-assigns Column_0..N during fit on
            # ndarray, then sklearn warns at each recursion step when the
            # predict input is also ndarray. Filter around the whole
            # recursion — Airflow's task runner resets module-level filters.
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="X does not have valid feature names",
                    category=UserWarning,
                )
                preds_by_model["LightGBM"] = _recurse_lightgbm(
                    lgbm, origin_val, feature_cols, future_ts,
                )
        except Exception as exc:
            print(f"LightGBM failed for ITEMID {itemid}: {exc}")

    # Keras (LSTM / GRU / PatchTST) — filtered by ENABLED_MODELS so we don't
    # spin up the TF runtime if no enabled keras model is available.
    df_tail = df.iloc[-TIME_STEPS:]
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
        # MinMaxScaler was fitted on a DataFrame at train time, so it
        # warns on every .transform(ndarray) call. Mirror the LightGBM
        # filter — Airflow's task runner resets module-level filters.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="X does not have valid feature names",
                category=UserWarning,
            )
            for model_name, path in keras_paths:
                if not os.path.exists(path):
                    continue
                try:
                    model = load_model(path)
                    preds_by_model[model_name] = _recurse_keras(
                        model, df_tail, scaler_X, scaler_y, feature_cols, future_ts,
                    )
                except Exception as exc:
                    print(f"{model_name} failed for ITEMID {itemid}: {exc}")

    n = _insert_predictions(itemid, future_ts, preds_by_model)
    print(
        f"ITEMID {itemid_label(itemid, dt_vars)}: inserted {n} prediction rows from "
        f"{len(preds_by_model)} models for horizons {future_ts[0]}..{future_ts[-1]}"
    )
    return n


@dag(
    dag_id="predict_minute_history_recursive_DT",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False,
    tags=["forecast", "recursive", "history"],
    params={"message": Param(default=None, type=["object", "null"])},
    doc_md=__doc__,
)
def predict_minute_history_recursive_DT(message: dict | None = None):
    forecast_and_insert()


predict_minute_history_recursive_DT()
