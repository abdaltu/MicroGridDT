"""Trains the 5-model direct 720->60 forecasting suite per ITEMID at minute
resolution for the history pipeline.

Mirrors `train_models_direct24_trend.py` (Dense(HORIZON) + MultiOutputRegressor for
LightGBM) but with:
  - Minute-resolution data from Zabbix `HISTORY` / `HISTORY_UINT` tables.
  - 30 days of training data (~43k rows/itemid).
  - LOOKBACK=720 (12-hour input), HORIZON=60 (1-hour forecast).
  - Extra cyclical features: minute_sin, minute_cos for intra-hour cycles.
  - SARIMAX ARIMA(1,1,1) — seasonal component dropped; minute_sin/cos +
    hour_sin/cos exog regressors carry the periodic signal at far lower
    fit cost than seasonal_order=(1,1,1,60) on a minute grid.

Manually triggered. One Airflow task per ITEMID.

Output layout (one folder per ITEMID):
  /home/ubuntu/DTmodels/direct60_history/{itemid}/feature_cols.json
  /home/ubuntu/DTmodels/direct60_history/{itemid}/scaler_X.pkl
  /home/ubuntu/DTmodels/direct60_history/{itemid}/scaler_y.pkl
  /home/ubuntu/DTmodels/direct60_history/{itemid}/SARIMAX.pkl
  /home/ubuntu/DTmodels/direct60_history/{itemid}/LightGBM.pkl
  /home/ubuntu/DTmodels/direct60_history/{itemid}/LSTM.keras
  /home/ubuntu/DTmodels/direct60_history/{itemid}/GRU.keras
  /home/ubuntu/DTmodels/direct60_history/{itemid}/PatchTST.keras
"""
from __future__ import annotations

import json
import logging
import os
import pickle
import warnings
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.sdk import Variable

from _atomic import atomic_model_dir
from _db import get_db_config
from _dt_vars import get_dt_variables, itemid_label
from _enabled import resolve_enabled_set


MODELS_DIR = "/home/ubuntu/DTmodels/direct60_history"
BACK_DATA_LIMIT = 3 * 24 * 60    # 3 days of minute rows = 4320

# Zabbix's two live history tables. value_type=0 → float, =3 → UINT, =1 → char (skip).
_VALUE_TYPE_TO_LIVE_TABLE = {0: "HISTORY", 3: "HISTORY_UINT"}


def _live_table_for(itemid, dt_vars: dict) -> str | None:
    """Resolve the live Zabbix history table to read for this itemid based on
    its value_type from DT_Variables. Returns None for char-typed itemids
    (caller should skip)."""
    vt = (dt_vars.get(str(itemid)) or {}).get("value_type")
    return _VALUE_TYPE_TO_LIVE_TABLE.get(vt)


TARGET_COL = "VALUE"
EXOG_COLS = [
    "minute_sin", "minute_cos",
    "hour_sin", "hour_cos",
    "day_of_week_sin", "day_of_week_cos",
    "month_sin", "month_cos",
    "day_of_year_sin", "day_of_year_cos",
]
FEATURE_COLS = EXOG_COLS + [TARGET_COL]

LOOKBACK = 720   # 12-hour input window at 1-minute resolution
HORIZON  = 60    # 1-hour forecast
SPLIT_RATIO = 0.80   # 3-day BACK_DATA_LIMIT only leaves 864 test rows at 0.80

# Which of the 5 models to train. Configurable at runtime via the Airflow
# Variable `enabledModelsHistory` (JSON array of model names). Defaults to all
# 5; unknown names are ignored, an empty resolved set falls back to all.
ALL_MODELS = ("SARIMAX", "LightGBM", "LSTM", "GRU", "PatchTST")
ENABLED_MODELS_VAR = "enabledModelsHistory"
ENABLED_MODELS = frozenset(resolve_enabled_set(
    ENABLED_MODELS_VAR, ALL_MODELS, ALL_MODELS,
))

# Run training tasks at lowered CPU priority via os.nice() so they yield to
# more time-sensitive work (prediction ticks, EWMA updates, the scheduler
# itself) when the host is contended. Configurable at runtime via the Airflow
# Variable `trainingNiceIncrement` (int, clamped to [0, 19]; 0 = default
# priority, 19 = lowest). os.nice() can only INCREASE niceness for non-root
# users, so we don't allow negative values.
NICE_INCREMENT_VAR = "trainingNiceIncrement"
DEFAULT_NICE_INCREMENT = 10
try:
    _raw_nice = int(Variable.get(NICE_INCREMENT_VAR))
except Exception as _exc:
    print(f"{NICE_INCREMENT_VAR} not set/unparseable ({_exc}); defaulting to {DEFAULT_NICE_INCREMENT}")
    _raw_nice = DEFAULT_NICE_INCREMENT
NICE_INCREMENT = max(0, min(19, _raw_nice))
if NICE_INCREMENT != _raw_nice:
    print(f"{NICE_INCREMENT_VAR}={_raw_nice} outside [0, 19]; clamped to {NICE_INCREMENT}")

# Airflow Pool used by every training DAG's per-itemid `train_itemid` task.
# Capacity is set OUTSIDE the code via `airflow pools set training_pool N ...`.
# Caps total concurrent training processes across all 4 trainer DAGs (so a
# drift fire-up of trend+history training won't double the cap). Tasks
# referring to a non-existent pool stay queued, so create the pool before
# unpausing these DAGs.
TRAINING_POOL = "training_pool"

ACTIVE_PIPELINE_VAR = "activePipeLine"
DEFAULT_PRED_ITEMIDS = (
    31316, 31317, 31318, 31319, 31320, 31322, 31332,
    33908, 33910, 33912, 33914, 33916,
    35911,
    36550, 36554, 36556, 36557, 36560, 36930,
)
try:
    ITEMIDS_TO_TRAIN = tuple(
        sorted(int(x) for x in json.loads(Variable.get(ACTIVE_PIPELINE_VAR)))
    )
except Exception as _exc:
    print(
        f"{ACTIVE_PIPELINE_VAR} not set/unparseable ({_exc}); "
        f"using defaults {list(DEFAULT_PRED_ITEMIDS)}"
    )
    ITEMIDS_TO_TRAIN = DEFAULT_PRED_ITEMIDS


def _fetch_history(itemid: int, live_table: str):
    import mysql.connector
    import pandas as pd

    conn = mysql.connector.connect(**get_db_config())
    try:
        query = (
            f"SELECT CLOCK, VALUE FROM {live_table} "
            f"WHERE ITEMID={itemid} ORDER BY CLOCK DESC LIMIT {BACK_DATA_LIMIT}"
        )
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    if df.empty:
        return None
    df["CLOCK"] = pd.to_datetime(df["CLOCK"], unit="s")
    df = df.set_index("CLOCK").sort_index()
    # Zabbix history can poll sub-minute; collapse duplicates onto the
    # 1-min grid via mean aggregation (asfreq would raise on duplicates).
    df = df[["VALUE"]].resample("1min").mean()
    df["VALUE"] = df["VALUE"].interpolate(method="linear").ffill().bfill()
    return df


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


def _prepare(df):
    """Stride-1 sliding windows: LOOKBACK input -> HORIZON output, at minute
    resolution. Mirrors _prepare in train_models_direct24_trend.py."""
    import numpy as np
    from sklearn.preprocessing import MinMaxScaler

    df = _add_cyclical(df)
    data = df[FEATURE_COLS].dropna().copy()

    split = int(len(data) * SPLIT_RATIO)
    train_df, test_df = data.iloc[:split], data.iloc[split:]

    scaler_X = MinMaxScaler()
    scaler_y = MinMaxScaler()
    X_train_sc = scaler_X.fit_transform(train_df[FEATURE_COLS])
    X_test_sc  = scaler_X.transform(test_df[FEATURE_COLS])
    y_train_sc = scaler_y.fit_transform(train_df[[TARGET_COL]].values).flatten()
    y_test_sc  = scaler_y.transform(test_df[[TARGET_COL]].values).flatten()
    y_train_raw = train_df[TARGET_COL].values
    y_test_raw  = test_df[TARGET_COL].values

    def make_blocks(X_sc, y_sc, y_raw):
        Xs, ys_sc, ys_raw = [], [], []
        last = len(X_sc) - LOOKBACK - HORIZON + 1
        for i in range(last):
            Xs.append(X_sc[i : i + LOOKBACK])
            ys_sc.append(y_sc[i + LOOKBACK : i + LOOKBACK + HORIZON])
            ys_raw.append(y_raw[i + LOOKBACK : i + LOOKBACK + HORIZON])
        return (
            np.array(Xs, dtype=np.float32),
            np.array(ys_sc, dtype=np.float32),
            np.array(ys_raw, dtype=np.float32),
        )

    X_tr_seq, y_tr_seq_sc, y_tr_seq_raw = make_blocks(X_train_sc, y_train_sc, y_train_raw)
    X_te_seq, y_te_seq_sc, y_te_seq_raw = make_blocks(X_test_sc,  y_test_sc,  y_test_raw)

    return dict(
        train_df=train_df, test_df=test_df,
        X_tr_seq=X_tr_seq, y_tr_seq_sc=y_tr_seq_sc, y_tr_seq_raw=y_tr_seq_raw,
        X_te_seq=X_te_seq, y_te_seq_sc=y_te_seq_sc, y_te_seq_raw=y_te_seq_raw,
        scaler_X=scaler_X, scaler_y=scaler_y,
    )


def _build_patchtst(input_shape, horizon, patch_size=4, d_model=64,
                    num_heads=4, num_layers=2, ff_dim=128, dropout=0.1):
    import tensorflow as tf
    seq_len, n_feat = input_shape
    assert seq_len % patch_size == 0, "LOOKBACK must be divisible by patch_size"
    n_patches = seq_len // patch_size
    inputs = tf.keras.Input(shape=input_shape)
    x = tf.keras.layers.Reshape((n_patches, patch_size * n_feat))(inputs)
    x = tf.keras.layers.Dense(d_model)(x)
    pos = tf.keras.layers.Embedding(input_dim=n_patches, output_dim=d_model)(
        tf.range(start=0, limit=n_patches, delta=1))
    x = x + pos
    for _ in range(num_layers):
        attn = tf.keras.layers.MultiHeadAttention(
            num_heads=num_heads, key_dim=d_model // num_heads, dropout=dropout
        )(x, x)
        x = tf.keras.layers.LayerNormalization(epsilon=1e-6)(x + attn)
        ff = tf.keras.layers.Dense(ff_dim, activation="gelu")(x)
        ff = tf.keras.layers.Dense(d_model)(ff)
        ff = tf.keras.layers.Dropout(dropout)(ff)
        x = tf.keras.layers.LayerNormalization(epsilon=1e-6)(x + ff)
    x = tf.keras.layers.Flatten()(x)
    x = tf.keras.layers.Dropout(dropout)(x)
    outputs = tf.keras.layers.Dense(horizon)(x)
    return tf.keras.Model(inputs, outputs)


def _train_all(prep, out_dir):
    """Train all 5 direct720->60 models and persist them."""
    # Silence TF's INFO/WARN C++ logs (e.g. "cpu_allocator_impl Allocation
    # of N exceeds 10% of free system memory" memory-pressure warnings).
    # Must be set before the TF import below.
    os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
    import gc
    import lightgbm as lgb
    import tensorflow as tf
    from keras.callbacks import EarlyStopping
    from keras.layers import Dense, Dropout, GRU, Input, LSTM
    from keras.models import Sequential
    from sklearn.multioutput import MultiOutputRegressor
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    # Cap TF threads before any op is built — concurrent itemid tasks would
    # otherwise each grab all cores and thrash. No-op if a graph node already
    # exists (which is why we set this here, before the first model is built).
    tf.config.threading.set_intra_op_parallelism_threads(4)
    tf.config.threading.set_inter_op_parallelism_threads(1)
    tf.keras.utils.set_random_seed(100)
    n_features = prep["X_tr_seq"].shape[2]
    shape_seq  = (LOOKBACK, n_features)

    print(f"  Enabled models: {sorted(ENABLED_MODELS)}")

    if "SARIMAX" in ENABLED_MODELS:
        # SARIMAX — fit once on full training endog/exog; rolling forecast at
        # inference time via .apply(extended_endog, extended_exog).get_forecast(60).
        #
        # Dropped seasonal_order=(1,1,1,60). With s=60 on a minute grid the
        # Kalman state has dimension ~60 and the per-iteration matrix work
        # dominates fit time (the original config took ~hours on ARM CPUs).
        # The minute_sin/cos + hour_sin/cos exog regressors already encode
        # the hourly cycle, so the seasonal component was redundant.
        #
        # concentrate_scale=True  — fit one fewer free parameter.
        # enforce_stationarity/invertibility=False — skip parameter-bound
        #                           checks during MLE.
        # low_memory=True         — skip storing smoothed states/covariances.
        # cov_type='none'         — skip parameter-covariance computation.
        print("  SARIMAX...")
        train_df = prep["train_df"]
        arima = SARIMAX(
            train_df[TARGET_COL], exog=train_df[EXOG_COLS],
            order=(1, 1, 1), seasonal_order=(0, 0, 0, 0),
            enforce_stationarity=False, enforce_invertibility=False,
            concentrate_scale=True,
        ).fit(disp=False, low_memory=True, cov_type="none")
        with open(os.path.join(out_dir, "SARIMAX.pkl"), "wb") as f:
            pickle.dump(arima, f)
        print(f"    AIC={arima.aic:.1f}")

    if "LightGBM" in ENABLED_MODELS:
        # LightGBM — multi-output (60 per-horizon regressors). Inputs flattened
        # (LOOKBACK * n_features,). Targets are RAW (unscaled).
        print("  LightGBM...")
        from joblib import parallel_backend
        X_tr_flat = prep["X_tr_seq"].reshape(prep["X_tr_seq"].shape[0], -1)
        base = lgb.LGBMRegressor(
            n_estimators=100, learning_rate=0.05, num_leaves=31,
            random_state=42, verbose=-1, n_jobs=1,
        )
        # Threading backend (not loky) so the 60 per-horizon regressors fit
        # in parallel WITHOUT process workers — no per-worker data pickling
        # (which under memory pressure with training_pool=3 was producing
        # BrokenProcessPool failures). LightGBM releases the GIL inside its
        # C++ tree builder, so threads still run in parallel on the CPU.
        # n_jobs=4 caps per-task thread fan-out; with pool=3 that's at most
        # 12 LightGBM-building threads on the box.
        lgb_model = MultiOutputRegressor(base, n_jobs=4)
        with parallel_backend("threading"):
            lgb_model.fit(X_tr_flat, prep["y_tr_seq_raw"])
        with open(os.path.join(out_dir, "LightGBM.pkl"), "wb") as f:
            pickle.dump(lgb_model, f)
        print(f"    trained {len(lgb_model.estimators_)} per-horizon regressors")
        # Free LightGBM's heap before TF/Keras spins up so the C++ allocator
        # has more headroom and stops issuing memory-pressure warnings.
        del X_tr_flat, lgb_model, base
        gc.collect()

    if "LSTM" in ENABLED_MODELS:
        print("  LSTM...")
        es = EarlyStopping(monitor="val_loss", patience=15, restore_best_weights=True)
        lstm = Sequential([
            Input(shape=shape_seq),
            LSTM(64, activation="relu", return_sequences=True), Dropout(0.2),
            LSTM(32, activation="relu"), Dropout(0.2),
            Dense(HORIZON),
        ])
        lstm.compile(optimizer="adam", loss="mse", jit_compile=True)
        lstm.fit(
            prep["X_tr_seq"], prep["y_tr_seq_sc"],
            epochs=100, batch_size=256,
            validation_data=(prep["X_te_seq"], prep["y_te_seq_sc"]),
            callbacks=[es], verbose=0,
        )
        lstm.save(os.path.join(out_dir, "LSTM.keras"))
        print(f"    stopped_epoch={es.stopped_epoch or 100}")

    if "GRU" in ENABLED_MODELS:
        print("  GRU...")
        es = EarlyStopping(monitor="val_loss", patience=15, restore_best_weights=True)
        gru = Sequential([
            Input(shape=shape_seq),
            GRU(64, activation="relu", return_sequences=True), Dropout(0.2),
            GRU(32, activation="relu"), Dropout(0.2),
            Dense(HORIZON),
        ])
        gru.compile(optimizer="adam", loss="mse", jit_compile=True)
        gru.fit(
            prep["X_tr_seq"], prep["y_tr_seq_sc"],
            epochs=100, batch_size=256,
            validation_data=(prep["X_te_seq"], prep["y_te_seq_sc"]),
            callbacks=[es], verbose=0,
        )
        gru.save(os.path.join(out_dir, "GRU.keras"))
        print(f"    stopped_epoch={es.stopped_epoch or 100}")

    if "PatchTST" in ENABLED_MODELS:
        print("  PatchTST...")
        es = EarlyStopping(monitor="val_loss", patience=30, restore_best_weights=True)
        ptst = _build_patchtst(shape_seq, horizon=HORIZON)
        ptst.compile(optimizer="adam", loss="mse")
        ptst.fit(
            prep["X_tr_seq"], prep["y_tr_seq_sc"],
            epochs=100, batch_size=64,
            validation_data=(prep["X_te_seq"], prep["y_te_seq_sc"]),
            callbacks=[es], verbose=0,
        )
        ptst.save(os.path.join(out_dir, "PatchTST.keras"))
        print(f"    stopped_epoch={es.stopped_epoch or 100}")


@dag(
    dag_id="train_models_direct60_history",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False,
    tags=["training", "direct60", "history"],
    max_active_runs=1,
    params={"itemids_override": Param(default=None, type=["array", "null"])},
    doc_md=__doc__,
)
def train_models_direct60_history(itemids_override: list | None = None):
    @task
    def resolve_itemids(override) -> list:
        """Use the per-run override if provided; otherwise the activePipeLine defaults."""
        if override:
            ids = sorted(int(x) for x in override)
            print(f"Using itemids_override conf: {ids}")
            return ids
        ids = list(ITEMIDS_TO_TRAIN)
        print(f"No override; training defaults: {ids}")
        return ids

    @task(task_id="train_itemid", pool=TRAINING_POOL, pool_slots=1)
    def train_for_itemid(itemid: int) -> dict:
        # Silence noisy training chatter for THIS task only.
        warnings.filterwarnings("ignore")
        for _noisy in ("tensorflow", "absl", "h5py", "lightgbm", "statsmodels"):
            logging.getLogger(_noisy).setLevel(logging.ERROR)

        # Lower CPU priority for this task's process. Threads spawned by
        # TF/Keras/LightGBM inherit the niceness, so this shapes the entire
        # training workload. Wrapped in try/except in case the platform
        # doesn't support os.nice (Windows) — a no-op there is fine.
        if NICE_INCREMENT > 0:
            try:
                _new_nice = os.nice(NICE_INCREMENT)
                print(f"  Renice: +{NICE_INCREMENT} -> niceness {_new_nice}")
            except (OSError, AttributeError) as _nice_exc:
                print(f"  Renice failed ({_nice_exc}); continuing at default priority")

        dt_vars = get_dt_variables()
        live_table = _live_table_for(itemid, dt_vars)
        if live_table is None:
            print(
                f"Skipping ITEMID {itemid_label(itemid, dt_vars)}: unsupported "
                f"value_type (non-numeric)."
            )
            return {"itemid": itemid, "trained": False}

        print(f"=== Training direct720->60 history models for ITEMID {itemid_label(itemid, dt_vars)} (source: {live_table}) ===")
        df = _fetch_history(itemid, live_table)
        if df is None or len(df) < (LOOKBACK + HORIZON + 24 * 60):
            print(f"Skipping ITEMID {itemid_label(itemid, dt_vars)}: insufficient data ({0 if df is None else len(df)} rows).")
            return {"itemid": itemid, "trained": False}

        prep = _prepare(df)
        final_dir = os.path.join(MODELS_DIR, str(itemid))
        # Atomic-publish via staging dir — see dags/_atomic.py.
        with atomic_model_dir(final_dir) as out_dir:
            with open(os.path.join(out_dir, "feature_cols.json"), "w") as f:
                json.dump(
                    {
                        "FEATURE_COLS": FEATURE_COLS,
                        "EXOG_COLS": EXOG_COLS,
                        "TARGET_COL": TARGET_COL,
                        "LOOKBACK": LOOKBACK,
                        "HORIZON": HORIZON,
                    },
                    f,
                )
            with open(os.path.join(out_dir, "scaler_X.pkl"), "wb") as f:
                pickle.dump(prep["scaler_X"], f)
            with open(os.path.join(out_dir, "scaler_y.pkl"), "wb") as f:
                pickle.dump(prep["scaler_y"], f)
            _train_all(prep, out_dir)
        print(f"=== ITEMID {itemid_label(itemid, dt_vars)} done. Artifacts in {final_dir} ===")
        return {"itemid": itemid, "trained": True}

    ids = resolve_itemids(itemids_override)
    train_for_itemid.expand(itemid=ids)


train_models_direct60_history()
