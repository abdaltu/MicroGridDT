"""Canonical per-mode table + DAG mapping for both pipelines.

Every selector-side DAG (`message_processor_*`, `select_adaptive_*`,
`predict_stream_*`), the prediction-tick DAGs, and the drift trigger
need the same mode → table/dag-id lookup. Before this module existed
each consumer hardcoded its own `_MODE_TO_PRED_TABLE`,
`_MODE_TO_STREAM_DAG`, etc. dicts — adding a new mode (or renaming a
table) required 8+ identical edits.

This file is a helper module, not a workflow definition; the
underscore-prefixed filename keeps it out of file-discovery scans.

## Usage

>>> from _modes import HISTORY_MODES, TREND_MODES
>>> HISTORY_MODES["recursive"].pred_table
'HISTORY_PRED_RECURSIVE'
>>> HISTORY_MODES["direct60"].stream_dag_id
'predict_stream_history_direct60'

## When to edit this file

- Adding a new mode (e.g. a hypothetical `direct120` for history): add
  an entry to `HISTORY_MODES` and verify all 8 callers handle the new
  key (most use `if mode in HISTORY_MODES` filtering).
- Renaming an underlying table: change the field here and the change
  propagates everywhere.
- Renaming a DAG id: same — change here and re-deploy.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModeTables:
    """All table + DAG identifiers a single (pipeline, mode) pair owns.

    Frozen so callers can't mutate the shared map at runtime. Fields:

    - `pred_table`           — per-model forecast output (one of the four
                               `*_PRED_RECURSIVE`/`*_PRED_DIRECT*` tables)
    - `error_ewma_table`     — per-(itemid, model) error EWMA, the
                               selector's score source
    - `selection_log_table`  — closed-loop decision timeline
    - `selection_state_table`— persistent selector machinery state per itemid
    - `pred_selected_table`  — stream-projected per-CLOCK adaptive output
    - `trigger_dag_id`       — worker prediction DAG fanned out by the tick
    - `stream_dag_id`        — stream DAG triggered by the selector
    - `trainer_dag_id`       — trainer DAG fired by drift retrain
    """
    pred_table:            str
    error_ewma_table:      str
    selection_log_table:   str
    selection_state_table: str
    pred_selected_table:   str
    trigger_dag_id:        str
    stream_dag_id:         str
    trainer_dag_id:        str


HISTORY_MODES: dict[str, ModeTables] = {
    "recursive": ModeTables(
        pred_table            = "HISTORY_PRED_RECURSIVE",
        error_ewma_table      = "HISTORY_ERROR_EWMA_RECURSIVE",
        selection_log_table   = "HISTORY_SELECTION_LOG_RECURSIVE",
        selection_state_table = "HISTORY_SELECTION_STATE_RECURSIVE",
        pred_selected_table   = "HISTORY_PRED_SELECTED_RECURSIVE",
        trigger_dag_id        = "predict_minute_history_recursive_DT",
        stream_dag_id         = "predict_stream_history_recursive",
        trainer_dag_id        = "train_models_recursive_history",
    ),
    "direct60": ModeTables(
        pred_table            = "HISTORY_PRED_DIRECT60",
        error_ewma_table      = "HISTORY_ERROR_EWMA_DIRECT60",
        selection_log_table   = "HISTORY_SELECTION_LOG_DIRECT60",
        selection_state_table = "HISTORY_SELECTION_STATE_DIRECT60",
        pred_selected_table   = "HISTORY_PRED_SELECTED_DIRECT60",
        trigger_dag_id        = "predict_minute_history_direct60_DT",
        stream_dag_id         = "predict_stream_history_direct60",
        trainer_dag_id        = "train_models_direct60_history",
    ),
}

TREND_MODES: dict[str, ModeTables] = {
    "recursive": ModeTables(
        pred_table            = "TRENDS_PRED_RECURSIVE",
        error_ewma_table      = "TRENDS_ERROR_EWMA_RECURSIVE",
        selection_log_table   = "TRENDS_SELECTION_LOG_RECURSIVE",
        selection_state_table = "TRENDS_SELECTION_STATE_RECURSIVE",
        pred_selected_table   = "TRENDS_PRED_SELECTED_RECURSIVE",
        trigger_dag_id        = "predict_hourly_trend_recursive_DT",
        stream_dag_id         = "predict_stream_trend_recursive",
        trainer_dag_id        = "train_models_recursive_trend",
    ),
    "direct24": ModeTables(
        pred_table            = "TRENDS_PRED_DIRECT24",
        error_ewma_table      = "TRENDS_ERROR_EWMA_DIRECT24",
        selection_log_table   = "TRENDS_SELECTION_LOG_DIRECT24",
        selection_state_table = "TRENDS_SELECTION_STATE_DIRECT24",
        pred_selected_table   = "TRENDS_PRED_SELECTED_DIRECT24",
        trigger_dag_id        = "predict_hourly_trend_direct24_DT",
        stream_dag_id         = "predict_stream_trend_direct24",
        trainer_dag_id        = "train_models_direct24_trend",
    ),
}

# Convenience defaults — every consumer that resolves `enabledModes*` falls
# back to "all enabled" via these lists.
DEFAULT_ENABLED_HISTORY_MODES = list(HISTORY_MODES)   # ["recursive","direct60"]
DEFAULT_ENABLED_TREND_MODES   = list(TREND_MODES)     # ["recursive","direct24"]
