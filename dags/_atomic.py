"""Atomic-publish helper for trainer artifact directories.

The four trainer DAGs each write ~6 files per itemid (feature_cols.json,
scaler_X.pkl, scaler_y.pkl, SARIMAX.pkl, LightGBM.pkl, *.keras). In-place
writes have two failure modes the predictors can't recover from:

  1. **Mid-training crash leaves inconsistent state.** Scaler files get
     written first, then the per-model files. If the trainer dies after
     scaler_X but before LSTM.keras, the next predictor loads the NEW
     scaler against the OLD LSTM model. Predictions silently go wrong —
     no exception, just numerically broken output.

  2. **Predictor mid-write read.** Predict ticks run every 10 min,
     training runs for hours. There's a real window where the predictor
     opens a half-written .pkl or sees a new feature_cols.json paired
     with old keras files.

`atomic_model_dir(final_dir)` fixes both by yielding a staging dir
(`<final_dir>.staging`). On clean exit it atomic-renames the staging dir
over `final_dir`, preserving the prior contents at `<final_dir>.previous`
for one-step rollback. On exception, the staging dir is removed and
`final_dir` is left untouched — predictors keep serving the old, fully
consistent model set until the next successful retrain.

This file is a helper module, not a workflow definition; the
underscore-prefixed filename keeps it out of file-discovery scans.

## Usage

>>> from _atomic import atomic_model_dir
>>> with atomic_model_dir("/home/ubuntu/DTmodels/recursive_history/31322") as out_dir:
...     # write feature_cols.json, scalers, SARIMAX.pkl, etc. into out_dir
...     ...
>>> # After the block exits cleanly, /home/.../31322 contains the new
>>> # model set; the previous one is at /home/.../31322.previous.

## Rollback

If a fresh training run produced a bad model, swap back manually:

    cd /home/ubuntu/DTmodels/recursive_history
    mv 31322 31322.bad && mv 31322.previous 31322
"""
from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def atomic_model_dir(final_dir: str) -> Iterator[str]:
    """Yield a staging dir for a trainer; on clean exit, atomic-publish.

    Parent directory of `final_dir` is created if missing — staging,
    final, and previous all live as siblings under that parent.

    Failure semantics: any exception (including `KeyboardInterrupt` or
    `SystemExit` via the `BaseException` catch) wipes the staging dir
    and re-raises. `final_dir` is never touched in this case — the prior
    model set keeps serving.

    Success semantics: two `os.rename()` calls flip the dirs. POSIX
    guarantees each rename is atomic; the ~microsecond window between
    them (where `final_dir` briefly doesn't exist) is short enough that
    the predictor's existing ``if not os.path.isdir(model_dir): skip``
    check absorbs any tick that happens to land inside it.
    """
    staging  = final_dir + ".staging"
    previous = final_dir + ".previous"

    os.makedirs(os.path.dirname(final_dir) or ".", exist_ok=True)
    shutil.rmtree(staging, ignore_errors=True)
    os.makedirs(staging, exist_ok=True)

    try:
        yield staging
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    # Swap. `os.rename` over an existing non-empty directory fails on
    # POSIX, so the prior `.previous` (if any) is wiped first, then the
    # current `final_dir` is moved aside to become the new `.previous`,
    # then staging is promoted into place.
    shutil.rmtree(previous, ignore_errors=True)
    if os.path.exists(final_dir):
        os.rename(final_dir, previous)
    os.rename(staging, final_dir)
