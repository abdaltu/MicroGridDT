"""Shared resolver for JSON-array "enabled set" Airflow Variables.

Both `enabledModes*` (which prediction modes run) and `enabledModels*`
(which forecasting models train/predict) follow the same shape: a JSON
array of strings, filtered against a known-valid set, fall back to a
default on missing/unparseable/empty. Before this helper existed the
parse pattern was copy-pasted into 16+ DAGs; any tweak to the failure
logging or filtering had to be replicated everywhere.

This file is a helper module, not a workflow definition; the
underscore-prefixed filename keeps it out of file-discovery scans.
"""
from __future__ import annotations

import json
from typing import Iterable


def resolve_enabled_set(var_name: str, default: Iterable[str],
                        valid: Iterable[str]) -> list[str]:
    """Resolve a JSON-array Airflow Variable to a filtered list.

    - `var_name`: Airflow Variable name (e.g. ``"enabledModesHistory"``).
    - `default`: fallback list applied on missing/unparseable/empty.
    - `valid`: set of allowed values; entries not in `valid` are dropped
      with a log line. Order in the result follows the user-supplied JSON
      order (NOT `valid` order) so the caller can express preference.

    Logs every divergence (unknown entries, fallback to default) via
    `print()`. Call from a DAG-parse-time context, not inside a task —
    each invocation hits the metadata DB once.
    """
    from airflow.sdk import Variable

    valid_set = set(valid)
    default_list = list(default)
    try:
        raw = json.loads(Variable.get(var_name))
    except Exception as exc:
        print(
            f"{var_name} not set/unparseable ({exc}); "
            f"defaulting to {default_list}"
        )
        return list(default_list)

    unknown = [m for m in raw if m not in valid_set]
    if unknown:
        print(f"{var_name}: ignoring unknown entries {unknown}")
    kept = [m for m in raw if m in valid_set]
    if not kept:
        print(
            f"{var_name}={raw} resolved to empty set; "
            f"defaulting to {default_list}"
        )
        return list(default_list)
    return kept
