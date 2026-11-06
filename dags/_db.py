"""Shared MySQL connection helper for the Airflow scheduler.

Reads the `mysql_dt` Airflow Connection at task time and returns a dict
suitable for `mysql.connector.connect(**...)`. This file is a helper
module, not a workflow definition; the underscore-prefixed filename
keeps it out of file-discovery scans.
"""
from __future__ import annotations


DB_CONN_ID = "mysql_dt"


def get_db_config() -> dict:
    """Return MySQL connection kwargs from Airflow Connection ``mysql_dt``.

    Call from inside Airflow tasks (not at parse time) — each call
    triggers a metadata-DB lookup.
    """
    from airflow.hooks.base import BaseHook

    c = BaseHook.get_connection(DB_CONN_ID)
    return {
        "host":     c.host,
        "user":     c.login,
        "password": c.password,
        "database": c.schema,
        "port":     c.port or 3306,
    }
