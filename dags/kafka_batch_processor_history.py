from __future__ import annotations

import base64
import json
import time

import pendulum

from airflow.decorators import dag
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.sdk import Variable

from _db import get_db_config


STAGE_TABLE = "HISTORY_KAFKA_STAGE"

DT_VAR_IDS_VAR = "DT_Var_Ids"
# The 19 itemids covered by run_all_variables_tuned.py (offline tuning study).
# Same set used by DEFAULT_PRED_ITEMIDS in the trend pipeline.
# Override at runtime via the Airflow Variable `DT_Var_Ids` (flat JSON int array).
DEFAULT_HISTORY_ITEMIDS = frozenset({
    31316, 31317, 31318, 31319, 31320, 31322, 31332,
    33908, 33910, 33912, 33914, 33916,
    35911,
    36550, 36554, 36556, 36557, 36560, 36930,
})

# Resolved at DAG-parse time. Update the Airflow Variable + wait one parse
# cycle (or restart the scheduler) for changes to take effect.
try:
    ENABLED_ITEMIDS = frozenset(
        int(x) for x in json.loads(Variable.get(DT_VAR_IDS_VAR))
    )
except Exception as _exc:
    print(
        f"{DT_VAR_IDS_VAR} not set/unparseable ({_exc}); "
        f"using defaults ({len(DEFAULT_HISTORY_ITEMIDS)} itemids)"
    )
    ENABLED_ITEMIDS = DEFAULT_HISTORY_ITEMIDS


def _schema_lookup(json_msg) -> dict:
    """Build a {field_name: field_schema} map from the message's embedded
    Kafka Connect schema. Empty dict if no schema.

    Built once per message so that decoding multiple fields doesn't
    re-iterate the schema's field list each time.
    """
    schema = json_msg.get("schema")
    if not isinstance(schema, dict):
        return {}
    return {
        f.get("field"): f
        for f in (schema.get("fields") or ())
        if isinstance(f, dict) and f.get("field")
    }


def _decode_field(payload, schema_fields: dict, field_name: str):
    """Decode a single Kafka Connect field by inspecting its declared schema.

    - Plain numeric type (int8/16/32/64, etc.) → returned as-is.
    - `bytes` Decimal logical type → base64 → big-endian signed int → divided
      by 10^scale (`scale` from the field's schema parameters).

    Returns None if the field is missing or fails to decode. Logs every
    decode failure cause via print() before returning None; absent fields
    are silent (legitimate "no data" case, distinct from corruption).
    Letting the history topic flex on ITEMID encoding (int64 in some
    setups, Decimal in others) without changing the call site.
    """
    if not isinstance(payload, dict):
        return None
    raw = payload.get(field_name)
    if raw is None:
        return None

    sf = schema_fields.get(field_name)
    if (
        sf is None
        or sf.get("type") != "bytes"
        or not str(sf.get("name", "")).endswith("Decimal")
    ):
        return raw  # plain numeric type — pass through

    try:
        scale = int((sf.get("parameters") or {}).get("scale", 0))
    except (TypeError, ValueError) as exc:
        print(
            f"  {field_name} schema scale unparseable ({exc!r}); "
            f"using scale=0"
        )
        scale = 0

    try:
        b = base64.b64decode(raw)
    except (ValueError, TypeError) as exc:
        print(f"  {field_name} base64 decode failed ({exc!r}); raw={raw!r:.60}")
        return None
    if not b:
        return 0.0
    int_val = int.from_bytes(b, byteorder="big", signed=True)
    return int_val / (10 ** scale) if scale else int_val


def process_message_batch(messages, batch_id: str) -> None:
    """Filter a Kafka batch to enabled itemids and INSERT IGNORE the kept
    rows into the MySQL staging table under the given batch_id.

    Called once per consumer batch by ConsumeFromTopicOperator. The
    downstream `message_processor_history` DAG looks up rows by batch_id
    rather than receiving them via XCom.
    """
    received = len(messages) if messages else 0
    # print(f"Kafka batch received: {received} record(s) (batch_id={batch_id})")
    if not messages:
        return

    # Unique sub-batch id per apply_function_batch invocation. Each Kafka
    # poll() result becomes its own MySQL commit under a distinct BATCH_ID,
    # so the snapshot in `message_processor_history` only sees fully-
    # committed sub-batches and can DELETE them race-free even while this
    # consumer keeps committing more sub-batches under the same parent
    # run_id.
    sub_batch_id = f"{batch_id}__{time.time_ns()}"

    rows = []
    skipped_parse = 0
    skipped_value = 0
    skipped_filter = 0
    skipped_filter_sample: set[int] = set()   # distinct itemids dropped by ENABLED_ITEMIDS filter
    for i, msg in enumerate(messages):
        try:
            json_msg = json.loads(msg.value())
        except (ValueError, TypeError) as exc:
            print(f"  [msg {i}] JSON parse error: {exc!r}")
            skipped_parse += 1
            continue
        payload = json_msg.get("payload") or {}
        schema_fields = _schema_lookup(json_msg)

        itemid = _decode_field(payload, schema_fields, "ITEMID")
        if itemid is None:
            if "ITEMID" not in payload:
                print(f"  [msg {i}] ITEMID missing from payload")
            # else _decode_field already logged the cause
            skipped_value += 1
            continue
        try:
            itemid = int(itemid)
        except (TypeError, ValueError) as exc:
            print(f"  [msg {i}] ITEMID int cast error ({exc!r}); raw={itemid!r}")
            skipped_value += 1
            continue
        if itemid not in ENABLED_ITEMIDS:
            skipped_filter += 1
            if len(skipped_filter_sample) < 5:
                skipped_filter_sample.add(itemid)
            continue

        clock = _decode_field(payload, schema_fields, "CLOCK")
        value = _decode_field(payload, schema_fields, "VALUE")
        if clock is None:
            if "CLOCK" not in payload:
                print(f"  [msg {i} itemid={itemid}] CLOCK missing from payload")
            skipped_value += 1
            continue
        if value is None:
            if "VALUE" not in payload:
                print(f"  [msg {i} itemid={itemid}] VALUE missing from payload")
            skipped_value += 1
            continue
        try:
            rows.append((sub_batch_id, itemid, int(clock), float(value)))
        except (TypeError, ValueError) as exc:
            print(
                f"  [msg {i} itemid={itemid}] cast error ({exc!r}); "
                f"clock={clock!r} value={value!r}"
            )
            skipped_value += 1

    if not rows:
        print(
            f"Batch: {received} raw, 0 staged "
            f"(parse_skipped={skipped_parse}, value_skipped={skipped_value}, "
            f"filter_skipped={skipped_filter})"
        )
        if skipped_filter_sample:
            print(
                f"  itemid filter sample (not in DT_Var_Ids): "
                f"{sorted(skipped_filter_sample)}"
            )

    import mysql.connector

    conn = mysql.connector.connect(**get_db_config())
    cursor = conn.cursor()
    try:
        cursor.executemany(
            f"INSERT IGNORE INTO {STAGE_TABLE} "
            f"(BATCH_ID, ITEMID, CLOCK, VALUE) VALUES (%s, %s, %s, %s)",
            rows,
        )
        conn.commit()
        print(
            f"Batch: {received} raw, {len(rows)} staged into {STAGE_TABLE} "
            f"for batch_id={sub_batch_id} "
            f"(parse_skipped={skipped_parse}, value_skipped={skipped_value}, "
            f"filter_skipped={skipped_filter})"
        )
        if skipped_filter_sample:
            print(
                f"  itemid filter sample (not in DT_Var_Ids): "
                f"{sorted(skipped_filter_sample)}"
            )
    finally:
        cursor.close()
        conn.close()


@dag(
    dag_id="kafka_batch_processor_dag_history",
    start_date=pendulum.datetime(2025, 7, 21, tz="UTC"),
    catchup=False,
    schedule="* * * * *",  # every minute
    tags=["kafka-consumer"],
    max_active_runs=1,
)
def kafka_batch_processor_dag():
    """Consumes the `ora-zab-history` (float) and `ora-zab-history-uint`
    (UINT) Kafka topics in batches, stages filtered payloads into MySQL
    `HISTORY_KAFKA_STAGE` keyed by the DAG run_id, and triggers
    `message_processor_history` to consume that batch_id.

    Requires Airflow Connection `kafkaHistory` and write access to the
    `HISTORY_KAFKA_STAGE` MySQL table. Filtered itemids are sourced from
    the `DT_Var_Ids` Airflow Variable (flat JSON int array).
    """

    # Consumer just stages messages under its run_id as BATCH_ID.
    # `message_processor_history` runs on its own minute cron and snapshots
    # all pending BATCH_IDs at start — no trigger needed here.
    ConsumeFromTopicOperator(
        task_id="consume_messages_from_kafka",
        topics=["ora-zab-history", "ora-zab-history-uint"],
        kafka_config_id="kafkaHistory",
        max_messages=10000,
        apply_function_kwargs={"batch_id": "{{ run_id }}"},
        apply_function_batch=process_message_batch,
        poll_timeout=10,
        commit_cadence="end_of_operator",  # pairs with enable.auto.commit=false on the connection
        do_xcom_push=False,
    )


kafka_batch_processor_dag()
