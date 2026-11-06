# Sample data: one month from the physical testbed

Zabbix recorded these readings on the solar-microgrid testbed from
**1 March 2026 00:00 UTC to 31 March 2026 23:59 UTC**. They cover the 25 items
listed in [`simulator/items.csv`](../../simulator/items.csv): the inverter (UPS),
the weather station, and the solar-radiation sensor. The values are the same as
in the Zabbix database export. Only two changes were made: the Zabbix internal
row `id` column was removed, and the item key was added.

| File | Contents | Rows |
|---|---|---|
| `history_2026-03.csv.gz` | Raw readings, about one per item per minute (Zabbix `history`, `history_uint`, `history_str`) | 1,116,176 |
| `trends_2026-03.csv.gz` | Hourly aggregates (Zabbix `trends`, `trends_uint`) | 17,856 |

Columns:
- history: `itemid,key,clock,ns,value`
- trends: `itemid,key,clock,num,value_min,value_avg,value_max`

`clock` is Unix time in seconds (UTC), and `ns` is the nanosecond part. In
trends, `clock` is the start of the hour, `num` is the number of readings
combined, and `value_*` are the minimum, the mean, and the maximum. Numbers use
Zabbix's export format (for example `5.45E+001`). Rows are sorted by `clock`,
then by `itemid`.

## Coverage

Each item has between 44,638 and 44,660 readings, close to the expected 44,640
(one per minute). A few items sometimes have two readings within the same second
(459 cases). Zabbix stores these as separate rows with different `ns`. The item
`ups.UpsStatus` is a text item (values such as `FSD ONLINE`), so it has no
trends. Every other item has 744 hourly trend rows (31 days × 24 hours).

## Use

You can replay the readings through the pipeline with the simulator. It reads
the `clock`, `key`, and `value` columns:

```bash
gunzip -k data/sample/history_2026-03.csv.gz
python3 simulator/simulate_microgrid.py --replay data/sample/history_2026-03.csv \
    -z <zabbix-server> -s <host> --speed 600
```

Values that contain spaces (the text item `ups.UpsStatus`) may be rejected by
`zabbix_sender`. Filter that item out if you do not need it.

You can also load the files with pandas (`pd.read_csv("history_2026-03.csv.gz")`),
or load them into the `HISTORY`, `HISTORY_UINT`, `TRENDS`, and `TRENDS_UINT`
tables of the DT database ([`schema/`](../../schema/)) for the storage and
forecasting exercises.

## Licence

CC BY 4.0.
