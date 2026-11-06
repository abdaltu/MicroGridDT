# MicroGridDT: an open-source digital twin of a solar microgrid

MicroGridDT monitors, stores, forecasts, and displays data from a small
physical solar microgrid. It uses only open-source tools:

- Zabbix: monitoring at the equipment (the edge)
- Apache Kafka: streaming of events
- Apache Airflow: scheduling of the data pipeline
- MySQL: storage of the data
- Grafana: visualization of the data

In the reference deployment, only the Zabbix agent runs next to the plant. The
other services run on free-tier cloud virtual machines. A simulator can replace
the physical plant.

## Plant

The physical plant has four parts:

- A solar panel array of about 1.7 kW that feeds a 5 kVA hybrid inverter with an
  MPPT charger. The inverter is read over USB-serial through a patched NUT driver.
- A lithium iron phosphate (LiFePO4) battery bank of 7.5 kWh.
- A weather station and a solar-radiation sensor. They communicate over Modbus
  RTU on an RS-485 line.
- A 20 A AC contactor, switched by a USB relay board. It selects either the
  utility supply or the solar supply.

## Data flow

```
sensors ─▶ Zabbix agent ─▶ Zabbix server ─▶ Kafka topics
                                            (ora-zab-history[-uint], ora-zab-trend[-uint])
                                                   │
                                                   ▼
           Airflow: ingestion ─▶ digestion (EWMA) ─▶ forecasting ─▶ adaptive model selection
                                        │                                │
                                        └──▶ drift detection ─▶ retraining
                                                   │
                                                   ▼
                                       MySQL (DT database) ─▶ Grafana
```

Two pipelines run in parallel. The **history** pipeline works on readings taken
once per minute and makes 60-step forecasts, both recursive and direct. The
**trend** pipeline works on hourly aggregates and makes 24-step forecasts, both
recursive and direct. See [`dags/DAG_DEPENDENCIES.md`](dags/DAG_DEPENDENCIES.md)
for the full DAG and table graph.

## Contents

| Folder | Contents |
|---|---|
| [`dags/`](dags/) | 27 Airflow DAGs and helper modules: reading from Kafka, digestion, forecasting, adaptive model selection, retraining triggered by drift, and MySQL housekeeping. Includes `requirements.txt`. |
| [`airflow/`](airflow/) | Scripts to start and maintain the Airflow host. |
| [`schema/`](schema/) | The MySQL schema of the `DT` database (structure only, no data). |
| [`dashboards/`](dashboards/) | Four Grafana dashboards: live power flow, forecast overview, and the history and trend prediction explorers. They are ready to import. |
| [`edge/nut-driver/`](edge/nut-driver/) | A patch to the NUT `nutdrv_qx` Voltronic-SUNNY subdriver so it works with the inverter's protocol. |
| [`edge/zabbix/`](edge/zabbix/) | Zabbix agent UserParameters for the inverter (more to be added). |
| [`edge/control/`](edge/control/) | The contactor control rule run by cron on the edge machine. It uses battery-voltage thresholds and the solar window. |
| [`simulator/`](simulator/) | Synthetic or replayed microgrid data sent to Zabbix with `zabbix_sender`. |
| [`data/sample/`](data/sample/) | One month (March 2026) of readings and hourly trends recorded from the testbed. |
| [`docs/`](docs/) | Suggested experiments on the running system. |

## Simulator (no hardware)

```bash
cd simulator
python3 simulate_microgrid.py --dry-run --steps 60            # print one hour of readings
python3 simulate_microgrid.py -z <zabbix-server> -s microgrid-sim --scenario cloudy
```

The simulator offers four scenarios: `clear` (clear sky), `cloudy`,
`fault` (the radiation sensor freezes, then stops sending data), and `loadstep`
(the load rises by 1.5 kW between 12:00 and 13:00). Use
`--replay file.csv` to replay recorded readings (columns `clock,key,value`). The
item keys match the physical plant ([`simulator/items.csv`](simulator/items.csv)).
Create them on the host given with `-s` as Zabbix *trapper* items.

## Configuration

This repository stores no credentials. The DAGs read database and Kafka settings
from Airflow Connections (`mysql_dt`, `kafkaHistory`, `kafkaTrend`) and their
parameters from Airflow Variables; see [`dags/README.md`](dags/README.md). When
you import the dashboards, Grafana asks for a MySQL data source and a Zabbix
data source.

## Running in containers (optional)

In the reference deployment, each component runs directly on the operating
system as a system service. Each component also has an official container image
(Zabbix, Apache Kafka, Apache Airflow, MySQL, Grafana). So you can run the whole
stack in containers on one machine, for example with Docker Compose or Podman.
This repository does not provide a Compose file.

## Licences

- Code and configuration in this repository: Apache-2.0 ([LICENSE](LICENSE)).
- Documentation, dashboards, and sample data: CC BY 4.0.
- `edge/nut-driver/`: GPL-2.0-or-later, because it is a derivative of Network UPS
  Tools.

The third-party components keep their own licences: Zabbix (AGPLv3), Kafka
(Apache-2.0), Airflow (Apache-2.0), MySQL Community (GPLv2), and Grafana (AGPLv3).
