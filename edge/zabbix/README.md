# Zabbix edge configuration

| File | Installed as | Purpose |
|---|---|---|
| `userparameter_nut.conf` | `/etc/zabbix/zabbix_agentd.conf.d/userparameter_nut.conf` | Zabbix agent items for the inverter (`ups.*`). The values are read from NUT with `upsc`. The item `ups.ACrelay` reads the contactor state written by `../control/solarCtr.py`. |

To be added from the edge machine:
- `zabbix_agentd.conf`, with the `Server=` and `ServerActive=` addresses and the
  PSK files removed.
- The other UserParameter files and the scripts they call (the Modbus RTU
  radiation sensor and the weather station).
- A host template exported from the Zabbix web UI
  (Data collection → Templates → Export, YAML).
