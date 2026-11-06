# Grafana dashboards

These dashboards were exported from the Grafana instance in the "share
externally" form. The data-source references are import inputs (`${DS_MYSQL}`,
`${DS_ZABBIX}`), so Grafana asks for your own data sources when you import a
dashboard (Dashboards → New → Import).

| File | Dashboard | Data source | Panels |
|---|---|---|---|
| `dt.json` | DT — live power flow (PV, battery, load, grid) | Zabbix plugin (`alexanderzobnin-zabbix-datasource`) | flow diagram (`lehmannch-flow-panel`) |
| `dt_forecast_overview.json` | DT — Forecast Overview: plant schematic with live values and forecasts | MySQL (`DT` database, see `schema/`) | canvas, stat, time series |
| `history_preds_explorer.json` | History Predictions — Explorer (1-minute pipeline) | MySQL | table, time series, state timeline, bar gauge |
| `trends_preds_explorer.json` | Trends Predictions — Explorer (hourly pipeline) | MySQL | table, time series, state timeline, bar gauge |

Required plugins: the Zabbix data source (`alexanderzobnin-zabbix-app`) and
`lehmannch-flow-panel` for `dt.json`. The other dashboards use only built-in
panels.
