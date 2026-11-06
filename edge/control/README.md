# Contactor control (edge)

This rule switches the 20 A AC contactor between the utility supply and the
solar supply. It runs on the edge machine next to the plant, as deployed.

| File | Installed as | Purpose |
|---|---|---|
| `solarCtr.py` | `/root/solarCtr.py` | Control rule (below) |
| `solarCron` | `/etc/cron.d/solarCron` | Runs the rule every 5 minutes |

## Rule

Every 5 minutes the script does four steps.

1. **Measure.** It reads the battery voltage from the twin's Zabbix item
   `ups.BatteryVoltage` on the local Zabbix agent:
   `zabbix_get -s 127.0.0.1 -k ups.BatteryVoltage`. The agent runs on the same
   edge computer, so the loop keeps working even when the cloud link is down. An
   empty reply, or a reply that is not a number, counts as 0 V, so the load falls
   back to the utility supply (this is fail-safe).
2. **Decide.** It uses the solar window, from sunrise + 60 minutes to sunset − 60
   minutes. This window is computed for the site coordinates with the `suntime`
   package:
   - Inside the window and battery voltage above 54.0 V: the load is on **solar**.
   - Inside the window and battery voltage below 51.0 V: the load is on **utility**.
   - Between 51.0 V and 54.0 V: no change (hysteresis).
   - Outside the window: the load is on **utility**.
3. **Command.** It commands the USB-HID relay board: `usbrelay BITFT_1=1`
   (solar) or `usbrelay BITFT_1=0` (utility). The relay drives the contactor.
4. **Record.** It writes the commanded state (`1` or `0`) to `/tmp/ACrelay.txt`.
   The Zabbix item `ups.ACrelay` reads this file
   (`../zabbix/userparameter_nut.conf`).

The response is measured separately by the inverter and reported to Zabbix. The
item `ups.UpsStatus` changes between `ONLINE` and `ON BATTERY`, and the items
`ups.BatteryDischargeCurrent` and `ups.BatteryVoltage` change. The March 2026
sample (`../../data/sample/`) contains 74 switching events. Most follow the
daily window; a few are low-voltage cut-offs, for example 16 March 2026,
11:49 local time.

`ups.ACrelay` is the commanded state, not the sensed position of the contactor.

## Requirements

You need: Python 3 with `suntime` (version 1.2.5 in the deployment); the Zabbix
agent with `../zabbix/userparameter_nut.conf`, which accepts passive checks from
127.0.0.1 (the `Server=` line in `zabbix_agentd.conf` must include it);
`zabbix_get` (Debian package `zabbix-get`); NUT with the inverter driver
(`../nut-driver/`); and `usbrelay` (Debian package `usbrelay`). The relay id
`BITFT_1` is specific to this board. List your own ids with `usbrelay`.

## Safety

The script switches mains AC. Test it with the relay disconnected from the
contactor, and supervise any change to the thresholds. For unsupervised student
work, use the simulator instead.
