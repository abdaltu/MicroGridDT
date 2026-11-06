#!/usr/bin/env python3
"""Simulated solar-microgrid source for the hardware-free pathway.

Generates synthetic readings for the same Zabbix item keys as the physical
testbed (see items.csv) and pushes them as Zabbix trapper items with the
`zabbix_sender` utility, so the Edge Layer and everything downstream is
exercised exactly as with real sensors.

Examples
  # print 24 h of 1-minute readings, no Zabbix needed
  python3 simulate_microgrid.py --dry-run --steps 1440 --scenario cloudy

  # stream live to a Zabbix server, 60x faster than real time
  python3 simulate_microgrid.py -z localhost -s microgrid-sim --speed 60

  # replay recorded readings (CSV columns: clock,key,value)
  python3 simulate_microgrid.py --replay recorded.csv -z localhost -s microgrid-sim

The physical model is deliberately simple and is not a validated plant
model: clear-sky irradiance on a sine curve, a 1.7 kWp PV array,
a 7.5 kWh LiFePO4 battery, and a household-sized load.
"""
import argparse
import csv
import math
import random
import subprocess
import sys
import time
from datetime import datetime, timezone

PV_KWP = 1.7            # array size, kW peak
PV_DERATE = 0.80        # wiring, temperature and MPPT losses
BATT_KWH = 7.5          # battery capacity
BATT_V_EMPTY, BATT_V_FULL = 48.0, 54.4   # 16s LiFePO4, approximate
PV_STRING_V = 105.0     # three panels in series, approximate MPP voltage
BASE_LOAD_W = 300.0

SCENARIOS = ("clear", "cloudy", "fault", "loadstep")


class Microgrid:
    def __init__(self, scenario, seed):
        self.scenario = scenario
        self.rng = random.Random(seed)
        self.soc = 0.6          # state of charge, 0..1
        self.cloud = 0.0        # 0 = clear, 1 = overcast
        self.contactor = 0      # 0 = utility, 1 = solar (set by the control rule)

    def step(self, t, dt_s):
        """Advance the model to time t (datetime, UTC) and return {key: value}."""
        hour = t.hour + t.minute / 60 + t.second / 3600
        rng = self.rng

        # Irradiance (W/m2): clear-sky sine between 06:00 and 18:00, with clouds.
        clear = 1000.0 * max(0.0, math.sin(math.pi * (hour - 6) / 12))
        if self.scenario == "cloudy":
            self.cloud = min(1.0, max(0.0, self.cloud + rng.gauss(0, 0.08)))
        else:
            self.cloud = max(0.0, self.cloud * 0.9 + rng.gauss(0, 0.01))
        irr = max(0.0, clear * (1 - 0.75 * self.cloud) + rng.gauss(0, 5))

        # PV power and electrical quantities.
        pv_w = min(PV_KWP * 1000 * PV_DERATE, PV_KWP * 1000 * PV_DERATE * irr / 1000)
        pv_v = PV_STRING_V + rng.gauss(0, 1.5) if irr > 20 else rng.uniform(0, 5)
        pv_a = pv_w / pv_v if pv_v > 1 else 0.0

        # Load: base load plus evening peak; 'loadstep' adds +1.5 kW between 12:00 and 13:00.
        load_w = BASE_LOAD_W + 400 * math.exp(-((hour - 20) ** 2) / 4) + rng.gauss(0, 20)
        if self.scenario == "loadstep" and 12 <= hour < 13:
            load_w += 1500
        load_w = max(50.0, load_w)

        # Battery energy balance (positive = charging).
        net_w = pv_w - load_w
        self.soc = min(1.0, max(0.05, self.soc + net_w * dt_s / 3600 / (BATT_KWH * 1000)))
        batt_v = BATT_V_EMPTY + (BATT_V_FULL - BATT_V_EMPTY) * self.soc + 0.002 * net_w
        chg_a = max(0.0, net_w) / batt_v
        dis_a = max(0.0, -net_w) / batt_v

        out_t = 22 + 8 * math.sin(math.pi * (hour - 9) / 12) + rng.gauss(0, 0.3)
        values = {
            "sr.SolRad": round(irr),
            "ups.ACrelay": self.contactor,
            "ups.BatteryCharge": round(self.soc * 100, 1),
            "ups.BatteryChargeCurrent": round(chg_a, 2),
            "ups.BatteryDischargeCurrent": round(dis_a, 2),
            "ups.BatteryVoltage": round(batt_v, 2),
            "ups.InputFrequency": round(50 + rng.gauss(0, 0.02), 2),
            "ups.InputVoltage": round(230 + rng.gauss(0, 2), 1),
            "ups.OutputPowerVa": round(load_w / 0.9, 1),
            "ups.OutputPowerW": round(load_w, 1),
            "ups.OutputVoltage": round(230 + rng.gauss(0, 0.5), 1),
            "ups.PvChargerPower": round(pv_w, 1),
            "ups.PvInputCurrent": round(pv_a, 2),
            "ups.PvInputVoltage": round(pv_v, 1),
            "ups.Temperature": round(30 + 10 * load_w / 2000 + rng.gauss(0, 0.5)),
            "ups.UpsLoad": round(100 * load_w / 5000),
            "ws.rtOutsideTempC": round(out_t, 1),
            "ws.rtOutsideHum": round(min(100, max(10, 60 - 1.5 * (out_t - 22) + rng.gauss(0, 2)))),
            "ws.rtWindSpeedKMH": round(max(0, rng.gauss(8, 4))),
        }

        # 'fault': the radiation sensor freezes at its last value between 10:00 and 11:00,
        # then drops out entirely between 11:00 and 11:30 (missing readings).
        if self.scenario == "fault":
            if 10 <= hour < 11:
                values["sr.SolRad"] = getattr(self, "_frozen", values["sr.SolRad"])
                self._frozen = values["sr.SolRad"]
            elif 11 <= hour < 11.5:
                del values["sr.SolRad"]
            else:
                self._frozen = values["sr.SolRad"]
        return values


def send(lines, args):
    """Send 'host key clock value' lines with zabbix_sender (-T: with timestamps)."""
    if args.dry_run:
        sys.stdout.write("".join(lines))
        return
    cmd = ["zabbix_sender", "-z", args.zabbix_server, "-p", str(args.port), "-T", "-i", "-"]
    res = subprocess.run(cmd, input="".join(lines), text=True, capture_output=True)
    if res.returncode not in (0, 2):  # 2 = some values rejected (e.g. unknown item keys)
        sys.stderr.write(res.stdout + res.stderr)
        sys.exit(res.returncode)


def run_model(args):
    grid = Microgrid(args.scenario, args.seed)
    t = args.start or int(time.time()) // args.interval * args.interval
    live = args.steps is None
    n = 0
    while live or n < args.steps:
        now = datetime.fromtimestamp(t, tz=timezone.utc)
        vals = grid.step(now, args.interval)
        send([f"{args.host} {k} {t} {v}\n" for k, v in vals.items()], args)
        n += 1
        t += args.interval
        if live:
            time.sleep(args.interval / args.speed)


def run_replay(args):
    with open(args.replay, newline="") as f:
        rows = sorted(csv.DictReader(f), key=lambda r: int(r["clock"]))
    prev = None
    for r in rows:
        clock = int(r["clock"])
        if prev is not None and not args.dry_run and args.speed > 0:
            time.sleep(max(0, clock - prev) / args.speed)
        send([f"{args.host} {r['key']} {clock} {r['value']}\n"], args)
        prev = clock


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("-z", "--zabbix-server", default="localhost")
    p.add_argument("-p", "--port", type=int, default=10051)
    p.add_argument("-s", "--host", default="microgrid-sim", help="Zabbix host name")
    p.add_argument("--scenario", choices=SCENARIOS, default="clear")
    p.add_argument("--interval", type=int, default=60, help="sampling interval, s")
    p.add_argument("--speed", type=float, default=1.0, help="simulated seconds per real second")
    p.add_argument("--steps", type=int, help="generate N samples and exit (default: run forever)")
    p.add_argument("--start", type=int, help="start time, Unix seconds (default: now)")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--replay", help="CSV file with columns clock,key,value")
    p.add_argument("--dry-run", action="store_true", help="print instead of sending")
    args = p.parse_args()
    run_replay(args) if args.replay else run_model(args)


if __name__ == "__main__":
    main()
