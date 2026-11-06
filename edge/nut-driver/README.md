# Modified NUT driver for the hybrid inverter

Licence: **GPL-2.0-or-later**, because this is a derivative of Network UPS
Tools (NUT).

## Upstream base

- NUT 2.7.3.1, branch `nutdrv_qx_voltronic-sunny_crc-commands`
  (project: https://github.com/networkupstools/nut)
- File: `drivers/nutdrv_qx_voltronic-sunny.c`, subdriver "Voltronic-SUNNY 0.02",
  Copyright (C) 2014 Daniele Pezzini.

## What the patch changes

`voltronic-sunny_mod.patch` changes only the file
`drivers/nutdrv_qx_voltronic-sunny.c`. It makes the subdriver work with the
5 kVA hybrid inverter used on the testbed (main CPU firmware VERFW 00017.03):

- It disables queries that the inverter does not answer (for example `QSVFW2`,
  `QDM`, `QFLAG`) and the settings that depend on them.
- It adjusts the expected replies of the status and rating queries (for example
  `QPIGS`, `QPIRI`, `QPIFS`, `QPIHF`) to match the inverter's real responses.

## Build

```bash
# from a checkout of the upstream branch above
patch -p1 < voltronic-sunny_mod.patch
./autogen.sh && ./configure --with-drivers=nutdrv_qx && make
```

Then configure the driver in `ups.conf` with `driver = nutdrv_qx` and
`protocol = voltronic-sunny`. Zabbix reads its variables with `upsc`
(see `edge/zabbix/`).
