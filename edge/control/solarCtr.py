import datetime
import os
from suntime import Sun, SunTimeException
import subprocess

# Coordinates for Tripoli, Libya
latitude = 32.839974
longitude = 13.298939

# delta in minuts
solarStartDelta = 60
solarEndDelta = 60

batLowCharge = 51.0
batHighCharge= 54.0

def write_flag(file_path, flag_value):
    with open(file_path, 'w') as f:
        f.write(f'{flag_value}')

# Usage
flag_file = '/tmp/ACrelay.txt'

# Create a Sun object with the coordinates
sun = Sun(latitude, longitude)

# Get the sunrise and sunset times for today
sunrise_time = sun.get_sunrise_time()
sunset_time = sun.get_sunset_time()

solarStartTime = sunrise_time + datetime.timedelta(minutes=solarStartDelta)
solarEndTime = sunset_time - datetime.timedelta(minutes=solarEndDelta)

currentTime = datetime.datetime.now(tz=solarEndTime.tzinfo)

# Read the battery voltage from the twin's Zabbix item ups.BatteryVoltage,
# through the local Zabbix agent (works even when the cloud link is down)
batVoltage = subprocess.check_output('zabbix_get -s 127.0.0.1 -k ups.BatteryVoltage 2> /dev/null; exit 0',shell=True)

# Convert the output to a float. An empty or non-numeric reply (agent down,
# item not supported, inverter not answering) counts as 0 V, so the load
# falls back to the utility.
try:
    batVoltage = float(batVoltage)
except ValueError:
    batVoltage = 0.0

if currentTime > solarStartTime and currentTime < solarEndTime:
    if batVoltage > batHighCharge:
        print("Solar power is on")    
        os.system("/usr/bin/usbrelay BITFT_1=1")
        write_flag(flag_file, '1')

    if batVoltage < batLowCharge:
        print("Battery voltage is low Solar power is off")
        os.system("/usr/bin/usbrelay BITFT_1=0")
        write_flag(flag_file, '0')    
else:
    print("Solar power is off") 
    os.system("/usr/bin/usbrelay BITFT_1=0")
    write_flag(flag_file, '0')
    # os.system("ls -l")
print(batVoltage)

# Print the results


# print(f"Sunrise time: {sunrise_time_local.strftime('%H:%M:%S')}")
# print(f"Sunset time: {sunset_time_local.strftime('%H:%M:%S')}")

# print(f"Start time local: {solarStartTime.strftime('%H:%M:%S')}")
# print(f"End time local: {solarEndTime.strftime('%H:%M:%S')}")

# print(f"current time local: {currentTime.strftime('%H:%M:%S')}")

