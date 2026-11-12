#!/usr/bin/env bash
# Print when SkyPilot's auto-teardown (autodown) will delete the cluster.
# Usage: scripts/cloud_expiry.sh [cluster-name]      (default: airflow-do)
#
# SkyPilot stores the timer on the VM itself. With `--wait-for none` the deadline is
# "time the timer was set + idle minutes", and nothing (SSH, jobs) resets it.
set -euo pipefail
CLUSTER="${1:-airflow-do}"

ssh -o BatchMode=yes -o ConnectTimeout=10 -o LogLevel=ERROR "$CLUSTER" '~/skypilot-runtime/bin/python - <<"PY"
import datetime as dt
from sky.skylet import autostop_lib, configs

c = autostop_lib.get_autostop_config()
if c.autostop_idle_minutes < 0:
    print("No auto-teardown is set on this cluster.")
    raise SystemExit
start = float(configs.get_config("autostop_last_active_time"))
end = start + c.autostop_idle_minutes * 60
now = dt.datetime.now(dt.timezone.utc).timestamp()
fmt = lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
action = "deleted (autodown)" if c.down else "stopped (autostop)"
print(f"Timer:      {c.autostop_idle_minutes} min, wait_for={c.wait_for.value}, action={action}")
print(f"Started:    {fmt(start)}")
print(f"Ends:       {fmt(end)}  (SkyPilot checks about once a minute)")
print(f"Remaining:  {max(0, int((end - now) // 60))} min")
PY'
