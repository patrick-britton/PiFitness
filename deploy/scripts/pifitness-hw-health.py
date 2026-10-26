#!/usr/bin/env python3
"""Dumps hardware health to JSON. Runs as root via systemd timer."""
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

OUT = Path("/home/god/Documents/PiFitness_Local/hardware_health.json")


def run(cmd):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        return r.stdout
    except Exception as e:
        return f"ERROR: {e}"


def parse_kv(raw):
    out = {}
    for line in raw.splitlines():
        if ":" in line:
            k, _, v = line.partition(":")
            out[k.strip()] = v.strip()
    return out


throttled_raw = run(["vcgencmd", "get_throttled"]).strip()
nvme_raw = run(["nvme", "smart-log", "/dev/nvme0n1"])
usage = shutil.disk_usage("/")

payload = {
    "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "throttled": throttled_raw,
    "throttled_ok": throttled_raw == "throttled=0x0",
    "nvme": parse_kv(nvme_raw),
    "disk_free_bytes": usage.free,
    "disk_total_bytes": usage.total,
}

OUT.parent.mkdir(parents=True, exist_ok=True)
tmp = OUT.with_suffix(".tmp")
tmp.write_text(json.dumps(payload, indent=2))
os.chmod(tmp, 0o644)
tmp.replace(OUT)