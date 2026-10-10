"""T09 live verify (read-only): end-to-end report latency + final contract.

Times the production endpoint in-process (FastAPI TestClient → real DB on the
Pi; no HTTP socket overhead, DB time dominates) and asserts the deprecated
T01 fields are gone from the payload (activity_type, show_efficiency_placeholder,
charts.pace, minute keys).
"""
import statistics
import time

from fastapi.testclient import TestClient
from backend.main import app

c = TestClient(app)

N = 5


def timed(path: str, params=None):
    times = []
    last = None
    for _ in range(N):
        t0 = time.perf_counter()
        r = c.get(path, params=params)
        times.append((time.perf_counter() - t0) * 1000.0)
        last = r
    return times, last


# --- Latency (AC-9: <= 2000 ms end-to-end) --------------------------------
default_times, r1 = timed("/api/activities/report")
d = r1.json()
print(f"default   status={r1.status_code} activity_id={d.get('activity_id')}")
print(
    f"default   ms: min={min(default_times):.1f} "
    f"avg={statistics.mean(default_times):.1f} max={max(default_times):.1f}"
)

explicit_id = d.get("activity_id")
id_times, r2 = timed("/api/activities/report", params={"activity_id": explicit_id})
print(
    f"explicit  status={r2.status_code} "
    f"ms: min={min(id_times):.1f} avg={statistics.mean(id_times):.1f} max={max(id_times):.1f}"
)

prev = (d.get("nav") or {}).get("prev_activity_id")
if prev:
    prev_times, r3 = timed("/api/activities/report", params={"activity_id": prev})
    print(
        f"prev {prev} status={r3.status_code} "
        f"ms: min={min(prev_times):.1f} avg={statistics.mean(prev_times):.1f} max={max(prev_times):.1f}"
    )

worst = max(max(default_times), max(id_times))
print(f"worst_ms={worst:.1f} within_2s={worst <= 2000.0}")

# --- Final contract (deprecated fields gone) ------------------------------
assert "activity_type" not in d, "deprecated top-level activity_type still present"
assert "show_efficiency_placeholder" not in d["header"], "deprecated header flag still present"
assert "pace" not in d["charts"], "deprecated charts.pace series still present"
for pt in d["charts"]["elevation"] + d["charts"]["heartrate"]:
    assert "minute" not in pt, f"deprecated minute key still present: {pt}"
    assert "distance_m" in pt, f"distance_m missing from chart point: {pt}"
print("contract  OK: no activity_type / show_efficiency_placeholder / pace / minute keys")
print("nav       ", d.get("nav"))
print("charts    ", {k: len(v) for k, v in d["charts"].items()})
