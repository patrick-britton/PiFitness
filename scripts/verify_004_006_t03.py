"""T03 live verify (read-only): default + prev neighbor + bad id."""
from fastapi.testclient import TestClient
from backend.main import app

c = TestClient(app)
r1 = c.get("/api/activities/report")
print("default", r1.status_code, r1.json().get("activity_id"))
d = r1.json()
print("nav", d.get("nav"))
print("header_keys", sorted(d.get("header", {}).keys()))
print("efficiency_null?", d.get("efficiency") is None)
prev = (d.get("nav") or {}).get("prev_activity_id")
if prev:
    r2 = c.get("/api/activities/report", params={"activity_id": prev})
    print("prev", r2.status_code, r2.json().get("activity_id"), r2.json().get("nav"))
r3 = c.get("/api/activities/report", params={"activity_id": 1})
print("bad", r3.status_code)
