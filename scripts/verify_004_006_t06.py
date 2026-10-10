"""T06 live verify (read-only): endpoint efficiency gating both ways."""
from fastapi.testclient import TestClient
from backend.main import app

c = TestClient(app)
for aid, want in [(24184819311, True), (23644388178, True), (24663109355, False)]:
    r = c.get("/api/activities/report", params={"activity_id": aid})
    d = r.json()
    eff = d.get("efficiency")
    ok = (eff is not None) == want
    print("PASS" if ok and r.status_code == 200 else "FAIL", aid,
          d["header"].get("activity_type_name"),
          "efficiency?", eff)
    assert ok and r.status_code == 200
print("ALL PASS")

