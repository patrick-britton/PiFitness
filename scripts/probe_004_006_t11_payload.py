"""T11-bug probe (read-only): efficiency payload for activity 24358931863."""
import json

from fastapi.testclient import TestClient
from backend.main import app

c = TestClient(app)
r = c.get("/api/activities/report", params={"activity_id": 24358931863})
print("status", r.status_code)
d = r.json()
print("efficiency:", json.dumps(d.get("efficiency"), indent=2))
print("course:", (d.get("course") or {}).get("name"))
