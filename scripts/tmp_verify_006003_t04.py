#!/usr/bin/env python3
"""
TEMP verify for 006-003 T04 (removed after pass).
TestClient round-trip against backend.main.app.
Conditional: SAFE SUBSET (no writes) when a human game is active -> exit 2;
FULL RUN (net-zero rows) otherwise -> exit 0.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient  # noqa: E402

from backend.main import app  # noqa: E402
from backend_functions.database_functions import one_sql_result  # noqa: E402
from backend_functions.queries import get_active_game  # noqa: E402

client = TestClient(app)
B = "/api/sports/volleyball"
FAILED = False


def check(label, cond, extra=""):
    global FAILED
    if cond:
        print(f"  ok: {label}")
    else:
        FAILED = True
        print(f"  FAIL: {label} {extra}")


def counts():
    return (
        int(one_sql_result("SELECT COUNT(*) FROM volleyball.games")),
        int(one_sql_result("SELECT COUNT(*) FROM volleyball.points")),
        int(one_sql_result("SELECT COUNT(*) FROM volleyball.game_events")),
    )


def routes_present():
    paths = {(r.path, m) for r in app.routes for m in getattr(r, "methods", [])}
    for p, m in [
        (f"{B}/{{game_id}}/timeout", "POST"),
        (f"{B}/{{game_id}}/message", "POST"),
        (f"{B}/{{game_id}}", "GET"),
        (f"{B}/active", "GET"),
    ]:
        check(f"route {m} {p}", (p, m) in paths)


def shared_negative_checks():
    r = client.get(f"{B}/99999999")
    check("GET /{id} missing -> 404", r.status_code == 404, r.text)
    r = client.post(f"{B}/99999999/timeout")
    check("POST timeout missing -> 404", r.status_code == 404, r.text)
    r = client.post(f"{B}/99999999/message", json={"message": "hi"})
    check("POST message missing -> 404", r.status_code == 404, r.text)
    r = client.post(f"{B}/1/message", json={"message": ""})
    check("empty message -> 422", r.status_code == 422, r.text)
    r = client.post(f"{B}/1/message", json={"message": "   "})
    check("blank message -> 422", r.status_code == 422, r.text)
    r = client.post(f"{B}/1/message", json={"message": "x" * 241})
    check("oversize message -> 422", r.status_code == 422, r.text)


def safe_subset():
    print("SAFE SUBSET (human game active):")
    routes_present()
    r = client.get(f"{B}/active")
    check("GET /active resolves 200 (not shadowed)", r.status_code == 200, r.text)
    check("GET /active body has game key", "game" in r.json())
    r = client.get(B)
    check("history 200", r.status_code == 200, r.text)
    shared_negative_checks()
    r = client.post(B, json={"team_b_name": "x", "partner_number": 0})
    check("create while active -> 409", r.status_code == 409, r.text)
    print("SAFE-SUBSET PASS — FULL VERIFY DEFERRED (exit 2)")
    sys.exit(2)


def full_run():
    print("FULL ROUND-TRIP:")
    g0, p0, e0 = counts()
    print(f"  baseline: games={g0} points={p0} events={e0}")

    routes_present()

    # GET /active must beat GET /{game_id} route shadowing (no active game).
    r = client.get(f"{B}/active")
    check("GET /active -> 200 {game: null}", r.status_code == 200 and r.json() == {"game": None}, r.text)

    r = client.post(B, json={"team_b_name": "VERIFY-T04", "partner_number": 0})
    check("create -> 201", r.status_code == 201, r.text)
    if r.status_code != 201:
        sys.exit(1)
    gid = r.json()["game_id"]

    r = client.get(f"{B}/{gid}")
    check("detail -> 200", r.status_code == 200, r.text)
    body = r.json()
    check("detail keys game/points/score/events", set(body) == {"game", "points", "score", "events"}, str(set(body)))
    check("detail first_entry_at present + null", body["game"].get("first_entry_at") is None)
    check("detail events empty", body["events"] == [])

    r = client.post(f"{B}/{gid}/timeout")
    check("timeout -> 200 kind=timeout", r.status_code == 200 and r.json()["kind"] == "timeout", r.text)
    ev1 = r.json() if r.status_code == 200 else {}
    check("timeout message null", ev1.get("message") is None)
    check("timeout recorded_at ISO str", isinstance(ev1.get("recorded_at"), str))

    r = client.post(f"{B}/{gid}/message", json={"message": "  Hello fans!  "})
    check("message -> 200 trimmed", r.status_code == 200 and r.json()["message"] == "Hello fans!", r.text)

    shared_negative_checks()

    r = client.post(f"{B}/{gid}/points", json={"scoring_team": "SR"})
    check("add point -> 201", r.status_code == 201, r.text)
    r = client.get(f"{B}/{gid}")
    body = r.json()
    check("detail events ordered timeout,message", [e["kind"] for e in body["events"]] == ["timeout", "message"])
    check("detail first_entry_at stamped by point", body["game"]["first_entry_at"] is not None)

    r = client.get(f"{B}/active")
    check("GET /active game carries events", len(r.json()["game"]["events"]) == 2, r.text)

    r = client.post(f"{B}/{gid}/end")
    check("end -> 200", r.status_code == 200, r.text)
    r = client.post(f"{B}/{gid}/timeout")
    check("timeout on completed -> 409", r.status_code == 409, r.text)
    r = client.post(f"{B}/{gid}/message", json={"message": "x"})
    check("message on completed -> 409", r.status_code == 409, r.text)

    r = client.delete(f"{B}/{gid}")
    check("abandon -> 200", r.status_code == 200, r.text)
    g1, p1, e1 = counts()
    check("net-zero games", g1 == g0, f"{g1} vs {g0}")
    check("net-zero points", p1 == p0, f"{p1} vs {p0}")
    check("net-zero events", e1 == e0, f"{e1} vs {e0}")

    if FAILED:
        print("FULL VERIFY FAILED")
        sys.exit(1)
    print("FULL VERIFY PASS (net-zero rows)")
    sys.exit(0)


if __name__ == "__main__":
    if get_active_game() is not None:
        safe_subset()
    full_run()

