#!/usr/bin/env python3
"""
Live verification: 009-003 T37 — bulk-reject is one atomic pass (Bug 009-003-36-1).
==================================================================================

Creates a throwaway segment ("ZZ Probe T37") from a confirmed-match activity,
runs find-matches steps 1-3 (match_activities, pair_generation,
matches_all_polygon — deliberately WITHOUT the step-4 mass confirmation so
pending candidates remain), then issues ONE bulk-reject request and asserts:
  * 200 {rejected: N} where N equals the pre-reject pending-candidate count,
  * the downselect candidate set is EMPTY afterwards (one pass, no leftovers),
  * the CALL chain commits cleanly on a single non-autocommit connection
    (a failure would surface as 422 "bulk reject failed" / leftover rows).
Always cleans up (probe delete via the segment-delete endpoint, which also
removes match/detail rows) in a `finally`.

Exit code 0 = every check passed.
"""
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from dotenv import load_dotenv

load_dotenv(os.path.join(project_root, 'backend', '.env'))

from fastapi.testclient import TestClient

from backend.main import app
from backend_functions.database_functions import sql_to_dict

client = TestClient(app)
checks = []


def check(name, ok, detail=''):
    checks.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}: {name}{' — ' + detail if detail else ''}")


def candidates(seg_id):
    data = client.get(f'/api/segments/{seg_id}/matches/candidates').json()
    return sorted(
        (int(d['activity_id']), int(d['best_start_dist']), int(d['best_end_dist']))
        for d in (data.get('data') or [])
    )


probe_id = None
try:
    # Same probe-activity selection as the T20 probe: a CONFIRMED match of an
    # existing segment provably passes some segment's gates, so find-matches
    # must produce candidates for it.
    act = sql_to_dict(
        """
        SELECT sm.activity_id
        FROM activities.segment_matches sm
        WHERE sm.match_confirmed
        ORDER BY sm.segment_id DESC, sm.activity_id DESC
        LIMIT 1
        """
    )[0]
    created = client.post('/api/segments/create', json={
        'activity_id': int(act['activity_id']),
        'start_m': 0,
        'end_m': 1500,
        'name': 'ZZ Probe T37',
        'is_course': False,
        'map_style': 'positron',
    })
    probe_id = (created.json() or {}).get('segment_id')
    check('probe segment created', created.status_code == 200 and probe_id,
          f'segment_id={probe_id}')

    # Steps 1-3 only: leave the candidates pending (skip mass_confirmation).
    labels = {1: 'match_activities', 2: 'pair_generation', 3: 'matches_all_polygon'}
    for step in (1, 2, 3):
        resp = client.post(f'/api/segments/{probe_id}/matches/find/{step}')
        check(f'step {step} ({labels[step]}) returned 200', resp.status_code == 200,
              f'status={resp.status_code}'
              + (f' detail={resp.text[:200]}' if resp.status_code != 200 else ''))

    pending = candidates(probe_id)
    check('pending candidates exist before reject', bool(pending), f'{len(pending)} candidates')

    # ONE bulk-reject request must empty the whole set (atomic pass).
    rejected_resp = client.post(f'/api/segments/{probe_id}/matches/bulk-reject', json={})
    check('bulk-reject returned 200', rejected_resp.status_code == 200,
          f'status={rejected_resp.status_code} detail={rejected_resp.text[:200]}')
    rejected_n = (rejected_resp.json() or {}).get('rejected')
    check('rejected count == pending candidate count', rejected_n == len(pending),
          f'rejected={rejected_n} pending={len(pending)}')

    after = candidates(probe_id)
    check('downselect emptied in one pass', after == [], f'{len(after)} leftover')

    # Idempotence guard: a second pass over the emptied set reports 0.
    second = client.post(f'/api/segments/{probe_id}/matches/bulk-reject', json={})
    check('second bulk-reject is {rejected: 0}', second.status_code == 200
          and (second.json() or {}).get('rejected') == 0,
          f'status={second.status_code} body={second.text[:100]}')
finally:
    if probe_id:
        deleted = client.delete(f'/api/segments/{probe_id}')
        ok = deleted.status_code == 200
        print(f"  cleanup: probe segment {probe_id} deleted={ok} "
              f"(status={deleted.status_code})")

failed = [name for name, ok, _ in checks if not ok]
print(f"\n{len(checks) - len(failed)}/{len(checks)} checks passed")
sys.exit(1 if failed else 0)
