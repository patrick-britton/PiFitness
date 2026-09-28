#!/usr/bin/env python3
"""
Live verification: 009-003 T02b / T06b contract additions (OQ-3).
================================================================

Re-runnable probe behind the two 2026-09-18 contract additions that unblock
Match Activities / Course Review route rendering:

- T02b: `GET /api/segments/list` rows carry the segment's reference-geometry
  pointers (activity_reference_id, reference_start_point, reference_end_point),
  which must equal the `activities.segments` row and must resolve to a real
  route through `GET /api/segments/{activity_id}/route`.
- T06b: `GET /api/segments/{id}/matches/candidates` rows carry the matched
  effort window (best_start_dist, best_end_dist), which must equal the
  `activities.vw_temp_segment_matches_downselect` values.

The T06b half needs at least one candidate row, which only the matching
pipeline produces, so it creates a throwaway segment ("ZZ Probe T06b"), runs
find-matches for it, and always cleans up (probe delete + probe temp rows) in a
`finally`. Read-only otherwise; no schema changes; no pandas.

Exit code 0 = every check passed.
"""
import json
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from dotenv import load_dotenv

load_dotenv(os.path.join(project_root, 'backend', '.env'))

from fastapi.testclient import TestClient

from backend.main import app
from backend_functions.database_functions import qec, sql_to_dict

client = TestClient(app)
checks = []


def check(name, ok, detail=''):
    checks.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}: {name}{' — ' + detail if detail else ''}")


REF_KEYS = ('activity_reference_id', 'reference_start_point', 'reference_end_point')
# 009-002 leaderboard list contract (must stay exactly these eight columns).
LB_KEYS = {
    'segment_id', 'segment_name', 'is_course', 'activity_type_name',
    'distance_mi', 'elevation_gain', 'last_effort', 'matched_activity_count',
}

print('\n009-003 T02b: picker rows carry the reference geometry')
rows = client.get('/api/segments/list', params={'kind': 'segment', 'min_distance': 0}).json()['data']
check('all three reference keys present', bool(rows) and all(k in rows[0] for k in REF_KEYS), f'{len(rows)} rows')

ids = [int(r['segment_id']) for r in rows]
db_rows = {int(r['segment_id']): r for r in sql_to_dict(
    """
    SELECT segment_id, activity_reference_id, reference_start_point, reference_end_point
    FROM activities.segments
    WHERE segment_id = ANY(%s)
    """,
    (ids,),
)}
mismatch = [
    r['segment_id'] for r in rows
    if any((db_rows.get(int(r['segment_id'])) or {}).get(k) != r[k] for k in REF_KEYS)
]
check('values equal activities.segments', not mismatch, f'mismatch={mismatch[:5]}')

sample = next((r for r in rows if r['activity_reference_id']), None)
check('a row resolves a reference activity', sample is not None,
      json.dumps({k: sample[k] for k in REF_KEYS}, default=str) if sample else 'no reference row')

if sample:
    route = client.get(
        f"/api/segments/{int(sample['activity_reference_id'])}/route",
        params={'start_m': sample['reference_start_point'], 'end_m': sample['reference_end_point']},
    )
    rdata = (route.json() or {}).get('data') or {}
    coords = rdata.get('path_coords') or []
    check('reference window serves a route',
          route.status_code == 200 and len(coords) > 1 and len(rdata.get('elevations') or []) == len(coords),
          f'status={route.status_code} points={len(coords)}')

lb = client.get('/api/activities/leaderboard/segments').json()
lb_rows = lb if isinstance(lb, list) else (lb.get('data') or [])
extra = sorted(set(lb_rows[0].keys()) - LB_KEYS) if lb_rows else []
missing = sorted(LB_KEYS - set(lb_rows[0].keys())) if lb_rows else []
print('\n009-003 T06b: candidate rows carry the matched effort window')
probe_id = None
try:
    act = sql_to_dict(
        """
        SELECT activity_id FROM activities.activities
        WHERE distance_m BETWEEN 2000 AND 20000
        ORDER BY start_timestamp_utc DESC LIMIT 1
        """
    )[0]
    created = client.post('/api/segments/create', json={
        'activity_id': int(act['activity_id']),
        'start_m': 0,
        'end_m': 1500,
        'name': 'ZZ Probe T06b',
        'is_course': False,
        'map_style': 'positron',
    })
    probe_id = (created.json() or {}).get('segment_id')
    check('probe segment created', created.status_code == 200 and probe_id, f'segment_id={probe_id}')

    found = client.post(f'/api/segments/{probe_id}/matches/find')
    cands = client.get(f'/api/segments/{probe_id}/matches/candidates').json()
    data = cands.get('data') or []
    check('probe produced candidates', found.status_code == 200 and bool(data),
          f'status={found.status_code} candidates={len(data)}')
    check('candidate rows expose the window keys',
          bool(data) and all('best_start_dist' in d and 'best_end_dist' in d for d in data))

    view = {
        int(r['activity_id']): (int(r['best_start_dist']), int(r['best_end_dist']))
        for r in sql_to_dict(
            """
            SELECT activity_id, best_start_dist, best_end_dist
            FROM activities.vw_temp_segment_matches_downselect
            WHERE segment_id = %s
            """,
            (probe_id,),
        )
    }
    bad = [
        d['activity_id'] for d in data
        if (int(d['best_start_dist']), int(d['best_end_dist'])) != view.get(int(d['activity_id']))
    ]
    check('windows equal the downselect view', not bad, f'mismatch={bad[:5]}')

    if data:
        wroute = client.get(
            f"/api/segments/{int(data[0]['activity_id'])}/route",
            params={'start_m': int(data[0]['best_start_dist']), 'end_m': int(data[0]['best_end_dist'])},
        )
        wdata = (wroute.json() or {}).get('data') or {}
        check('candidate window serves a route',
              wroute.status_code == 200 and len(wdata.get('path_coords') or []) > 1,
              f"status={wroute.status_code} points={len(wdata.get('path_coords') or [])}")
finally:
    if probe_id:
        temp_sql = (
            "DELETE FROM activities.temp_segment_matches_downselect WHERE segment_id = %s",
            "DELETE FROM activities.temp_segment_matches_v1 WHERE segment_id = %s",
            "DELETE FROM activities.temp_segment_matching_segments WHERE segment_id = %s",
            "DELETE FROM staging.temp_segment_matches WHERE segment_id = %s",
        )
        errs = [err for sql in temp_sql for err in (qec(sql, (probe_id,)),) if err]
        deleted = client.delete(f'/api/segments/{probe_id}')
        check('probe cleaned up', deleted.status_code == 200 and not errs,
              f'delete={deleted.status_code} temp_errors={errs[:1]}')

failed = [c for c in checks if not c[1]]
print(f"\n{len(checks) - len(failed)}/{len(checks)} checks passed")
sys.exit(1 if failed else 0)
check('009-002 leaderboard list unchanged', not extra and not missing, f'extra={extra} missing={missing}')
