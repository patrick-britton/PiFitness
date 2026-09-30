#!/usr/bin/env python3
"""
Live verification: 009-003 T20 find-matches step endpoints (+ T21 post-repair).
==============================================================================

Creates a throwaway segment ("ZZ Probe T20"), runs the four step endpoints
(POST /api/segments/{id}/matches/find/{step}, 1..4) in sequence — the same SPs
in the same order as the blocking all-in-one POST /matches/find — then runs the
all-in-one flow on the SAME probe and asserts the candidate set is unchanged
(parity). Also checks the 422 (unknown step) and 404 (unknown segment) paths.
Always cleans up (probe delete + probe temp rows) in a `finally`.

Serves double duty as the Bug 009-003-21-1 post-repair check: pair_generation
(previously the SP that hit the corrupt heap page) runs inside step 2.

Exit code 0 = every check passed.
"""
import os
import sys
import time

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


def snapshot(seg_id):
    cands = client.get(f'/api/segments/{seg_id}/matches/candidates').json()
    return sorted(
        (int(d['activity_id']), int(d['best_start_dist']), int(d['best_end_dist']),
         round(float(d['confidence']), 6))
        for d in (cands.get('data') or [])
    )


probe_id = None
try:
    # The strongest functional check: build the probe from an activity that is
    # a CONFIRMED match of an existing segment — it provably passes some
    # segment's gates, so the pipeline must produce candidates for it. (The
    # 2-20 km "newest" choice that worked pre-repair now yields 0 because the
    # repair changed the dataset; see Bug 009-003-20-1.)
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
        'name': 'ZZ Probe T20',
        'is_course': False,
        'map_style': 'positron',
    })
    probe_id = (created.json() or {}).get('segment_id')
    check('probe segment created', created.status_code == 200 and probe_id, f'segment_id={probe_id}')

    # The four step endpoints, in pipeline order (T20).
    labels = {1: 'match_activities', 2: 'pair_generation', 3: 'matches_all_polygon',
              4: 'mass_confirmation'}
    for step in (1, 2, 3, 4):
        t0 = time.monotonic()
        resp = client.post(f'/api/segments/{probe_id}/matches/find/{step}')
        check(f'step {step} ({labels[step]}) returned 200', resp.status_code == 200,
              f'status={resp.status_code} elapsed={time.monotonic() - t0:.1f}s'
              + (f' detail={resp.text[:200]}' if resp.status_code != 200 else ''))

    after_steps = snapshot(probe_id)
    check('step flow produced candidates', bool(after_steps), f'{len(after_steps)} candidates')

    # Parity: the all-in-one flow on the same probe must leave the candidate
    # set identical (same SPs, deterministic pipeline; mass_confirmation is
    # idempotent over the pending set).
    allin = client.post(f'/api/segments/{probe_id}/matches/find')
    check('all-in-one find returned 200', allin.status_code == 200,
          f'status={allin.status_code}' + (f' detail={allin.text[:200]}' if allin.status_code != 200 else ''))
    after_allin = snapshot(probe_id)
    check('candidate set identical across both flows', after_steps == after_allin,
          f'steps={len(after_steps)} allin={len(after_allin)}')

    # Validation paths.
    bad_step = client.post(f'/api/segments/{probe_id}/matches/find/9')
    check('unknown step -> 422', bad_step.status_code == 422, f'status={bad_step.status_code}')
    missing = client.post('/api/segments/999999999/matches/find/1')
    check('unknown segment -> 404', missing.status_code == 404, f'status={missing.status_code}')
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
