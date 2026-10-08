#!/usr/bin/env python3
"""004-005 T05 — build the corrective SQL file from the LIVE procedure body.
Fetches pg_get_functiondef from PRD (read-only SELECT), splices in the
monotonic_base CTE + repoints the two downstream FROM sources, writes
scripts/004-005_T05_resample_fix.sql. See HEADER inside for full notes.
Run from repo root:  .venv/Scripts/python.exe scripts/build_004_005_t05_sql.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / 'backend' / '.env')

from backend_functions.database_functions import sql_to_dict
from t05_sql_header import HEADER, MONOTONIC_CTE, OUT_NAME

OUT_FILE = project_root / 'scripts' / OUT_NAME


def main():
    rows = sql_to_dict(
        "SELECT pg_get_functiondef(p.oid) AS def "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'activities' "
        "AND p.proname = 'resample_activity_to_distance'")
    live = rows[0]['def']
    assert 'segment_bounds' in live, 'live body missing segment_bounds'
    assert live.count('FROM normalized_base') == 2, (
        f"expected 2 FROM normalized_base, found {live.count('FROM normalized_base')}")

    fixed = live.replace('FROM normalized_base', 'FROM monotonic_base')
    assert fixed.count('FROM monotonic_base') == 2
    # remaining ref is the CTE definition header ", normalized_base as (";
    # the splice below adds "FROM normalized_base nb" as the new CTE source
    assert fixed.count('normalized_base') == 1

    import re
    anchor = '            ground_contact_balance_left, performance_condition from base'
    assert fixed.count(anchor) == 1, 'normalized_base anchor not unique'
    # Close normalized_base, then open the new CTE at the same WITH level.
    # The regex consumes normalized_base's own live close-paren so it is not
    # doubled: "... from base \n ) \n , monotonic_base as (...) \n ,segment_bounds".
    pat = re.compile(re.escape(anchor) + r'\s*\r?\n\s*\)')
    assert len(pat.findall(fixed)) == 1, 'anchor+close not unique'
    fixed = pat.sub(
        anchor + '\n\n        )' + MONOTONIC_CTE.rstrip('\n'), fixed, count=1)

    OUT_FILE.write_text(HEADER + fixed + '\n', encoding='utf-8')
    print(f'wrote {OUT_FILE} ({len(fixed)} body chars)')


if __name__ == '__main__':
    main()
