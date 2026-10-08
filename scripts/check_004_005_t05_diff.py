#!/usr/bin/env python3
"""004-005 T05 scratch: verify the generated fix file is surgical.
Compares the generated procedure body against the live pg_get_functiondef:
parens balanced, exactly one monotonic_base CTE added, exactly two FROM
repoints, and a whitespace-insensitive diff showing ONLY the intended lines.
Read-only vs PRD. Run from repo root.
"""
import difflib
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / 'backend' / '.env')

from backend_functions.database_functions import sql_to_dict
from scripts.t05_sql_header import HEADER  # noqa: E402  (repo-root import)


def main():
    live = sql_to_dict(
        "SELECT pg_get_functiondef(p.oid) AS def "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'activities' "
        "AND p.proname = 'resample_activity_to_distance'")[0]['def']
    gen = (project_root / 'scripts' / '004-005_T05_resample_fix.sql'
           ).read_text(encoding='utf-8')
    assert gen.startswith(HEADER), 'header mismatch'
    body = gen[len(HEADER):].rstrip('\n') + '\n'

    print('parens live:', live.count('('), live.count(')'),
          '| body:', body.count('('), body.count(')'))
    assert body.count('(') == body.count(')'), 'unbalanced parens in fix'
    # +5: SELECT * FROM ( ... MAX( ... OVER ( ... PRECEDING ... ) frontier(CTE)
    assert body.count('(') == live.count('(') + 5, 'unexpected paren delta'

    diff = list(difflib.unified_diff(
        live.splitlines(), body.splitlines(), lineterm=''))
    adds = [ln[1:] for ln in diff if ln.startswith('+')
            and not ln.startswith('+++')]
    dels = [ln[1:] for ln in diff if ln.startswith('-')
            and not ln.startswith('---')]
    print(f'added lines: {len(adds)}, removed lines: {len(dels)}')
    for ln in dels:
        print('DEL:', repr(ln))
    norm = lambda s: ' '.join(s.split())  # noqa: E731
    expected_add = {
        '-- 004-005 T05 monotonic-distance frontier (see header).',
        ', monotonic_base as (',
        'SELECT * FROM (',
        'SELECT nb.*,',
        'MAX(nb.distance_mm) OVER (',
        'ORDER BY nb.elapsed_duration_s, nb.distance_mm',
        'ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING',
        ') AS prev_max_distance_mm',
        'FROM normalized_base nb',
        ') frontier',
        'WHERE prev_max_distance_mm IS NULL',
        'OR distance_mm > prev_max_distance_mm',
        ')',
        'FROM monotonic_base',
        '(SELECT (MAX(distance_mm)/1000)::int FROM monotonic_base',
    }
    unexpected_add = [ln for ln in adds
                      if ln.strip() and norm(ln) not in expected_add]
    expected_del = {
        'FROM normalized_base',
        '(SELECT (MAX(distance_mm)/1000)::int FROM normalized_base',
    }
    unexpected_del = [ln for ln in dels if norm(ln) not in expected_del]
    print('unexpected adds:', unexpected_add)
    print('unexpected dels:', unexpected_del)
    assert not unexpected_add and not unexpected_del, 'non-surgical diff'
    print('surgical diff OK')


if __name__ == '__main__':
    main()
