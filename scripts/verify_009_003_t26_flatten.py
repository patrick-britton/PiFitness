#!/usr/bin/env python3
"""
009-003 T26 — staging.flatten_activity_details ON CONFLICT repair (Bug 009-003-26-1).
====================================================================================

`staging.flatten_activity_details(integer, text)` (the last step of activity
processing) raises

    there is no unique or exclusion constraint matching the ON CONFLICT specification

because its upsert targets `ON CONFLICT (ts_utc)` while the only unique
constraint on `activities.activity_details` is the primary key
`(activity_id, ts_utc)`. The fix is a `CREATE OR REPLACE PROCEDURE` whose conflict
target is `(activity_id, ts_utc)` with the DO UPDATE SET list unchanged.

The agent never modifies PRD, so the DDL is delivered as a file for the HUMAN to
apply. This script supplies the mechanical evidence (no guessing) and the
verification:

(default)     read-only checks, before or after the human applies:
              * the live definition exists and which conflict target it uses
              * the unique constraints/indexes on activities.activity_details
              * a whitespace-insensitive diff of the authored DDL against the
                live definition — only the conflict target may differ
              * EXPLAIN-only planning of the upsert: the corrected target must
                plan, the old target must reproduce the production error
--dump-def    print the live definition verbatim (used to author the files)
--post-apply  after the human applies: re-reads the definition, re-plans the
              upsert, and reports how many rows the next run will touch. The
              rolled-back CALL is opt-in (`--live-probe`) because the upsert
              selects the WHOLE view (~3.9k rows), so a dry run would duplicate
              the work of the real run on the Pi; the production run itself is
              the actual verification.
--live-probe  with --post-apply: CALL the SP for the probe task and for the
              activity with the most GPS rows, always ROLLED BACK.
--data-state  read-only report for the T26 data-state question.

Note on ids: the procedure's first argument is a TASK id (it deletes
staging.api_imports by task_id), not an activity id — task 19 is the "Pirate
Activity Details" import (activities.details on Pirate Garmin). Activity 19 does
not exist in activities.activities, so the earlier "activity 19 has no GPS rows"
observation was comparing the view's activity_id against a task id; the view
itself holds ~3.9k GPS rows.

Exit code 0 = every check passed.
"""
import argparse
import contextlib
import io
import re
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / 'backend' / '.env')

from backend_functions.database_functions import con_cur, sql_to_dict

FIX_FILE = project_root / '.features' / 'migrations' / '009-003_t26_flatten_activity_details_fix.sql'
SIG = 'staging.flatten_activity_details(integer,text)'
PROBE_ACTIVITY = 19
PROBE_NAME = 'Pirate Activity Details'
# The defect is the conflict target, so both sides are compared as column lists
# (the live definition writes `ON CONFLICT(ts_utc)` without a space).
OLD_COLS = ['ts_utc']
NEW_COLS = ['activity_id', 'ts_utc']
TARGET_RE = re.compile(r'ON\s+CONFLICT\s*\([^)]*\)', re.IGNORECASE)


def with_target(statement, columns):
    """Rewrite a statement's parenthetical ON CONFLICT target."""
    return TARGET_RE.sub('ON CONFLICT (' + ', '.join(columns) + ')', statement)


def without_target(statement):
    """Blank the conflict target so the rest of a statement can be compared."""
    return with_target(statement, ['<target>'])

checks = []


def check(name, ok, detail=''):
    checks.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}: {name}{' — ' + detail if detail else ''}")


def strip_comments(text):
    """Remove -- and /* */ comments so statements can be collapsed to one line."""
    text = re.sub(r'/\*.*?\*/', ' ', text, flags=re.DOTALL)
    return re.sub(r'--[^\n]*', ' ', text)


def normalise(text):
    """Collapse whitespace so formatting can never mask (or fake) a change."""
    return re.sub(r'\s+', ' ', text).strip()


def body_statements(definition):
    """Whitespace-normalised statements of a procedure body, header dropped."""
    body = definition.split('$procedure$', 1)[-1].rsplit('$procedure$', 1)[0]
    return [s for s in (normalise(part) for part in strip_comments(body).split(';')) if s]


def live_definition():
    rows = sql_to_dict('SELECT pg_get_functiondef(%s::regprocedure) AS def', (SIG,))
    if not rows:
        return None
    return rows[0]['def'] if rows else None


def explain(statement):
    """Plan-only EXPLAIN: returns (plan_lines, error_message)."""
    conn, cur = con_cur()
    try:
        cur.execute('EXPLAIN ' + statement)
        return [str(r[0]) for r in cur.fetchall()], None
    except Exception as exc:  # noqa: BLE001 - the message is the evidence
        conn.rollback()
        return None, str(exc).strip().splitlines()[0]
    finally:
        cur.close()
        conn.close()


def upsert_statement(statements):
    """The activity_details upsert among a procedure body's statements."""
    for stmt in statements:
        if stmt.upper().startswith('INSERT INTO ACTIVITIES.ACTIVITY_DETAILS'):
            return stmt
    return None


def unique_indexes(table):
    """Unique (non-partial) index column lists for a table."""
    return sql_to_dict(
        """
        SELECT i.indexrelid::regclass AS index_name,
               i.indisprimary,
               array_to_string(
                   ARRAY(SELECT a.attname
                         FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                         JOIN pg_attribute a
                           ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                         ORDER BY k.ord), ', ') AS columns
        FROM pg_index i
        WHERE i.indrelid = %s::regclass
          AND i.indisunique
          AND i.indpred IS NULL
        ORDER BY i.indisprimary DESC, i.indexrelid::regclass::text
        """,
        (table,),
    )


def conflict_targets(statement):
    """(table, [column list]) for every ON CONFLICT target in a statement."""
    match = re.search(r'INSERT\s+INTO\s+([\w.]+)', statement, re.IGNORECASE)
    if not match:
        return None
    targets = re.findall(r'ON\s+CONFLICT\s*\(([^)]*)\)', statement, re.IGNORECASE)
    if not targets:
        return None
    return match.group(1).lower(), [
        [c.strip().lower() for c in t.split(',') if c.strip()] for t in targets
    ]


def fix_statements():
    """Statements of the authored fix file (the DDL delivered for human apply)."""
    if not FIX_FILE.exists():
        return None
    return body_statements(FIX_FILE.read_text(encoding='utf-8'))


def pre_apply_checks():
    """Catalog + plan-only evidence that the authored DDL is the correct fix."""
    definition = live_definition()
    check('live definition of staging.flatten_activity_details(integer,text) found', bool(definition))
    if not definition:
        return

    live_stmts = body_statements(definition)
    live_upsert = upsert_statement(live_stmts)
    live_targets = conflict_targets(live_upsert)[1] if live_upsert else []
    check('live upsert targets ON CONFLICT (ts_utc) — the defect',
          live_targets == [OLD_COLS],
          f'target={live_targets}'
          + (' (already corrected in PRD)' if live_targets == [NEW_COLS] else ''))

    idx = unique_indexes('activities.activity_details')
    cols = [(r['index_name'], r['columns']) for r in idx]
    check('activities.activity_details PK is (activity_id, ts_utc)',
          any(r['indisprimary'] and r['columns'] == 'activity_id, ts_utc' for r in idx),
          'unique indexes: ' + '; '.join(f'{n} ({c})' for n, c in cols))
    check('no standalone unique index on ts_utc (what the old target needs)',
          not any(c == 'ts_utc' for _, c in cols))

    authored = fix_statements()
    check('authored fix file present', authored is not None, str(FIX_FILE))
    if not authored:
        return
    authored_upsert = upsert_statement(authored)
    authored_targets = conflict_targets(authored_upsert)[1] if authored_upsert else []
    check('authored upsert targets ON CONFLICT (activity_id, ts_utc) — the PK',
          authored_targets == [NEW_COLS], f'target={authored_targets}')
    if not (live_upsert and authored_upsert):
        return

    same_count = len(live_stmts) == len(authored)
    check('statement count unchanged', same_count, f'live={len(live_stmts)} authored={len(authored)}')
    only_target_changed = all(
        (b == authored_upsert) if a == live_upsert else (a == b)
        for a, b in zip(live_stmts, authored)
    )
    check('every other statement byte-identical after whitespace normalisation', only_target_changed)
    check('the conflict target is the only change to the upsert',
          without_target(live_upsert) == without_target(authored_upsert))

    plan, err = explain(authored_upsert)
    check('corrected upsert plans — conflict target resolves to a unique index',
          err is None and bool(plan), err or f'{len(plan or [])} plan lines')

    _plan_old, err_old = explain(with_target(authored_upsert, OLD_COLS))
    check('old target still reproduces the production error (plan-only EXPLAIN)',
          bool(err_old) and 'no unique or exclusion constraint matching the ON CONFLICT specification' in err_old,
          err_old or 'unexpectedly planned')

    # Recurrence prevention (Bug 009-003-26-1): EVERY ON CONFLICT target in the
    # body must match a live unique index on its table — this is a class of
    # defect, not a one-off, so the audit covers the whole procedure.
    audit_detail = []
    audit_ok = True
    for stmt in authored:
        found = conflict_targets(stmt)
        if not found:
            continue
        table, targets = found
        colsets = [sorted(r['columns'].split(', ')) for r in unique_indexes(table)]
        for target in targets:
            audit_detail.append(f'{table} ({", ".join(target)})')
            if sorted(target) not in colsets:
                audit_ok = False
    check('every ON CONFLICT target in the body matches a live unique index', audit_ok,
          '; '.join(audit_detail) or 'no ON CONFLICT targets found')

    sys.path.insert(0, str(project_root / 'scripts'))
    from validate_sql import validate as validate_sql_text

    # Capture the validator's own output: it prints emoji verdicts, which raise
    # UnicodeEncodeError on a non-UTF-8 stdout (piped output on Windows — Bug
    # 009-003-26-2). The returned boolean is all this check needs.
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        validator_ok = validate_sql_text(FIX_FILE.read_text(encoding='utf-8'))
    check('scripts/validate_sql.py accepts the fix file table references', validator_ok,
          'captured validator output' if captured.getvalue() else '')


def count_details(activity_id):
    rows = sql_to_dict(
        'SELECT count(*) AS n FROM activities.activity_details WHERE activity_id = %s',
        (activity_id,),
    )
    return int(rows[0]['n'])


def call_rolled_back(activity_id, friendly_name):
    """CALL the SP inside a transaction that is ALWAYS rolled back."""
    conn, cur = con_cur()
    try:
        # psycopg2 already holds an open transaction here (autocommit is off), so
        # no explicit BEGIN is issued; the rollback below undoes everything the
        # call did — the procedure's writes and any DDL never commit.
        cur.execute('CALL staging.flatten_activity_details(%s, %s)', (activity_id, friendly_name))
        cur.execute(
            'SELECT count(*) FROM activities.activity_details WHERE activity_id = %s',
            (activity_id,),
        )
        return int(cur.fetchone()[0]), None
    except Exception as exc:  # noqa: BLE001 - the message is the evidence
        return None, str(exc).strip().splitlines()[0]
    finally:
        conn.rollback()
        cur.close()
        conn.close()


def gps_probe_activity():
    """An activity the SP's source view has GPS rows for (exercises the insert)."""
    rows = sql_to_dict(
        """
        SELECT activity_id
        FROM staging.vw_activity_flatten
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        GROUP BY activity_id
        ORDER BY count(*) DESC, activity_id DESC
        LIMIT 1
        """
    )
    return int(rows[0]['activity_id']) if rows else None


def post_apply_checks(live_probe=False):
    """
    After the human applies the DDL: definition + plan checks always; the
    rolled-back CALL only with `live_probe`, because the upsert selects the WHOLE
    view (thousands of rows) and a dry run would do that work twice on the Pi.
    """
    definition = live_definition()
    check('live definition still found', bool(definition))
    if definition:
        upsert = upsert_statement(body_statements(definition))
        targets = conflict_targets(upsert)[1] if upsert else []
        check('PRD now upserts with ON CONFLICT (activity_id, ts_utc)',
              targets == [NEW_COLS], f'target={targets}')
        plan, err = explain(upsert)
        check('live upsert plans — the production error is gone',
              err is None and bool(plan), err or f'{len(plan or [])} plan lines')

    view = sql_to_dict(
        'SELECT count(*) AS rows, count(latitude) AS with_lat FROM staging.vw_activity_flatten'
    )[0]
    print(f"  NOTE: the upsert selects the whole view — {view['with_lat']} GPS rows of "
          f"{view['rows']} will be upserted on the next processing run (legacy scope, unchanged by T26)")

    if not live_probe:
        print('  NOTE: rolled-back CALL skipped. The real verification is the human running '
              'activity processing (task 19, "Pirate Activity Details") once — that is the '
              'production path this bug broke. --live-probe forces an extra rolled-back run.')
        return

    before = count_details(PROBE_ACTIVITY)
    rows, err = call_rolled_back(PROBE_ACTIVITY, PROBE_NAME)
    check(f'task {PROBE_ACTIVITY} processes without the constraint error', err is None,
          err or f'{rows} detail rows inside the transaction')
    after = count_details(PROBE_ACTIVITY)
    check('probe left no data behind (transaction rolled back)', before == after,
          f'before={before} after={after}')

    # The SP's first argument is a TASK id (integer), not an activity id: the
    # upsert has no per-activity filter, so the GPS activity is not passed in.
    # Calling with the GPS activity_id raised "procedure ... (bigint, unknown)
    # does not exist" — a probe defect, not a product defect.
    gps = gps_probe_activity()
    if gps is None:
        check('an activity with GPS rows exists to exercise the insert path', False,
              'none in staging.vw_activity_flatten — data-state question, see Bug 009-003-26-1')
        return
    before_gps = count_details(gps)
    rows, err = call_rolled_back(PROBE_ACTIVITY, PROBE_NAME)
    check(f'task {PROBE_ACTIVITY} upserts GPS rows into activity {gps} '
          'without the constraint error', err is None,
          err or f'{rows} detail rows for activity {gps} inside the transaction')
    after_gps = count_details(gps)
    check('GPS probe left no data behind (transaction rolled back)', before_gps == after_gps,
          f'before={before_gps} after={after_gps}')

def data_state_report():
    """Read-only: would activity 19 have GPS rows even after the fix? (T26 detail)"""
    whole = sql_to_dict(
        'SELECT count(*) AS rows, count(latitude) AS with_lat, count(loc_point) AS with_point '
        'FROM staging.vw_activity_flatten'
    )[0]
    print(f"  staging.vw_activity_flatten (whole view): rows={whole['rows']} "
          f"with_latitude={whole['with_lat']} with_loc_point={whole['with_point']}")

    per_activity = sql_to_dict(
        'SELECT count(*) AS rows, count(latitude) AS with_lat '
        'FROM staging.vw_activity_flatten WHERE activity_id = %s',
        (PROBE_ACTIVITY,),
    )[0]
    print(f"  staging.vw_activity_flatten (activity {PROBE_ACTIVITY}): rows={per_activity['rows']} "
          f"with_latitude={per_activity['with_lat']}")

    payloads = sql_to_dict(
        'SELECT count(*) AS n FROM staging.activity_payload WHERE activity_id = %s',
        (PROBE_ACTIVITY,),
    )[0]
    print(f"  staging.activity_payload (activity {PROBE_ACTIVITY}): payloads={payloads['n']}")

    locations = sql_to_dict('SELECT count(*) AS n FROM staging.temp_activity_locations')[0]
    print(f"  staging.temp_activity_locations (no activity_id column): rows={locations['n']}")

    activity = sql_to_dict(
        'SELECT activity_type_name, distance_m, duration_s, is_downloaded '
        'FROM activities.activities WHERE activity_id = %s',
        (PROBE_ACTIVITY,),
    )
    print(f"  activities.activities (activity {PROBE_ACTIVITY}): "
          f"{dict(activity[0]) if activity else 'NOT FOUND'}")

    imports = sql_to_dict(
        """
        SELECT count(*) AS imports,
               count(*) FILTER (WHERE payload::text ILIKE '%%latitude%%') AS payloads_mentioning_latitude,
               min(event_time_utc) AS first_utc,
               max(event_time_utc) AS last_utc
        FROM staging.api_imports
        WHERE task_id = %s
        """,
        (PROBE_ACTIVITY,),
    )[0]
    print(f"  staging.api_imports (task_id {PROBE_ACTIVITY}): {dict(imports)}")

    library = sql_to_dict(
        'SELECT api_service_name, api_function_name, flatten_sproc, value_recency '
        'FROM api_services.function_library WHERE friendly_name = %s',
        (PROBE_NAME,),
    )
    print(f"  api_services.function_library ('{PROBE_NAME}'): "
          f"{dict(library[0]) if library else 'NOT FOUND'}")


def main():
    parser = argparse.ArgumentParser(description='009-003 T26 flatten_activity_details checks')
    parser.add_argument('--dump-def', action='store_true',
                        help='print the live procedure definition and exit')
    parser.add_argument('--post-apply', action='store_true',
                        help='verify after the human applies the DDL (definition + plan checks)')
    parser.add_argument('--live-probe', action='store_true',
                        help='with --post-apply: also CALL the SP inside a rolled-back transaction')
    parser.add_argument('--data-state', action='store_true',
                        help='read-only report: does the flatten source have GPS rows at all?')
    args = parser.parse_args()

    if args.dump_def:
        print(live_definition() or '')
        return 0

    if args.data_state:
        print('009-003 T26 — flatten data-state report (read-only)')
        data_state_report()
        return 0

    print('009-003 T26 — staging.flatten_activity_details conflict-target checks')
    if args.post_apply:
        post_apply_checks(live_probe=args.live_probe)
    else:
        pre_apply_checks()

    failed = [c for c in checks if not c[1]]
    print(f'\n{len(checks) - len(failed)}/{len(checks)} checks passed')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())


