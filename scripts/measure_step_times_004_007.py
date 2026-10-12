"""
004-007 T01 — 1-off step-timing measurement tool.

Times every NON-playlist processing step for the three fixed test activities,
`iterations` times each (default 10), clearing the human-approved,
activity-scoped table list before each iteration (the final iteration's data
remains in the database — AC-8). Rows append to
local_storage/step_timing_004_007.csv for the T03 formula fit.

Playlist steps (lookup_playlist .. report_shuffle) are EXEMPT — static 250 ms
estimate per the feature description.

Modes (run from repo root):
  --plan       read-only: print the clear SQL + step plan + a sanity check of
               the three test activities; create the CSV header if missing.
               Executes NO database writes.
  --dry-run    one activity x one iteration: clear statements PRINTED but NOT
               executed; the pipeline steps execute; timing rows are written.
  (default)    full campaign: APPROVE gate, then 3 activities x N iterations
               with clears executed before every iteration.

Approval gate (OQ-2): destructive clears run only after the operator types
APPROVE at the prompt (or passes --i-approve-clears once the list has been
human-approved). The gate is asked once per session; all iterations then run
automatically.

No pandas. Stdlib (csv/argparse/sys) + existing repo modules only.

  .venv/Scripts/python.exe scripts/measure_step_times_004_007.py --plan
"""
import argparse
import csv
import sys
from pathlib import Path

from backend_functions.database_functions import sql_to_dict, qec
from backend_functions.ultimate_task_executioner_v2 import ultimate_task_executioner
from backend_functions.activity_smoothing import activity_post_processing_steps
from backend.api.activities import _run_step, _resolve_activity_id

CSV_PATH = Path(__file__).resolve().parent.parent / "local_storage" / "step_timing_004_007.csv"
CSV_HEADER = ["step_id", "activity_id", "iteration", "distance_m", "elapsed_ms", "status", "error"]

TEST_ACTIVITIES = [
    (22447361779, "short"),
    (21133164016, "medium"),
    (17325964906, "long"),
]

# ---------------------------------------------------------------------------
# Clear list (OQ-2 — human-approved once, then automatic).
# Everything here is scoped to the activity being re-processed, except the
# heartrate delete, which is window-scoped to the activity's own timestamps on
# the global health.heartrate_raw table (flagged for approval).
#
# EXCLUDED on purpose:
#   activities.activities base row — the resolve view and the details
#       eligibility view depend on it; task 4 is a date-range Garmin pull
#       (`get_activities_by_date`) that may never re-fetch an old id, so
#       deleting the row risks PERMANENT data loss. Only its derived columns
#       (is_downloaded, activity_path) are reset by the last statement.
#   activities.elevation_reference — global 5 m grid keyed by grid_5m_00_id,
#       written with ON CONFLICT DO UPDATE (idempotent); not activity-scoped.
#   activities.temp_* — every segment SPROC TRUNCATEs its own temp tables first.
#   logging.*, tasks.task_executions — operational logs, never cleared.
#   staging.* — extract refresh source (task 4/19 overwrite).
#   music.* — playlist steps are exempt from measurement.
# ---------------------------------------------------------------------------
def build_clears(aid):
    """Return [(label, sql, params)] — HR window captured before any delete."""
    rows = sql_to_dict(
        "SELECT min(ts_utc) AS lo, max(ts_utc) AS hi "
        "FROM activities.activity_details WHERE activity_id = %s", (aid,))
    lo = rows[0]["lo"] if rows else None
    hi = rows[0]["hi"] if rows else None
    clears = []
    if lo is not None and hi is not None:
        clears.append((
            "health.heartrate_raw (activity ts window)",
            "DELETE FROM health.heartrate_raw WHERE ts_utc >= %s AND ts_utc <= %s",
            (lo, hi),
        ))
    else:
        print(f"[warn] activity {aid}: no activity_details rows - "
              "heartrate window delete skipped this round")
    clears += [
        ("activities.activity_details",
         "DELETE FROM activities.activity_details WHERE activity_id = %s", (aid,)),
        ("activities.activity_details_distance",
         "DELETE FROM activities.activity_details_distance WHERE activity_id = %s", (aid,)),
        ("activities.segments_details",
         "DELETE FROM activities.segments_details WHERE activity_id = %s", (aid,)),
        ("activities.segment_matches",
         "DELETE FROM activities.segment_matches WHERE activity_id = %s", (aid,)),
        ("activities.activities (derived cols only: is_downloaded, activity_path)",
         "UPDATE activities.activities SET is_downloaded = FALSE, "
         "activity_path = NULL WHERE activity_id = %s", (aid,)),
    ]
    return clears


def print_clears(aid):
    for label, sql, params in build_clears(aid):
        print(f"  [{label}]\n    {sql}  -- params: {params}")


def run_clears(aid):
    """Execute the approved clears for one activity (gate already passed)."""
    for label, sql, params in build_clears(aid):
        res = qec(sql, params)
        if res is not None:
            raise RuntimeError(f"CLEAR FAILED [{label}]: {res[0]}")
    print(f"[clear] activity {aid}: clears executed")


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------
def activity_info(aid):
    rows = sql_to_dict(
        "SELECT activity_type_name, distance_m, is_downloaded "
        "FROM activities.activities WHERE activity_id = %s", (aid,))
    if not rows:
        raise SystemExit(f"activity {aid} not found in activities.activities")
    return rows[0]


def resolve_kind(activity_type_name):
    """'Walk' or 'Run' — the key vw_last_activity_id_by_type expects."""
    return "Walk" if ("walk" in activity_type_name or "hike" in activity_type_name) else "Run"


def ensure_csv_header():
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow(CSV_HEADER)
        print(f"[csv] header written -> {CSV_PATH}")


def write_row(step_id, aid, iteration, distance, ms, status, error=""):
    """Append one timing row and echo it immediately (crash-visible progress)."""
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerow(
            [step_id, aid, iteration, distance, ms, status, error])
    line = f"  {step_id},{aid},{iteration},{distance},{ms},{status}"
    if error:
        line += f" | {error}"
    print(line)


def run_iteration(aid, kind, iteration, distance, execute_clears):
    """Run every non-playlist step once for one activity. False = step error."""
    if execute_clears:
        run_clears(aid)
    else:
        print(f"[dry-run] clear statements for activity {aid} (NOT executed):")
        print_clears(aid)

    # Step 1: sync_activities — same production wrapper as _process_generator
    r = _run_step("sync_activities", ultimate_task_executioner, force_task_id=4)
    write_row("sync_activities", aid, iteration, distance,
              r.elapsed_ms, r.status, r.error or "")
    if r.status == "error":
        return False

    # Step 2: sync_details (task 19 — re-downloads details when is_downloaded=FALSE)
    r = _run_step("sync_details", ultimate_task_executioner, force_task_id=19)
    write_row("sync_details", aid, iteration, distance,
              r.elapsed_ms, r.status, r.error or "")
    if r.status == "error":
        return False

    # Step 3: resolve_activity (read-only view select, production wrapper)
    r = _run_step("resolve_activity", _resolve_activity_id, kind)
    write_row("resolve_activity", aid, iteration, distance,
              r.elapsed_ms, r.status, r.error or "")
    if r.status == "error":
        return False

    # Step 4+: post-processing substeps for THIS activity id.
    # The generator yields a start marker (step_id, None, None) before each
    # sub-step and the terminal (step_id, elapsed_ms, error) after; markers
    # are not timed rows — skip them (004-007 T09).
    for step_id, ms, error in activity_post_processing_steps(aid):
        if ms is None and error is None:
            continue
        status = "error" if error else "complete"
        write_row(step_id, aid, iteration, distance, ms, status, error or "")
        if error:
            return False
    return True


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def confirm_gate():
    print("=" * 72)
    print("DESTRUCTIVE CLEAR LIST - ENABLED FOR THIS SESSION IF YOU APPROVE:")
    for aid, label in TEST_ACTIVITIES:
        print(f"\nactivity {aid} ({label}):")
        print_clears(aid)
    print("\n" + "=" * 72)
    answer = input("Type APPROVE to run these clears before every iteration: ")
    return answer.strip() == "APPROVE"


def cmd_plan():
    print("STEP PLAN (non-playlist; playlist steps exempt = static 250 ms):")
    print("  1 sync_activities (forced task 4)")
    print("  2 sync_details (forced task 19)")
    print("  3 resolve_activity (vw_last_activity_id_by_type select)")
    print("  4.. activity_post_processing_steps(activity_id):")
    print("     insert_heartrate, elevation time x4, resample, elevation distance x4,")
    print("     build_activity_path, segment matching x9")
    ensure_csv_header()
    print("\nCLEAR LIST per iteration:")
    for aid, label in TEST_ACTIVITIES:
        info = activity_info(aid)
        print(f"\nactivity {aid} ({label}): type={info['activity_type_name']} "
              f"distance_m={info['distance_m']} is_downloaded={info['is_downloaded']}")
        print_clears(aid)
    print("\n[plan] read-only - nothing was written to the database.")


def cmd_dry_run(aid):
    ensure_csv_header()
    info = activity_info(aid)
    kind = resolve_kind(info["activity_type_name"])
    distance = float(info["distance_m"])
    ok = run_iteration(aid, kind, 1, distance, execute_clears=False)
    print(f"[dry-run] {'OK' if ok else 'STEP ERROR (row recorded)'} - "
          f"rows appended to {CSV_PATH}")


def cmd_campaign(iterations, skip_gate):
    ensure_csv_header()
    if not skip_gate and not confirm_gate():
        print("Approval not given - aborting before any destructive statement.")
        sys.exit(1)
    for aid, label in TEST_ACTIVITIES:
        info = activity_info(aid)
        kind = resolve_kind(info["activity_type_name"])
        distance = float(info["distance_m"])
        for it in range(1, iterations + 1):
            print(f"\n== {label} activity {aid} iteration {it}/{iterations} ==")
            if not run_iteration(aid, kind, it, distance, execute_clears=True):
                print("[abort] step failure - campaign stopped (error row recorded)")
                sys.exit(2)
    print("\n[campaign] complete - final iteration data remains in the database.")


def main():
    ap = argparse.ArgumentParser(
        description="004-007 step-timing measurement tool (no pandas)")
    ap.add_argument("--plan", action="store_true",
                    help="read-only plan + clear list + CSV header")
    ap.add_argument("--dry-run", action="store_true",
                    help="1 activity x 1 iteration; clears printed, NOT executed")
    ap.add_argument("--activity", type=int, default=TEST_ACTIVITIES[0][0],
                    help="activity id for --dry-run (default: short test id)")
    ap.add_argument("--iterations", type=int, default=10,
                    help="iterations per activity for the campaign")
    ap.add_argument("--i-approve-clears", action="store_true",
                    help="skip the interactive APPROVE gate (list already human-approved)")
    args = ap.parse_args()
    if args.plan:
        cmd_plan()
    elif args.dry_run:
        cmd_dry_run(args.activity)
    else:
        cmd_campaign(args.iterations, args.i_approve_clears)


if __name__ == "__main__":
    main()
