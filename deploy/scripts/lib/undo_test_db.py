#!/usr/bin/env python3
"""
deploy/scripts/lib/undo_test_db.py  --  exit-handler undo for the TEST database.

    undo_test_db.py --run-dir <dir> --outcome success|incomplete|failure --exit-status <n>

Registered (name: test_db) by step_db_prepare, and later by the migrate step, BEFORE they
make the TEST database dirty.  Contract: see step_db_prepare.py and spec 4.4 / section 6.

  outcome == success                        -> SKIPPED (GO: the database is kept)
  TEST state marker is not 'dirty'          -> SKIPPED (nothing to undo)
  run.state DB_PREPARE_STATE == running     -> SKIPPED (TEST step 4 itself did not finish;
                                               the marker stays dirty and the next run's
                                               preflight restores the database, spec 4.4)
  otherwise                                 -> restore the EXACT file recorded as
                                               baseline_dump in STATE_FILE (never "the
                                               newest"), then set the marker to clean.

The restore is TestDatabase.rebuild() from step_db_prepare: the same code that built the
baseline, including its "write target is not PRD" checks.  If the rollback fails, the marker
is left 'dirty' (so the next run restores) and this helper FAILS.

Idempotent: a second run finds the marker clean and skips.
Exit: 0 done, 1 failed, 2 nothing to do.  Python 3 standard library only.
"""
import os
import sys
import time

from common import (HelperError, Parser, SkipStep, evidence, human_age, human_size, info,
                    load_context, read_db_marker, run_cmd, step_main, update_db_marker)
from step_db_prepare import STATE_KEY, TestDatabase


def parse_args(argv):
    parser = Parser(prog="undo_test_db.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--outcome", required=True, choices=("success", "incomplete", "failure"))
    parser.add_argument("--exit-status", required=True, type=int)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def main():
    args = parse_args(sys.argv[1:])
    ctx = load_context(args.run_dir)
    evidence("outcome", args.outcome)
    if args.outcome == "success":
        raise SkipStep("GO, database kept")

    # Cheap checks first: no database connection and no prd.env needed to decide "nothing to do".
    state_file = ctx.env_value("STATE_FILE")
    marker, values = read_db_marker(state_file)
    evidence("test_state_marker", marker)
    if marker != "dirty":
        raise SkipStep("TEST state marker is '%s', not 'dirty': nothing to undo" % marker)
    if ctx.state.get(STATE_KEY) == "running":
        raise SkipStep("TEST step 4 did not finish: the database stays dirty for the next run")

    # The exact baseline recorded in the state file.
    baseline = values.get("baseline_dump", "")
    if not baseline:
        raise HelperError("the TEST database is dirty but no baseline_dump is recorded, so it "
                          "cannot be rolled back (the next run will restore it)", state_file)
    if not os.path.isfile(baseline):
        raise HelperError("the recorded baseline dump is missing: %s (the next run will restore "
                          "the database)" % baseline, state_file)
    size = os.path.getsize(baseline)
    age = time.time() - os.path.getmtime(baseline)
    evidence("baseline_dump", "%s (%s, %s old)" % (baseline, human_size(size), human_age(age)))
    if size == 0:
        raise HelperError("the recorded baseline dump is empty", baseline)
    # Prove it is readable BEFORE the database is dropped.
    toc = run_cmd(["pg_restore", "--list", baseline], timeout=300)
    if not toc.ok:
        raise HelperError("the baseline dump is not readable by pg_restore: %s"
                          % toc.first_error_line(), toc.command())

    db = TestDatabase(ctx)
    info("rolling the TEST database back to %s" % os.path.basename(baseline))
    started = time.monotonic()
    db.rebuild(baseline)                       # drop, create, pg_restore, TEST marker comment
    update_db_marker(state_file, "clean")      # keeps baseline_dump
    evidence("rolled_back_to", baseline)
    evidence("rollback_seconds", "%.0f" % (time.monotonic() - started))
    evidence("test_state_marker", "clean")
    evidence("prd_database", "PRD database was not modified")


if __name__ == "__main__":
    sys.exit(step_main(main))
