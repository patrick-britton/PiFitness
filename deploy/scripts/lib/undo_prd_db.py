#!/usr/bin/env python3
"""
deploy/scripts/lib/undo_prd_db.py  --  exit-handler undo for the PRD database.

    undo_prd_db.py --run-dir <dir> --outcome success|incomplete|failure --exit-status <n>

Registered (name: prd_db) by step_db_baseline and step_migrate BEFORE making the PRD
database dirty.

Contract (spec 4.5 and PRD Step 5/6):
  outcome == success                        -> if dirty, set marker to 'migrated';
                                               exit 2 (SKIPPED: database kept)
  PRD state marker is not 'dirty'           -> exit 2 (SKIPPED: nothing to undo)
  otherwise                                 -> restore the EXACT file recorded as
                                               baseline_dump in STATE_FILE, then set
                                               the marker to 'clean'.

Exit: 0 done, 1 failed, 2 nothing to do. Python 3 standard library only.
"""
import os
import sys
import time

from common import (HelperError, Parser, PgTarget, SkipStep, evidence, human_age,
                    human_size, info, load_context, read_db_marker, run_cmd, step_main,
                    update_db_marker)
from step_db_baseline import rebuild_database
from step_db_prepare import MAINT_DB


def parse_args(argv):
    parser = Parser(prog="undo_prd_db.py", allow_abbrev=False)
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

    state_file = ctx.env_value("STATE_FILE")
    marker, values = read_db_marker(state_file)

    if args.outcome == "success":
        if marker == "dirty":
            update_db_marker(state_file, "migrated")
            evidence("prd_state_marker", "migrated")
        raise SkipStep("success: PRD database kept as migrated")

    evidence("prd_state_marker", marker)
    if marker != "dirty":
        raise SkipStep("PRD state marker is '%s', not 'dirty': nothing to undo" % marker)

    baseline = values.get("baseline_dump", "")
    if not baseline:
        raise HelperError("the PRD database is dirty but no baseline_dump is recorded in %s"
                          % state_file, state_file)
    if not os.path.isfile(baseline):
        raise HelperError("the recorded PRD baseline dump is missing: %s" % baseline, state_file)

    size = os.path.getsize(baseline)
    age = time.time() - os.path.getmtime(baseline)
    evidence("baseline_dump", "%s (%s, %s old)" % (baseline, human_size(size), human_age(age)))
    if size == 0:
        raise HelperError("the recorded PRD baseline dump is empty: %s" % baseline, state_file)

    toc = run_cmd(["pg_restore", "--list", baseline], timeout=300)
    if not toc.ok:
        raise HelperError("the recorded PRD baseline dump is unreadable by pg_restore: %s"
                          % toc.first_error_line(), toc.command())

    target = PgTarget.from_values("PRD", ctx.envvals, ctx.env_path)
    maint = target.for_db(MAINT_DB)

    info("rolling the PRD database back to %s" % os.path.basename(baseline))
    started = time.monotonic()
    rebuild_database(target, maint, baseline)
    update_db_marker(state_file, "clean")

    evidence("rolled_back_to", baseline)
    evidence("rollback_seconds", "%.0f" % (time.monotonic() - started))
    evidence("prd_state_marker", "clean")


if __name__ == "__main__":
    sys.exit(step_main(main))
