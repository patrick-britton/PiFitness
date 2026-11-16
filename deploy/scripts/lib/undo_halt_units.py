#!/usr/bin/env python3
"""
deploy/scripts/lib/undo_halt_units.py  --  exit-handler undo: stop the new units FIRST.

    undo_halt_units.py --run-dir <dir> --outcome success|incomplete|failure --exit-status <n>

Registered by step_start_services BEFORE it installs anything.  The exit handler runs
undo actions last-registered-first, so this one runs before the undo actions of the
checkout and database steps (registered earlier) and before undo_units (registered by
step_stop_services, earliest of all).  The resulting rollback order is:

    1. undo_halt_units   stop every unit this run installed      (this helper)
    2. database undo     restore the pinned baseline             (nothing is connected)
    3. checkout undo     swap the previous checkout back         (nothing executes from it)
    4. undo_units        remove new unit files, put the originals back, start the
                         recorded restore set

Why it exists: the new units run from the new checkout and hold database connections.
Restoring the database or swapping the checkout underneath them would fail or corrupt.
Step order cannot fix that (the units must start after migrations and tests need them
running), so the halt is a separate undo registered at the point the units come up.

It stops exactly the units listed in units.json "installed" (timers first, then
services), which includes a timer-paired service that a timer fired during the tests.
It changes no files and does not mark the record as undone: undo_units still does the
revert (or, on PRD success, the keep).

  PRD run, outcome success:   SKIPPED (the new units are kept running)
  everything else:            stop them; every stop is attempted even if one fails, and
                              any failure makes this undo FAIL.

Exit: 0 done, 1 failed, 2 nothing to do.  Python 3 standard library only.
"""
import os
import sys

from common import HelperError, SkipStep, evidence, info, load_context, step_main
from unitctl import Failures, UnitEnv, manifest_load, parse_args, stop_order, stop_unit


def main():
    args = parse_args("undo_halt_units.py", sys.argv[1:], outcome=True)
    ctx = load_context(args.run_dir)
    ue = UnitEnv(ctx)

    if ctx.environment == "prd" and args.outcome == "success":
        raise SkipStep("PRD success: the new units are kept running")

    record = manifest_load(ue)
    if record is None:
        raise SkipStep("units were never stopped or moved, so none were installed")
    if record.get("undone"):
        raise SkipStep("units already %s by an earlier undo of this run" % record["undone"])

    names = [os.path.basename(path) for path in record.get("installed", [])]
    if not names:
        raise SkipStep("this run installed no units")

    evidence("environment", ctx.environment)
    evidence("outcome", args.outcome)
    evidence("units_to_halt", " ".join(stop_order(names)))
    info("halting the units this run installed before anything else is rolled back")

    failures = Failures()
    for unit in stop_order(names):
        def halt(unit=unit):
            ue.check_name(unit)
            return stop_unit(ue, unit)
        state = failures.attempt("stop " + unit, halt)
        if state is not None:
            evidence("halted_" + unit, state)

    if failures.items:
        raise HelperError("could not halt the new units, so the database / checkout rollback "
                          "may run underneath them: %s" % "; ".join(failures.items),
                          ue.manifest_path)


if __name__ == "__main__":
    sys.exit(step_main(main))
