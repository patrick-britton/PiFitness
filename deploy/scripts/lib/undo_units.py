#!/usr/bin/env python3
"""
deploy/scripts/lib/undo_units.py  --  exit-handler undo for the unit steps.

    undo_units.py --run-dir <dir> --outcome success|incomplete|failure --exit-status <n>

Registered by step_stop_services BEFORE it changes anything, so it is the FIRST
registered unit undo and therefore runs LAST in the exit handler.  The new units are
already stopped by then: step_start_services registers undo_halt_units, which runs
first (before the database restore and the checkout swap-back).  Step 1 below is a
harmless repeat of that, and the only stop when this helper is run by hand.
Works only from <run-dir>/units.json and run.state; idempotent; safe if the change
never happened.

  TEST run (every outcome, including GO)  and  PRD run (failure / incomplete):
      REVERT  1. stop what this run started (timers first)
              2. disable what this run enabled, delete the files it installed
              3. move the backed-up originals (and <unit>.d dirs) back
              4. daemon-reload, re-enable what was enabled before
              5. start the recorded restore set (standalone services, then timers)
      Every sub-step is attempted even if an earlier one fails; any failure makes the
      undo FAIL (a partial rollback is reported as one).

  PRD run, outcome success:
      KEEP    the new units stay installed, enabled and running; the old unit files
              that were moved aside are deleted.

Exit: 0 done, 1 failed, 2 nothing to do.  Python 3 standard library only.
"""
import os
import sys

from common import (HelperError, SkipStep, evidence, info, load_context, step_main)
from unitctl import (Failures, UnitEnv, manifest_load, manifest_save, parse_args,
                     start_and_wait, stop_order, stop_unit)


def keep_new_units(ue, record):
    """PRD success: the new units stay; delete the moved-aside old files."""
    failures = Failures()
    removed = 0
    for entry in record.get("displaced", []):
        for key in ("backup", "dropin_backup"):
            path = entry.get(key, "")
            if not path or not os.path.lexists(path):
                continue

            def delete(path=path):
                ue.check_inside(path, ue.backup_dir)
                ue.priv_run(["rm", "-rf", "--", path])
            failures.attempt("delete " + path, delete)
            removed += 1
    evidence("old_unit_files_deleted", removed)
    evidence("new_units_kept", " ".join(record.get("installed", [])) or "none")
    if failures.items:
        raise HelperError("could not delete old unit backup(s): %s" % "; ".join(failures.items),
                          ue.backup_dir)
    record["undone"] = "kept"
    manifest_save(ue, record)


def revert(ue, record):
    failures = Failures()

    # 1. stop what this run started
    for unit in stop_order(record.get("started", [])):
        state = failures.attempt("stop " + unit, lambda unit=unit: stop_unit(ue, unit))
        if state is not None:
            evidence("stopped_" + unit, state)

    # 2. disable what this run enabled; delete what it installed
    for unit in record.get("enabled_new", []):
        if os.path.lexists(ue.unit_path(unit)):
            failures.attempt("disable " + unit,
                             lambda unit=unit: ue.sc_ok("disable", unit, timeout=60))
    # A path whose original has already been moved back (a re-run after a partial undo)
    # now holds the ORIGINAL, not our file: it must not be deleted again.
    already_restored = {e["src"] for e in record.get("displaced", [])
                        if not os.path.lexists(e["backup"])}
    for path in record.get("installed", []):
        if path in already_restored:
            continue

        def remove(path=path):
            ue.check_inside(path, ue.install_dir)
            ue.check_name(os.path.basename(path))
            if os.path.lexists(path):
                ue.priv_run(["rm", "-f", "--", path])
        failures.attempt("remove " + path, remove)
    evidence("installed_units_removed", " ".join(record.get("installed", [])) or "none")

    # 3. put the originals back
    restored = []
    for entry in record.get("displaced", []):
        for src_key, bak_key in (("src", "backup"), ("dropin_src", "dropin_backup")):
            src, backup = entry.get(src_key, ""), entry.get(bak_key, "")
            if not src or not os.path.lexists(backup):
                continue    # never moved (or already restored)

            def put_back(src=src, backup=backup):
                ue.check_inside(src, ue.install_dir)
                ue.check_inside(backup, ue.backup_dir)
                if os.path.lexists(src):
                    raise HelperError("%s exists and is not a file this run installed" % src, src)
                ue.priv_run(["mv", "--", backup, src])
            if failures.attempt("restore " + src, put_back) is None and os.path.lexists(src):
                restored.append(entry["name"] if src_key == "src" else entry["name"] + ".d")
    evidence("unit_files_restored", " ".join(restored) or "none")

    # 4. reload, re-enable
    failures.attempt("daemon-reload", ue.daemon_reload)
    for unit in record.get("was_enabled", []):
        failures.attempt("enable " + unit, lambda unit=unit: ue.sc_ok("enable", unit, timeout=60))
    evidence("re_enabled", " ".join(record.get("was_enabled", [])) or "none")

    # 5. bring back what was running
    restore = record.get("restore_units", [])
    for unit in restore:
        result = failures.attempt("start " + unit, lambda unit=unit: start_and_wait(ue, unit))
        if result is not None:
            evidence("restored_" + unit, result)
    if not restore:
        evidence("restored_units", "none (no unit was active when the run began)")

    if failures.items:
        raise HelperError("unit rollback incomplete, the Pi may NOT be in its pre-run state: %s"
                          % "; ".join(failures.items), ue.manifest_path)
    record["undone"] = "reverted"
    manifest_save(ue, record)


def main():
    args = parse_args("undo_units.py", sys.argv[1:], outcome=True)
    ctx = load_context(args.run_dir)
    ue = UnitEnv(ctx)
    record = manifest_load(ue)
    if record is None:
        raise SkipStep("units were never stopped or moved (PRD was never touched)")

    if record.get("undone"):
        raise SkipStep("already %s by an earlier undo of this run" % record["undone"])

    keep = ctx.environment == "prd" and args.outcome == "success"
    evidence("environment", ctx.environment)
    evidence("outcome", args.outcome)
    evidence("action", "keep new units, delete old files" if keep
             else "revert: remove new units, restore the originals")
    info("unit record: %s" % ue.manifest_path)
    if keep:
        keep_new_units(ue, record)
    else:
        revert(ue, record)


if __name__ == "__main__":
    sys.exit(step_main(main))
