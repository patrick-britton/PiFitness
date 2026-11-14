#!/usr/bin/env python3
"""
deploy/scripts/lib/step_stop_services.py  --  TEST step 7 / PRD step 2.

    step_stop_services.py --run-dir <dir>

Identical for TEST and PRD.  Frees the unit names and ports and moves every old
PiFitness unit out of the way, remembering exactly how to put it back.

  1. Poll the Pi NOW for every PiFitness unit (UNIT_NAME_GLOB), active or not.
     (Preflight's copy may be minutes old on a TEST run; this one is authoritative
     and overwrites ACTIVE_SERVICES / ACTIVE_TIMERS in run.state.)
  2. Work out the restore set (timers + standalone services that were active) and
     the files to move aside.  All read-only: a unit this tooling cannot manage fails
     here, before anything is stopped.
  3. Register the undo, then write units.json (the undo's only input).
  4. Stop timers first, then services; prove each is down.
  5. Disable the ones that were enabled, move their unit files (and <unit>.d
     directories) into <run-dir>/units_backup.
  6. daemon-reload; prove no PiFitness unit is still active.

State written to run.state:
    ACTIVE_SERVICES, ACTIVE_TIMERS   what was active when this step ran
    RESTORE_UNITS                    what the undo starts (standalone services, then timers)
    UNITS_RECORD                     path of units.json
    UNITS_STOP_RECORDED              yes

Exit: 0 pass, 1 fail.  Python 3 standard library only.
"""
import os
import sys

from common import (HelperError, evidence, info, load_context, register_undo,
                    standalone_services, step_main, update_state, warn)
from unitctl import (UnitEnv, manifest_save, parse_args, stop_order, stop_unit)


def main():
    args = parse_args("step_stop_services.py", sys.argv[1:])
    ctx = load_context(args.run_dir)
    ue = UnitEnv(ctx)

    # ---- 1-2. read-only: what is there, what would be moved, what to restore
    info("polling the Pi for %s units ..." % ue.name_glob)
    matching, active_services, active_timers = ue.poll()
    displaced = ue.plan_displaced()
    file_names = {e["name"] for e in displaced}

    unmanaged = [u for u, _l, _a in matching
                 if u not in file_names and (u in active_services or u in active_timers)]
    for unit in unmanaged:
        warn("%s is active but has no unit file in %s (transient?): it will be stopped "
             "but cannot be restored" % (unit, ue.install_dir))

    known_timers = {n for n in file_names if n.endswith(".timer")} | \
                   {u for u, _l, _a in matching if u.endswith(".timer")}
    restore = (standalone_services([s for s in active_services if s in file_names], known_timers)
               + [t for t in active_timers if t in file_names])
    was_enabled = [e["name"] for e in displaced if e["state"] == "enabled"]
    to_stop = stop_order(active_timers + active_services)

    # ---- 3. record BEFORE changing anything
    register_undo(ctx.run_dir, "units")
    os.makedirs(ue.backup_dir, mode=0o700, exist_ok=True)
    record = {
        "active_services": active_services, "active_timers": active_timers,
        "restore_units": restore, "displaced": displaced, "was_enabled": was_enabled,
        "backup_dir": ue.backup_dir, "install_dir": ue.install_dir,
        "installed": [], "enabled_new": [], "started": []}
    manifest_save(ue, record)
    update_state(ctx.run_dir, {
        "ACTIVE_SERVICES": " ".join(active_services), "ACTIVE_TIMERS": " ".join(active_timers),
        "RESTORE_UNITS": " ".join(restore), "UNITS_RECORD": ue.manifest_path,
        "UNITS_STOP_RECORDED": "yes"})
    evidence("active_services", " ".join(active_services) or "none")
    evidence("active_timers", " ".join(active_timers) or "none")
    evidence("restore_units", " ".join(restore) or "none")
    evidence("unit_files_to_move_aside", " ".join(sorted(file_names)) or "none")
    evidence("enabled_before", " ".join(was_enabled) or "none")
    evidence("unit_record", ue.manifest_path)

    # ---- 4. stop (timers first)
    for unit in to_stop:
        before = "active" if unit in active_services or unit in active_timers else "inactive"
        after = stop_unit(ue, unit)
        evidence("stopped_" + unit, "%s -> %s" % (before, after))

    # ---- 5. disable, then move aside
    for name in was_enabled:
        ue.sc_ok("disable", name, timeout=60)
        evidence("disabled_" + name, "yes")
    for entry in displaced:
        ue.check_name(entry["name"])
        ue.check_inside(entry["src"], ue.install_dir)
        ue.priv_run(["mv", "--", entry["src"], entry["backup"]])
        if entry["dropin_src"]:
            ue.priv_run(["mv", "--", entry["dropin_src"], entry["dropin_backup"]])
        evidence("moved_" + entry["name"], "%s -> %s%s" % (
            entry["src"], entry["backup"], " (+ drop-ins)" if entry["dropin_src"] else ""))

    # ---- 6. reload and prove it
    ue.daemon_reload()
    _m, still_services, still_timers = ue.poll()
    if still_services or still_timers:
        raise HelperError("PiFitness unit(s) still active after stop: %s"
                          % " ".join(still_timers + still_services), "systemctl list-units")
    evidence("active_pifitness_units_after", "none")
    evidence("backup_dir", ue.backup_dir)


if __name__ == "__main__":
    sys.exit(step_main(main))
