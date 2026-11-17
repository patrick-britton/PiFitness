#!/usr/bin/env python3
"""
deploy/scripts/lib/step_start_services.py  --  TEST step 8 / PRD step 7.

    step_start_services.py --run-dir <dir>

Identical for TEST and PRD.  Installs the commit's units and brings them up.

  1. Require that step_stop_services ran (it registered the undo and moved the old
     units aside).
  2. Render the commit's deploy/systemd files: @KEY@ placeholders (UNIT_PLACEHOLDERS)
     are filled from this environment's Pi env file.  An unresolved placeholder fails.
  3. Register the halt_units undo, record what is about to be installed / enabled /
     started in units.json, then install into UNIT_INSTALL_DIR and daemon-reload.
     Prove each unit loaded from the file just written.
     halt_units is registered HERE (after the checkout / database steps registered
     theirs) so that in the exit handler's last-registered-first order it runs FIRST:
     the new units are stopped before the database is restored or the previous
     checkout is swapped back.  undo_units (registered by stop_services) still runs
     last and puts the old units back.
  4. enable the startable units (standalone services + timers), prove they are enabled.
  5. start them: standalone services first, then timers (a timer-paired service is
     never started directly).  Wait until each is healthy.
  6. Prove nothing PiFitness is running except what the commit defines.

Exit: 0 pass, 1 fail.  Python 3 standard library only.
"""
import os
import shutil
import sys

from common import (HelperError, evidence, info, load_context, register_undo,
                    run_cmd, startable_units, step_main, words)
from unitctl import (UnitEnv, manifest_load, manifest_save, parse_args, start_and_wait)


def main():
    args = parse_args("step_start_services.py", sys.argv[1:])
    ctx = load_context(args.run_dir)
    ue = UnitEnv(ctx)

    record = manifest_load(ue)
    if record is None or ctx.state.get("UNITS_STOP_RECORDED") != "yes":
        raise HelperError("stop_services has not run: the old units have not been recorded "
                          "or moved aside, so installing the new ones would overwrite them",
                          ue.manifest_path)

    services = words(ctx.state.get("SERVICES_LIST", ""))
    timers = words(ctx.state.get("TIMERS_LIST", ""))
    defined = timers + services
    if not defined:
        raise HelperError("this commit defines no systemd units (deploy/systemd is empty or "
                          "was not extracted); refusing to leave the Pi with none",
                          ue.unit_dir)
    to_start = startable_units(services, timers)

    # ---- 2. render (read-only)
    rendered = ue.render(defined)
    evidence("units_rendered", " ".join(defined))
    evidence("placeholders", " ".join("@%s@=%s" % (k, ctx.env_value(k))
                                      for k in ue.placeholders) or "none")

    # ---- 3. register the halt, record, then install
    for name in defined:
        ue.check_name(name)
    dests = [ue.unit_path(n) for n in defined]
    register_undo(ctx.run_dir, "halt_units")
    record["installed"] = dests
    record["enabled_new"] = list(to_start)
    record["started"] = list(to_start)
    manifest_save(ue, record)

    os.makedirs(ue.render_dir, mode=0o700, exist_ok=True)
    for name in defined:
        staged = os.path.join(ue.render_dir, name)
        with open(staged, "w", encoding="utf-8") as handle:
            handle.write(rendered[name])
        ue.priv_run(["install", "-m", "0644", "-o", "root", "-g", "root",
                     "--", staged, ue.unit_path(name)])
    ue.daemon_reload()
    for name in defined:
        props = ue.show(name, "LoadState", "FragmentPath")
        if props.get("LoadState") != "loaded" or \
                os.path.realpath(props.get("FragmentPath", "")) != os.path.realpath(ue.unit_path(name)):
            raise HelperError("unit %s did not load from the file just installed "
                              "(LoadState=%s, FragmentPath=%s)" % (
                                  name, props.get("LoadState"), props.get("FragmentPath")),
                              ue.unit_path(name))
        evidence("installed_" + name, ue.unit_path(name))

    # ---- 4. enable (boot persistence)
    for name in to_start:
        ue.sc_ok("enable", name, timeout=60)
        state = ue.sc("is-enabled", name, timeout=30).stdout.strip()
        if state != "enabled":
            raise HelperError("unit %s is %r after enable: it needs an [Install] section to "
                              "survive a reboot" % (name, state), ue.unit_path(name))
        evidence("enabled_" + name, state)

    # ---- 5. start (standalone services first, then timers)
    if "pifitness-frontend.service" in to_start:
        frontend_dir = os.path.join(ctx.env_value("APP_DIR"), "frontend", "pifitness")
        next_dir = os.path.join(frontend_dir, ".next")
        if not os.path.isdir(next_dir):
            info("frontend .next missing in %s; running npm run build ..." % frontend_dir)
            npm = shutil.which("npm") or "/usr/bin/npm"
            res = run_cmd([npm, "run", "build"], cwd=frontend_dir, timeout=300)
            if not res.ok:
                raise HelperError("npm run build failed: %s" % res.first_error_line(), frontend_dir)
            evidence("frontend_build", "ok, %s" % next_dir)

    info("starting: %s" % " ".join(to_start))
    for name in to_start:
        evidence("started_" + name, start_and_wait(ue, name))

    # ---- 6. nothing but the commit's units may be running
    _m, running_services, running_timers = ue.poll()
    extra = [u for u in running_timers + running_services if u not in set(defined)]
    if extra:
        raise HelperError("unexpected PiFitness unit(s) running that this commit does not "
                          "define: %s" % " ".join(extra), "systemctl list-units")
    evidence("paired_services_not_started_directly", " ".join(
        s for s in services if s not in to_start) or "none")
    evidence("old_units_active", "none")


if __name__ == "__main__":
    sys.exit(step_main(main))
