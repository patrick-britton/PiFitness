#!/usr/bin/env python3
"""
deploy/scripts/lib/unitctl.py  --  systemd unit handling shared by
step_preflight, step_stop_services, step_start_services, undo_halt_units and
undo_units.

Not executable on its own.  Nothing here is specific to an environment or to a
particular unit: every name, path and command comes from autodeploy.conf, the Pi env
file (through RunContext) or from systemd itself.

THE PROCESS (identical for TEST and PRD)
----------------------------------------
  stop_services    1. poll the Pi for every PiFitness unit (UNIT_NAME_GLOB), active or not
                   2. record everything needed to put it back (units.json), register undo
                   3. stop them (timers first, then services)
                   4. disable the ones that were enabled, move their files (and any
                      <unit>.d drop-in directory) into <run-dir>/units_backup
                   5. daemon-reload
  start_services   6. register the halt_units undo (see below), render the commit's
                      deploy/systemd files (@KEY@ placeholders are filled from the Pi env
                      file) and install them into UNIT_INSTALL_DIR
                   7. daemon-reload, enable the startable units, start them, check health
  undo (exit handler, last registered runs first) -- TWO undo actions, at opposite ends
  of the rollback, so that nothing is restored while a unit of the new deploy still runs:
  undo_halt_units  8. registered by start_services, so it runs FIRST: stop every unit this
                      run installed (timers first), before the database is restored or the
                      previous checkout is swapped back.  Skipped on a PRD success.
  undo_units       9. registered by stop_services, so it runs LAST: TEST (any outcome) and
                      PRD (failure / incomplete):
                         stop what this run started, disable it, delete the installed
                         files, move the backed-up originals back, daemon-reload,
                         re-enable what was enabled, start the recorded restore set
                      PRD (success): keep the new units, delete the backed-up old files.
  The ONLY environment difference is the outcome condition in steps 8 and 9.

units.json (in the run dir) is the single record the undo works from.  It is written
BEFORE each phase changes anything, so a crash mid-phase can still be undone.

Python 3 standard library only.
"""
import fnmatch
import json
import os
import re
import shutil
import socket
import time

from common import (ACTIVE_UNIT_STATES, UNIT_FILE_RE, HelperError, Parser,
                    list_matching_units, run_cmd, systemd_unit_dir, warn,
                    write_text_atomic)

CONF_KEYS = ("UNIT_NAME_GLOB", "SYSTEMCTL", "UNIT_INSTALL_DIR",
             "UNIT_STOP_TIMEOUT_SECONDS", "UNIT_START_TIMEOUT_SECONDS")
CONF_MAY_BE_EMPTY = ("PRIVILEGE_CMD", "UNIT_PLACEHOLDERS")

# `systemctl list-unit-files` states this tooling does not know how to move aside.
UNSUPPORTED_FILE_STATES = ("masked", "masked-runtime", "alias", "generated", "transient")

# Unit file names that may be moved/removed with elevated privileges. Guards every
# mv/rm below so a bad value can never point them at anything but a unit file.
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9:_.@-]*\.(service|timer)$")
PLACEHOLDER_RE = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)@")


def parse_args(prog, argv, outcome=False):
    """--run-dir (steps) or --run-dir/--outcome/--exit-status (undo helpers)."""
    parser = Parser(prog=prog, allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    if outcome:
        parser.add_argument("--outcome", required=True,
                            choices=("success", "incomplete", "failure"))
        parser.add_argument("--exit-status", required=True, type=int)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def stop_order(names):
    """Timers first (so nothing new is triggered), then services."""
    names = list(names)
    return [n for n in names if n.endswith(".timer")] + \
           [n for n in names if not n.endswith(".timer")]


class UnitEnv:
    """Everything the unit helpers need, read from the run context. No defaults."""

    def __init__(self, ctx):
        self.ctx = ctx
        ctx.require(conf=CONF_KEYS, conf_may_be_empty=CONF_MAY_BE_EMPTY)
        self.systemctl = ctx.conf_words("SYSTEMCTL")
        self.priv = ctx.conf_words("PRIVILEGE_CMD")
        self.name_glob = ctx.conf_value("UNIT_NAME_GLOB")
        self.install_dir = os.path.normpath(ctx.conf_value("UNIT_INSTALL_DIR"))
        if not os.path.isabs(self.install_dir):
            raise HelperError("UNIT_INSTALL_DIR must be an absolute path (got %r)"
                              % self.install_dir, ctx.conf_path)
        self.placeholders = ctx.conf_words("UNIT_PLACEHOLDERS")
        ctx.require(env=self.placeholders)
        self.stop_timeout = self._seconds("UNIT_STOP_TIMEOUT_SECONDS")
        self.start_timeout = self._seconds("UNIT_START_TIMEOUT_SECONDS")
        self.run_dir = ctx.run_dir
        self.unit_dir = systemd_unit_dir(ctx.script_dir)       # the commit's deploy/systemd
        self.backup_dir = os.path.join(ctx.run_dir, "units_backup")
        self.render_dir = os.path.join(ctx.run_dir, "units_render")
        self.manifest_path = os.path.join(ctx.run_dir, "units.json")

    def _seconds(self, key):
        value = self.ctx.conf_value(key)
        try:
            seconds = int(value)
            if seconds <= 0:
                raise ValueError
        except ValueError:
            raise HelperError("%s must be a positive whole number (got %r)" % (key, value),
                              self.ctx.conf_path)
        return seconds

    # ------------------------------------------------------------ commands
    def sc(self, *args, timeout=60):
        return run_cmd(self.systemctl + list(args), timeout=timeout)

    def sc_ok(self, *args, timeout=60):
        result = self.sc(*args, timeout=timeout)
        if not result.ok:
            raise HelperError("systemctl %s failed: %s" % (args[0], result.first_error_line()),
                              result.command())
        return result

    def priv_run(self, argv, timeout=120):
        result = run_cmd(self.priv + list(argv), timeout=timeout)
        if not result.ok:
            raise HelperError("%s failed: %s" % (argv[0], result.first_error_line()),
                              result.command())
        return result

    def daemon_reload(self):
        self.sc_ok("daemon-reload", timeout=60)

    # --------------------------------------------------------------- paths
    def unit_path(self, name):
        return os.path.join(self.install_dir, name)

    def check_name(self, name):
        """Refuse anything that is not a plain PiFitness unit file name."""
        if not SAFE_NAME_RE.match(name) or not fnmatch.fnmatchcase(name, self.name_glob):
            raise HelperError("refusing to touch %r: not a plain unit name matching %s"
                              % (name, self.name_glob), "unit name guard")
        return name

    def check_inside(self, path, parent):
        if not path or os.path.realpath(path) == os.path.realpath(parent) \
                or not path.startswith(parent.rstrip(os.sep) + os.sep):
            raise HelperError("refusing to touch %r: not inside %s" % (path, parent),
                              "path guard")
        return path

    # ------------------------------------------------------------- systemd
    def show(self, unit, *props):
        result = self.sc("show", "--property=" + ",".join(props), unit, timeout=30)
        if not result.ok:
            raise HelperError("cannot query unit %s: %s" % (unit, result.first_error_line()),
                              result.command())
        return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)

    def list_unit_files(self):
        """-> sorted [(unit, file state)] for every PiFitness unit FILE, active or not."""
        result = self.sc("list-unit-files", "--no-legend", "--no-pager", "--plain",
                         "--type=service,timer", self.name_glob, timeout=30)
        rows = set()
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[0].endswith((".service", ".timer")):
                rows.add((parts[0], parts[1]))
        # Some systemd versions exit 1 when the pattern matches nothing: that is "none".
        if not result.ok and not (result.rc == 1 and not rows):
            raise HelperError("cannot list unit files matching %r: %s"
                              % (self.name_glob, result.first_error_line()), result.command())
        return sorted(rows)

    def poll(self):
        """What is running on the Pi right now -> (matching, active_services, active_timers)."""
        matching = list_matching_units(self.systemctl, self.name_glob)
        running = [u for u, _load, active in matching if active in ACTIVE_UNIT_STATES]
        return (matching,
                [u for u in running if u.endswith(".service")],
                [u for u in running if u.endswith(".timer")])

    def plan_displaced(self):
        """Every PiFitness unit file that must be moved aside (active or not).

        -> [{name, state, src, backup, dropin_src, dropin_backup}]
        Fails (read-only) if a unit cannot be managed, so nothing is stopped first.
        """
        entries, problems = [], []
        for name, state in self.list_unit_files():
            if state in UNSUPPORTED_FILE_STATES:
                problems.append("%s (state %s is not supported)" % (name, state))
                continue
            if not SAFE_NAME_RE.match(name):
                problems.append("%s (not a plain unit file name)" % name)
                continue
            src = self.unit_path(name)
            if not os.path.lexists(src):
                where = self.show(name, "FragmentPath").get("FragmentPath", "") or "unknown"
                problems.append("%s (not in %s; loaded from %s)" % (name, self.install_dir, where))
                continue
            dropin = src + ".d"
            has_dropin = os.path.isdir(dropin)
            entries.append({
                "name": name, "state": state, "src": src,
                "backup": os.path.join(self.backup_dir, name),
                "dropin_src": dropin if has_dropin else "",
                "dropin_backup": os.path.join(self.backup_dir, name + ".d") if has_dropin else ""})
        if problems:
            raise HelperError("PiFitness unit(s) this tooling cannot move aside: %s"
                              % "; ".join(problems), self.install_dir)
        return entries

    def render(self, names):
        """Read the commit's unit files and fill @KEY@ placeholders from the Pi env file.

        Which keys are placeholders is UNIT_PLACEHOLDERS (autodeploy.conf); their values
        come from the Pi env file, so TEST and PRD differ only by data.
        -> {unit name: rendered text}.  Any @KEY@ left over fails.
        """
        values = {key: self.ctx.env_value(key) for key in self.placeholders}
        rendered = {}
        for name in names:
            path = os.path.join(self.unit_dir, name)
            try:
                with open(path, encoding="utf-8") as handle:
                    text = handle.read()
            except OSError as exc:
                raise HelperError("cannot read unit file: %s" % exc.strerror, path)
            for key, value in values.items():
                text = text.replace("@%s@" % key, value)
            left = sorted(set(PLACEHOLDER_RE.findall(text)))
            if left:
                raise HelperError("unit file %s has placeholder(s) with no value: %s (add the "
                                  "key to UNIT_PLACEHOLDERS and to the Pi env file)"
                                  % (name, ", ".join("@%s@" % k for k in left)), path)
            rendered[name] = text
        return rendered

    def check_privilege(self):
        """Read-only proof that the configured commands can write the unit directory."""
        result = run_cmd(self.priv + ["test", "-w", self.install_dir], timeout=30)
        if not result.ok:
            raise HelperError("cannot write to %s with PRIVILEGE_CMD=%r (%s)"
                              % (self.install_dir, " ".join(self.priv),
                                 result.first_error_line()), result.command())


# ----------------------------------------------------------------------------
# units.json: the one record the undo works from
# ----------------------------------------------------------------------------
def manifest_load(ue):
    try:
        with open(ue.manifest_path, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise HelperError("cannot read unit record: %s" % exc.strerror, ue.manifest_path)
    try:
        return json.loads(text)
    except ValueError:
        raise HelperError("unit record is corrupt", ue.manifest_path)


def manifest_save(ue, data):
    write_text_atomic(ue.manifest_path, json.dumps(data, indent=2, sort_keys=True) + "\n")


class Failures:
    """Attempt every sub-step of an undo even if an earlier one failed; report them all."""

    def __init__(self):
        self.items = []

    def attempt(self, label, func):
        try:
            return func()
        except HelperError as exc:
            warn("%s: %s" % (label, exc.reason))
            self.items.append("%s (%s)" % (label, exc.reason))
        return None


# ----------------------------------------------------------------------------
# Stop / start with verification
# ----------------------------------------------------------------------------
def stop_unit(ue, unit):
    """Stop one unit and PROVE it is down. A unit that is not loaded counts as down."""
    result = ue.sc("stop", unit, timeout=ue.stop_timeout)
    props = ue.show(unit, "LoadState", "ActiveState")
    if props.get("LoadState") == "not-found":
        return "not loaded"
    state = props.get("ActiveState", "unknown")
    if state in ACTIVE_UNIT_STATES:
        raise HelperError("unit %s is still %s after stop (%s)"
                          % (unit, state, result.first_error_line()), result.command())
    return state


def start_and_wait(ue, unit):
    """Start one unit and wait until it is healthy. Raises if it is not."""
    props = ue.show(unit, "Type", "LoadState")
    if props.get("LoadState") != "loaded":
        raise HelperError("unit %s is not loaded (LoadState=%s)"
                          % (unit, props.get("LoadState")), unit)
    if props.get("Type") == "oneshot":
        # A oneshot's `start` blocks until the job ends; success means it ran cleanly.
        result = ue.sc("start", unit, timeout=ue.start_timeout)
        if not result.ok:
            raise HelperError("oneshot unit %s did not complete: %s"
                              % (unit, result.first_error_line()), result.command())
        return "oneshot completed"
    ue.sc_ok("start", "--no-block", unit, timeout=30)
    deadline = time.monotonic() + ue.start_timeout
    while True:
        now = ue.show(unit, "ActiveState", "SubState", "Result")
        state = now.get("ActiveState", "")
        if state == "active":
            return "active/%s" % now.get("SubState", "")
        if state == "failed":
            raise HelperError("unit %s failed to start (Result=%s)"
                              % (unit, now.get("Result", "?")), "systemctl status " + unit)
        if time.monotonic() >= deadline:
            raise HelperError("unit %s not active within %ds (last state %s)"
                              % (unit, ue.start_timeout, state or "unknown"),
                              "systemctl status " + unit)
        time.sleep(1)


# ----------------------------------------------------------------------------
# Port checking and legacy process helpers
# ----------------------------------------------------------------------------
def is_port_in_use(port, host="127.0.0.1"):
    """Check if TCP port is currently open/listening on host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def find_pm2():
    """Look for pm2 executable in standard locations."""
    for p in [shutil.which("pm2"), "/usr/local/bin/pm2", "/usr/bin/pm2", "/home/god/.npm-global/bin/pm2"]:
        if p and os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    return None


def check_legacy_pm2_process(pm2_cmd, name="pifitness-next"):
    """Returns True if the legacy PM2 process is currently online."""
    res = run_cmd([pm2_cmd, "jlist"], timeout=15)
    if not res.ok:
        return False
    try:
        data = json.loads(res.stdout)
        for proc in data:
            if proc.get("name") == name and proc.get("pm2_env", {}).get("status") == "online":
                return True
    except Exception:
        pass
    return False


def stop_legacy_pm2_process(pm2_cmd, name="pifitness-next"):
    run_cmd([pm2_cmd, "stop", name], timeout=30)


def restart_legacy_pm2_process(pm2_cmd, name="pifitness-next"):
    run_cmd([pm2_cmd, "restart", name], timeout=30)


def delete_legacy_pm2_process(pm2_cmd, name="pifitness-next"):
    run_cmd([pm2_cmd, "delete", name], timeout=30)
    run_cmd([pm2_cmd, "save", "--force"], timeout=30)

