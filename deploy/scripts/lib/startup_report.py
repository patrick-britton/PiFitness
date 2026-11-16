#!/usr/bin/env python3
"""
deploy/scripts/lib/startup_report.py  --  print the run banner, read-only.

Called by autodeploy.sh right after logging starts:

    startup_report.py --run-dir <dir>

CONTRACT
--------
stdout/stderr : live progress (captured into the log by autodeploy.sh).
exit          : 0 = run state is sound; 1 = run.state is missing/inconsistent
                (autodeploy.sh then aborts and runs cleanup).
Changes nothing on disk.

What it does, one stage each:
  (1/4) Read run state     proves run.state (written by new_run.py) round-trips:
                           required keys present, RUN_DIR matches --run-dir,
                           'steps' file and log file exist.
  (2/4) Run identity       environment, commit, --through, log/run dir,
                           config file + hash, env file + key count.
  (3/4) Host               hostname, user, OS, Python, UTC time, and whether a
                           terminal (/dev/tty) is available for prompts.
  (4/4) Step plan          every step in order with 'built' or 'NOT IMPLEMENTED'
                           (lib/step_<name>.py present or not), and what this run
                           is therefore expected to do.

Python 3 standard library only.
"""
import datetime
import getpass
import os
import platform
import socket
import sys

from common import HelperError, Parser, Reporter, guarded, read_state, run_stages

TAG = "[startup]"
R = Reporter(TAG)

REQUIRED_STATE_KEYS = (
    "ENVIRONMENT", "COMMIT", "THROUGH", "LOG_FILE", "RUN_DIR", "LOG_DIR",
    "STARTED_UTC", "SCRIPT_DIR", "CONF_FILE", "CONF_SHA256", "CONF_KEYS_LOADED",
    "ENV_FILE", "ENV_KEYS_LOADED",
)


def read_steps(run_dir):
    path = os.path.join(run_dir, "steps")
    try:
        with open(path, encoding="utf-8") as handle:
            return [line.strip() for line in handle if line.strip()]
    except OSError as exc:
        raise HelperError("cannot read step list: %s" % exc.strerror, path)


def terminal_available():
    try:
        os.close(os.open("/dev/tty", os.O_RDWR))
        return True
    except OSError:
        return False


# ----------------------------------------------------------------------------
# Stages
# ----------------------------------------------------------------------------
def stage_read_state(ctx):
    parser = Parser(prog="startup_report.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(ctx["argv"])

    run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(run_dir):
        raise HelperError("run directory does not exist", run_dir)

    state = read_state(run_dir)
    missing = [key for key in REQUIRED_STATE_KEYS if key not in state]
    if missing:
        raise HelperError("run.state is missing key(s): %s" % ", ".join(missing),
                          os.path.join(run_dir, "run.state"))
    if os.path.realpath(state["RUN_DIR"]) != os.path.realpath(run_dir):
        raise HelperError("RUN_DIR in run.state (%s) does not match --run-dir"
                          % state["RUN_DIR"], run_dir)
    if not os.path.isfile(state["LOG_FILE"]):
        raise HelperError("log file named in run.state does not exist",
                          state["LOG_FILE"])

    ctx["run_dir"] = run_dir
    ctx["state"] = state
    ctx["steps"] = read_steps(run_dir)
    return [("run_state", os.path.join(run_dir, "run.state")),
            ("state_keys", len(state)),
            ("required_keys_present", "%d of %d" % (len(REQUIRED_STATE_KEYS),
                                                    len(REQUIRED_STATE_KEYS))),
            ("steps_file", "%d step(s)" % len(ctx["steps"]))]


def stage_identity(ctx):
    s = ctx["state"]
    return [("environment", s["ENVIRONMENT"]),
            ("commit", s["COMMIT"]),
            ("through", s["THROUGH"] or "(not set)"),
            ("run_started_utc", s["STARTED_UTC"]),
            ("log_file", s["LOG_FILE"]),
            ("run_dir", s["RUN_DIR"]),
            ("config_file", s["CONF_FILE"]),
            ("config_sha256", s["CONF_SHA256"][:16] + "..."),
            ("config_keys_loaded", s["CONF_KEYS_LOADED"]),
            ("env_file", s["ENV_FILE"]),
            ("env_keys_loaded", s["ENV_KEYS_LOADED"])]


def stage_host(ctx):
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ctx["tty"] = terminal_available()
    return [("host", socket.gethostname()),
            ("user", getpass.getuser()),
            ("os", platform.platform()),
            ("python", platform.python_version()),
            ("now_utc", now),
            ("terminal_available", "yes" if ctx["tty"] else "no")]


def stage_step_plan(ctx):
    lib_dir = os.path.dirname(os.path.abspath(__file__))
    total = len(ctx["steps"])
    evidence = []
    first_unbuilt = None
    for number, name in enumerate(ctx["steps"], 1):
        built = os.path.isfile(os.path.join(lib_dir, "step_%s.py" % name))
        if not built and first_unbuilt is None:
            first_unbuilt = number
        evidence.append(("step_%02d/%02d" % (number, total),
                         "%s  [%s]" % (name, "built" if built else "NOT IMPLEMENTED")))

    built_count = total if first_unbuilt is None else sum(
        1 for name in ctx["steps"]
        if os.path.isfile(os.path.join(lib_dir, "step_%s.py" % name)))
    evidence.append(("steps_built", "%d of %d" % (built_count, total)))

    through = int(ctx["state"]["THROUGH"]) if ctx["state"]["THROUGH"] else None
    if first_unbuilt is not None and (through is None or first_unbuilt <= through):
        expected = ("stops cleanly at step %d (%s): NOT IMPLEMENTED"
                    % (first_unbuilt, ctx["steps"][first_unbuilt - 1]))
    elif through is not None:
        expected = "stops cleanly after step %d (--through)" % through
    else:
        expected = "runs all %d steps" % total
    evidence.append(("expected_outcome", expected))
    return evidence


STAGES = [
    ("Read run state", stage_read_state),
    ("Run identity", stage_identity),
    ("Host", stage_host),
    ("Step plan", stage_step_plan),
]


def main():
    ctx = {"argv": sys.argv[1:], "stage": "(startup)"}
    R.say("startup report ...")
    status = guarded(R, ctx, lambda: run_stages(R, STAGES, ctx),
                     "Run state is unusable; the run will be aborted.")
    if status == 0:
        R.say("startup report complete")
    return status


if __name__ == "__main__":
    sys.exit(main())
