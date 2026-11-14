#!/usr/bin/env python3
"""
deploy/scripts/lib/new_run.py  --  validate and initialise one autodeploy run.

Called by autodeploy.sh as the very first helper, with the launcher's
arguments untouched:

    new_run.py --environment test|prd --commit <40-hex SHA> [--through <n>]

CONTRACT (what autodeploy.sh and later helpers rely on)
-------------------------------------------------------
stdout : EXACTLY ONE LINE on success - the absolute log file path
         <LOG_DIR>/autodeploy-<env>-<UTC timestamp>-<short SHA>.log
         Nothing else is ever written to stdout.
stderr : live progress for the user (this runs before logging is active, so
         the same progress lines are also appended to the log file on success).
exit   : 0 = run initialised; 1 = anything wrong (nothing has been deployed,
         no cleanup is run by autodeploy.sh).

Creates the run directory (log path with ".log" replaced by ".d") containing:
    run.state   KEY=value lines, values shlex-quoted (safe to parse with
                parse_kv_file below). Keys:
                  ENVIRONMENT COMMIT THROUGH LOG_FILE RUN_DIR LOG_DIR
                  STARTED_UTC SCRIPT_DIR CONF_FILE CONF_SHA256
                  CONF_KEYS_LOADED ENV_FILE ENV_KEYS_LOADED
                THROUGH is empty when --through was not given.
                Later steps append their own keys (written atomically).
    steps       ordered step names for this environment, one per line.

INPUTS
------
deploy/scripts/autodeploy.conf  (found relative to this file: ../autodeploy.conf)
    Required here : <ENV>_STEPS   e.g. TEST_STEPS="preflight checkout ..."
                    (space-separated, each name [a-z][a-z0-9_]*, no duplicates;
                     each name maps to lib/step_<name>.py)
                    ENV_FILE_DIR  absolute directory holding <env>.env. This is the ONE
                                  location the env file itself cannot tell us, so it
                                  lives in autodeploy.conf; there is no default here.
                    REQUIRED_ENV_KEYS="PG_HOST PG_PORT ..." - keys that must be
                    present and non-empty in the Pi env file. This is how the
                    "missing key fails the run immediately" rule grows with the
                    repo without changing this script.
<ENV_FILE_DIR>/<env>.env   (not in git)
    Required here : LOG_DIR (absolute path), plus every REQUIRED_ENV_KEYS entry.

File format for both: plain KEY=value lines, optional "export ", # comments,
optionally quoted values. Values are NOT shell-expanded ($VAR is literal) and
an unquoted value may not contain spaces. Secret values are never printed.

DESIGN
------
Each stage below is one small function: stage_xxx(ctx) -> [(key, value), ...]
evidence. It raises HelperError(reason, where) to fail. common.run_stages()
prints "(i/N) title ... OK" plus the evidence, so progress is visible live.
File parsing, state writing and error reporting live in lib/common.py.

Python 3 standard library only.
"""
import datetime
import hashlib
import os
import re
import sys

from common import (HelperError, Parser, Reporter, guarded, parse_kv_file,
                    run_stages, write_state, write_text_atomic)

TAG = "[new_run]"
ENVIRONMENTS = ("test", "prd")

SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")
STEP_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Live progress goes to stderr (stdout is reserved for the log path).
R = Reporter(TAG, sys.stderr)


# ----------------------------------------------------------------------------
# Stages
# ----------------------------------------------------------------------------
def stage_parse_args(ctx):
    parser = Parser(prog="autodeploy.sh", allow_abbrev=False)
    parser.add_argument("--environment", required=True, choices=ENVIRONMENTS)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--through", type=int, default=None)
    args = parser.parse_args(ctx["argv"])

    if not SHA_RE.match(args.commit):
        raise HelperError("--commit must be exactly 40 hex characters "
                        "(got %d characters)" % len(args.commit),
                        "--commit %s" % args.commit)
    if args.through is not None and args.through < 1:
        raise HelperError("--through must be 1 or greater", "--through %d" % args.through)

    ctx["environment"] = args.environment
    ctx["commit"] = args.commit.lower()
    ctx["through"] = args.through
    return [("environment", ctx["environment"]),
            ("commit", ctx["commit"]),
            ("through", args.through if args.through else "(not set)")]


def stage_locate_config(ctx):
    lib_dir = os.path.dirname(os.path.abspath(__file__))
    script_dir = os.path.dirname(lib_dir)
    conf_file = os.path.join(script_dir, "autodeploy.conf")
    if not os.path.isfile(conf_file):
        raise HelperError("autodeploy.conf not found next to autodeploy.sh "
                        "(commit it at deploy/scripts/autodeploy.conf)", conf_file)
    with open(conf_file, "rb") as handle:
        data = handle.read()
    ctx["script_dir"] = script_dir
    ctx["conf_file"] = conf_file
    ctx["conf_sha256"] = hashlib.sha256(data).hexdigest()
    return [("script_dir", script_dir),
            ("config_file", conf_file),
            ("config_sha256", ctx["conf_sha256"][:16] + "..."),
            ("config_bytes", len(data))]


def stage_load_config(ctx):
    ctx["conf"] = parse_kv_file(ctx["conf_file"])
    return [("config_keys_loaded", len(ctx["conf"]))]


def stage_load_env_file(ctx):
    conf = ctx["conf"]
    app_env_key = "%s_APP_ENV" % ctx["environment"].upper()
    missing_conf = [k for k in ("ENV_FILE_DIR", "REQUIRED_ENV_KEYS", app_env_key)
                    if not conf.get(k)]
    missing_conf += [k for k in ("REQUIRED_ENV_KEYS_MAY_BE_EMPTY",) if k not in conf]
    if missing_conf:
        raise HelperError("autodeploy.conf is missing or has empty key(s): %s"
                          % ", ".join(missing_conf), ctx["conf_file"])
    env_dir = conf["ENV_FILE_DIR"]
    if not os.path.isabs(env_dir):
        raise HelperError("ENV_FILE_DIR must be an absolute path", ctx["conf_file"])

    env_file = os.path.join(env_dir, "%s.env" % ctx["environment"])
    if not os.path.isfile(env_file):
        raise HelperError("Pi env file for %s not found" % ctx["environment"], env_file)
    env = parse_kv_file(env_file)

    # test.env and prd.env have identical keys. LOG_DIR and APP_ENV are always required.
    required = list(dict.fromkeys(["LOG_DIR", "APP_ENV"] + conf["REQUIRED_ENV_KEYS"].split()))
    may_be_empty = conf["REQUIRED_ENV_KEYS_MAY_BE_EMPTY"].split()
    missing = [k for k in required + may_be_empty if k not in env]
    empty = [k for k in required if k in env and not env[k]]
    if missing or empty:
        problems = []
        if missing:
            problems.append("missing: %s" % ", ".join(missing))
        if empty:
            problems.append("empty: %s" % ", ".join(empty))
        raise HelperError("env file has incomplete required keys (%s)" % "; ".join(problems),
                          env_file)

    # The file must belong to the environment it was selected for (catches a copied or
    # mislabelled file BEFORE anything is touched).
    if env["APP_ENV"] != conf[app_env_key]:
        raise HelperError("%s has APP_ENV=%r but this is a %s run (autodeploy.conf %s=%r)"
                          % (os.path.basename(env_file), env["APP_ENV"], ctx["environment"],
                             app_env_key, conf[app_env_key]), env_file)

    ctx["env_file"] = env_file
    ctx["env"] = env
    return [("env_file", env_file),
            ("env_keys_loaded", len(env)),
            ("required_keys_checked", "%d (all present%s)" % (
                len(required) + len(may_be_empty), "; empty allowed for: " + " ".join(may_be_empty)
                if may_be_empty else "")),
            ("app_env", "%s (matches %s)" % (env["APP_ENV"], ctx["environment"])),
            ("log_dir", env["LOG_DIR"])]


def stage_resolve_steps(ctx):
    key = "%s_STEPS" % ctx["environment"].upper()
    steps = ctx["conf"].get(key, "").split()
    if not steps:
        raise HelperError("%s is missing or empty in autodeploy.conf" % key,
                        ctx["conf_file"])
    for name in steps:
        if not STEP_RE.match(name):
            raise HelperError("invalid step name %r (allowed: [a-z][a-z0-9_]*)" % name,
                            "%s in %s" % (key, ctx["conf_file"]))
    duplicates = sorted(set(n for n in steps if steps.count(n) > 1))
    if duplicates:
        raise HelperError("duplicate step name(s): %s" % ", ".join(duplicates),
                        "%s in %s" % (key, ctx["conf_file"]))
    if ctx["through"] is not None and ctx["through"] > len(steps):
        raise HelperError("--through %d is beyond the last step (%d steps)"
                        % (ctx["through"], len(steps)), "--through %d" % ctx["through"])
    ctx["steps"] = steps
    return [("step_list_key", key),
            ("step_count", len(steps)),
            ("steps", " ".join(steps))]


def stage_create_run(ctx):
    log_dir = ctx["env"]["LOG_DIR"]
    if not os.path.isabs(log_dir):
        raise HelperError("LOG_DIR must be an absolute path", ctx["env_file"])
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError as exc:
        raise HelperError("cannot create LOG_DIR: %s" % exc.strerror, log_dir)
    if not os.access(log_dir, os.W_OK):
        raise HelperError("LOG_DIR is not writable by this user", log_dir)

    now = datetime.datetime.now(datetime.timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    log_file = os.path.join(log_dir, "autodeploy-%s-%s-%s.log"
                            % (ctx["environment"], stamp, ctx["commit"][:7]))
    run_dir = log_file[:-len(".log")] + ".d"
    try:
        os.mkdir(run_dir, 0o700)
    except OSError as exc:
        raise HelperError("cannot create run directory: %s" % exc.strerror, run_dir)

    write_text_atomic(os.path.join(run_dir, "steps"), "\n".join(ctx["steps"]) + "\n")
    write_state(os.path.join(run_dir, "run.state"), {
        "ENVIRONMENT": ctx["environment"],
        "COMMIT": ctx["commit"],
        "THROUGH": ctx["through"] if ctx["through"] else "",
        "LOG_FILE": log_file,
        "RUN_DIR": run_dir,
        "LOG_DIR": log_dir,
        "STARTED_UTC": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "SCRIPT_DIR": ctx["script_dir"],
        "CONF_FILE": ctx["conf_file"],
        "CONF_SHA256": ctx["conf_sha256"],
        "CONF_KEYS_LOADED": len(ctx["conf"]),
        "ENV_FILE": ctx["env_file"],
        "ENV_KEYS_LOADED": len(ctx["env"]),
    })
    ctx["log_file"] = log_file
    ctx["run_dir"] = run_dir
    return [("log_file", log_file),
            ("run_dir", run_dir),
            ("files_written", "run.state, steps")]


STAGES = [
    ("Parse and validate arguments", stage_parse_args),
    ("Locate autodeploy.conf", stage_locate_config),
    ("Load autodeploy.conf", stage_load_config),
    ("Load Pi env file", stage_load_env_file),
    ("Resolve step list", stage_resolve_steps),
    ("Create run directory and state", stage_create_run),
]


def append_progress_to_log(log_file):
    try:
        with open(log_file, "a", encoding="utf-8") as handle:
            handle.write("\n".join(R.lines) + "\n")
    except OSError as exc:
        raise HelperError("cannot write log file: %s" % exc.strerror, log_file)


def main():
    ctx = {"argv": sys.argv[1:], "stage": "(startup)"}

    def body():
        run_stages(R, STAGES, ctx)
        R.say("run initialised; handing over to autodeploy.sh")
        append_progress_to_log(ctx["log_file"])

    R.say("initialising run ...")
    status = guarded(R, ctx, body, "No deployment action was taken.")
    if status == 0:
        print(ctx["log_file"])
    return status


if __name__ == "__main__":
    sys.exit(main())
