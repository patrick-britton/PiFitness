#!/usr/bin/env python3
"""
deploy/scripts/lib/step_regression.py  --  TEST Step 6 / PRD Step 8: regression tests (pytest and npm).

    step_regression.py --run-dir <dir>

Objective: the automated tests pass against the database and the new code.
Pass criterion: every suite exits 0.

THE tests.conf CONTRACT  (<APP_DIR>/deploy/tests.conf, in the commit, read AFTER checkout)
    Plain KEY=value lines (same parser as the other .conf files). Every key that starts with
    REGRESSION_ is one suite; the rest of the key is its name; suites run in file order.
    The value is a shell command, run with `bash -c`, so $VARIABLES expand there (nothing is
    substituted by this script). These variables are exported to the command:
        APP_DIR      the checkout                    VENV_DIR   the Python venv (outside APP_DIR)
        LOG_DIR      the autodeploy log dir          REPORT     the junit XML file THIS suite must write
        REPORT_DIR   <run-dir>/reports               (inside the run dir: logged with the run)
    The command must `cd` itself where it needs to be, exit 0 only when the suite passed, and should
    write junit XML to $REPORT (that is where passed/failed/skipped counts and failing test names come
    from; a suite without a report still passes or fails on its exit code, with counts "unavailable").
    The SMOKE_* keys of the same file belong to the smoke step and are ignored here.

What it does
    1. read-only checks: tests.conf parses and defines >= 1 REGRESSION_ suite; backend/.env in the
       checkout matches this run's env file (so the tests reach this environment's database).
    2. arms the database undo: on TEST registers test_db; sets the state marker to dirty if it is
       not dirty already (the tests may write to the database).
    3. runs EVERY suite even if one fails, prints per suite: command, exit code, passed/failed/
       skipped, duration, report path; on failure the failing test names and the last output lines.
    4. fails the step if any suite failed.

Exit: 0 pass, 1 fail (REASON:/WHERE: printed).  Never 10 (reserved).  Python 3 standard library only.
"""
import filecmp
import os
import sys
import xml.etree.ElementTree as ET

from common import (HelperError, Parser, PgTarget, evidence, info, load_context, parse_kv_file, read_db_marker,
                    register_undo, run_cmd, step_main, update_db_marker, warn)

PREFIX = "REGRESSION_"
TESTS_CONF = os.path.join("deploy", "tests.conf")      # relative to APP_DIR (constant)
BACKEND_ENV = os.path.join("backend", ".env")          # the checkout step links it to this run's env file
SUITE_TIMEOUT = 1800
HEARTBEAT = 30
FAIL_NAMES_SHOWN = 20
TAIL_LINES = 15


def parse_args(argv):
    parser = Parser(prog="step_regression.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def read_suites(conf_path, prefix=PREFIX):
    """tests.conf -> [(name, command)] for keys starting with `prefix`, in file order."""
    if not os.path.isfile(conf_path):
        raise HelperError("tests.conf does not exist in the checkout", conf_path)
    values = parse_kv_file(conf_path)
    suites = [(key[len(prefix):].lower(), value) for key, value in values.items()
              if key.startswith(prefix) and len(key) > len(prefix)]
    for name, command in suites:
        if not command.strip():
            raise HelperError("suite %s has an empty command" % name, conf_path)
    if not suites:
        raise HelperError("tests.conf defines no %s* suite" % prefix, conf_path)
    return suites


def read_junit(path):
    """junit XML -> (total, passed, failed, skipped, [failing test names])."""
    root = ET.parse(path).getroot()
    total = failed = skipped = 0
    names = []
    for case in root.iter("testcase"):
        total += 1
        kinds = {child.tag for child in case}
        if kinds & {"failure", "error"}:
            failed += 1
            names.append(" > ".join(p for p in (case.get("classname", ""), case.get("name", "")) if p))
        elif "skipped" in kinds:
            skipped += 1
    return total, total - failed - skipped, failed, skipped, names


def run_suite(name, command, env, report):
    """Run one suite. -> dict(name, command, rc, seconds, counts, failing, tail, problem)."""
    if os.path.exists(report):
        os.remove(report)                       # never read a stale report
    result = run_cmd(["bash", "-c", command], extra_env=env, timeout=SUITE_TIMEOUT,
                     echo_prefix="    ", heartbeat=HEARTBEAT)
    row = {"name": name, "command": command, "rc": result.rc, "seconds": result.seconds,
           "timed_out": result.timed_out, "report": report, "counts": None, "failing": [],
           "tail": result.tail(TAIL_LINES), "problem": ""}
    if os.path.isfile(report):
        try:
            total, passed, failed, skipped, names = read_junit(report)
            row["counts"] = (total, passed, failed, skipped)
            row["failing"] = names
        except (ET.ParseError, OSError) as exc:
            row["problem"] = "report is unreadable (%s)" % exc
    return row


def judge(row):
    """-> '' if the suite passed, else the reason."""
    if row["timed_out"]:
        return "timed out after %s" % elapsed(row["seconds"])
    if row["rc"] != 0:
        return "exit status %d" % row["rc"]
    if row["counts"] is not None and row["counts"][0] == 0:
        return "exit status 0 but the report contains no tests"
    return ""


def report_suite(row, reason):
    c = row["counts"]
    evidence("suite_%s_command" % row["name"], row["command"])
    evidence("suite_%s_exit_code" % row["name"], row["rc"])
    evidence("suite_%s_tests" % row["name"],
             "unavailable (%s)" % (row["problem"] or "no report written to $REPORT") if c is None
             else "passed %d, failed %d, skipped %d (total %d)" % (c[1], c[2], c[3], c[0]))
    evidence("suite_%s_duration" % row["name"], "%.1fs" % row["seconds"])
    evidence("suite_%s_report" % row["name"], row["report"] if c is not None else "none")
    evidence("suite_%s_result" % row["name"], "PASS" if not reason else "FAIL (%s)" % reason)
    if reason:
        for test in row["failing"][:FAIL_NAMES_SHOWN]:
            info("    failed: %s" % test)
        if len(row["failing"]) > FAIL_NAMES_SHOWN:
            info("    ... and %d more" % (len(row["failing"]) - FAIL_NAMES_SHOWN))
        info("    last output lines:")
        for line in row["tail"]:
            info("      " + line[:160])


def elapsed(seconds):
    seconds = int(seconds)
    return "%dm%02ds" % divmod(seconds, 60) if seconds >= 60 else "%ds" % seconds


class Regression:
    def __init__(self, ctx):
        self.ctx = ctx
        ctx.require(env=["APP_DIR", "VENV_DIR", "LOG_DIR", "STATE_FILE"])
        self.app_dir = ctx.env_value("APP_DIR")
        self.suites = []

    def run(self):
        stages = [("Checks", self.stage_checks), ("Arm: undo action, state marker dirty", self.stage_arm),
                  ("Suites", self.stage_suites)]
        for number, (title, function) in enumerate(stages, 1):
            info("(%d/%d) %s ..." % (number, len(stages), title))
            function()
        if self.ctx.is_test:
            evidence("prd_database", "PRD database was not modified")

    def stage_checks(self):
        c = self.ctx
        conf = os.path.join(self.app_dir, TESTS_CONF)
        self.suites = read_suites(conf)
        evidence("tests_conf", conf)
        evidence("suites", " ".join(n for n, _c in self.suites))
        if c.is_test and c.state.get("DB_PREPARE_STATE") == "running":
            raise HelperError("run.state says the TEST database step did not finish (DB_PREPARE_STATE=running)",
                              os.path.join(c.run_dir, "run.state"))
        target = PgTarget.from_values(c.environment.upper(), c.envvals, c.env_path)
        evidence("database", target.describe())
        env_in_checkout = os.path.join(self.app_dir, BACKEND_ENV)
        if not os.path.exists(env_in_checkout):
            raise HelperError("the checkout has no %s (the checkout step wires it)" % BACKEND_ENV,
                              env_in_checkout)
        if os.path.islink(env_in_checkout):
            if os.path.realpath(env_in_checkout) != os.path.realpath(c.env_path):
                raise HelperError("%s symlink does not resolve to this run's env file (%s)"
                                  % (env_in_checkout, c.env_path), env_in_checkout)
        else:
            if not filecmp.cmp(env_in_checkout, c.env_path, shallow=False):
                raise HelperError("%s does not match this run's env file (%s)"
                                  % (env_in_checkout, c.env_path), env_in_checkout)
        evidence("backend_env", "%s (matches %s)" % (env_in_checkout, c.env_path))

    def stage_arm(self):
        c = self.ctx
        state_file = c.env_value("STATE_FILE")
        marker, _values = read_db_marker(state_file)
        evidence("state_marker_before", marker)
        if c.is_test:
            register_undo(c.run_dir, "test_db")
            evidence("undo_registered", "test_db")
        if marker != "dirty":
            update_db_marker(state_file, "dirty")
        evidence("state_marker", "dirty (the tests may write to the database)")

    def stage_suites(self):
        c = self.ctx
        report_dir = os.path.join(c.run_dir, "reports")
        os.makedirs(report_dir, exist_ok=True)
        failed = []
        for name, command in self.suites:
            report = os.path.join(report_dir, "%s.xml" % name)
            env = {"APP_DIR": self.app_dir, "VENV_DIR": c.env_value("VENV_DIR"),
                   "LOG_DIR": c.env_value("LOG_DIR"), "REPORT_DIR": report_dir, "REPORT": report}
            info("suite %s: %s" % (name, command))
            row = run_suite(name, command, env, report)
            reason = judge(row)
            report_suite(row, reason)
            if reason:
                failed.append(name)
        if failed:
            warn("state marker stays 'dirty': the exit handler restores the baseline on failure")
            raise HelperError("%d of %d suite(s) failed: %s" % (len(failed), len(self.suites),
                                                               ", ".join(failed)),
                              os.path.join(self.app_dir, TESTS_CONF))
        evidence("regression", "all %d suite(s) passed" % len(self.suites))


def main():
    args = parse_args(sys.argv[1:])
    Regression(load_context(args.run_dir)).run()


if __name__ == "__main__":
    sys.exit(step_main(main))
