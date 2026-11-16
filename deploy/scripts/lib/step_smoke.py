#!/usr/bin/env python3
"""
deploy/scripts/lib/step_smoke.py  --  TEST Step 9 / PRD Step 9: smoke tests against running stack.

    step_smoke.py --run-dir <dir>

Objective (spec Step 9): the deployed stack, as running under systemd, passes the automated
end-to-end checks against the live services and database. Pass criterion: every suite exits 0.

THE tests.conf CONTRACT  (<APP_DIR>/deploy/tests.conf, in the commit, read AFTER checkout)
    Plain KEY=value lines (same parser as the other .conf files). Every key that starts with
    SMOKE_ is one suite; the rest of the key is its name; suites run in file order.
    The value is a shell command, run with `bash -c`, so $VARIABLES expand there.
    These variables are exported to the command:
        APP_DIR      the checkout                    VENV_DIR   the Python venv (outside APP_DIR)
        LOG_DIR      the autodeploy log dir          REPORT     the junit XML file THIS suite must write
        REPORT_DIR   <run-dir>/reports               API_URL    http://127.0.0.1:8000
    The command must `cd` itself where it needs to be, exit 0 only when the suite passed, and should
    write junit XML to $REPORT.
    A smoke failure ends the run as a failed deployment.

Exit: 0 pass, 1 fail (REASON:/WHERE: printed).  Never 10 (reserved).  Python 3 standard library only.
"""
import os
import sys
import xml.etree.ElementTree as ET

from common import (HelperError, Parser, evidence, info, load_context, parse_kv_file,
                    run_cmd, step_main)

PREFIX = "SMOKE_"
TESTS_CONF = os.path.join("deploy", "tests.conf")      # relative to APP_DIR (constant)
DEFAULT_API_URL = "http://127.0.0.1:8000"
SUITE_TIMEOUT = 300
HEARTBEAT = 15
FAIL_NAMES_SHOWN = 20
TAIL_LINES = 15


def parse_args(argv):
    parser = Parser(prog="step_smoke.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def elapsed(seconds):
    seconds = int(seconds)
    return "%dm%02ds" % divmod(seconds, 60) if seconds >= 60 else "%ds" % seconds


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


class Smoke:
    def __init__(self, ctx):
        self.ctx = ctx
        ctx.require(env=["APP_DIR", "VENV_DIR", "LOG_DIR"])
        self.app_dir = ctx.env_value("APP_DIR")
        self.suites = []

    def run(self):
        stages = [("Checks", self.stage_checks), ("Suites", self.stage_suites)]
        for number, (title, function) in enumerate(stages, 1):
            info("(%d/%d) %s ..." % (number, len(stages), title))
            function()

    def stage_checks(self):
        conf = os.path.join(self.app_dir, TESTS_CONF)
        self.suites = read_suites(conf)
        evidence("tests_conf", conf)
        evidence("suites", " ".join(n for n, _c in self.suites))
        evidence("api_url", DEFAULT_API_URL)

    def stage_suites(self):
        c = self.ctx
        report_dir = os.path.join(c.run_dir, "reports")
        os.makedirs(report_dir, exist_ok=True)
        failed = []
        for name, command in self.suites:
            report = os.path.join(report_dir, "smoke_%s.xml" % name)
            env = {"APP_DIR": self.app_dir, "VENV_DIR": c.env_value("VENV_DIR"),
                   "LOG_DIR": c.env_value("LOG_DIR"), "REPORT_DIR": report_dir, "REPORT": report,
                   "API_URL": DEFAULT_API_URL}
            info("suite %s: %s" % (name, command))
            row = run_suite(name, command, env, report)
            reason = judge(row)
            report_suite(row, reason)
            if reason:
                failed.append(name)
        if failed:
            raise HelperError("%d of %d smoke suite(s) failed: %s" % (len(failed), len(self.suites),
                                                                     ", ".join(failed)),
                              os.path.join(self.app_dir, TESTS_CONF))
        evidence("smoke", "all %d suite(s) passed" % len(self.suites))


def main():
    args = parse_args(sys.argv[1:])
    Smoke(load_context(args.run_dir)).run()


if __name__ == "__main__":
    sys.exit(step_main(main))
