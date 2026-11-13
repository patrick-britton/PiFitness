#!/usr/bin/env python3
"""
deploy/scripts/lib/run_step.py  --  run ONE step and own all per-step presentation.

Called by autodeploy.sh once per step:

    run_step.py --run-dir <dir> --step <name> --number <n> --total <N>

CONTRACT
--------
Exit codes (autodeploy.sh acts on these):
    0    step PASSED or was SKIPPED           -> continue with the next step
    10   stop the run cleanly                 -> NOT IMPLEMENTED, or --through reached
    1    step FAILED (or run_step.py itself failed)
    130  interrupted (Ctrl-C)

Output (stdout, captured into the log):
    [STEP n/N] <name> ................ START
      <live output of the step helper, indented 2 spaces>
      EVIDENCE: <key> = <value>
    [STEP n/N] <name> ................ PASS (4.2s)
  or  ... SKIPPED (<reason>)
  or  ... FAIL (4.2s)
      REASON: <one-line cause>
      WHERE: <command / file / line>
        last output: <final non-EVIDENCE lines of the step's output>
  or  [STEP n/N] <name> ........ NOT IMPLEMENTED
While a step is silent for HEARTBEAT_SECONDS a "... still running" line with the
elapsed time is printed so a long step never looks hung.

Records (for cleanup.py and the final summary):
    <run-dir>/results     appended, one TAB-separated line per step:
                          number  name  STATUS  seconds  detail
                          STATUS = PASS | FAIL | SKIPPED | NOT_IMPLEMENTED | INTERRUPTED
    <run-dir>/run.state   CURRENT_STEP, CURRENT_STEP_NUMBER, CURRENT_STEP_STATUS
                          (RUNNING while the step executes, then the STATUS above).
                          Written BEFORE the step starts, so cleanup always knows
                          which step was in progress, even after a crash.

STEP HELPER CONTRACT  (lib/step_<name>.py --run-dir <dir>, run with this Python)
    exit 0 = pass, 1 (or any other non-zero) = fail, 2 = skipped.
    10 is reserved and treated as a failure.
    stdout/stderr lines are shown live. Special lines (at the start of a line):
        EVIDENCE: <key> = <value>   shown as normal output
        SKIPPED: <reason>           (with exit 2) shown in the SKIPPED status
        REASON: <one-line cause>    (with a failure) shown after the FAIL line
        WHERE: <command/file/line>  (with a failure) shown after the FAIL line
    Prompts for a human must use /dev/tty (stdout is a pipe). stdin is inherited.
    If a failing helper prints no REASON/WHERE, run_step.py reports the exit
    status and the helper path itself.

Python 3 standard library only.
"""
import os
import re
import select
import subprocess
import sys
import time
from collections import deque

from common import (HelperError, Parser, Reporter, guarded, read_state,
                    update_state)

TAG = "[run_step]"
R = Reporter(TAG)  # only for run_step.py's OWN problems, not step output

LIB_DIR = os.path.dirname(os.path.abspath(__file__))
STEP_RE = re.compile(r"^[a-z][a-z0-9_]*$")

HEARTBEAT_SECONDS = 30
TAIL_LINES = 15
BANNER_WIDTH = 60

EXIT_CONTINUE = 0
EXIT_FAIL = 1
EXIT_STOP = 10
EXIT_INTERRUPTED = 130


# ----------------------------------------------------------------------------
# Small pieces
# ----------------------------------------------------------------------------
def out(text=""):
    print(text, flush=True)


def banner(number, total, name, status):
    label = "[STEP %d/%d] %s " % (number, total, name)
    dots = "." * max(3, BANNER_WIDTH - len(label))
    out("%s%s %s" % (label, dots, status))


def parse_args(argv):
    parser = Parser(prog="run_step.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--step", required=True)
    parser.add_argument("--number", required=True, type=int)
    parser.add_argument("--total", required=True, type=int)
    args = parser.parse_args(argv)
    if not STEP_RE.match(args.step):
        raise HelperError("invalid step name %r (allowed: [a-z][a-z0-9_]*)" % args.step,
                          "--step %s" % args.step)
    if not 1 <= args.number <= args.total:
        raise HelperError("--number must be between 1 and --total (%d)" % args.total,
                          "--number %d" % args.number)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def through_from_state(state):
    value = state.get("THROUGH", "")
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        raise HelperError("THROUGH in run.state is not an integer: %r" % value,
                          "run.state")


def record_result(run_dir, number, name, status, seconds, detail=""):
    detail = re.sub(r"[\t\r\n]+", " ", detail).strip()
    line = "%d\t%s\t%s\t%.1f\t%s\n" % (number, name, status, seconds, detail)
    try:
        fd = os.open(os.path.join(run_dir, "results"),
                     os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, line.encode("utf-8"))
        finally:
            os.close(fd)
    except OSError as exc:
        raise HelperError("cannot append to results: %s" % exc.strerror,
                          os.path.join(run_dir, "results"))


def mark_state(run_dir, number, name, status):
    update_state(run_dir, {"CURRENT_STEP": name,
                           "CURRENT_STEP_NUMBER": number,
                           "CURRENT_STEP_STATUS": status})


# ----------------------------------------------------------------------------
# Running the step helper with live, indented output
# ----------------------------------------------------------------------------
def stream_output(proc, on_line, on_quiet):
    """Feed complete lines to on_line; call on_quiet after each silent interval."""
    fd = proc.stdout.fileno()
    buffer = b""
    while True:
        ready, _, _ = select.select([fd], [], [], HEARTBEAT_SECONDS)
        if not ready:
            on_quiet()
            continue
        chunk = os.read(fd, 65536)
        if not chunk:
            break
        buffer += chunk
        while b"\n" in buffer:
            raw, buffer = buffer.split(b"\n", 1)
            on_line(raw.decode("utf-8", "replace").rstrip("\r"))
    if buffer:
        on_line(buffer.decode("utf-8", "replace").rstrip("\r"))


def run_helper(helper, run_dir, step):
    """Run lib/step_<name>.py; return a dict describing what happened."""
    result = {"rc": None, "interrupted": False, "reason": "", "where": "",
              "skipped": "", "tail": deque(maxlen=TAIL_LINES)}
    started = time.monotonic()

    def on_line(line):
        if line.startswith("REASON:"):
            result["reason"] = line[len("REASON:"):].strip()
        elif line.startswith("WHERE:"):
            result["where"] = line[len("WHERE:"):].strip()
        elif line.startswith("SKIPPED:"):
            result["skipped"] = line[len("SKIPPED:"):].strip()
        else:
            out("  " + line)
            if not line.startswith("EVIDENCE:"):
                result["tail"].append(line)

    def on_quiet():
        out("  ... still running: %s (elapsed %ds)"
            % (step, int(time.monotonic() - started)))

    proc = subprocess.Popen([sys.executable, helper, "--run-dir", run_dir],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        stream_output(proc, on_line, on_quiet)
        result["rc"] = proc.wait()
    except KeyboardInterrupt:
        result["interrupted"] = True
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    result["seconds"] = time.monotonic() - started
    return result


def classify(result):
    if result["interrupted"]:
        return "INTERRUPTED"
    if result["rc"] == 0:
        return "PASS"
    if result["rc"] == 2:
        return "SKIPPED"
    return "FAIL"


def failure_details(result, helper):
    """(reason, where) with sensible fallbacks when the helper printed none."""
    rc = result["rc"]
    reason = result["reason"]
    if not reason:
        if rc is not None and rc < 0:
            reason = "step helper was killed by signal %d" % -rc
        elif rc == EXIT_STOP:
            reason = "step helper used reserved exit code 10"
        else:
            reason = "step helper exited with status %s and printed no REASON line" % rc
    return reason, (result["where"] or helper)


def print_failure(result, helper):
    reason, where = failure_details(result, helper)
    out("  REASON: %s" % reason)
    out("  WHERE: %s" % where)
    if result["tail"]:
        out("  last output:")
        for line in result["tail"]:
            out("    | " + line)
    return reason


# ----------------------------------------------------------------------------
# One step, start to finish
# ----------------------------------------------------------------------------
def run_one_step(args):
    state = read_state(args.run_dir)
    through = through_from_state(state)
    number, total, name = args.number, args.total, args.step
    helper = os.path.join(LIB_DIR, "step_%s.py" % name)

    mark_state(args.run_dir, number, name, "RUNNING")

    if not os.path.isfile(helper):
        banner(number, total, name, "NOT IMPLEMENTED")
        out("  expected helper: %s" % helper)
        out("  the run stops cleanly here; later steps are not run")
        record_result(args.run_dir, number, name, "NOT_IMPLEMENTED", 0.0, helper)
        mark_state(args.run_dir, number, name, "NOT_IMPLEMENTED")
        return EXIT_STOP

    banner(number, total, name, "START")
    result = run_helper(helper, args.run_dir, name)
    status = classify(result)
    seconds = result["seconds"]
    detail = ""

    if status == "PASS":
        banner(number, total, name, "PASS (%.1fs)" % seconds)
    elif status == "SKIPPED":
        detail = result["skipped"] or "no reason given"
        banner(number, total, name, "SKIPPED (%s)" % detail)
    elif status == "INTERRUPTED":
        banner(number, total, name, "INTERRUPTED (%.1fs)" % seconds)
        detail = "interrupted by user"
    else:
        banner(number, total, name, "FAIL (%.1fs)" % seconds)
        detail = print_failure(result, helper)

    record_result(args.run_dir, number, name, status, seconds, detail)
    mark_state(args.run_dir, number, name, status)

    if status == "INTERRUPTED":
        return EXIT_INTERRUPTED
    if status == "FAIL":
        return EXIT_FAIL
    if through is not None and number == through:
        out("  --through %d reached; the run stops cleanly after this step" % through)
        return EXIT_STOP
    return EXIT_CONTINUE


def main():
    ctx = {"argv": sys.argv[1:], "stage": "(startup)"}
    holder = {"code": EXIT_FAIL}

    def body():
        ctx["stage"] = "parse arguments"
        args = parse_args(ctx["argv"])
        ctx["stage"] = "step %d/%d (%s)" % (args.number, args.total, args.step)
        holder["code"] = run_one_step(args)

    status = guarded(R, ctx, body,
                     "run_step.py itself failed (this is not a step failure).")
    return holder["code"] if status == 0 else status


if __name__ == "__main__":
    sys.exit(main())
