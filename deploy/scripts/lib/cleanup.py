#!/usr/bin/env python3
"""
deploy/scripts/lib/cleanup.py  --  the exit handler's worker. Knows NO step.

Called by autodeploy.sh on EVERY exit once the run directory exists
(success, failure, clean stop, Ctrl-C, SIGTERM):

    cleanup.py --run-dir <dir> --exit-status <n>

CONTRACT
--------
Exit code = THE FINAL EXIT CODE OF THE WHOLE RUN (autodeploy.sh exits with it):
    --exit-status != 0  -> that same status is returned (a failure stays a failure)
    --exit-status == 0  -> 0, unless an undo action failed -> 1
Never raises: if cleanup itself breaks it says so and still returns a non-zero code.
Ignores SIGINT/SIGTERM while it runs, so a rollback cannot be cut short by a
second Ctrl-C (child undo helpers inherit this).

OUTCOME handed to undo helpers
    success     exit status 0 AND every step in the list PASSED or was SKIPPED
    incomplete  exit status 0 but the run stopped early (--through or
                NOT IMPLEMENTED): treat like failure; only "success" keeps changes
    failure     any non-zero exit status, a failed step, or an interruption

THREE STAGES
    (1/3) Assess the run          reads run.state, steps, results; decides outcome
    (2/3) Undo registered actions runs lib/undo_<name>.py for each name registered
                                  in <run-dir>/undo, LAST registered FIRST, and keeps
                                  going if one fails (a partial rollback is reported
                                  as a failure, never hidden). A registered undo
                                  whose helper file is missing counts as FAILED.
    (3/3) Summary                 table of every step (PASS/FAIL/SKIPPED/
                                  NOT IMPLEMENTED/ABORTED/not run), the undo
                                  actions, the outcome and the log path.

UNDO HELPER CONTRACT (lib/undo_<name>.py, registered by a step via
common.register_undo BEFORE the step changes anything):
    undo_<name>.py --run-dir <dir> --outcome success|incomplete|failure --exit-status <n>
    exit 0 = done, 2 = nothing to do (print "SKIPPED: <reason>"), other = FAILED.
    Prints EVIDENCE:/REASON:/WHERE: lines exactly like a step helper. Must be
    idempotent and safe if the change it undoes never happened.

Python 3 standard library only.
"""
import os
import signal
import sys

from common import (HelperError, Parser, Reporter, dotted_line, guarded, out,
                    print_failure, read_results, read_state, read_steps,
                    read_undo, run_streaming)

TAG = "[cleanup]"
R = Reporter(TAG)  # only for cleanup's OWN problems

LIB_DIR = os.path.dirname(os.path.abspath(__file__))
HEARTBEAT_SECONDS = 30
RULE = "=" * 72

PASSING = ("PASS", "SKIPPED")


# ----------------------------------------------------------------------------
# Stage 1: assess
# ----------------------------------------------------------------------------
def parse_args(argv):
    parser = Parser(prog="cleanup.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--exit-status", required=True, type=int)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def compute_outcome(exit_status, steps, results_by_number):
    if exit_status != 0:
        return "failure"
    for number in range(1, len(steps) + 1):
        row = results_by_number.get(number)
        if row is None or row["status"] not in PASSING:
            return "incomplete" if (row is None or row["status"] == "NOT_IMPLEMENTED") \
                else "failure"
    return "success"


def assess(ctx):
    args = ctx["args"]
    ctx["state"] = read_state(args.run_dir)
    ctx["steps"] = read_steps(args.run_dir)
    ctx["results"] = {row["number"]: row for row in read_results(args.run_dir)}
    ctx["outcome"] = compute_outcome(args.exit_status, ctx["steps"], ctx["results"])

    counts = {}
    for row in ctx["results"].values():
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    running = ctx["state"].get("CURRENT_STEP_STATUS") == "RUNNING"

    out("(1/3) Assess the run ...")
    out("    EVIDENCE: exit_status = %d" % args.exit_status)
    out("    EVIDENCE: steps_recorded = %d of %d" % (len(ctx["results"]), len(ctx["steps"])))
    out("    EVIDENCE: step_statuses = %s" % (
        ", ".join("%s=%d" % kv for kv in sorted(counts.items())) or "(none)"))
    if running:
        out("    EVIDENCE: step_in_progress_at_exit = %s (no result recorded)"
            % ctx["state"].get("CURRENT_STEP", "?"))
    out("    EVIDENCE: outcome = %s" % ctx["outcome"])
    out("(1/3) Assess the run ... OK")


# ----------------------------------------------------------------------------
# Stage 2: undo registered actions (LIFO)
# ----------------------------------------------------------------------------
def classify(result):
    if result["rc"] == 0:
        return "PASS"
    if result["rc"] == 2:
        return "SKIPPED"
    return "FAIL"


def run_one_undo(ctx, index, total, name):
    args = ctx["args"]
    label = "[UNDO %d/%d] %s" % (index, total, name)
    helper = os.path.join(LIB_DIR, "undo_%s.py" % name)

    if not os.path.isfile(helper):
        out(dotted_line(label, "FAIL"))
        out("  REASON: undo helper is missing, so this change could NOT be undone")
        out("  WHERE: %s" % helper)
        return {"name": name, "status": "FAIL", "seconds": 0.0,
                "detail": "undo helper missing"}

    out(dotted_line(label, "START"))
    result = run_streaming(
        helper,
        ["--run-dir", args.run_dir, "--outcome", ctx["outcome"],
         "--exit-status", str(args.exit_status)],
        name, HEARTBEAT_SECONDS)
    status = classify(result)
    detail = ""
    if status == "PASS":
        out(dotted_line(label, "PASS (%.1fs)" % result["seconds"]))
    elif status == "SKIPPED":
        detail = result["skipped"] or "no reason given"
        out(dotted_line(label, "SKIPPED (%s)" % detail))
    else:
        out(dotted_line(label, "FAIL (%.1fs)" % result["seconds"]))
        detail = print_failure(result, helper, "undo helper")
    return {"name": name, "status": status, "seconds": result["seconds"],
            "detail": detail}


def run_undos(ctx):
    names = list(reversed(read_undo(ctx["args"].run_dir)))
    ctx["undo_results"] = []
    out("(2/3) Undo registered actions (last registered first) ...")
    if not names:
        out("    EVIDENCE: undo_actions_registered = 0 (this run changed nothing that needs undoing)")
    else:
        out("    EVIDENCE: undo_actions_registered = %d (%s)" % (len(names), ", ".join(names)))
    for index, name in enumerate(names, 1):
        ctx["undo_results"].append(run_one_undo(ctx, index, len(names), name))
    failed = [r for r in ctx["undo_results"] if r["status"] == "FAIL"]
    out("(2/3) Undo registered actions ... %s"
        % ("DONE" if not failed else "FAILED (%d of %d undo action(s) failed)"
           % (len(failed), len(names))))


# ----------------------------------------------------------------------------
# Stage 3: summary
# ----------------------------------------------------------------------------
def step_row(ctx, number, name):
    row = ctx["results"].get(number)
    if row:
        shown = {"NOT_IMPLEMENTED": "NOT IMPLEMENTED"}.get(row["status"], row["status"])
        return shown, "%.1fs" % row["seconds"], row["detail"]
    if (ctx["state"].get("CURRENT_STEP") == name
            and ctx["state"].get("CURRENT_STEP_STATUS") == "RUNNING"):
        return "ABORTED", "-", "was running when the run ended"
    return "not run", "-", ""


def final_exit_code(ctx):
    exit_status = ctx["args"].exit_status
    undo_failed = any(r["status"] == "FAIL" for r in ctx.get("undo_results", []))
    if exit_status != 0:
        return exit_status
    return 1 if undo_failed else 0


def print_summary(ctx):
    state, steps = ctx["state"], ctx["steps"]
    final = final_exit_code(ctx)
    undo_failed = [r for r in ctx["undo_results"] if r["status"] == "FAIL"]

    out("(3/3) Summary")
    out(RULE)
    out("RUN SUMMARY   environment=%s   commit=%s"
        % (state.get("ENVIRONMENT", "?"), state.get("COMMIT", "?")[:12]))
    out(RULE)
    out("%3s  %-18s %-16s %8s  %s" % ("#", "Step", "Status", "Time", "Detail"))
    for number, name in enumerate(steps, 1):
        status, seconds, detail = step_row(ctx, number, name)
        out("%3d  %-18s %-16s %8s  %s" % (number, name, status, seconds, detail))
    out("")
    if ctx["undo_results"]:
        out("Undo actions (in the order they ran):")
        for row in ctx["undo_results"]:
            out("     %-24s %-8s %s" % (row["name"], row["status"], row["detail"]))
    else:
        out("Undo actions: none registered")
    out("")
    out("Outcome : %s" % ctx["outcome"])
    if undo_failed:
        out("CLEANUP FAILED: %d undo action(s) did not complete (%s); "
            "the system may NOT be in its pre-run state."
            % (len(undo_failed), ", ".join(r["name"] for r in undo_failed)))
    if final == 0:
        verdict = {"success": "PASS - all %d steps completed" % len(steps),
                   "incomplete": "STOPPED EARLY - clean stop, changes undone"}.get(
                       ctx["outcome"], "PASS")
    else:
        verdict = "FAIL (exit status %d)" % final
    out("Result  : %s" % verdict)
    out("Log     : %s" % state.get("LOG_FILE", "?"))
    out(RULE)


# ----------------------------------------------------------------------------
def main():
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal.SIG_IGN)

    ctx = {"stage": "(startup)", "undo_results": []}
    try:
        ctx["args"] = parse_args(sys.argv[1:])
    except HelperError as exc:
        R.say("FAIL parsing arguments: %s (%s)" % (exc.reason, exc.where))
        return 1
    exit_status = ctx["args"].exit_status

    out("")
    out("[cleanup] running cleanup; Ctrl-C is ignored until it finishes")

    def body():
        ctx["stage"] = "(1/3) Assess the run"
        assess(ctx)
        ctx["stage"] = "(2/3) Undo registered actions"
        run_undos(ctx)
        ctx["stage"] = "(3/3) Summary"
        print_summary(ctx)

    status = guarded(R, ctx, body,
                     "cleanup itself failed; the system may NOT be in its pre-run state.")
    if status != 0:
        return exit_status if exit_status != 0 else 1
    return final_exit_code(ctx)


if __name__ == "__main__":
    sys.exit(main())
