#!/usr/bin/env bash
# =============================================================================
# deploy/scripts/autodeploy.sh  --  PiFitness dumb orchestrator
#
# Invoked ONLY by the Pi launcher (pi_bootstrap_shell.sh) as:
#
#     bash autodeploy.sh --environment test|prd --commit <40-hex SHA> [--through <n>]
#
# This script contains NO deployment logic. It:
#   1. asks a helper to validate/initialise the run,
#   2. mirrors all output to the log file,
#   3. installs an exit handler that calls a cleanup helper,
#   4. calls one helper per step, in the order a helper tells it,
#   5. stops on the first failure.
#
# Everything else (validation, config, step list, banners, timing, evidence,
# rollback, summary table, prompts) lives in the helpers below. Those helpers
# are a separate engineering task; THIS FILE IS THE CONTRACT THEY MUST MEET.
#
# -----------------------------------------------------------------------------
# GENERAL RULES FOR ALL HELPERS
# -----------------------------------------------------------------------------
#  * Location: deploy/scripts/lib/ (this script finds them relative to itself).
#  * Language: Python 3, standard library only, run with /usr/bin/python3.
#    (Shell out to canonical tools - git, dbmate, uv, npm, pg_dump, pg_restore,
#    systemctl - rather than reimplementing them.)
#  * The ONLY argument every helper after new_run.py receives is
#        --run-dir <path>
#    (plus the few extra flags listed per helper). Environment, commit, --through,
#    config values, paths etc. are all read from the run directory / config by
#    the helper, never passed through this script.
#  * stdout and stderr are both captured into the log, so helpers just print.
#    This script runs with PYTHONUNBUFFERED=1 so output appears immediately.
#  * Helpers that prompt a human must read/write via /dev/tty (stdout is a pipe
#    to tee, so isatty(stdout) is always False). "Is a terminal present?" means
#    "can /dev/tty be opened?". Do not redirect stdin.
#  * Exit code 10 is reserved for "stop the run cleanly" (see run_step.py).
#    Any other non-zero exit is a failure; this script reports the helper
#    name, its arguments and the exit status, so helpers should print their
#    own specific cause (REASON:/WHERE: lines) to stdout or stderr first.
#  * A helper must never assume a previous step's shell variables. Anything a
#    later step or the cleanup helper needs is written to the run directory.
#
# -----------------------------------------------------------------------------
# THE RUN DIRECTORY  (created by new_run.py)
# -----------------------------------------------------------------------------
#  Path = the log file path with the trailing ".log" replaced by ".d"
#         e.g. /home/god/pifitness-logs/autodeploy-test-20250101T120000Z-abc1234.d
#  Contents (new_run.py creates the first two; helpers own the rest):
#    run.state  KEY=value lines. new_run.py writes at least: ENVIRONMENT,
#               COMMIT, THROUGH (empty if not given), LOG_FILE, STARTED_UTC.
#               Steps append whatever cleanup needs (e.g. recorded active PRD
#               units, baseline dump path, DB state, previous checkout path,
#               GO decision). Write atomically (temp file + rename).
#    steps      Ordered step names for this environment, one per line.
#    results    Appended by run_step.py: one line per step (number, name,
#               status, seconds). Read by cleanup.py for the summary table.
#
# -----------------------------------------------------------------------------
# HELPER 1: lib/new_run.py "$@"
# -----------------------------------------------------------------------------
#  Receives the launcher's arguments untouched.
#  Must:
#    - Validate: --environment is test|prd, --commit is exactly 40 hex chars,
#      --through (optional) is an integer within the environment's step count.
#      Reject unknown arguments.
#    - Load deploy/scripts/autodeploy.conf (located relative to its own path)
#      and /home/god/.config/pifitness/<env>.env. Fail immediately, before
#      anything is touched, if a required key is missing.
#    - Create LOG_DIR and the run directory; write run.state and steps.
#      The step list comes from autodeploy.conf keys TEST_STEPS / PRD_STEPS
#      (space-separated, ordered). Suggested names, one helper each:
#        TEST_STEPS="preflight checkout dependencies db_prepare migrate
#                    regression stop_services start_services smoke manual_gate"
#        PRD_STEPS="preflight stop_services checkout dependencies db_baseline
#                   migrate start_services regression smoke"
#      (Step number/total shown to the user is simply position/length of this
#      list: 10 for TEST, 9 for PRD.)
#  Output contract:
#    stdout: EXACTLY ONE LINE - the absolute log file path
#            <LOG_DIR>/autodeploy-<env>-<UTC timestamp>-<short SHA>.log
#    stderr: human-readable error text on failure.
#  Exit: 0 ok; non-zero = invalid args/config (nothing has been touched, no
#        cleanup is run, launcher sees the non-zero exit).
#
# -----------------------------------------------------------------------------
# HELPER 2: lib/startup_report.py --run-dir <dir>
# -----------------------------------------------------------------------------
#  Prints the "startup evidence" block (runs after logging is active):
#  log file path, config path + hash, env file path, number of keys loaded,
#  environment, commit, --through value, step list.
#  Exit: 0 ok, non-zero = fail the run.
#
# -----------------------------------------------------------------------------
# HELPER 3: lib/run_step.py --run-dir <dir> --step <name> --number <n> --total <N>
# -----------------------------------------------------------------------------
#  Runs ONE step and owns all per-step presentation. Must:
#    - Print "[STEP n/N] <name> ........ START".
#    - Execute lib/step_<name>.py --run-dir <dir>  (the step helper).
#      If that file does not exist: print "[STEP n/N] <name> NOT IMPLEMENTED",
#      record it in results, and exit 10.
#    - Time the step and print PASS (4.2s) / FAIL (4.2s) / SKIPPED (<reason>),
#      appending the same to the results file.
#    - On FAIL ensure REASON:, WHERE: and the tail of the tool output are shown.
#    - After a step PASSES (or SKIPS), if --through (from run.state) equals n,
#      exit 10.
#  Exit codes (this script acts on them):
#      0   step passed or was skipped -> continue with next step
#      10  stop cleanly (NOT IMPLEMENTED, or --through reached) -> run ends
#          with exit status 0 after cleanup
#      any other non-zero -> step failed -> run ends non-zero after cleanup
#
#  Step helper contract (lib/step_<name>.py --run-dir <dir>):
#      stdout lines of the form   EVIDENCE: <key> = <value>
#      failure prints             REASON: <one-line cause>
#                                 WHERE: <command / file / line>
#      exit 0 = pass, 1 = fail, 2 = skipped (print "SKIPPED: <reason>"),
#      exit code 10 is RESERVED for run_step.py and must not be used.
#      Each step reads environment/commit/config from the run dir and must
#      write to run.state whatever cleanup.py will need to undo it BEFORE it
#      makes the change (e.g. record active PRD units before stopping them).
#
# -----------------------------------------------------------------------------
# HELPER 4: lib/cleanup.py --run-dir <dir> --exit-status <n>
# -----------------------------------------------------------------------------
#  Called on EVERY exit once the run directory exists (success, failure,
#  NOT IMPLEMENTED stop, Ctrl-C, SIGTERM). --exit-status is the status the
#  orchestrator was about to exit with (130 = Ctrl-C, 143 = SIGTERM).
#  Must decide everything from run.state, results and --exit-status:
#    - stop TEST services (no-op if down); restore PRD units/code/database as
#      recorded in run.state per the requirements doc (sections 4.2, 4.4, 4.5, 6);
#      a partial rollback is itself a failure.
#    - print the final summary table (from results) plus the cleanup outcome
#      and the log file path.
#  Exit code = THE FINAL EXIT CODE OF THE WHOLE RUN. This script exits with
#  exactly that value (so cleanup may turn a pass into a failure, e.g. failed
#  rollback, and must return 0 only when everything passed and cleanup worked).
#  Must be idempotent and safe if some state keys are absent (step never ran).
# =============================================================================

set -Eeuo pipefail

PYTHON="/usr/bin/python3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="$SCRIPT_DIR/lib"

export PYTHONUNBUFFERED=1

note() { printf '[autodeploy] %s\n' "$*"; }
err()  { printf '[autodeploy] ERROR: %s\n' "$*" >&2; }

# Any unexpected shell-level failure says where it happened.
trap 'err "unexpected failure at line $LINENO: $BASH_COMMAND"' ERR

# Run one helper. Explains a missing helper (with directory listings) and any
# non-zero exit (except 10, the reserved "stop cleanly" code). Returns the
# helper's exit code unchanged.
run_helper() {
    local helper="$1"; shift
    local rc=0

    if [[ ! -f "$LIB/$helper" ]]; then
        err "helper not found: $LIB/$helper"
        err "  called as: $helper $*"
        err "  contents of $SCRIPT_DIR:"
        { ls -la "$SCRIPT_DIR" 2>&1 | sed 's/^/[autodeploy]     /' >&2; } || true
        err "  contents of $LIB:"
        { ls -la "$LIB" 2>&1 | sed 's/^/[autodeploy]     /' >&2; } || true
        return 127
    fi

    "$PYTHON" "$LIB/$helper" "$@" || rc=$?

    if [[ $rc -ne 0 && $rc -ne 10 ]]; then
        err "helper failed: $helper exited with status $rc"
        err "  called as: $helper $*"
    fi
    return "$rc"
}

note "starting: $0 $*"
note "script dir: $SCRIPT_DIR"

# --- 1. Validate and initialise (nothing is touched if this fails) -----------
rc=0
LOG_FILE="$(run_helper new_run.py "$@")" || rc=$?
if [[ $rc -ne 0 ]]; then
    err "run initialisation failed (exit $rc); nothing was changed on the Pi."
    exit "$rc"
fi
RUN_DIR="${LOG_FILE%.log}.d"

# --- 2. Mirror all output to the log file -------------------------------------
TEE_PID=""
exec > >(tee -a "$LOG_FILE") 2>&1
TEE_PID=$!
note "log file:      $LOG_FILE"
note "run directory: $RUN_DIR"

# --- 3. Exit handler (runs on every exit; its exit code is the run's) ---------
on_exit() {
    local status=$?
    local final=0
    trap - EXIT INT TERM ERR

    note "shutting down (status before cleanup: $status); running cleanup..."
    run_helper cleanup.py --run-dir "$RUN_DIR" --exit-status "$status" || final=$?
    note "run finished: exit status $final (log: $LOG_FILE)"

    # Close our output so tee flushes, then wait for it so no lines are lost.
    exec >&- 2>&-
    wait "$TEE_PID" 2>/dev/null || true
    exit "$final"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- 4. Startup evidence -------------------------------------------------------
run_helper startup_report.py --run-dir "$RUN_DIR"

# --- 5. Run the steps in order; first failure ends the run ---------------------
mapfile -t STEPS < "$RUN_DIR/steps"
TOTAL="${#STEPS[@]}"
NUMBER=0
note "steps to run: $TOTAL (${STEPS[*]})"

for STEP in "${STEPS[@]}"; do
    NUMBER=$((NUMBER + 1))
    rc=0
    run_helper run_step.py \
        --run-dir "$RUN_DIR" \
        --step "$STEP" \
        --number "$NUMBER" \
        --total "$TOTAL" || rc=$?

    if [[ $rc -eq 10 ]]; then   # clean stop: NOT IMPLEMENTED or --through reached
        note "step sequence stopped cleanly at step $NUMBER/$TOTAL ($STEP)"
        break
    elif [[ $rc -ne 0 ]]; then  # step failed; exit handler does the rest
        err "step $NUMBER/$TOTAL ($STEP) failed with exit status $rc"
        exit "$rc"
    fi
done

exit 0