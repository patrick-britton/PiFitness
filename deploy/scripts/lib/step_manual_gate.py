#!/usr/bin/env python3
"""
deploy/scripts/lib/step_manual_gate.py  --  TEST Step 10: Manual GO / NO-GO gate.

    step_manual_gate.py --run-dir <dir>

Objective (spec TEST Step 10):
  The developer runs the manual test script against the running TEST stack, then
  explicitly decides whether to promote the commit to PRD.

Required behaviour:
  1. Assert this is a TEST run.  PRD has no human gate.
  2. Print the TEST URL, the deployed SHA, and the instruction to run the manual
     test script.
  3. Prompt "Enter GO or NO-GO" on /dev/tty.  Accept only those two answers
     (case-insensitive); reprompt on anything else.
  4. 30-minute timeout with no answer = treated as NO-GO (failure).
     Countdown/reminder lines are printed to /dev/tty at intervals.
  5. GO : set the TEST DB state marker to 'migrated'; print the exact git
     commands to merge feature-test into main; print the launcher instruction;
     exit 0  (cleanup outcome = success → undo_test_db skips the restore).
  6. NO-GO / timeout: raise HelperError (exit 1 → cleanup restores PRD + TEST DB).

Required evidence: prompt, answer (or "timeout after 30m"), elapsed_seconds,
and merge_command on GO.

Output channel notes:
  autodeploy.sh redirects this script's stdout/stderr into the log file via
  "exec >> LOG 2>&1".  The only channel that reaches the SSH caller (and
  therefore the DevHub browser console) is /dev/tty, which flows through the
  SSH pseudo-TTY (-tt flag on run-bootstrap.ps1).

  The DevHub sentinel line "DEVHUB_PROMPT:gonogo" is therefore written to
  /dev/tty, not stdout, so it arrives in the DevHub SSE stream and triggers
  the GO/NO-GO button bar in the browser.

  User input from the DevHub GO/NO-GO buttons is sent via POST /api/stdin →
  written to the subprocess stdin pipe → SSH client forwards it through the
  PTY → arrives on /dev/tty on the Pi side, where select.select picks it up.

WHAT IS AND IS NOT VERIFIED
  Verified  : the prompt is presented, valid/invalid inputs are handled, the
              state marker is updated on GO, the step exits 0/1 correctly.
  Untested  : actual terminal interaction (the Pi run IS the test); the git
              commands are printed but not executed by this script.

Exit: 0 pass (GO), 1 fail (NO-GO / timeout / not a TEST run).
Python 3 standard library only.
"""
import os
import select
import sys
import time

from common import (HelperError, Parser, evidence, info, load_context,
                    open_tty, step_main, update_db_marker)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TIMEOUT_SECONDS = 30 * 60          # 30 minutes
REMINDER_INTERVAL = 5 * 60         # remind every 5 minutes
VALID_ANSWERS = {"GO", "NO-GO"}


def parse_args(argv):
    parser = Parser(prog="step_manual_gate.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


# ---------------------------------------------------------------------------
# Timed TTY prompt
# ---------------------------------------------------------------------------

def ask_tty_with_timeout(tty, prompt_text, timeout, reminder_interval):
    """Prompt on /dev/tty with a timeout.

    Returns the upper-cased answer string, or None on timeout.
    Reprompts on invalid input; prints reminder lines at reminder_interval.

    Input arrives via the SSH PTY (the same fd as /dev/tty on the Pi).
    When running from DevHub, the user's GO/NO-GO button click sends the
    answer via POST /api/stdin → proc.stdin → SSH → PTY → /dev/tty here.
    """
    deadline = time.monotonic() + timeout
    next_reminder = time.monotonic() + reminder_interval

    tty.write(prompt_text)
    tty.flush()

    fd = tty._reader.fileno()

    line_buf = ""
    while True:
        now = time.monotonic()
        remaining = deadline - now

        if remaining <= 0:
            tty.write("\n  Timeout: no answer received after 30 minutes.\n")
            tty.flush()
            return None

        # Wake at the earlier of: deadline or next reminder
        wait = min(remaining, max(0.0, next_reminder - now))

        ready, _, _ = select.select([fd], [], [], wait)

        if ready:
            ch = os.read(fd, 1)
            if not ch:
                raise HelperError(
                    "terminal closed while waiting for GO / NO-GO",
                    "read('/dev/tty')")
            decoded = ch.decode("utf-8", "replace")
            if decoded in ("\r", "\n"):
                answer = line_buf.strip().upper()
                line_buf = ""
                if answer in VALID_ANSWERS:
                    tty.write("\n")
                    tty.flush()
                    return answer
                if answer:
                    tty.write("  Please answer GO or NO-GO: ")
                    tty.flush()
                else:
                    tty.write(prompt_text)
                    tty.flush()
            else:
                line_buf += decoded
        else:
            # Reminder time (or just woke for deadline check)
            now = time.monotonic()
            if now >= next_reminder and now < deadline:
                remaining_m = int((deadline - now) / 60) + 1
                tty.write(
                    "\n  [reminder] still waiting for GO / NO-GO — "
                    "%d minute(s) remaining\n  Enter GO or NO-GO: "
                    % remaining_m)
                tty.flush()
                next_reminder = now + reminder_interval


# ---------------------------------------------------------------------------
# Main step logic
# ---------------------------------------------------------------------------

def elapsed_str(seconds):
    seconds = int(seconds)
    if seconds >= 60:
        return "%dm%02ds" % divmod(seconds, 60)
    return "%ds" % seconds


class ManualGate:
    def __init__(self, ctx):
        self.ctx = ctx
        ctx.require(env=["APP_DIR", "STATE_FILE"],
                    conf=["HEALTH_URL"])
        self.sha = ctx.commit
        self.state_file = ctx.env_value("STATE_FILE")
        self.health_url = ctx.conf_value("HEALTH_URL")
        # Derive the public TEST URL from the health URL (strip /api/health suffix)
        self.test_url = self.health_url.rstrip("/")
        if self.test_url.endswith("/api/health"):
            self.test_url = self.test_url[: -len("/api/health")]

    def run(self):
        ctx = self.ctx

        if not ctx.is_test:
            raise HelperError(
                "step_manual_gate is only valid for TEST runs; "
                "PRD runs have no human gate",
                "ctx.environment")

        tty = open_tty()
        if tty is None:
            raise HelperError(
                "no interactive terminal (/dev/tty cannot be opened); "
                "run-bootstrap.ps1 must use 'ssh -tt' to allocate a PTY",
                "open('/dev/tty')")

        started = time.monotonic()

        # --- 1. Announce ---
        preamble = (
            "\n"
            "  ============================================================\n"
            "  TEST STACK IS RUNNING — MANUAL TESTING PHASE\n"
            "  ============================================================\n"
            "  TEST URL : %s\n"
            "  SHA      : %s\n"
            "\n"
            "  Open the URL in a browser and exercise the feature branch.\n"
            "  When done, use the DevHub GO / NO-GO buttons, or type your\n"
            "  answer directly if running in an interactive terminal.\n"
            "  (30-minute timeout — reminder every 5 minutes)\n"
            "  ============================================================\n"
        ) % (self.test_url, self.sha)

        try:
            # Emit the DevHub sentinel FIRST through /dev/tty (the SSH PTY).
            # This is a complete line ending in \n, so dev_hub.py's
            # "for line in proc.stdout" yields it immediately.
            # dev_hub.html intercepts it silently and shows the GO/NO-GO bar.
            # It must go via tty.write, NOT print/sys.stdout, because
            # autodeploy.sh redirects stdout to the log file.
            tty.write("DEVHUB_PROMPT:gonogo\n")
            tty.flush()

            tty.write(preamble)
            tty.flush()

            prompt_text = "\n  Enter GO or NO-GO: "

            info("prompt presented on /dev/tty (DevHub sentinel emitted)")
            evidence("test_url", self.test_url)
            evidence("sha", self.sha)
            evidence("prompt", "Enter GO or NO-GO (waits up to 30 minutes)")

            answer = ask_tty_with_timeout(
                tty, prompt_text, TIMEOUT_SECONDS, REMINDER_INTERVAL)

            elapsed = time.monotonic() - started

            if answer is None:
                evidence("answer", "timeout after 30m")
                evidence("elapsed_seconds", "%.0f" % elapsed)
                raise HelperError(
                    "GO / NO-GO prompt timed out after 30 minutes "
                    "(treated as NO-GO)",
                    "ask_tty_with_timeout")

            evidence("answer", answer)
            evidence("elapsed_seconds", "%.0f" % elapsed)

            if answer == "NO-GO":
                raise HelperError(
                    "developer chose NO-GO — aborting deployment; "
                    "cleanup will stop TEST and restore PRD",
                    "manual_gate prompt")

            # --- GO path ---
            update_db_marker(self.state_file, "migrated")
            evidence("test_db_marker", "migrated")

            merge_cmd = (
                "git checkout main && "
                "git merge --no-ff feature-test && "
                "git push origin main"
            )
            evidence("merge_command", merge_cmd)

            tty.write(
                "\n"
                "  GO received.  Next steps on your local machine:\n"
                "\n"
                "    %s\n"
                "\n"
                "  Then re-run the launcher and select PRD.\n"
                "\n"
                % merge_cmd)
            tty.flush()

            info("GO accepted — TEST DB marker set to 'migrated'")
            info("merge command printed to terminal")

        finally:
            tty.close()


def main():
    args = parse_args(sys.argv[1:])
    ManualGate(load_context(args.run_dir)).run()


if __name__ == "__main__":
    sys.exit(step_main(main))
