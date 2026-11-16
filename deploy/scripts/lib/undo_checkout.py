#!/usr/bin/env python3
"""
deploy/scripts/lib/undo_checkout.py  --  exit-handler action registered by step_checkout.py.

    undo_checkout.py --run-dir <dir> --outcome success|incomplete|failure --exit-status <n>

Runs from cleanup.py (LIFO, so on a PRD run it comes after halt_units / the database
restore and before the unit files are put back). Decides everything from run.state:

    CHECKOUT_APP_DIR  CHECKOUT_STAGE_DIR  CHECKOUT_NEW_DIR_ID
    CHECKOUT_KEEP_PREVIOUS (yes = PRD)   CHECKOUT_PREVIOUS_EXISTED

"Is the new checkout live?" is answered by comparing directory inodes, so the answer is
right whatever moment the step died.

    new checkout never went live   remove the temporary checkout            (any env/outcome)
    TEST, new checkout live        remove the previous checkout if still there (retry)
    PRD,  outcome success          delete the previous checkout
    PRD,  any other outcome        swap the previous checkout back, delete the new one
                                   (if there was no previous checkout: remove the new one)

A failure to RESTORE is FAILED (a partial rollback is a failure). A failure to merely
delete leftover files is only a WARNING: the system is in the right state.
Idempotent; exit 0 done, 2 nothing to do, other = FAILED.
Python 3 standard library only.
"""
import os
import sys

from common import (HelperError, Parser, SkipStep, evidence, read_state, step_main, warn)
import checkoutlib as cl


def parse_args(argv):
    parser = Parser(prog="undo_checkout.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--outcome", required=True, choices=("success", "incomplete", "failure"))
    parser.add_argument("--exit-status", required=True, type=int)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def remove_leftover(app_dir, path, what):
    """Delete a directory this feature created. Failure is a warning, never a FAIL."""
    if not cl.is_ours(app_dir, path):
        warn("refusing to delete %s: not a directory this run created" % path)
        return False
    try:
        cl.remove_tree(path)
        evidence(what, "removed %s" % path)
        return True
    except OSError as exc:
        warn("could not delete %s: %s" % (path, exc.strerror))
        evidence(what, "NOT removed %s (%s)" % (path, exc.strerror))
        return False


def main():
    args = parse_args(sys.argv[1:])
    state = read_state(args.run_dir)
    app_dir = state.get("CHECKOUT_APP_DIR", "")
    stage = state.get("CHECKOUT_STAGE_DIR", "")
    if not app_dir or not stage:
        raise SkipStep("the checkout step never recorded a change")
    new_id = state.get("CHECKOUT_NEW_DIR_ID", "")
    keep_previous = state.get("CHECKOUT_KEEP_PREVIOUS") == "yes"
    previous_existed = state.get("CHECKOUT_PREVIOUS_EXISTED") == "yes"
    tmp = cl.swap_tmp_path(app_dir)

    evidence("outcome", args.outcome)
    evidence("app_dir", app_dir)

    # An interrupted three-rename fallback leaves the old tree at <app>.swap-tmp and no
    # app dir at all: put it back first.
    if not os.path.lexists(app_dir) and os.path.lexists(tmp):
        try:
            os.rename(tmp, app_dir)
        except OSError as exc:
            raise HelperError("cannot heal an interrupted swap: %s" % exc.strerror, "%s -> %s" % (tmp, app_dir))
        evidence("healed_interrupted_swap", "%s -> %s" % (tmp, app_dir))

    live_is_new = cl.has_id(app_dir, new_id)

    # --- the new checkout never went live ------------------------------------------
    if not live_is_new:
        if os.path.lexists(stage) and (not new_id or cl.has_id(stage, new_id)):
            if remove_leftover(app_dir, stage, "temporary_checkout"):
                evidence("live_checkout", "unchanged (the previous checkout never stopped being live)")
                return
            return
        raise SkipStep("the new checkout never went live and no temporary checkout remains")

    # --- the new checkout is live ----------------------------------------------------
    previous = None
    if os.path.lexists(stage) and not cl.has_id(stage, new_id):
        previous = stage
    elif os.path.lexists(tmp):
        previous = tmp

    if not keep_previous:                       # TEST: the new checkout is the TEST app
        if previous is None:
            raise SkipStep("the new checkout is live and no previous checkout remains")
        remove_leftover(app_dir, previous, "previous_checkout")
        return

    if args.outcome == "success":               # PRD success
        if previous is None:
            raise SkipStep("the new checkout is live and no previous checkout remains")
        remove_leftover(app_dir, previous, "previous_checkout")
        return

    # PRD, anything but success: put the previous checkout back.
    if previous is None:
        if previous_existed:
            raise HelperError("the previous checkout is missing, so it cannot be restored; "
                              "APP_DIR still holds the NEW checkout",
                              "expected %s (or %s)" % (stage, tmp))
        # There was no checkout before this run: return to that state.
        try:
            os.rename(app_dir, stage)
        except OSError as exc:
            raise HelperError("cannot remove the new checkout: %s" % exc.strerror, app_dir)
        remove_leftover(app_dir, stage, "new_checkout")
        evidence("live_checkout", "none (APP_DIR did not exist before this run)")
        return

    try:
        method = cl.swap_in(previous, app_dir)
    except OSError as exc:
        raise HelperError("cannot swap the previous checkout back: %s" % exc.strerror,
                          "%s <-> %s" % (previous, app_dir))
    if cl.has_id(app_dir, new_id):
        raise HelperError("after the swap-back APP_DIR is still the new checkout", app_dir)
    evidence("previous_checkout_restored", "%s (%s)" % (app_dir, method))
    # `previous` now holds the failed new checkout.
    remove_leftover(app_dir, previous, "failed_new_checkout")


if __name__ == "__main__":
    sys.exit(step_main(main))
