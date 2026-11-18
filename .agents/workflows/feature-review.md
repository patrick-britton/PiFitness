---
description: Audit the finished, UAT-approved feature before TEST. Reports findings as bugs; fixes nothing. Re-run after /feature-polish.
---

# /feature-review

Audit the finished, UAT-approved feature before TEST. Review reports; it does not fix. Every failure is logged as a bug. It runs no tests and never sets the feature `complete`. Re-runnable: after /feature-polish, run it again and it re-checks the bugs it logged. First read `.agents/rules/20-feature-common.md`.

## Rules

- Hand-edit nothing. `fd.py` writes bugs and the log; `fd.py uat import --apply` is the only thing that changes the design file (AC boxes). You may delete only an untracked scratch file or build artifact this run created; report any other you find.
- Do not run tests or `tools/verify.py`; run only the two checks in step 4.
- A finding is a defect in this feature's change. A problem in untouched code goes to `fd.py backlog add <item>`, not a bug.

## 1. Gate

Standard gate; required status `in-progress` (`complete`: say so and stop). `.features/designs_active/<id>_feedback.md` must exist, otherwise tell the human to run UAT first. Run `python tools/workflow/fd.py uat import <id> --apply`, then `fd.py bugs <id>`. Stop and tell the human what to do unless all hold (this is a gate, not a finding): every task `complete` (else /feature-execute); 0 fail and 0 pending (else finish UAT, or /feature-polish for failures); no AC-linked bug unresolved (else re-run UAT and import, or /feature-polish); no bug `open` or `not-fixed` (else /feature-polish). Non-AC bugs `in-review` are fine: step 6 re-checks them.

## 2. Changed files

Run `git status --porcelain` and `git diff main --stat`. Every changed or untracked file must be: a `modify`, `create` or `delete` anchor of a task (`python tools/workflow/fd.py task <id> T01 T02 ...` for every task); a path after `files:` in a bug's Fix (`fd.py bugs <id> --show all`); or `.features/designs_active/<id>_*`, `.features/descriptions/<id>.md`, `docs/workflow/BACKLOG.md`, or a generated file under `docs/lineage/`. Any other file is a finding.

## 3. Test growth

Find new and changed test files and functions (tests live under `tests/`). Read `fd.py section <id> Acceptance`. A new test file or function is allowed only if an AC `auto:` line says `new test <path>::<name>` for it; a changed existing test only if a task anchors it as `modify` or a bug's Fix names it. Anything else is a finding; do not delete it.

## 4. Checks

Run `python tools/lint_comments.py` and `python tools/check_docs.py cites`. Each failure is a finding (one per file and rule broken, with file and line).

## 5. Evidence audit

Run `python tools/workflow/fd.py log <id> -n 200`. Every task needs a `start` entry and a latest `done` entry with `exit=0`. A `forced` entry is not a pass. Any gap is a finding; report it, do not repair it.

## 6. Re-check earlier findings

For each `in-review` bug with no AC link, read its Symptom to see which check found it and whether steps 2 to 5 found the same problem again. Found again: `python tools/workflow/fd.py bugs <id> --update BUG-n --status not-fixed` (not a new finding). Gone: `--status fixed`.

## 7. Log findings

For each remaining finding: `python tools/workflow/fd.py bug <id> "<one-line summary>" --symptom "<what is wrong, file and line, which check found it>"`. Write the symptom so a later run can tell whether it is still there. No AC link. If `fd.py` rejects it as a duplicate, skip it.

## 8. Verdict

1. `python tools/workflow/fd.py bugs <id> --unresolved`; the review passes only if none is unresolved.
2. `python tools/workflow/fd.py log <id> "review: PASS"` or `"review: FAIL (<n> findings: <bug ids>)"`.
3. Reply (at most 8 lines).
   - FAIL: "REVIEW FAILED: <n> findings logged as bugs." List each as `BUG-n: <summary>`, then: "Run /feature-polish to fix them, then run /feature-review again. Re-run UAT first if a fix changed what the user sees."
   - PASS: files changed, new tests, and "Review passed. Commit everything on `feature-test` (suggested message `feat(<id>): <summary>`, including `.features/designs_active/<id>_*`), push it, and deploy to TEST."
