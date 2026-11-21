# /feature-review

Audit the finished, UAT-approved feature before it goes to TEST. Review reports; it does not fix. Every failure it finds is logged as a bug and the human is told. Review runs no tests (the autodeploy bootstrap runs them) and never sets the feature `complete`.

This workflow is re-runnable. After the human has had the bugs fixed with /feature-polish, run it again: it re-checks the bugs it logged.

## Rules

- Read at most 200 lines of any file at a time.
- By hand, edit nothing. `fd.py` writes the bugs and the log, and `fd.py uat import --apply` is the only thing that changes the design file (the AC boxes). Never edit source, tests, docs, or the design. The one thing you may delete is an untracked scratch file or build artifact that this review run itself created; report any other you find.
- Work only on the `feature-test` branch. Git is read-only for you: the only git commands you may run are `git branch --show-current`, `git status`, `git diff`, `git log`, and `git show`. Never stage or commit anything.
- Never pass `--force` to any `fd.py` command.
- Use command forms from `.clinerules/00-environment.md`. Do not invent commands, flags, or test paths.
- Do not run tests or `tools/verify.py`. Run only the two checks in step 4.
- Chat is not a record. An answer given in chat is ignored; reply: "Write it in the Answer line of OQ-n in the design file."
- A finding is a defect in this feature's change. A problem in code the feature did not touch goes to `python tools/workflow/fd.py backlog add <item>`, not into a bug.
- Never write log text that begins with a task id followed by `start`, `done`, or `forced`.

## 1. Locate and gate

1. Run `git branch --show-current`. It must print `feature-test`. Otherwise stop and tell the human to switch.
2. List `.features/designs_active/`. It must hold exactly one `*_design.md`. Its `<id>` is the file prefix. Otherwise stop and tell the human.
3. Run `python tools/workflow/fd.py oq <id> --open`. If it lists any open question, stop and tell the human to write their answers in the design file and run /feature-design.
4. Run `python tools/workflow/fd.py hot <id> --brief`. The status must be `in-progress`. `complete`: stop and tell the human the feature is already complete.
5. `.features/designs_active/<id>_feedback.md` must exist. If it does not, stop and tell the human to run UAT first.
6. Run `python tools/workflow/fd.py uat import <id> --apply`, then `python tools/workflow/fd.py bugs <id>`. Stop, and tell the human what to do, unless all of these hold. This is a gate, not a finding: log no bug.
   - Every task is `complete`. Otherwise: run /feature-execute.
   - The summary shows 0 fail and 0 pending. Otherwise: finish UAT, or run /feature-polish for the failures.
   - No bug linked to an AC is unresolved. Otherwise: re-run UAT and `fd.py uat import <id> --apply`, or run /feature-polish.
   - No bug is `open` or `not-fixed`. Otherwise: run /feature-polish.
   Bugs not linked to an AC that are `in-review` are fine: step 6 re-checks them.

## 2. Changed files

1. Run `git status --porcelain` and `git diff main --stat`.
2. Account for every changed or untracked file. A file is accounted for when it is one of:
   - a `modify`, `create`, or `delete` anchor of a task (`python tools/workflow/fd.py task <id> T01 T02 ...` for every task),
   - a path named after `files:` in a bug's Fix (`python tools/workflow/fd.py bugs <id> --show all`),
   - `.features/designs_active/<id>_*`, `.features/descriptions/<id>.md`, `docs/workflow/BACKLOG.md`, or a generated file under `docs/lineage/`.
   A file you cannot account for is a finding.

## 3. Test growth

1. From the file list, find new and changed test files and the test functions in them. Find where tests live in `.clinerules/00-environment.md`.
2. Read the Acceptance Criteria with `python tools/workflow/fd.py section <id> Acceptance`. A new test file or function is allowed only if an AC `auto:` line says `new test <path>::<name>` for it. A changed existing test is allowed only if a task anchors it as `modify` or a bug's Fix names it. Anything else is a finding. Do not delete it.

## 4. Checks

Run `python tools/lint_comments.py` and `python tools/check_docs.py cites`. Each failure is a finding, one per file and rule broken, with the file and line.

## 5. Evidence audit

Run `python tools/workflow/fd.py log <id> -n 200`. Every task needs a `start` entry and a `done` entry with `exit=0` as its latest `done`. A `forced` entry is not a pass. A task that lacks either is a finding. Report the gap; do not repair it.

## 6. Re-check earlier review findings

For each bug that is `in-review` and has no AC link, look at its Symptom to see which check found it, then see whether this run's steps 2 to 5 found the same problem again.
- Found again: `python tools/workflow/fd.py bugs <id> --update BUG-n --status not-fixed`. Do not log it as a new finding.
- Gone: `python tools/workflow/fd.py bugs <id> --update BUG-n --status fixed`.

## 7. Log the findings

For each remaining finding, run:

`python tools/workflow/fd.py bug <id> "<one-line summary>" --symptom "<what is wrong, the file and line, and which check found it>"`

Write the symptom so that a later run can tell whether the same problem is still there. Do not add an AC link. If `fd.py` rejects a finding as a duplicate, it is already logged: skip it.

## 8. Verdict and report

1. Run `python tools/workflow/fd.py bugs <id> --unresolved`. The review passes only when no bug is unresolved.
2. Run `python tools/workflow/fd.py log <id> "review: PASS"`, or `"review: FAIL (<n> findings: <bug ids>)"`.
3. Reply with at most 8 lines.
   - FAIL: open with "REVIEW FAILED: <n> findings logged as bugs." List each as `BUG-n: <summary>`. End with: "Run /feature-polish to fix them, then run /feature-review again. Re-run UAT first if a fix changed what the user sees."
   - PASS: files changed, new tests, and: "Review passed. Commit everything on `feature-test` (suggested message `feat(<id>): <summary>`, including `.features/designs_active/<id>_*`), push it, and deploy to TEST."

Do not set the feature `complete`, do not commit, and do not fix anything.
