---
description: Fix what the human's UAT rejected (or what /feature-review logged). Diagnose root causes, fix, and hand back for UAT.
---

# /feature-polish

Fix what the human's UAT rejected. Diagnose each failure's root cause, fix it, and hand it back for the human to verify. UAT is the only proof a fix works: write no tests, run no test suite, never mark a bug `fixed` yourself. Re-runnable after UAT failures, /feature-review findings, a TEST NO-GO, or an answered question. First read `.agents/rules/20-feature-common.md`.

## Rules

- Hand-edit only (a) source files the fix needs (any file; task anchors do not limit polish) and (b) `## Open Questions` in the design file, to add a question. Never edit the Task Plan, AC text or `<id>_bugs.md`; bugs change only through `fd.py`.
- Never set a bug to `fixed` or `not-fixed`: `fd.py uat import --apply` does that from UAT feedback, and /feature-review does it for bugs it found.
- Make the smallest change that makes the behavior match its AC. You may run an existing command to reproduce or diagnose, but a passing command is not proof. Leave no scratch files.
- Migrations are additive only with a `COMMENT` on every new table and column.

## 1. Gate

Standard gate; required status `in-progress` with every task `complete` (otherwise tell the human to finish /feature-execute; `complete` status: say so and stop). `.features/designs_active/<id>_feedback.md` must exist; if not, tell the human to run UAT (`<id>_uat.html`), export the feedback and save it in `.features/designs_active/`.

## 2. Sync and record

1. `python tools/workflow/fd.py uat import <id> --apply` (ticks passing ACs, unticks the rest, settles `in-review` bugs: `fixed` if their AC passed, `not-fixed` if it failed again; a bug with no newer feedback stays `in-review`). Then `fd.py bugs <id>`.
2. For each AC reported `Fail` with no unresolved bug (none `open`, `in-review` or `not-fixed` on that AC): `python tools/workflow/fd.py bug <id> "<one-line summary>" --symptom "<the human's note>" --ac AC-#` (empty note: `AC-# failed in UAT (no note)`). No root cause yet. If the AC already has an unresolved bug, work that one instead.

## 3. Fix

1. `python tools/workflow/fd.py bugs <id> --unresolved`. Work every `open` or `not-fixed` bug (AC-linked first, then id order); skip `in-review` (waiting for the human). If none, go to step 5 and say what is waiting.
2. Per bug: `fd.py bugs <id> --show BUG-n` (for `not-fixed`, read the `Attempt` lines and the latest feedback note: do not repeat the earlier fix). Diagnose with `fd.py task <id> T##` for tasks whose `Traces-to` includes the AC and `lineage.py describe <target>` for data flow. Never guess a root cause.
3. Fix with targeted changes, then record it: `python tools/workflow/fd.py bugs <id> --update BUG-n --root-cause "<what was wrong and why>" --fix "<what you changed>; files: <every path you touched>" --status in-review` (/feature-review uses the `files:` list).

## 4. Blocked

- **Needs a decision** (the AC leaves correct behavior open, or two valid fixes differ for the user): raise a blocking question naming the affected bugs, leave the bug, run `fd.py set <id> resume`, and continue with other bugs.
- **Cannot diagnose:** `fd.py bugs <id> --update BUG-n --root-cause "undetermined: <what you checked>"`, leave the status, continue.

## 5. Checkpoint and report

1. `python tools/workflow/fd.py log <id> "polish: <n> bugs -> in-review (<ids>); <n> blocked (<ids>); <n> awaiting UAT (<ids>)"`, then `git status` and `fd.py bugs <id> --unresolved`.
2. Reply (at most 8 lines): bugs moved to `in-review`, blocked bugs and why, bugs awaiting UAT, changed files. Then:
   - Any bug `in-review` or OQ raised: "Review the diff and commit (suggested message `fix(<id>): <summary>`, including `.features/designs_active/<id>_*`)." For an OQ add: "Write the answer in the design file and run /feature-design, then re-run /feature-polish."
   - AC-linked bugs `in-review`: "Re-run UAT: open `.features/designs_active/<id>_uat.html`, export the feedback, save it in that folder, then run `python tools/workflow/fd.py uat import <id> --apply`. If every AC passes, run /feature-review. Otherwise run /feature-polish."
   - Non-AC bugs `in-review` (from /feature-review): "Run /feature-review again to confirm them."
   - Nothing unresolved: "Run /feature-review."
