# /feature-polish

Fix what the human's UAT rejected. Diagnose the root cause of each failure, fix it, and hand it back for the human to verify. The human's UAT is the only proof that a fix works: write no tests, run no test suite, and never mark a bug `fixed` yourself.

Re-runnable. Run it after UAT failures, after /feature-review logs findings, after a TEST NO-GO, or after the human answers a question. Each run starts by syncing with the human's latest UAT file.

Common rules: `.clinerules/20-feature-common.md`.

## Rules

- By hand, edit only: (a) source files the fix needs (any file; the Task Plan's anchors do not limit polish), and (b) the `## Open Questions` section of the design file, to add a question. Never edit the Task Plan, the text of any Acceptance Criterion, or `<id>_bugs.md`. Bug entries change only through `fd.py`.
- Never set a bug to `fixed` or `not-fixed` yourself. `fd.py uat import --apply` does that from the UAT feedback, and /feature-review does it for the bugs it found.
- Make the smallest change that makes the failing behavior match its Acceptance Criterion. No refactoring, renaming, or reformatting beyond it.
- You may run an existing command to reproduce or diagnose a problem, but a passing command is not proof of a fix. Leave no scratch scripts or temporary files.
- A migration is additive only and adds a `COMMENT` for every new table and column.
- Use the DEV database only.

## 1. Gate

1. Run the standard gate. Required status: `in-progress`, with every task `complete`.
   - A task is not complete: stop and tell the human to finish /feature-execute first.
   - Status `complete`: stop and tell the human the feature is already complete.
2. `.features/designs_active/<id>_feedback.md` must exist. If not, stop and tell the human to run UAT (open `<id>_uat.html`), export the feedback, and save it in `.features/designs_active/`.

## 2. Sync with the UAT feedback

1. Run `python tools/workflow/fd.py uat import <id> --apply`. It prints each AC's result and the human's note, ticks passing ACs, unticks the rest, and settles bugs that are `in-review`: `fixed` when their AC passed, `not-fixed` when it failed again. A bug with no feedback newer than its fix stays `in-review` (awaiting UAT).
2. Run `python tools/workflow/fd.py bugs <id>` to see every bug and its status.

## 3. Record new failures

For each AC the import reports as `Fail` that has no unresolved bug (none on that AC in status `open`, `in-review`, or `not-fixed`), run:

`python tools/workflow/fd.py bug <id> "<one-line summary>" --symptom "<the human's note>" --ac AC-#`

Use the human's note as the symptom. If the note is empty, write `AC-# failed in UAT (no note)`. Add no root cause here. If the AC already has an unresolved bug, work that bug instead of recording another.

## 4. Pick the bugs

1. Run `python tools/workflow/fd.py bugs <id> --unresolved`.
2. Work every bug in status `open` or `not-fixed`: bugs linked to an AC first, then the rest, in id order. Skip bugs `in-review`; they wait for the human.
3. If no bug is `open` or `not-fixed`, go to step 7 and say what is waiting: bugs `in-review` need the human to re-run UAT; nothing unresolved means the human can run /feature-review.

## 5. Fix each bug

1. Run `python tools/workflow/fd.py bugs <id> --show BUG-n`. For a `not-fixed` bug, read its `Attempt` lines: the earlier fix did not work, so do not repeat it. The human's latest note in the feedback file says what is still wrong.
2. Diagnose. Run `python tools/workflow/fd.py task <id> T##` for the tasks whose `Traces-to` includes the AC, and `python tools/data_dictionary/lineage.py describe <target>` for data flow. Read only what you need. Never guess a root cause; if you cannot determine it from the code and the feedback, go to step 6 as unable to diagnose.
3. Fix it with targeted changes in whatever files the fix needs.
4. Record it and hand it to the human:
   `python tools/workflow/fd.py bugs <id> --update BUG-n --root-cause "<what was wrong and why>" --fix "<what you changed>; files: <every path you touched>" --status in-review`
   The command refuses an empty root cause or fix. /feature-review uses the `files:` list, so list every path.

## 6. Blocked: stop cleanly

- **The fix needs a decision** (the Acceptance Criterion leaves the correct behavior open, or two valid fixes would look different to the user): raise a blocking question (common rules). The Rationale names the bugs the answer would change. Leave the bug as it is, run `python tools/workflow/fd.py set <id> resume`, and carry on with bugs that do not depend on the answer.
- **You cannot diagnose it:** run `python tools/workflow/fd.py bugs <id> --update BUG-n --root-cause "undetermined: <what you checked>"` and leave the status alone. Carry on with the next bug.

## 7. Checkpoint and report

1. Run `python tools/workflow/fd.py log <id> "polish: <n> bugs -> in-review (<ids>); <n> blocked (<ids>); <n> awaiting UAT (<ids>)"`.
2. Run `git status` and `python tools/workflow/fd.py bugs <id> --unresolved`.
3. Reply with at most 8 lines: bugs moved to `in-review`, bugs blocked and why, bugs still awaiting UAT, and the changed files from `git status`. Then:
   - Any bug moved to `in-review` or any OQ raised: "Review the diff and commit (suggested message `fix(<id>): <summary>`, including `.features/designs_active/<id>_*`)." For an OQ add: "Write the answer in the design file and run /feature-design, then re-run /feature-polish."
   - Bugs linked to an AC are `in-review`: "Re-run UAT: open `.features/designs_active/<id>_uat.html`, export the feedback, save it in that folder, then run `python tools/workflow/fd.py uat import <id> --apply`. If every AC passes, run /feature-review. Otherwise run /feature-polish."
   - Bugs not linked to an AC are `in-review` (found by /feature-review): "Run /feature-review again to confirm them."
   - Nothing unresolved: "Run /feature-review."

Reminder: never set bug status to fixed, write no tests, and never commit.
