# /feature-execute <budget>

Carry out the reviewed Task Plan one task at a time until the story-point budget is used. `<budget>` is a whole number of story points. This is the only workflow that edits source code, and it edits only the files a task anchors.

Re-runnable. Run it again after an interruption or after the human answers a question; it picks up at the next task.

Common rules: `.clinerules/20-feature-common.md`.

## Rules

- By hand, edit only: (a) files the current task anchors as `modify`, `create`, or `delete`, and (b) the `## Open Questions` section of the design file, to add a question. Never edit the Task Plan, the Acceptance Criteria, or any task field. Task status changes only through `fd.py set`.
- Never touch a file the task does not anchor, even if it looks wrong or would make the work easier. That is a plan problem: step 5.
- A task is complete only when `fd.py done` exited 0 for it. Never set `complete` any other way.
- Do only what the task says. No fixing, refactoring, renaming, or reformatting beyond it.
- Write a test only if the task anchors it: a file the task `create`s for an approved `new test` AC, or an existing test the task `modify`s because this change broke it.
- Use the DEV database only.

## 1. Gate

1. Run the standard gate. Required status: `reviewed` or `in-progress`.
   - `draft`: stop and tell the human to review the plan, then run `python tools/workflow/fd.py set <id> feature reviewed`.
   - `complete`: stop and tell the human the feature is already complete.
2. `<budget>` must be a whole number of 1 or more. If it is missing or not a number, stop and ask the human for one.

## 2. Pick the task

1. Run `python tools/workflow/fd.py next <id>`.
   - `next: none`: every task is complete. Go to step 6.
   - `ACTION: stop`: do what it says, then stop.
2. If the task's points are more than the budget left, do not start it. Go to step 6 and say why. If it is the first task of this session, tell the human to run /feature-execute again with a budget of at least that many points.
3. If the task is `interrupted`, or `python tools/workflow/fd.py log <id> -n 20` shows a `start` for it with no `done`, run `python tools/workflow/fd.py brief <id> T##` first. It shows the log, `git status`, and a verify run. Check `git diff` for the task's anchored files so you build on partial work instead of repeating it.

## 3. Do the task

1. Run `python tools/workflow/fd.py start <id> T##`.
2. Read only what the task needs: its `pattern` and `read` anchors, and the parts of its `modify` files you will change. For a `planned` anchor, use the `Interface:` lines of the task that creates the file; do not read ahead in the plan or guess.
3. Make the change described in `Detail`, in the anchored files only.
   - Follow the task's surface principle document in `docs/` (`UI_PRINCIPLES.md` for frontend, `DATABASE_PRINCIPLES.md` for database/queries, `INTEGRATION_PRINCIPLES.md` for external sync/workers).
   - A file with `Interface:` lines must match them exactly (path, symbols, signatures).
   - A migration is additive only and adds a `COMMENT` for every new table and column.
   - Match the naming and style of the task's `pattern` anchor.
4. If you cannot do the task inside its anchors, or its `Verify` command cannot pass because the command itself is wrong, stop and go to step 5. Do not edit extra files and do not change the `Verify` line.

## 4. Prove it and move on

1. Run `python tools/workflow/fd.py done <id> T##`. It runs the task's `Verify` command and records the exit code.
   - If `done` hits the 30,000 ms limit, do not run it again in the foreground. Go to step 5 as a plan problem: Verify is too slow for the foreground.
2. If it exits 0, run `python tools/workflow/fd.py set <id> T## complete`. Ignore any hint it prints about acceptance criteria.
3. If it does not exit 0, read the output tail, fix the cause inside the anchored files, and run `done` again. After three failed `done` runs, go to step 5 as a plan problem.
4. Subtract the task's points from the budget. If budget remains, go back to step 2. Otherwise go to step 6.

## 5. Blocked: stop cleanly

Pick the case that fits, then stop. Do not start another task.

- **The work needs a decision** (a label, number, default, order, or behavior the design does not settle): raise a blocking question (common rules). The Rationale names the tasks the answer would change. If you had started the task, run `python tools/workflow/fd.py set <id> T## interrupted`.
- **The plan is wrong** (a missing or wrong anchor, a needed file the task does not list, a bad or too-slow `Verify`, or three failed `done` runs): run `python tools/workflow/fd.py set <id> T## interrupted`. The plan is not yours to edit.

In both cases, record why with `python tools/workflow/fd.py log <id> "T## blocked: <one-line reason>"`, then run `python tools/workflow/fd.py set <id> resume`. Tell the human: for a decision, write the answer in the design file, run /feature-design, then re-run /feature-execute; for a plan problem, what to change in the Task Plan.

## 6. Checkpoint and report

1. Run `python tools/workflow/fd.py log <id> "execute: <tasks completed> complete, <points> points; stopped: <budget used | blocked | all tasks complete>"`.
2. Run `git status` and `python tools/workflow/fd.py hot <id> --brief`.
3. Reply with at most 8 lines: tasks completed this session, points used, tasks left, why you stopped, and the changed files from `git status`. Then:
   - Work remains: "Review the diff and commit (suggested message `feat(<id>): T01-T03 <summary>`, including `.features/designs_active/<id>_*`). Next: `fd.py resume <id>`, then /feature-execute <budget>."
   - All tasks complete: "Review the diff and commit. Then run UAT: open `.features/designs_active/<id>_uat.html`, export the feedback, save it in that folder, and run `python tools/workflow/fd.py uat import <id> --apply`. If every AC passes, run /feature-review. Otherwise run /feature-polish."

Reminder: touch only anchored files, complete a task only after `fd.py done` exits 0, and never commit.
