---
description: Execute the Task Plan one task at a time until the story-point budget is used (default 3 points, one task). Usage - /feature-execute [budget]
---

# /feature-execute [budget]

Carry out the Task Plan one task at a time until the story-point budget is used. `[budget]` is the optional whole number of story points typed after the command. If it is omitted, the budget is 3 points and you stop after one completed task. Never ask for it. This is the only workflow that edits source code, and only the files a task anchors. Re-runnable: it picks up at the next task. First read `.agents/rules/20-feature-common.md`.

## Rules

- Hand-edit only (a) files the current task anchors as `modify`, `create` or `delete`, and (b) `## Open Questions` in the design file, to add a question. Never edit the Task Plan, Acceptance Criteria or any task field; task status changes only through `fd.py set`.
- Setting status `draft` to `reviewed` in step 1 is the one status change you make outside a task.
- Never touch a file the task does not anchor, even if it looks wrong. That is a plan problem (step 5).
- A task is complete only when `fd.py done` exited 0 for it.
- Do only what the task says. No refactoring, renaming or reformatting beyond it.
- Write a test only if the task anchors it (a file it `create`s for an approved `new test` AC, or an existing test it `modify`s because this change broke it).

## 1. Gate

Standard gate; required status `reviewed` or `in-progress`.
- `draft`: invoking /feature-execute counts as review approval. First confirm the Task Plan has at least one task (`fd.py next <id>` does not report a structural error); if it has none, stop and tell the human to run /feature-plan. Otherwise run `python tools/workflow/fd.py set <id> feature reviewed`, then `fd.py log <id> "execute: plan approved by invocation"`, and continue.
- `complete`: stop and say so.
- Budget: if a budget was typed but is not a whole number of 1 or more, ask for a valid one. If none was typed, use the default (3 points, one task).

## 2. Pick the task

1. `python tools/workflow/fd.py next <id>`. `next: none`: go to step 6. `ACTION: stop`: do what it says, then stop.
2. If the task's points exceed the budget left, do not start it: go to step 6 and say why (if it is the first task this session, tell the human to re-run with a budget of at least that many points).
3. If the task is `interrupted`, or `fd.py log <id> -n 20` shows a `start` without a `done`, run `fd.py brief <id> T##` and check `git diff` of its anchored files so you build on partial work.

## 3. Do the task

1. `python tools/workflow/fd.py start <id> T##`.
2. Read the task's `Finding:` lines first and treat them as established. Do not re-run lineage or search for anything they state. Then read only what the task needs: its `pattern` and `read` anchors and the parts of `modify` files you will change. For a `planned` anchor use the creating task's `Interface:` lines; do not read ahead in the plan. If a `Finding:` turns out wrong, go to step 5 as a plan problem.
3. Make the change in `Detail`, in the anchored files only. Follow the principle doc for the task's surface; match `Interface:` lines exactly; match the `pattern` anchor's style; migrations are additive only with a `COMMENT` on every new table and column.
4. If the task cannot be done inside its anchors, or its `Verify` is itself wrong, go to step 5. Do not edit extra files or change `Verify`.

## 4. Prove it

1. `python tools/workflow/fd.py done <id> T##` (runs `Verify`, records the exit code). If it does not finish in about 30 seconds, do not re-run it in the foreground: go to step 5 as a plan problem (Verify too slow).
2. Exit 0: `python tools/workflow/fd.py set <id> T## complete` (ignore any acceptance-criteria hint). Otherwise read the output tail, fix the cause inside the anchors, and run `done` again; after three failed runs go to step 5 as a plan problem.
3. Subtract the points. With the default budget, go to step 6 after this task. Otherwise, budget left: step 2; none left: step 6.

## 5. Blocked: stop cleanly

- **Needs a decision** the design does not settle: raise a blocking question whose Rationale names the tasks it would change. If you had started the task, `fd.py set <id> T## interrupted`.
- **Plan is wrong** (missing or wrong anchor, needed file not listed, bad or slow `Verify`, a wrong `Finding:`, three failed `done` runs): `fd.py set <id> T## interrupted`. The plan is not yours to edit.

Then `python tools/workflow/fd.py log <id> "T## blocked: <one-line reason>"`, `fd.py set <id> resume`, and tell the human: for a decision, answer in the design file, run /feature-design, then re-run /feature-execute; for a plan problem, what to change in the Task Plan. Stop.

## 6. Checkpoint and report

1. `python tools/workflow/fd.py log <id> "execute: <tasks completed> complete, <points> points; stopped: <budget used | blocked | all tasks complete>"`, then `git status` and `fd.py hot <id> --brief`.
2. Reply (at most 8 lines) with tasks completed, points used, tasks left, why you stopped and the changed files. Then:
   - Work remains: "Review the diff and commit (suggested message `feat(<id>): T01-T03 <summary>`, including `.features/designs_active/<id>_*`). Next: `fd.py resume <id>`, then /feature-execute [budget]."
   - All complete: "Review the diff and commit. Then run UAT: open `.features/designs_active/<id>_uat.html`, export the feedback, save it in that folder, and run `python tools/workflow/fd.py uat import <id> --apply`. If every AC passes, run /feature-review. Otherwise run /feature-polish."
