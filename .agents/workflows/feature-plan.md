---
description: Break the approved design into small verified tasks in the design file's Task Plan. Planning only, no code edits.
---

# /feature-plan

Break the approved design into small tasks in the design file's `## Task Plan`. Planning only: no code edits. First read `.agents/rules/20-feature-common.md`.

## Rules

- Hand-edit only `.features/designs_active/<id>_design.md` (files `fd.py` writes itself are fine).
- Every path and symbol in a task must be verified to exist or be marked `create`. No guesses.
- If something is unclear, stop and ask; a blocking question becomes an OQ in the design file.

## 1. Gate and read

1. Standard gate (no status requirement; `hot` reporting `no '### T##' blocks` is expected). `python tools/workflow/fd.py section <id> "Task Plan"` must hold no `### T##` blocks; otherwise stop and tell the human.
2. Read `fd.py section <id> Architecture`, `... Acceptance`, `... "Open Questions"`. If Architecture or Acceptance still hold template placeholders (`<...>`), stop and tell the human to run /feature-design. Do not read the request file.

## 2. Verify anchors

For each surface, run `python tools/data_dictionary/lineage.py describe <target>` first, then read only the files it points to (`lineage.py trace <node> --files` lists a data-flow chain). Exit 2: `lineage.py search <name>`, then describe the exact id. Skip lineage for `(new)` things. `No graph`: stop and tell the human to run `lineage.py build`. Confirm every file you will touch exists and every symbol exists in it. A file that does not exist is `create`; one an earlier task creates is `planned`.

## 3. Decompose

Order: database, backend, frontend. **One task = one concern, one Verify command, at most 3 files** (count `modify`, `create`, `delete` and `planned` anchors; `pattern` and `read` do not count). Points: 1 = 1 file, 2 = up to 2, 3 = up to 3; more than 3 means split. `fd.py check` rejects tasks with Points not 1-3, more than 3 files, or more files than points.

- Every AC is covered by a task and every task traces to an AC. The task that completes an AC uses that AC's `auto:` command as Verify when it can pass at that point.
- Plan 3 to 12 tasks; if more are needed, stop and ask the human to split the feature. No code-review or test-only tasks.
- Migrations are additive only, with a `COMMENT` for every new table and column.
- Verify must finish in about 30 seconds or less in the foreground. If the AC's `auto:` is slower, use the nearest fast existing check and say so in Detail.
- **Tests:** the design's `auto:` lines are the complete list of new tests. Create a test file only for an AC whose `auto:` says `new test`, named exactly as the AC does (prefer adding to an existing file). An existing test this change breaks may be modified in the same task. Never create tests for implementation details, scratch checks or snapshots. For `auto: none (...)`, Verify is the nearest existing check.
- **Verify from `auto:`:** copy the command out of its backticks (a Verify never has backticks); for `new test`, use the command that runs that one test.
- **New files later tasks use:** the creating task's Detail gives the path and one `Interface:` line per exported symbol (signature or shape, and what it does). Each task that uses it has a `planned` anchor repeating that symbol and signature. `Interface:` lines do not count toward the Detail sentence limit.

## 4. Write the Task Plan

Replace everything under `## Task Plan` with task blocks in the format of the template's commented example. Fields:

- `Surface`: frontend, backend, database, deployment or integration (this selects the principle doc).
- `Skill`: one or two words (react, fastapi, postgres). `Points`: 1, 2 or 3. `Depends-on`: task ids or `none`. `Traces-to`: AC ids.
- `Anchors`: one line per file, `` `role`: `path` `symbol` ``. Roles: `modify`, `delete`, `pattern`, `read`, `create`, `planned` (written `` `planned` <- T01 `path` `symbol` ``). Never `search`; `greenfield` only with no existing code to anchor to.
- `Detail`: at most 3 sentences plus `Interface:` lines; no code beyond a signature.
- `Verify`: one plain command on one line, no backticks, pipes or `&&`, exiting 0 only when this task's change works.
- `Status`: `todo`.

## 5. Finish

1. Run `python tools/workflow/fd.py set <id> resume` (it writes `points:`, `last-updated` and Resume Here).
2. Run `python tools/workflow/fd.py check <id> --anchors`: 0 errors, 0 warnings, `0 bad`, `0 unresolved`. Then confirm by reading what it cannot check: no task touches more than 3 files counting `planned`; every `planned` anchor matches its creator's `Interface:` line; every new test file traces to a `new test` AC; every AC id appears in some `Traces-to`.
3. Reply with task count, total points, T01's title, and "Review the plan. Then run `python tools/workflow/fd.py set <id> feature reviewed` and /feature-execute <budget>." Do not start execution.
