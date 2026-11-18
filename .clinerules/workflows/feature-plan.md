# /feature-plan

Break the approved design into small tasks in the design file's `## Task Plan`. Planning only: no code edits.

Common rules: `.clinerules/20-feature-common.md`.

## Rules

- By hand, edit only `.features/designs_active/<id>_design.md`. Files `fd.py` writes itself are fine.
- Every file path and symbol in a task must be verified to exist, or be marked `create`. No guesses.
- If something is unclear, stop and ask. A question that blocks the plan becomes an open question in the design file (see "Raising a blocking question" in the common rules).

## 1. Gate

1. Run the standard gate. No status requirement applies: before planning, `hot` reports `no '### T##' blocks`, which is expected.
2. Run `python tools/workflow/fd.py section <id> "Task Plan"`. If it already holds `### T##` blocks, stop and tell the human.

## 2. Read the design

1. Run `python tools/workflow/fd.py section <id> Architecture`, then `... section <id> Acceptance`, then `... section <id> "Open Questions"` for the decisions.
2. If Architecture or Acceptance Criteria still hold template placeholders (text in `<...>`), stop and tell the human to run /feature-design.
3. Do not read the request file. The design is the source.

## 3. Verify anchors

1. For each surface in the design, run `python tools/data_dictionary/lineage.py describe <target>` first, then read only the files it points to. To list the files in a data-flow chain, use `lineage.py trace <node> --files`. If describe exits 2, run `lineage.py search <name>` and re-run with the exact id. Skip lineage for anything the design marks `(new)`. If lineage reports `No graph`, stop and tell the human to run `python tools/data_dictionary/lineage.py build`.
2. For every file you will touch, confirm it exists. For every function, component, or table you will touch, confirm the symbol exists in that file.
3. A file that does not exist yet is `create`. A file that an earlier task creates is `planned`.

## 4. Decompose

Order tasks by dependency: database, then backend, then frontend.

**One task = one concern, one Verify command, at most 3 files.** Count files from `modify`, `create`, and `delete` anchors. `pattern` and `read` anchors do not count. A `planned` anchor does count, although `fd.py check` does not count it, so count it yourself.

| Points | Files | Typical task |
|---|---|---|
| 1 | 1 | one-file edit, label, CSS, index |
| 2 | up to 2 | route and query, or component and helper |
| 3 | up to 3 | route, model, and migration, or component, hook, and helper |
| more than 3 | not allowed | split it |

`fd.py check` rejects a task whose Points is not 1, 2, or 3, that touches more than 3 files, or that touches more files than it has points.

- Every AC is covered by at least one task, and every task traces to at least one AC.
- The task that completes an AC uses that AC's `auto:` command as its Verify, when the command can pass at that point.
- Plan 3 to 12 tasks. If you need more than 12, stop and ask the human to split the feature.
- Do not add a code-review task or a test-only task. Review is /feature-review and the human tests in UAT.
- A migration task is additive only and adds a `COMMENT` for every new table and column.
- Keep each Verify under the 30-second foreground limit. If the AC's `auto:` command is slower, use the nearest fast existing check as Verify and say so in Detail.

**Tests.** The design's `auto:` lines are the complete list of new tests.
- Create a test file only for an AC whose `auto:` says `new test`. Name it exactly as the AC does. Prefer adding to an existing test file.
- An existing test that this change breaks may be modified in the same task.
- Never create a test for implementation details, a scratch check, a snapshot, or a temporary script.
- If an AC's `auto:` is `none (<reason>)`, Verify is the nearest existing check: typecheck, lint, or an existing targeted test.

**Verify from an `auto:` line:** copy the command out of its backticks (a Verify never has backticks). For a `new test` line, use the command that runs that one test, in the form from `.clinerules/00-environment.md`.

**New files that later tasks use.** The creating task writes the spec in Detail: the path, then one `Interface:` line per exported symbol with its signature or shape and what it does. Each task that uses the file has a `planned` anchor repeating that symbol and signature, so nobody guesses or reads ahead. `Interface:` lines do not count toward the Detail sentence limit.

## 5. Write the Task Plan

Replace everything under `## Task Plan` with task blocks in the format of the template's commented example.

- `Surface`: one of frontend, backend, database, deployment, integration. The surface dictates which principle document in `docs/` governs the task:
  - `frontend` -> `docs/UI_PRINCIPLES.md`
  - `database` -> `docs/DATABASE_PRINCIPLES.md`
  - `integration` -> `docs/INTEGRATION_PRINCIPLES.md`
  - `backend` -> `docs/ARCHITECTURE.md`
  - `deployment` -> `docs/PI_SPECS.md`
- `Skill`: the expertise needed, one or two words (react, fastapi, postgres).
- `Points`: `1`, `2`, or `3`.
- `Depends-on`: task ids, or `none`.
- `Traces-to`: the AC ids this task serves.
- `Anchors`: one line per file, as `` `role`: `path` `symbol` ``. Roles: `modify`, `delete`, `pattern`, `read`, `create`, `planned`. A `planned` line names the creating task and the symbol: `` `planned` <- T01 `path` `symbol` ``. Never use `search`. Use `greenfield` only when there is no existing code to anchor to.
- `Detail`: at most 3 sentences on what to change, plus `Interface:` lines where needed. No code beyond a signature.
- `Verify`: one plain command on one line, with no backticks, no pipes, and no `&&`. It must exit 0 only when this task's change works.
- `Status`: `todo`.

## 6. Update the design file

Run `python tools/workflow/fd.py set <id> resume`. It writes the frontmatter `points:` total, `last-updated`, and Resume Here. Do not edit those by hand.

## 7. Validate

1. Run `python tools/workflow/fd.py check <id> --anchors`. It must report 0 errors, 0 warnings, `0 bad`, and `0 unresolved`. Fix everything it reports.
2. Confirm by reading that:
   - every AC id appears in some `Traces-to`;
   - no task touches more than 3 files, counting `planned` anchors;
   - every new test file traces to an AC whose `auto:` says `new test`;
   - every `planned` anchor matches the `Interface:` line of the task that creates the file.

## 8. Report and stop

Reply with: task count, total points, T01's title, and "Review the plan. Then run `python tools/workflow/fd.py set <id> feature reviewed` and /feature-execute <budget>." Do not start execution.

Reminder: edit only the design file, verify every path and symbol, and stop after reporting.
