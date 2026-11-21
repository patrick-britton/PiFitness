---
description: Turn the feature request into a design file with open questions for the human to answer. Re-run to process written answers.
---

# /feature-design

Turn the feature request into a design and record everything you are unsure about as open questions (OQs) in the design file. Design only: no code edits, no planning. Re-runnable: the first run creates the design; later runs process the human's written answers. First read `.agents/rules/20-feature-common.md`.

## Rules

- Hand-edit only `.features/designs_active/<id>_design.md` (files `fd.py` writes itself are fine).
- The request is frozen once the design exists; to change it the human deletes the design and re-runs.
- Existing files, tables and endpoints come from the request or lineage. Anything new is added only if the feature needs it, marked `(new)`, and follows the nearest naming pattern; with no pattern, ask an OQ.
- Every B# is covered by at least one AC and every AC covers at least one B#.
- A design with no open questions is wrong. Only the human's written answer resolves an OQ; you judge whether it is enough (step 8).
- With no Task Plan, `fd.py next/resume/set` refuse and `hot` reports a structural error. Use `check`, `oq`, `section`, `uat build`, and edit Resume Here by hand. Once a Task Plan exists, use `fd.py set <id> resume`.

## 1. Locate

Branch must be `feature-test`; `.features/descriptions/` must hold exactly one `.md` whose frontmatter `feature-id` (= `<id>`) matches its filename; `designs_active/` must hold no other feature's file. Otherwise stop and tell the human. No `<id>_design.md` yet: step 2. Exists: step 8.

## 2. First run

1. Copy `.features/templates/_FEATURE_DESIGN_TEMPLATE.md` to `.features/designs_active/<id>_design.md`. Fill the frontmatter from the request: `status` stays `draft`, `created` and `last-updated` today, leave `surfaces` and `points`.
2. Write OQ-1 (step 5) from the request alone, assuming scope is exactly B1 to Bn.
3. If section 5 of the request lists anything but `- None`, add one OQ per item: the item as written plus `Default: ...`.
4. Resume Here: `progress` `0/0 tasks complete`, `next` `review`, `blocked-by` every open OQ id.
5. If the request had questions, go to step 7 now: no research or drafting. Otherwise continue.

## 3. Map the surfaces (only when no OQ other than OQ-1 is open)

For each screen, table, endpoint or job the request touches, run `python tools/data_dictionary/lineage.py describe <target>` before searching or reading source (bare name for tables and functions; node ids such as `page:/runs` or `endpoint:GET /api/runs` for screens and endpoints). Exit 2: run `lineage.py search <name>` once, then describe the exact id; if still nothing, do one targeted repo search and raise an OQ. Never look up `(new)` things; describe their nearest existing pattern. `No graph`: stop and tell the human to run `lineage.py build`. `WARNING graph may be stale`: mention it in your final reply, do not rebuild. Read source only for files lineage points to.

Everything you learn here goes into the Surfaces lines in step 4, because the planner will not repeat this research and the executor works from what the planner copies out of it.

## 4. Write the design

1. Fill `surfaces` and `last-updated`.
2. **Architecture**, at most 250 words excluding Surfaces: **Surfaces** (for each target: the lineage result; the exact file path; the functions, components or tables the change touches, with signatures; the nearest pattern file; and any gotcha or constraint you hit, one short line each; for a bug fix also where the bug lives and why, as far as you found out; never paste code; Surfaces has no word limit but stays facts only); Principles alignment (the principle doc for each surface); Approach (plain steps); Data and migrations (additive only, or `None`); API changes (or `None`).
3. **Acceptance Criteria:** one checkbox line plus one `auto:` line each.
   - The AC line is what the human does and sees, checked at 360x740 and 448x875 for UI: `- [ ] AC-1: <do> -> <see> (covers B1)`. It is the human's UAT step; there is no separate `human:` line.
   - `auto:` is one of:
     - An existing compile, type-check or import check (default): `npx tsc --noEmit` (cwd `frontend\pifitness`), `python -m py_compile <path>`, or an existing test that already covers the area.
     - `new test <path>::<name> [invariant|contract|smoke|journey]`: only for a lasting rule other features could break, and only after the human approves it in an OQ. Until then use the nearest existing check or `none (<reason>)` and add `(pending OQ-n)` to the AC line. Never use `new test` for an expected value, label, count, order, text or layout, for a one-time request or a bug reproduction, or for anything the human checks in UAT.
     - `none (<reason>)`, only when truly impossible.
   - The `auto:` lines are the complete list of new tests; fewer is better and zero is normal.
   - Bug fix: AC-1 reproduces the original problem. This is the human's UAT step, not a test. An AC that depends on an unanswered OQ is written for your default and marked `(pending OQ-n)`.
4. Add any new OQs (step 5). Resume Here as in step 2. Leave Task Plan as the template placeholder and `points:` empty.

## 5. Open Questions

Template format, `Status: open`, `Answer: (to be provided)`, `Decision: (to be recorded)`.

**OQ-1 is always "Confirm understanding":** restate the feature in 2-3 sentences (what changes for the user, what stays the same), list at least one assumption (or state that scope is exactly B1 to Bn), and end `Default: yes, this is right.` You may add assumptions while it is open. Once resolved, never edit it; its Decision is the agreed scope and overrides the request where they differ.

**Guess check.** Each "yes" becomes an OQ unless one already covers it. Per B#: vague word with no observable measure? empty, loading, failed, first-use or offline behavior unspecified? did I pick a label, number, default or order to write its AC? For the whole feature: does lineage show the target feeds screens, jobs or tables the request omits? does existing behavior conflict with a B#? would two or more approaches differ for the user or stored data? Do not raise test questions by default. Ask `Add test <path>::<name>? It guards <rule>.` with `Default: no` only when a lasting invariant is at risk; this is the only way a test enters the design.

**Wording.** Closed questions (A or B, yes or no, a concrete value) with `Default: ...` in the Question; the human accepts it by writing `default`. Rationale says what changes with the answer. At most 5 OQs of your own beyond OQ-1 and the request's; keep those that change the most ACs, test questions last.

## 6. Validate

1. `python tools/workflow/fd.py check <id>`: with no Task Plan, the only expected error is `no '### T##' blocks under '## Task Plan'`. Fix anything else.
2. `python tools/workflow/fd.py oq <id>` must list OQ-1.
3. Every B# appears in a `(covers ...)` and every AC has an `auto:` line.
4. `python tools/workflow/fd.py uat build <id>` (skip while the design is not drafted).

## 7. Ask and stop

Reply with the design path and AC count (if drafted), then: "Open questions are in the design file under ## Open Questions. Write each answer on its `Answer:` line in your own words (or `default` to accept my default), save the file, then run /feature-design again." List each open OQ as: the question, your default, what changes with the answer. Then stop.

## 8. Re-run: process the answers

1. Run `fd.py oq <id> --open` and `fd.py oq <id> --unanswered`; an open OQ that is not unanswered has text on its `Answer:` line.
2. Judge each answer:
   - **Enough** (you can proceed without guessing; `default` means accept the Default): set `Decision` to the outcome in one line and `Status: resolved`. Leave `Answer` exactly as written.
   - **Not enough** (unclear, partial, off-topic, contradictory, or raising a new question): do not resolve or guess. Rewrite the Question as `You said: "<their words>". Still open: <what is missing>.`, keep a useful Default, reset `Answer` to `(to be provided)`, keep `Status: open` and the same id.
   - **Still the placeholder:** leave unchanged.
3. OQ-1 follows the same rule; a correction to your restatement is "not enough".
4. If Architecture is still the placeholder and no OQ other than OQ-1 is open, do steps 3 to 6 now.
5. Update ACs marked `(pending OQ-n)` whose OQ is resolved, and any architecture text the answer changes (including Surfaces lines). An approved test becomes a `new test` line; a declined one keeps the existing check. Do not renumber ACs. If a Task Plan exists, do not edit it; list the tasks or bugs an answer affects in your reply.
6. A new unknown becomes a new OQ; never reopen a resolved one.
7. Update `blocked-by` and `last-updated` (by hand without a Task Plan, otherwise `fd.py set <id> resume`). Re-run step 6, including `uat build`, if an AC changed.
8. If any OQ is open, go to step 7. If none: with no Task Plan set `blocked-by` to `none` and reply "Design ready. Review it, then run /feature-plan."; with a Task Plan list the affected tasks or bugs and reply "Questions resolved." plus /feature-execute [budget] if any task is incomplete, otherwise /feature-polish.
