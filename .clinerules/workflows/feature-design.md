# /feature-design

Turn the feature request into a design. Record everything you are unsure about as open questions (OQs) in the design file, then stop so the human can answer them in that file. Design only: no code edits, no planning.

Re-runnable. The first run creates the design. Each later run processes the human's written answers.

Common rules: `.clinerules/20-feature-common.md`.

## Rules

- By hand, edit only `.features/designs_active/<id>_design.md`. Files `fd.py` writes itself (`tools/workflow/uat.json`, `docs/workflow/BACKLOG.md`) are fine.
- The request is frozen once the design file exists. To change it, the human deletes the design file and runs this workflow again.
- Every existing file, table, or endpoint in the design comes from the request or a lineage result. A new table, column, endpoint, or file is allowed only when the feature needs it: mark it `(new)` and follow the nearest existing naming pattern. If no pattern exists, ask an OQ.
- Every B# in the request is covered by at least one AC. Every AC covers at least one B#.
- You are expected to be unsure. A design with no open questions is wrong. Only the human's written answer resolves an OQ; you only judge whether it is enough (step 8).
- Before a Task Plan exists, `fd.py next`, `resume`, and `set` refuse to run and `hot` reports a structural error (no tasks yet). Use only `check`, `oq`, `section`, and `uat build`, and edit Resume Here by hand. Once a Task Plan exists, use `fd.py set <id> resume`.

## 1. Locate

1. `git branch --show-current` must print `feature-test`. Otherwise stop and tell the human to switch.
2. `.features/descriptions/` must hold exactly one `.md` file. Otherwise stop and tell the human.
3. Read it. Its frontmatter `feature-id` is `<id>` and must match the filename. Otherwise stop.
4. `.features/designs_active/` must hold no file for another feature. Otherwise stop and tell the human.
5. If `<id>_design.md` does not exist, this is the first run: go to step 2. If it exists, go to step 8.

## 2. First run: create the design and record questions

1. Copy `.features/templates/_FEATURE_DESIGN_TEMPLATE.md` to `.features/designs_active/<id>_design.md`.
2. Fill the frontmatter from the request. `status` stays `draft`. Set `created` and `last-updated` to today. Leave `surfaces` and `points` for later.
3. Write OQ-1 (step 5) from the request alone. Its assumption is that the scope is exactly B1 to Bn.
4. If section 5 of the request lists anything other than `- None`, add one OQ per item (OQ-2, OQ-3, ...). The Question is the item as written plus `Default: ...` with your recommended answer. `Status: open`, `Answer: (to be provided)`, `Decision: (to be recorded)`.
5. Set Resume Here: `progress` `0/0 tasks complete`, `next` `review`, `blocked-by` every open OQ id.
6. If the request had questions, go to step 7 now. Do not research or draft. If it had none, continue.

## 3. Map the surfaces

Do this only when no OQ other than OQ-1 is open.

1. List each screen, table, endpoint, or job the request touches.
2. For each, run `python tools/data_dictionary/lineage.py describe <target>` before searching or reading any source. A bare name works for tables and functions. For a screen or endpoint use its node id, for example `page:/runs` or `endpoint:GET /api/runs`.
3. If describe exits 2 (no match, or ambiguous), run `lineage.py search <name>` once, then re-run describe with the exact id. If lineage still has nothing, run one targeted repo search for that name and raise an OQ about it. This applies only to things that already exist. Lineage describes the repo before this feature, so never look up anything marked `(new)` and never ask about it: describe its nearest existing pattern instead.
4. If lineage reports `No graph`, stop and tell the human to run `python tools/data_dictionary/lineage.py build`.
5. If lineage prints `WARNING graph may be stale` on stderr, say so in your final reply. Do not rebuild it.
6. Read source only for files lineage points to, and only what the design needs.

## 4. Write the design

1. Fill `surfaces` in the frontmatter. Set `last-updated` to today.
2. **Architecture**, at most 250 words total:
   - Surfaces: each target with its lineage result in one line.
   - Approach: how the change works, in plain steps.
   - Data and migrations: additive only, or `None`.
   - API changes: or `None`.
3. **Acceptance Criteria:** one checkbox line per AC, plus one `auto:` sub-line.
   - The AC line is what the human does and sees, checked at 360x740 and 448x875 for any UI: `- [ ] AC-1: <do> -> <see> (covers B1)`. It is the human's UAT step; there is no separate `human:` line.
   - `auto:` takes exactly one of three forms:
     - An existing command or existing test. This is the default.
     - `new test <path>::<name> [invariant|contract|smoke|journey]`. Allowed only when the AC guards a lasting rule other features could break, and only after the human approved it in an OQ. Until then write `auto:` for the nearest existing command or test, or `none (<reason>)`, and add `(pending OQ-n)` to the AC line. Never for behavior that matters only to this feature's one-off implementation.
     - `none (<reason>)`, only when truly impossible.
   - The `auto:` lines are the complete list of new tests this feature may add. Fewer is better; do not propose tests to be thorough.
   - Bug Fix: AC-1 must reproduce the original problem.
   - If an AC depends on an unanswered OQ, write it for your recommended default and add `(pending OQ-n)` to the AC line.
4. **Open Questions:** add any new ones using step 5.
5. **Resume Here:** `progress` stays `0/0 tasks complete`, `next` is `review`, `blocked-by` lists every open OQ id.
6. Leave **Task Plan** as the template placeholder and `points:` empty.

## 5. Open Questions

Write each OQ in the template format with `Status: open`, `Answer: (to be provided)`, `Decision: (to be recorded)`.

**OQ-1 is always "Confirm understanding".** Its Question restates the feature in 2-3 sentences in your own words (what changes for the user, what stays the same), lists at least one assumption, and ends with `Default: yes, this is right.` If you made no other assumption, state that the scope is exactly B1 to Bn and nothing else. While OQ-1 is open you may add assumptions to it. Once resolved, never edit it; add a new OQ instead. Its Decision is then the agreed scope: where it differs from the request, follow the Decision.

**Guess check.** Before finishing, go through every B# and answer each question. Each "yes" becomes an OQ, unless an existing OQ already covers it:

1. Does it use a vague word (better, faster, clean, properly) with no observable measure?
2. Does it leave out what happens when data is empty, loading, failed, first-use, or offline?
3. Did I have to pick a label, number, default, or order, or guess anything, to write its AC?

Then answer for the feature as a whole:

4. Does lineage show the target also feeds screens, jobs, or tables the request does not mention?
5. Does existing behavior I found conflict with a B#?
6. Would two or more reasonable approaches look different to the user or change stored data?
7. Would an AC be better guarded by a new automated test? Ask `Add test <path>::<name>? It guards <rule>.` with `Default: no`. This is the only way a new test enters the design.

**Wording.**
- Closed questions: A or B, yes or no, or a concrete value.
- Put your recommendation in the Question as `Default: ...`. The human accepts it by writing the single word `default` on the Answer line.
- Rationale says what in the design changes with the answer.
- Add at most 5 OQs of your own, not counting OQ-1 and the request's own questions. If you have more, keep the ones that change the most ACs. Test questions count and rank last.

## 6. Validate

1. Run `python tools/workflow/fd.py check <id>`. While no Task Plan exists, its only expected error is `no '### T##' blocks under '## Task Plan'`. Fix any other error.
2. Run `python tools/workflow/fd.py oq <id>`. It must list OQ-1.
3. Confirm every B# appears in at least one `(covers ...)` and every AC has an `auto:` line.
4. Run `python tools/workflow/fd.py uat build <id>`. This writes `tools/workflow/uat.json` (read by `uat_launcher.py` at UAT time). Skip this while the design is not drafted yet (first run with request questions).

## 7. Ask and stop

Reply with the design path and the AC count (if drafted), then say:

"Open questions are in the design file under ## Open Questions. Write each answer on its `Answer:` line in your own words (or the word `default` to accept my default), save the file, then run /feature-design again."

List every open OQ as a numbered list: the question, your default, and what in the design changes with the answer. The list is a convenience; the file is the record. Then stop. Do not start planning.

## 8. Re-run: process the written answers

1. Run `python tools/workflow/fd.py oq <id> --open` and `python tools/workflow/fd.py oq <id> --unanswered`. An open OQ that is not unanswered has text on its `Answer:` line.
2. Judge each written answer. Accept any wording that settles the question. Three outcomes:
   - **Enough to proceed:** you can write the design without guessing. The word `default` means "accept the Default in the Question". Set `Decision` to the outcome in one line and `Status: resolved`. Leave `Answer` exactly as the human wrote it.
   - **Not enough:** unclear, partial, off-topic, contradicting the request or another answer, or raising a new question. Do not resolve it and do not guess. Rewrite the Question as `You said: "<their words>". Still open: <what is missing>.` Keep any useful Default, set `Answer` back to `(to be provided)`, leave `Status: open`, and keep the same OQ id.
   - **Still the placeholder:** leave it open and unchanged.
3. OQ-1 follows the same rule. A correction to your restatement is "not enough": rewrite the restatement and assumptions to include it and ask again. Once OQ-1 is resolved, its Decision is the scope. A B# it drops or changes follows the Decision; do not edit the request.
4. If Architecture still holds the template placeholder and no OQ other than OQ-1 is open, do steps 3 to 6 now.
5. Update any AC marked `(pending OQ-n)` whose OQ is now resolved, and any architecture text the answer changes. For an approved test, change that AC's `auto:` to the `new test` line. For a declined one, keep the existing check. Do not renumber ACs. If the Task Plan already exists, do not edit it; list the tasks or bugs an answer affects in your reply.
6. If an answer raises a new unknown, add a new open OQ. Never reopen a resolved one.
7. Update `blocked-by` and `last-updated`: by hand if there is no Task Plan, with `python tools/workflow/fd.py set <id> resume` if there is. Re-run step 6, including `uat build` (which refreshes `tools/workflow/uat.json`) whenever an AC changed.
8. If any OQ is open, go to step 7. If none is open:
   - No Task Plan yet: set `blocked-by` to `none` and reply: "Design ready. Review it, then run /feature-plan."
   - Task Plan exists: list the tasks or bugs any answer affects, then reply "Questions resolved." plus the workflow to re-run: /feature-execute <budget> if any task is not complete, otherwise /feature-polish.

Reminder: edit only the design file, never resolve an OQ yourself, and stop after asking.
