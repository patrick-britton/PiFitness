# Test Audit, Pass 2: PiFitness regression suite

## Session inputs (the only lines the user edits between sessions)

- BASE_REF: `<git commit or tag from just BEFORE pass 1 began; used only to audit deleted tests>`
- SCOPE: every test in the repo: `tests/**` and `frontend/pifitness/src/**/__tests__/**`. This includes files whose tests all passed and were never touched in pass 1.

---

## 1. Context

`autodeploy.sh` runs two suites as a deploy gate (TEST Step 6, later PRD Step 8), listed in `deploy/tests.conf`: backend `pytest` (config in `pytest.ini`) and frontend `vitest`. Both must exit 0 or the deploy stops. Every test therefore has to justify its place as a gate.

This is a personal, single-developer app. Do not over-engineer.

Pass 1 already deleted obvious junk and marked each remaining test `REVIEWED:` or `REVIEWED-FAILING:`. This pass is a **second, skeptical audit** of that work. It looks for two things:

1. **Misidentification by the previous agent**: wrong keeps, wrong deletions, inaccurate purpose comments, wrong failure causes.
2. **Acceptance-criteria tests**: tests written during feature development that pin one narrow detail of how the feature was built, instead of testing the feature or code properly.

**Deleting is cheap and reversible (git). Keeping a bad test costs a blocked deploy. The user explicitly wants you to delete when you are not sure,** with the one exception in section 5d.

## 2. Outcomes

Decide one outcome per test and apply it immediately.

| Outcome | Action | Marker you leave |
|---|---|---|
| **DELETE** | No value, or too narrow or implementation-coupled to protect anything real. | none (gone) |
| **KEEP** | Real behavior-level value. | `# VERIFIED: <purpose>` |
| **KEEP-FAILING** | Real value, but it fails or a decision is needed. Do not repair. | `# VERIFIED-FAILING: <CAUSE-TAG> \| <what is wrong>` |

The marker goes on the line directly above the test (above decorators). Frontend uses `//` instead of `#`. When you keep a test, **replace** its old `REVIEWED:` / `REVIEWED-FAILING:` marker with the `VERIFIED` form. You may rewrite the purpose text so that it is accurate (section 5a).

Cause tags for `VERIFIED-FAILING`: `STALE-AFTER-REORG`, `BRITTLE-MESSAGE`, `POSSIBLE-REAL-DEFECT`, `BELONGS-IN-SMOKE`, `DECISION-NEEDED`.

Do not add pytest markers. `pytest.ini` uses `--strict-markers`, so an unregistered marker breaks collection of the whole suite.

## 3. Hard boundaries

**Never:**
- run tests, `pytest`, `npm`, `uv`, `pip`, or anything touching the database, network or systemd
- edit anything outside test files (`tests/**`, `frontend/**/__tests__/**`)
- edit production code, scripts or `deploy/**`, even if you conclude something is now orphaned (report it)
- repair or rewrite tests. Whole-test decisions only: delete, or keep and mark.
- run git commands other than read-only ones (`git status`, `git diff`, `git log`, `git show`). No commit, stash, checkout, restore, reset or push.
- guess about behavior you cannot verify by reading. Say what is unverified.

**After deleting,** remove a helper, fixture or import in the same file only if `grep` proves nothing else uses it. Touch `conftest.py` only to remove a fixture with zero remaining users (prove it with `grep -r`). If every test in a file is gone, delete the file.

**Preserve each file's line endings and encoding** (CRLF and BOMs exist in this repo).

## 4. Resume protocol (read this first, every session)

The source files are the only progress record. Never rely on memory of an earlier session.

- **Remaining work** is every test that still carries a `REVIEWED:` or `REVIEWED-FAILING:` marker, plus any test with no marker at all (never reviewed; give it a full review and mark it directly with `VERIFIED`).
- **Finished** means the test carries `VERIFIED:` or `VERIFIED-FAILING:`. Never re-audit these.
- Find remaining work with `grep -rn "REVIEWED" tests frontend/pifitness/src`, plus a check for test functions that lack any marker above them.
- Work **one file at a time**, and apply each decision (delete or re-mark) the moment you make it, so an interruption loses nothing.
- Order: first all live tests (sections 5 and 6), then the deleted-test audit (section 7). The deleted-test audit is read-only and cheap to redo if interrupted.

## 5. How to audit each test

### 5a. Check the previous agent's claims
- Read the test body, not just its name and comment. Does the `REVIEWED:` purpose comment describe what the test **actually asserts**? If the comment is aspirational ("verifies X is handled correctly") but the assertions pin something narrower, either delete the test or rewrite the comment to match reality.
- For `REVIEWED-FAILING`, verify the stated cause by reading the production code. A test tagged `BRITTLE-MESSAGE` or `STALE-AFTER-REORG` might really be a `POSSIBLE-REAL-DEFECT`. A test tagged `POSSIBLE-REAL-DEFECT` might just be stale. Correct the tag, and quote the assertion plus the production line that explains it.
- Check that the code under test still exists and is still in use (`grep -rl` the symbol across `backend/`, `frontend/`, `deploy/`). Tests of code that no longer has callers are deleted, and the code is listed as orphaned in your report.

### 5b. Hunt for acceptance-criteria tests
Ask three questions of every test. If any answer is bad, it is a delete candidate:

1. **Bug question.** Name a plausible real bug in this code (wrong branch, off-by-one, dropped row, swallowed error, wrong status code). Would this test fail on it? If you cannot name one, delete.
2. **Different-implementation question.** Could a correct rewrite of the code, with the same behavior but different internals, fail this test? If yes, the test is coupled to the implementation. Delete.
3. **Generalization question.** Does it check the behavior over more than the one example the developer had in mind while building it (an edge, a boundary, an invariant, an error path)? A single hand-picked happy-path example with its literal expected value usually fails this.

Common tells:
- Name encodes a ticket, feature id or story detail, or says `..._not_reintroduced`, `..._exactly_...`, `..._still_...`, `..._default_...`.
- Asserts a **constant or default** (a timeout, a default list, a config value, a column list, an enum's full contents).
- Asserts an **entire literal dict, payload or key set** when callers only use some of the fields.
- Mirrors the implementation: expected value is computed with the same formula or logic as the code.
- Asserts on private helpers (`_underscore` names), call counts, call order, or "was called with" on a mock.
- Asserts SQL text, log text, UI copy, CSS classes, test-ids, or exact error-message wording.
- One example, one parametrize case, or a hand-built fixture shaped exactly like what the code expected at feature time.
- Frontend: snapshots, "renders without crashing", checks of internal component state, or a pinned default (for example "adds a step with a default 60s timer"), instead of what the user can do and see. **When scanning, flag any test that asserts a literal default value (e.g., timer_seconds = 60, pageSize = 20, default sort order) or contains keywords like "default", "exactly", "_default_", "_exactly_", "not_reintroduced". Such tests are likely acceptance‑criteria tests and should be considered for deletion.**
- The test exists to confirm a spec bullet, not to catch a regression.

**What a good test looks like here:** it exercises real code through the boundary callers depend on (HTTP status and payload fields callers use, return values, persisted rows, what the user can do in the UI), with several inputs including edges, or asserts an invariant (atomicity, idempotency, fail-loud on bad config, no placeholder for missing data). It stays green through a behavior-preserving refactor and goes red for a real bug. A cheap `smoke` import/route-shape check is fine, one per area.

### 5c. Always valid, regardless of the above
- Tests must be deterministic, with no network and no dependence on running services, shared ports, Git Bash, or Windows (spec TEST Step 6 requires this). A test that needs a running stack is `BELONGS-IN-SMOKE` if it is valuable, otherwise deleted.
- Tests that write to the database are accepted for now (spec decision D22). That alone is not a reason to delete.

### 5d. The one exception to "delete when unsure"
If a test guards against **using the wrong database (PRD instead of TEST), data loss, or lost atomicity**, and you are unsure about it, **keep it and mark it `VERIFIED-FAILING: DECISION-NEEDED | ...`** (or `VERIFIED` if it passes). Say so in the report. These are the failures a deploy gate most needs to catch, and the user does not want them removed by accident. If the test is clearly over-specific even for these, delete it.

## 6. What you may read (nothing else)

1. This prompt.
2. `autodeploy_objectives.md`, only: "TEST Step 6: Regression tests", "PRD Step 8: Regression tests" (including the write-to-PRD caveat) and "10. Legacy paths, parameters and settings". Locate them with `grep -n '^#'`, then read just those ranges.
3. `pytest.ini` and `deploy/tests.conf`.
4. The test files in scope.
5. For each test, the **minimum** production code needed to answer the three questions in 5b and the existence check in 5a. Locate with `grep` on the symbol, then open only that file or range.

Do not read `deploy/scripts/**`, launcher scripts, log directories, `node_modules` or the venv, unless an in-scope test directly targets them.

## 7. Deleted-test audit (last)

If `BASE_REF` is blank, skip this section and say so in the report.

1. List tests removed since `BASE_REF`: `git diff BASE_REF --stat -- tests frontend/pifitness/src` to find the files, then `git diff BASE_REF -- <file>` for each.
2. Apply the **same section 5b questions** to each deleted test. A deletion is a **restore candidate** only if the test was behavior-level (it would catch a real bug), still targets code that exists, and was deterministic.
3. Do not restore anything yourself. Report each restore candidate with file, test name, the commit to restore from, and your reason in one line. Skepticism cuts both ways here: most pass-1 deletions were probably right, so only flag the clear mistakes.

## 8. Ground rules

1. `autodeploy_objectives.md` is the spec. If something critical contradicts it, say so explicitly. Do not quietly change course.
2. Do not assume. Separate what you **verified** (and how) from what is **untested**. You cannot run anything, so every conclusion about behavior comes from reading only.
3. Do not create files. If the user needs to do something manually, give step-by-step instructions that name the machine, file and exact commands.

## 9. Finish and report

Stop when no test carries a `REVIEWED` marker and no test lacks a marker. Do not go on to repair anything.

Final message, in this order:

1. **Totals**: tests audited, deleted, KEEP (`VERIFIED`), `VERIFIED-FAILING` by cause tag, files deleted entirely, comments corrected.
2. **Deleted this pass**: one line each: `file::test | tell that triggered it | which of the three questions it failed`.
3. **Corrected claims**: tests where pass 1's purpose comment or failure cause was wrong, with the corrected version.
4. **VERIFIED-FAILING**: one line each, `POSSIBLE-REAL-DEFECT` first.
5. **Kept under the 5d exception**: each one, with why you were unsure.
6. **Restore candidates** from the deleted-test audit.
7. **Orphaned code candidates**: scripts or modules that have no remaining tests or callers. List only.
8. **Decisions for the user**, each as a specific question, and **untested / unverified** items.