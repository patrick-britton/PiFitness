### Principles

1. **Tools enforce; prose advises.** If a rule matters, a script checks it.
2. **Evidence before done.** Nothing closes without recorded command output. Humans confirm only what humans can judge.
3. **Design for the weakest model you might get mid-task.** Tasks are session-sized, and state lives in files, never in a chat.
4. **One home per fact.** Every piece of information lives in exactly one place; everything else points to it.
5. **Agents flag; the human decides scope.** Extra findings go to a backlog, not into the current feature.
6. **Generate where possible.** Schema map, UAT page, task briefs and folder health are produced by scripts, not written by hand.

### Verifiable statements

**Repo and docs**

- **R1.** Every folder is listed in `REPO_MAP.md` with its purpose, who writes there, what must not be there, and allowed dependencies. *Check: repo\_health fails on any unmapped folder.*
- **R2.** `ARCHITECTURE.md` covers layers, request flow, deployment topology and Pi performance constraints, and every claim cites a path. *Check: claims without a path are flagged.*
- **R3.** `docs/INDEX.md` lists each doc, what it owns and what it must not repeat. *Check: human review plus a duplicate-heading scan.*
- **R4.** `UI_PRINCIPLES.md` exists, and each rule names how it is checked (auto or human).

**Data layer**

- **D1.** `schema.generated.md` matches the live DEV catalog. *Check: regeneration produces no diff.*
- **D2.** Every table and column has a database COMMENT, and `dictionary.md` covers each domain with the grain of key tables, the canonical source of each concept, and business rules marked enforced or convention.
- **D3.** DDL changes only through numbered migration files. The tool applies them to DEV, the human applies them to PRD, and the DEV refresh replays pending migrations.
- **D4.** A design that adds a table or column includes a `dd.py find` result showing nothing equivalent exists.

**Verification**

- **V1.** One `verify` command runs the full gate and exits 0 on a clean baseline.
- **V2.** Every test has a purpose docstring and a class (invariant, contract, smoke or journey). No test mocks the unit under test, and none cites a bug or feature ID. A Keep/Quarantine/Delete manifest exists.
- **V3.** Comment lint passes: no feature, bug or task IDs, and no change-history comments.
- **V4.** `repo_health` checks structure against the map, duplicate definitions, import direction, dead files and performance smells (unbounded queries, SELECT \*, N+1, pandas in the production path).
- **V5.** Mobile checks run at 448x875 and 360x740: no horizontal scroll, tap targets of at least 44px, correct input types, clean console.

**Workflow**

- **W1.** The workflow tool refuses to close a task without recorded verify output and a prior start entry. Force is human-only.
- **W2.** A task is one concern, at most three files and one verify command. Anything larger is split.
- **W3.** A generated brief exists per task, and `resume` restores an interrupted session with no human re-explanation.
- **W4.** Every problem statement in a feature brief maps to at least one AC or an explicit out-of-scope line. *Check: tool at review time.*
- **W5.** Each AC has an `auto` line, a `human` line, or an explicit auto-only mark. A tool generates the UAT HTML page, and human results are imported back.
- **W6.** A feature closes only with both an agent pass and a human pass. Failures are logged against AC IDs.
- **W7.** Out-of-scope findings go to a backlog file. Agents do not expand scope.
- **W8.** At feature end, a retro converts each recurring lesson into a check or a rule line.
- **W9.** A module-by-module walkthrough checklist exists and has been run on the phone, and every finding is in the backlog.

**Environment**

- **E1.** DEV runs locally. The agent environment holds no PRD credentials, and the backend refuses writes when the DB host is not local.
- **E2.** A baseline template database allows a DEV reset in seconds.
- **E3.** Bootstrap fetches the latest deployment scripts and requirements from the deployment branch, and offers a choice of `main` (PRD) or `react-ui` (test on Pi).
- **E4.** A Pi post-deploy checklist exists and is run.
- **E5.** The app installs from Chrome as a PWA over HTTPS.

**Agents**

- **A1.** The always-on rules file is about 2 KB or less. There are at most four skills, each holding only project-specific facts and each surviving an ablation test.
- **A2.** `model-notes.md` lists each model, what it is usable for, its quirks and the date last verified.
- **A3.** A three-task benchmark and the capability probe are re-run whenever the active model changes.