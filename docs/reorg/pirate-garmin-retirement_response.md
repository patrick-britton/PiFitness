I now have all the context needed. Let me record my analysis and produce the plan.

## Analysis Summary (gathered, not yet acted on)

**Phase A — login pattern:** `backend_functions/service_logins.py` (820 LOC) is where all login code lives today — both Spotify (`get_spotify_token`, `get_spotify_client`, `spotify_creds`, lines ~80-330) and Garmin (`pirate_garmin_login` line 742, `garmin_login` line 490, `get_garmin_client` line 501, `garmin_creds` line 66). The module-scope `pirate_garmin` imports are at lines 738-739. Routers (`pifitness/auth/router.py`, `pifitness/config/router.py`) contain only HTTP handlers — no login logic. `pifitness/integrations/` does NOT exist physically; BRIEF §5 proposes `integrations/garmin/` + `integrations/spotify/`, but PLAN §4 marks `3.integrations` SUPERSEDED (logic stays in `service_logins.py`) and `3.worker` is TODO. Spotify and Garmin share the same "reuse-first" shape (DB-backed token/session → return `{"client", "token", "token_age}` dict). No documented repo-standard integration interface exists yet.

**Phase B — module inventory:** LOC measured (auth.py 678, browser_login.py 332, cli.py 238, client.py 100, endpoints.py 393, __init__.py 3). External deps: httpx (auth, client), playwright (browser_login), typer (cli). No subprocess/shell-out to the CLI anywhere in the repo or deploy script. `browser_login` is used transitively (auth.py imports it; GarminClient.AuthManager headless fallback). CLI is unused in prod.

**pi5_deploy.sh:** lines 154-160 install `pirate-garmin_clone` (editable) + Playwright Chromium. `.pth` points to `pirate-garmin_clone/src`.

Now producing the plan.

# Plan: retire pirate-garmin_clone without losing functionality

## Executive summary

`pirate_garmin` is live, not dead code (report §Executive summary). It serves two production paths: the static FastAPI `POST /api/auth/garmin/login` → `service_logins.garmin_login` → `pirate_garmin_login` → `GarminClient.from_credentials`, and the dynamic DB-backed task registry (report §Classification table, §Integration inventory) with 25 DB rows across `api_services.api_service_list`, `tasks.vw_task_info`, and `tasks.vw_task_execution` referencing `pirate_garmin_login`, `get_garmin_client`, and `extract_pirate_universal`. Only `render_endpoint` is import-only/dead (report §Classification table).

This plan **preserves and relocates** the library — it does not delete functionality. All symbol names referenced in DB rows (`pirate_garmin_login`, `get_garmin_client`, `extract_pirate_universal`, plus `resolve_endpoint` via `get_pirate_data`) remain unchanged in `service_logins.py` and `json_extractors.py`; only the physical location of the `pirate_garmin` package changes. Therefore **DB impact is none** for every phase below (the report's 13-step DELETE plan, §"What would need to change," is explicitly not this plan).

Recommendation: **Option D now** (vendor/move to `backend/_vendor/pirate_garmin/`, a pure locational fix with zero import changes), then **Option A later** (move into `pifitness/integrations/garmin/` once the human establishes the `integrations/` pattern per BRIEF §5). Deletion of `pirate-garmin_clone/` is a standalone final phase, only after the vendored copy is proven green.

## Phase A findings — repo-standard login pattern

**Current pattern (as it exists today).** Login logic is concentrated in a single 820-line file, `backend_functions/service_logins.py`, which holds both Spotify and Garmin auth: Spotify (`spotify_creds` l.44, `get_spotify_token` l.240, `get_spotify_client` l.81, rate-limit handling l.155+) and Garmin (`garmin_creds` l.66, deprecated `garmin_login` l.490, `pirate_garmin_login` l.742, `get_garmin_client` l.501). The two module-scope `pirate_garmin` imports live at `service_logins.py:738-739` (report §Raw evidence, Phase 1). The routers are thin HTTP handlers only — `pifitness/auth/router.py` imports `garmin_login` (l.24) and calls it in `trigger_garmin_login` (l.292) and `garmin_login()` again in `test_garmin_auth` (l.359) and `auth_health` (l.415); `pifitness/config/router.py` has no login logic at all (only Mapbox config). So today the "pattern" is a convention — login code lives in `service_logins.py` — not a documented integration interface.

**Target pattern (per BRIEF + migration state).** BRIEF §5 proposes domain-sliced `integrations/garmin/` and `integrations/spotify/` slices. However, `backend/pifitness/integrations/` does not physically exist today, and PLAN §4 marks `3.integrations` SUPERSEDED ("Garmin/Spotify login logic remains in `service_logins.py`") while `3.worker` is TODO. The target would be per-service integration modules under `pifitness/integrations/` with a shared interface (DB-backed token/session reuse → return `{"client", "token", "token_age}` dict — the same shape Spotify and Garmin already follow; note `pirate_garmin_login`'s own docstring says "Uses the same DB-backed session pattern as Spotify"), with `service_logins.py` reduced to a thin re-export shim or retired.

**The gap.** There is no repo-standard integration methodology right now: no `integrations/` package, no shared interface, no per-service login modules — just one monolithic `service_logins.py`. Adopting Option A (`pifitness/integrations/garmin/`) now would require establishing that pattern first (create the package, extract per-service login, define the interface, update `service_logins.py` and `json_extractors.py` imports), which is more structural change than this relocation task warrants.

**One sentence.** No, a repo-standard login methodology does not exist yet — `service_logins.py` is a convention, not an interface; establishing one would require creating `pifitness/integrations/`, extracting per-service login modules (Garmin + Spotify) behind a common `{"client","token","token_age}` return contract, and routing `service_logins.py`/`json_extractors.py` through it before vendoring into `integrations/garmin/`.

## Phase B findings — module inventory

| module | LOC | external deps | used by (from report) | keep/drop | rationale |
|---|---|---|---|---|---|
| `__init__.py` | 3 | none | package marker | keep | required for `pirate_garmin` package |
| `auth.py` | 678 | `httpx`; imports `browser_login` at module scope | `GarminAuthError`, `MissingCredentialsError` (LIVE-PROD, caught at `service_logins.py:811`; report §Integration inventory) | keep | exception classes are production-caught; module also supplies `ProfileBundle` used by `endpoints.py` |
| `client.py` | 100 | `httpx`; imports from `auth` | `GarminClient.from_credentials` (LIVE-PROD; report §Integration inventory) | keep | core live symbol; instantiated by `pirate_garmin_login` |
| `browser_login.py` | 332 | `playwright` | imported by `auth.py:13`; used transitively by `GarminClient.AuthManager.ensure_authenticated()` headless-fallback inside `pirate_garmin_login` | keep | part of the live browser-login fallback path; not called directly by main repo but transitively reachable |
| `cli.py` | 238 | `typer` | **not invoked by main repo; no `subprocess.run` calls; no deploy shell-outs** (searched repo) | **drop candidate** | CLI only; no production caller. Vendoring it pulls `typer` for no prod value — human decision (see §Open questions) |
| `endpoints.py` | 393 | imports from `auth` & `client` | `resolve_endpoint` LIVE-DYNAMIC (`json_extractors.py:488`; report §Integration inventory); `render_endpoint` IMPORT-ONLY (`json_extractors.py:485`, never called) | **keep both** | `resolve_endpoint` is live; `render_endpoint` is import-only — vendoring "functionally identical" keeps it; dropping is a trim decision for the human |

Package-level external deps: `httpx`, `playwright`, `typer` (CLI only). No `garminconnect`, no websockets, no Android-emulation libraries inside `pirate_garmin` itself (note: `service_logins.py` imports `garminconnect` separately, but that's not part of the vendored package).

**CLI-specific check:** nothing in the main repo invokes `cli.py`; no `subprocess.run` to it; `deploy/scripts/pi5_deploy.sh` does not shell out to it (only `pip install -e` + `python -m playwright install chromium` at lines 154-160). Confirmed via repo-wide search — zero matches for `pirate-garmin_clone`/`pirate_garmin_clone` as a subprocess target.

## Options comparison

Scoring key: alignment with how Spotify/auth/config are structured *today*; sequencing safety vs. `3.worker` (which moves `backend_tasks.py` → `pifitness/worker/`, per PLAN §4 and does not touch `service_logins.py`); deploy impact (`pi5_deploy.sh`, .pth, Pi reinstall); test impact; reversibility; six-month failure mode.

| Option | Alignment with repo pattern | Sequencing vs. 3.worker | Deploy impact | Test impact | Reversibility | Six-month failure mode |
|---|---|---|---|---|---|---|
| **A** `pifitness/integrations/garmin/` | Low — presupposes `integrations/` pattern that doesn't exist (BRIEF intent only; `3.integrations` superseded in PLAN §4) | Safe before or after; independent | `pi5_deploy.sh` must install new location; rides on `pifitness` editable install (no new `.pth` if under `pifitness/`); Pi reinstall of backend package | `service_logins.py:738-739` + `json_extractors.py:485` import changes (2 files); `test_empty_queue_noop.py` unaffected (monkeypatches `service_logins`/`json_extractors`, not `pirate_garmin`); clone-internal tests relocate | Medium — import churn across 2 backend files + relocation | Broken `from pifitness.integrations.garmin...` imports if `pifitness` package not refreshed on Pi; silent import of stale clone if `.pth` not updated |
| **B** `pifitness/auth/garmin/` | Moderate — colocates with `pifitness/auth/` (exists today with router only, no login logic) but live symbols live in `backend_functions/service_logins.py`, not `pifitness/auth` | Safe before or after | Same as A | Same import changes as A | Medium | Stale `auth/` module diverging from `service_logins.py`; confusing two Garmin homes |
| **C** `pifitness/worker/garmin/` | Low — `worker/` doesn't exist yet (`3.worker` TODO); `get_garmin_client`/`extract_pirate_universal` live in `backend_functions`, not worker | **Should come after 3.worker** ideally; `worker/` absent today | Same as A | Same as A | Medium | References `worker/` before the worker slice exists; dead-end if `3.worker` scopes differently |
| **D** `backend/_vendor/pirate_garmin/` (this plan) | Low-Med — no `_vendor/` pattern exists today, but it's a pure locational fix: keeps the `pirate_garmin` import name and internal structure intact | Safe before or after `3.worker`; fully independent | `pi5_deploy.sh` lines 154-160 retargeted to new location + new/updated `.pth` pointing to `backend/_vendor/`; Pi needs the vendored path on sys.path (deploy step) | **Zero import changes** to `service_logins.py`/`json_extractors.py` (still `from pirate_garmin...`); `test_empty_queue_noop.py` unaffected; clone-internal tests move with the package | **High** — `git mv` folder, revert by moving back | Stale `.pth` pointing to deleted `pirate-garmin_clone/` breaks `import pirate_garmin` everywhere if deploy `.pth` step missed |
| **E** Keep at root, rename folder | Low — keeps the root-level clone problem; rename only | Safe before or after | `pip install -e <newname>` + `.pth` update; still a root-level clone | Zero (import name `pirate_garmin` unchanged if `src/pirate_garmin` kept) | High | Root-level alarming folder persists; doesn't solve locational concern |

## Recommendation

**None are cleanly dominant; recommend "do D now, plan A later."**

Option D (`backend/_vendor/pirate_garmin/`) is the safest immediate step: it is a pure locational fix that keeps the `pirate_garmin` top-level import name working, requires **zero import changes** to `service_logins.py` or `json_extractors.py`, touches none of the 25 DB rows, is safe before or after `3.worker`, and is fully reversible via `git reset --hard`. Its downside — needing a `.pth`/deploy entry for `backend/_vendor/` — is minor and explicit.

Option A (`pifitness/integrations/garmin/`) is BRIEF's intent but the pattern doesn't exist yet (`pifitness/integrations/` absent; `3.integrations` superseded). Choosing A now would mean establishing the integration pattern first, which is a larger structural decision than this task warrants. If the human later commits to `integrations/`, the pirate_garmin code can move there (Option A) as a follow-up requiring import updates in `service_logins.py:738-739` and `json_extractors.py:485`.

Do not bundle deletion with vendoring: deletion of `pirate-garmin_clone/` is the standalone last phase, only after tests prove the vendored copy works (report §Test impact shows 4 main-repo tests + clone-internal tests touch this code).

## Execution plan (PG-0 through PG-5)

Verification gates per phase (PLAN.md §8 / block-based process): `compileall`, `pytest -q` (targeted then full), `import backend.main`, OpenAPI snapshot unchanged, `scripts/repo_audit all` + `check` exit 0. Each phase is an independent commit ending tests-green, reversible via `git reset --hard <phase-start-sha>`. Branch naming: `reorg/pg-<n>-<slug>` (BRIEF §2 git discipline). No `git push`/`reset --hard` on shared branches per rules — rollback uses the phase-start SHA recorded at phase start.

**PG-0 — Capture state + rollback point**
- Files changed: none (documentation only; record a baseline commit SHA).
- Actions:
  1. Record current HEAD SHA as `PG-0_START`.
  2. Capture read-only evidence: current `pirate_garmin` import graph (`service_logins.py:738-739`, `json_extractors.py:485`), `pi5_deploy.sh:154-160`, `.pth` target (`_editable_impl_pirate_garmin.pth → pirate-garmin_clone/src`), the 25 DB row values (read-only probe, report §DB impact), and full test baseline.
  3. Confirm `pytest -q` green at baseline (report §Test baseline: 210 passed).
- Verification: baseline suite green; evidence log written to `docs/reorg/LOG.md`.
- Rollback: none needed (pre-change); `PG-0_START` is the rollback anchor for all later phases.
- Risk: low — read-only capture.
- Test impact: none.
- DB impact: none.

**PG-1 — Vendor/move `pirate_garmin` into `backend/_vendor/pirate_garmin/`**
- Files changed: `pirate-garmin_clone/src/pirate_garmin/*` → `backend/_vendor/pirate_garmin/*` (via `git mv`); clone-internal tests `pirate-garmin_clone/tests/*` → `backend/_vendor/pirate_garmin/tests/*`.
- Actions:
  1. `git mv pirate-garmin_clone/src/pirate_garmin backend/_vendor/pirate_garmin` (Windows: case-only rename two steps if needed; BRIEF §2).
  2. `git mv pirate-garmin_clone/tests backend/_vendor/pirate_garmin/tests`.
  3. Leave non-package clone files (`README.md`, `LICENSE`, `docs/`, `pyproject.toml`, `uv.lock`) in `pirate-garmin_clone/` for now.
  4. Confirm `import pirate_garmin` resolves from `backend/_vendor/` (via existing `.pth` still pointing to old location for now — PG-2 fixes `.pth`/deploy; do a direct path-insert smoke test in this phase if needed).
- Verification: `compileall backend/_vendor/pirate_garmin`; clone-internal tests under new location pass (`pytest backend/_vendor/pirate_garmin/tests`); main-repo `pytest tests/test_empty_queue_noop.py` green (it doesn't import `pirate_garmin` directly, but confirms the consumer path still works via `service_logins`).
- Rollback: `git reset --hard PG-0_START`.
- Risk: low — pure move, no import changes.
- Test impact: clone-internal tests relocated (still 3 test modules); main-repo 4 tests unaffected.
- DB impact: none.

**PG-2 — Repoint deploy wiring (`.pth` + `pi5_deploy.sh`)**
- Files changed: `deploy/scripts/pi5_deploy.sh` (lines 154-160).
- Actions:
  1. Change `pip install -e pirate-garmin_clone` → install/point to `backend/_vendor/pirate_garmin` (create/refresh `.pth` → `backend/_vendor/` or equivalent editable install so `import pirate_garmin` resolves to the new location).
  2. Keep `python -m playwright install chromium` (browser_login dep; report §Raw evidence pi5_deploy.sh:158-160).
  3. Validate shell syntax (`bash -n`).
- Verification: `bash -n deploy/scripts/pi5_deploy.sh` exit 0; smoke `import pirate_garmin` from a clean interpreter using the new `.pth`/path.
- Rollback: `git reset --hard PG-1_START`.
- Risk: med — if `.pth`/deploy path is wrong, `import pirate_garmin` breaks production and dynamic tasks.
- Test impact: none.
- DB impact: none.

**PG-3 — Verify full suite green with vendored copy**
- Files changed: none (verification only).
- Actions:
  1. Full `pytest -q` (incl. `test_empty_queue_noop.py` 4 tests and relocated clone tests).
  2. `compileall -q backend backend_functions agents scripts`.
  3. `import backend.main` + OpenAPI snapshot diff (must be empty).
  4. `scripts/repo_audit all` + `check` exit 0.
  5. Grep for stale `pirate-garmin_clone/src` references (only `pi5_deploy.sh` new target and `.pth` should reference the new location).
- Verification: all green, OpenAPI unchanged, no stale root-clone import references.
- Rollback: `git reset --hard PG-2_START`.
- Risk: low.
- Test impact: full suite (210+) green.
- DB impact: none.

**PG-4 — (Optional human gate) Confirm trim decisions**
- Human decides: drop `cli.py`? drop `render_endpoint`? Keep vendored copy functionally identical? (see §Open questions). If trim approved, execute as a small follow-up commit; otherwise skip.
- Verification: suite still green after any trim.
- Rollback: `git reset --hard PG-3_START`.
- Risk: med if trimming removes a symbol something imports (re-verify imports).
- DB impact: none.

**PG-5 — Delete `pirate-garmin_clone/`, remove `.pth`/deploy lines, final verify** (standalone deletion phase — NOT bundled with earlier phases)
- Files changed: `pirate-garmin_clone/` (delete entire folder via `git rm -r`), `deploy/scripts/pi5_deploy.sh` (remove the now-obsolete install lines — only if the new location is installed via the main backend package; otherwise keep the retargeted lines from PG-2), `.venv/Lib/site-packages/_editable_impl_pirate_garmin.pth` + `pirate_garmin-0.1.0.dist-info` (venv artifact, not repo-tracked; remove via deploy/cleanup).
- Actions:
  1. Confirm PG-3 suite green (gate: do not proceed if red).
  2. `git rm -r pirate-garmin_clone/`.
  3. Remove `pi5_deploy.sh` lines that referenced the old clone install (only the lines superseded by PG-2's new target; keep any lines still needed for the new location).
  4. Remove venv `.pth`/dist-info for the old clone.
  5. Full `pytest -q`, `compileall`, OpenAPI diff, `repo_audit all` + `check`.
- Verification: full suite green; `pirate-garmin_clone/` absent; no repo reference to old clone path; `import pirate_garmin` resolves from `backend/_vendor/`.
- Rollback: `git reset --hard PG-4_START` (restores clone folder + deploy lines).
- Risk: med — deletion is one-way from the repo but reversible via the phase-start SHA; the only production risk is a missed `.pth`/deploy update (caught by PG-5 verification).
- Test impact: clone-internal tests deleted with the folder (they tested the now-relocated library; already relocated in PG-1). Main-repo 4 tests unaffected.
- DB impact: none (all 25 DB rows still reference `service_logins`/`json_extractors` symbols, which are unchanged; see report §DB impact — those rows matter only for the separate full-DELETE plan, not this relocation).

## Open questions for the human

1. **Which of the 25 DB rows are updatable via the app's UI vs. requiring direct SQL?** The audit (report §DB impact) lists the tables/values but not which are UI-reachable. Note: this relocation plan deliberately does not touch DB rows at all; they matter only if you later pursue the full 13-step deletion.
2. **Should the vendored copy be functionally identical to the current library, or trimmed?** Flagged in inventory: `cli.py` (no prod caller; pulls `typer`) and `render_endpoint` (import-only, never called) are trim candidates. Recommendation: keep functionally identical in PG-1 to minimize risk, trim later only if you approve.
3. **Vendor now (D) or after `3.worker`?** D is independent of `3.worker` (which moves `backend_tasks.py`, not `service_logins.py`). A (`pifitness/integrations/garmin/`) could be paired with or follow `3.worker` if you establish the `integrations/` pattern then. Your call on sequencing relative to `3.worker`.

## Confidence assessment

- **Import graph: high** — all production `pirate_garmin` references are the two module-scope imports in `service_logins.py:738-739` and the function-scope import in `json_extractors.py:485` (report §Raw evidence, Phase 1); zero subprocess/shell-out to the CLI.
- **Dynamic dispatch: high** — 25 DB rows verified in DEV (report §DB impact, Phase 4); none of their referenced symbol names change under this plan.
- **Test coverage: high** — 4 main-repo tests in `test_empty_queue_noop.py` (report §Test impact) plus 3 clone-internal test modules; all accounted for in the plan.
- **Overall: confident** — this is a relocation, not deletion. The plan preserves every production symbol and DB row; risk is confined to the `.pth`/deploy pathing (PG-2/PG-5), which is explicitly verified and reversible.

STOP — plan produced, no files modified, no mutations executed. Awaiting your `go phase N` or answers to the open questions before any execution.