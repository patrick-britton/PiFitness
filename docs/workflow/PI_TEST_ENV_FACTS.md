# Pi TEST Environment Facts

> **Status:** Template — replace every `TODO` with a verified value or an explicit `Not applicable` / `Not verified` result before asking Cline to use this document.
>
> **Purpose:** This is the concise, authoritative context document for engineering the Pi-hosted TEST harness. It records verified machine-specific facts and indexes the raw discovery evidence. It is not a place to store credentials or duplicate full command output.

## 1. Discovery provenance

| Field | Verified value |
|---|---|
| Discovery date/time (Pi local time) | TODO |
| Discovery directory on Pi | `/home/god/pifitness-test-discovery-20261110-054732` |
| Person who ran discovery | TODO |
| Pi hostname | TODO |
| Pi OS / release / architecture | TODO |
| Production checkout path | `/home/god/PiFitness` (verify) |
| Production checkout branch | TODO |
| Production checkout commit | TODO |
| Repository commit used to prepare this document | TODO |

Discovery outputs were collected read-only. State any exception here: TODO / None known.

## 2. Host paths and filesystem

| Purpose | Exact path | Owner:group | Mode / permissions | Verified notes |
|---|---|---|---|---|
| Production checkout | TODO | TODO | TODO | |
| Proposed disposable TEST checkout | TODO | TODO | TODO | |
| Persistent TEST configuration directory | TODO | TODO | TODO | |
| Persistent TEST environment file | TODO | TODO | TODO | Do not record its contents or secrets |
| Harness/script staging directory | TODO | TODO | TODO | |
| Persistent evidence/log directory | TODO | TODO | TODO | |
| Database dump / backup location | TODO | TODO | TODO | Include available disk space and retention considerations |

Filesystem capacity and free space at discovery time: TODO.

## 3. PostgreSQL — actual PRD and TEST database configuration

Record the actual running cluster, not values inferred from repository defaults or old notes.

| Field | Verified value |
|---|---|
| PostgreSQL server version | TODO |
| Cluster/service identity | TODO |
| Listening address(es) | TODO |
| TCP port | TODO |
| Unix socket directory (if relevant) | TODO |
| PRD database name | TODO |
| PRD database owner | TODO |
| Application connection role (role name only) | TODO |
| TEST database name reserved for the harness | `pifitness_test` (confirm availability and naming) |
| TEST connection role (role name only) | TODO |
| Authentication method used by scripts | TODO |
| Client tool versions/paths (`psql`, `pg_dump`, `pg_restore`) | TODO |
| Is a separate TEST cluster used? | TODO |
| How the application currently reaches PRD PostgreSQL | TODO |

### 3.1 Clone/restore feasibility

| Question | Verified result / evidence |
|---|---|
| Can the intended role create the TEST database? | TODO |
| Can it create a database from a template, if that is the proposed method? | TODO |
| Are there ownership/privilege constraints? | TODO |
| Are there extensions or objects that constrain cloning/restoring? | TODO |
| Is the PRD database configured with tablespaces or other non-default storage? | TODO |
| Are there active external clients that could block a template clone? | TODO |
| Chosen repeatable clone method | TODO — do not choose until evidence supports it |
| Was the method tested against a disposable database? | TODO |

Do not include passwords, connection strings containing passwords, `.pgpass` contents, tokens, or private keys in this document or its evidence files.

## 4. PostgreSQL extensions and schema facts

| Extension / feature | Installed in PRD? | Version / evidence | Actually used by application schema? |
|---|---|---|---|
| `timescaledb` | TODO | TODO | TODO |
| Other extensions | TODO | TODO | TODO |

Relevant schema facts (including whether proposed baseline tables exist): TODO.

List exact verified schema-qualified tables suitable for read-only before/after row-count checks. Do not silently omit missing tables:

| Schema-qualified table | Exists? | Why selected | Baseline query verified? |
|---|---|---|---|
| TODO | TODO | TODO | TODO |

## 5. Production services and process state

Record exact installed units and observed state. Distinguish a service from its timer and from a service triggered by a timer.

| Component | Exact unit/process name | Installed source/config path | State at discovery | Must TEST harness stop/manage it? | Evidence file |
|---|---|---|---|---|---|
| FastAPI | TODO | TODO | TODO | TODO | TODO |
| Frontend / PM2 | TODO | TODO | TODO | TODO | TODO |
| Background agent | TODO | TODO | TODO | TODO | TODO |
| Timer(s) | TODO | TODO | TODO | TODO | TODO |
| Timer-triggered service(s) | TODO | TODO | TODO | TODO | TODO |
| Other production writer(s) | TODO | TODO | TODO | TODO | TODO |

Record any difference between repository unit files and installed production unit files. Do not overwrite or normalize unexplained production differences: TODO.

## 6. Nginx and health-check routing

| Field | Verified value |
|---|---|
| Nginx active config file(s) | TODO |
| HTTPS server name | TODO |
| HTTPS listen address/port | TODO |
| Certificate path and issuer/expiry status (no private key contents) | TODO |
| Frontend upstream | TODO |
| API upstream | TODO |
| API health endpoint | TODO |
| Verified local health-check command with TLS validation | TODO |
| Does the health response identify the application environment and database? | TODO |

The TEST swap must not change/reload Nginx unless a separately reviewed plan explicitly requires it.

## 7. Runtime and OS prerequisites

Record what is installed and its version. Do not infer a package name from a missing executable; identify the package only after confirming the Pi OS package mapping.

| Capability / executable | Installed? | Version/path | Required by which harness step? | Missing package, if verified |
|---|---|---|---|---|
| `python3` | TODO | TODO | TODO | TODO |
| `python3-venv` support | TODO | TODO | TODO | TODO |
| `pip` | TODO | TODO | TODO | TODO |
| `node` | TODO | TODO | TODO | TODO |
| `npm` | TODO | TODO | TODO | TODO |
| `psql` | TODO | TODO | TODO | TODO |
| `pg_dump` | TODO | TODO | TODO | TODO |
| `pg_restore` | TODO | TODO | TODO | TODO |
| `curl` | TODO | TODO | TODO | TODO |
| `git` | TODO | TODO | TODO | TODO |
| `rsync` (if needed) | TODO | TODO | TODO | TODO |

Other verified prerequisites and package manager details: TODO.

## 8. Integrations and external side effects

Record which integrations are configured in the application and how TEST mode prevents external side effects. Do not record token values.

| Integration / background capability | Configured in PRD? | Credentials/tokens present in cloned DB or files? | TEST behavior / guard verified? | Evidence |
|---|---|---|---|---|
| Spotify | TODO | TODO | TODO | TODO |
| Garmin | TODO | TODO | TODO | TODO |
| Email / notifications | TODO | TODO | TODO | TODO |
| Scheduled/background jobs | TODO | TODO | TODO | TODO |
| Other outbound integrations | TODO | TODO | TODO | TODO |

Unresolved side-effect risks: TODO / None identified.

## 9. Evidence file index

Every file collected by the discovery exercise should be listed here. Describe what it records, which discovery section it supports, and whether it is safe to commit. Keep raw output intact; redact secrets in a copy rather than editing original evidence silently.

| File name (relative to evidence directory) | What it contains | Relevant section(s) | Sensitive values checked/redacted? | Commit? |
|---|---|---|---|---|
| TODO | TODO | TODO | TODO | TODO |

Evidence directory in the repository: `docs/workflow/pi-test-env-discovery-20261110-054732/` (adjust to actual location).

## 10. Decisions and constraints for implementation

Only record decisions supported by the discovery evidence or explicitly approved by the human operator.

1. PostgreSQL cluster/endpoint the harness must use: TODO.
2. PRD database name and immutable safety boundary: TODO.
3. TEST database name and ownership: TODO.
4. Persistent TEST `.env` path and required non-secret settings: TODO.
5. Disposable TEST checkout path: TODO.
6. Harness and evidence paths: TODO.
7. Production units/timers/processes requiring state capture and quiescence: TODO.
8. Verified health-check URL/command and expected TEST identity: TODO.
9. Required OS packages (only confirmed missing packages): TODO.
10. TimescaleDB handling: TODO — retain Timescale-specific behavior only if the discovery evidence establishes it is required.
11. Database clone/restore method: TODO.
12. Any unresolved question that blocks safe implementation: TODO.

## 11. Instructions to Cline

Before implementing or revising the Pi TEST harness:

1. Read this document and every evidence file listed above that is marked safe to commit.
2. Treat the verified values in Sections 2–8 as the Pi environment facts. Do not substitute repository defaults, historical notes, or guessed values.
3. Do not assume TimescaleDB is installed or used. Keep TimescaleDB-specific code only if the discovery evidence demonstrates it is required by the actual PRD database and chosen clone/restore method.
4. Do not install packages, change PostgreSQL configuration, grant privileges, modify production units, or change Nginx as an incidental implementation step.
5. Before asking the human to provision anything, produce exact commands and explain the purpose, expected result, and safe rollback for each operation. Name the exact package(s), exact paths, exact environment keys, ownership, permissions, and unit file contents. Never ask the human to guess what is missing.
6. Treat PRD as immutable. All destructive database operations must target only the verified TEST database and must validate the target identity immediately before execution.
7. Keep unresolved facts explicit. If a required fact is missing or contradictory, stop and ask for a targeted discovery action rather than guessing.
8. Do not copy secrets into this document, committed evidence, logs, generated reports, or task output.

## 12. Open questions / human decisions

| Question | Why it matters | Evidence already checked | Required answer / owner |
|---|---|---|---|
| TODO | TODO | TODO | TODO |

---

**Document maintenance:** Update this file whenever a verified Pi environment fact changes. Record the discovery date and source evidence. Do not treat old observations as current merely because this file is committed.
