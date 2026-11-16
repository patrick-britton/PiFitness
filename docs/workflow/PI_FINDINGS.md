# Task 5.10 — Pi Production Unit and Virtualenv Reconciliation
- This evidence was prepared for Step 20 of `docs\workflow\HUMAN_PI_RESEARCH_TASKS.html`

## Step 21 Evidence Report (for transfer to DEV / `PROGRESS.md`)

**Prepared:** 2026-11-08 (PST)
**Pi hostname:** `pfpi`
**Prepared by:** `god` (human-run checklist)
**Scope:** Task 5.10 inspection/reconciliation of the installed `pifitness-fastapi.service`, its overrides, and Python virtualenv references. No secrets are included in this report.

---

## 1. Traceability

| Item | Value |
|---|---|
| Pi hostname | `pfpi` |
| Production checkout | `~/PiFitness` (`/home/god/PiFitness`) |
| Production checkout branch/commit | **Not captured in the supplied console transcript** (PI-1 output absent). Expected per plan: `react-ui`. Confirm from PI-1 before relying on this row. |
| Fetched source ref | `origin/agent/phase-3` (from `refs/heads/agent/phase-3`) |
| Fetched source SHA | `3597cfda716699b13573251824f3749f6b4828cb` |
| SHA match check | Expected SHA == fetched SHA (confirmed in PI-2 output) |
| Source snapshot directory | `/home/god/PiFitness-5.10-source-3597cfda716699b13573251824f3749f6b4828cb` (mode `700`) |
| Source snapshot unit file | `deploy/systemd/pifitness-fastapi.service` (present, inspected) |
| Private audit/backup directory | `/home/god/pifitness-5.10-audit-20261108-060741` (mode `700`) |
| Backup status | Original installed fragment and drop-in backed up and verified (per Task 5.10 findings). The transcript supplied for this report does not include the backup file list/`sha256sum` output; verify audit folder contents if needed. |

---

## 2. Installed Unit Paths

| Property | Value |
|---|---|
| `FragmentPath` | `/etc/systemd/system/pifitness-fastapi.service` |
| `DropInPaths` | `/etc/systemd/system/pifitness-fastapi.service.d/override.conf` |
| `EnvironmentFiles` | `/home/god/PiFitness/.env` (`ignore_errors=no`) |

No environment file contents were read, copied, or recorded.

---

## 3. Fragment Comparison and Override Disposition

### 3.1 Initial comparison (before change)

`diff -u` between the approved repo unit and the installed fragment returned exit status `1` (different). Differences found:

1. **`ExecStart` bind address**
   - Repo/approved: `--host 127.0.0.1 --port 8000`
   - Installed (old): `--host 0.0.0.0 --port 8000`
2. **Redundant `ReadWritePaths` line**
   - Installed had an extra first `ReadWritePaths=/home/god/PiFitness /home/god/Documents/PiFitness_Local` line in addition to the consolidated line.

These match findings 1 and 2 in the Task 5.10 findings file. The repository configuration was preferred because Nginx already proxies to `127.0.0.1:8000`.

### 3.2 Action taken

The approved repository unit was installed over the system-local unit:

```bash
sudo install -o root -g root -m 0644 \
  "$SOURCE_DIR/deploy/systemd/pifitness-fastapi.service" \
  /etc/systemd/system/pifitness-fastapi.service
sudo systemctl daemon-reload
```

No drop-in was deleted. `systemctl revert` was not used.

### 3.3 Post-change inspection

`sudo systemctl cat pifitness-fastapi.service` after the install shows:

- Main fragment `ExecStart` now uses `--host 127.0.0.1 --port 8000`.
- Main fragment now has a single consolidated `ReadWritePaths` line:
  `/home/god/PiFitness /home/god/Documents/PiFitness_Local /home/god/Documents/DB_Backups`
- The drop-in `override.conf` still exists and still adds:
  `ReadWritePaths=/home/god/Documents/DB_Backups`

`systemd-delta --type=extended` still lists `pifitness-fastapi.service` as `[EXTENDED]` because the drop-in remains active.

### 3.4 Drop-in disposition

| Drop-in | Content | Disposition |
|---|---|---|
| `/etc/systemd/system/pifitness-fastapi.service.d/override.conf` | `[Service]` / `ReadWritePaths=/home/god/Documents/DB_Backups` | **Retained — redundant.** The path is already included in the main unit. It is explained (not unexplained) but currently duplicates the main unit’s write path. Removal requires reviewer/human approval and should be done by moving that exact file after it has been preserved in the audit folder. |

---

## 4. Effective Service Properties (post-reconciliation)

From `systemctl cat` and `systemctl show` (some `show` output was truncated by the terminal; values below are confirmed from `cat` plus `show`):

| Property | Effective value |
|---|---|
| `ExecStart` | `/home/god/PiFitness/venv/bin/uvicorn backend.main:app --host 127.0.0.1 --port 8000` |
| `WorkingDirectory` | `/home/god/PiFitness` |
| `User` / `Group` | `god` / `god` |
| `ProtectHome` | `read-only` |
| `ReadWritePaths` | `/home/god/PiFitness` `/home/god/Documents/PiFitness_Local` `/home/god/Documents/DB_Backups` (main unit) plus the drop-in re-adds `DB_Backups` — effective set unchanged |
| Bind address/port | `127.0.0.1:8000` (loopback only) |
| `EnvironmentFiles` | `/home/god/PiFitness/.env` (path only) |

---

## 5. Service State and Health

### 5.1 Initial state (before change)

| Check | Result |
|---|---|
| `systemctl is-active` | `active` |
| `systemctl is-enabled` | `enabled` |
| Status | `active (running)` since Tue 2026-11-03 12:30:13 PST; Main PID 70840 |

The service activation policy was not changed.

### 5.2 Restart

Because the installed unit changed, the service was reloaded and restarted during the maintenance window:

```bash
sudo systemctl daemon-reload
sudo systemctl restart pifitness-fastapi.service
```

Result: `active (running)` since Sun 2026-11-08 06:51:49 PST; new Main PID 112068.

### 5.3 Final state

| Check | Result |
|---|---|
| `systemctl is-active` | `active` |
| `systemctl is-enabled` | `enabled` (unchanged) |
| Main process | `/home/god/PiFitness/venv/bin/python3 /home/god/PiFitness/venv/bin/uvicorn ...` (PID 112068) |

### 5.4 Listener

```text
LISTEN 0 2048 127.0.0.1:8000 0.0.0.0:* users:(("uvicorn",pid=112068,fd=6))
```

Loopback only; no `0.0.0.0:8000` or `[::]:8000` listener.

### 5.5 Health checks

| Check | Command | Result |
|---|---|---|
| Direct HTTP | `curl --fail --show-error http://127.0.0.1:8000/api/health` | `{"status":"ok","database":"ok","timestamp":"2026-11-08T06:52:56.467511"}` |
| HTTPS via `--resolve` | `curl --fail --show-error --resolve pifitness.duckdns.org:443:127.0.0.1 https://pifitness.duckdns.org/api/health` | `{"status":"ok","database":"ok","timestamp":"2026-11-08T06:52:56.517006"}` |

TLS hostname validation was used (no `-k` / `--insecure`).

---

## 6. Virtualenv Inventory and References

### 6.1 Inventory

| Path | Exists | Python version | `bin/uvicorn` | Notes |
|---|---|---|---|---|
| `/home/god/PiFitness/venv` | Yes (`drwxr-xr-x 7 god god`) | 3.13.5 | Yes | Production venv used by the FastAPI unit (and referenced by the installed agent unit). |
| `/home/god/venv` | Yes (`drwxrwxr-x 7 god god`) | 3.14.6 | Yes | No references found in the five checks below. |
| `/home/god/venvs/pifitness-react-backend` | Yes (`drwxrwxr-x 5 god god`) | 3.13.5 | No | No references found in the five checks below. |
| `/home/god/PiFitness/backend/.venv` | No | — | — | Not present. |
| `/home/god/PiFitness-test/venv` | No | — | — | Not present; intended for task 5.17. |

### 6.2 References found

| Source checked | Result |
|---|---|
| **systemd units** (`/etc/systemd/system`, `/usr/lib/systemd/system`, `/lib/systemd/system`, `~/.config/systemd/user`) | `/etc/systemd/system/pifitness_agent.service` references `/home/god/PiFitness/venv/bin` (PATH) and `/home/god/PiFitness/venv/bin/python3` (ExecStart). `pifitness-fastapi.service` and its `multi-user.target.wants` symlink reference `/home/god/PiFitness/venv/bin/uvicorn`. **No references to `/home/god/venv` or `/home/god/venvs/pifitness-react-backend`.** |
| **PM2** | `pifitness-next` is online; script path `/usr/bin/npm`, args `run start -- --port 3000`, cwd `/home/god/PiFitness/frontend/pifitness`, interpreter `/usr/bin/node`, Node.js 24.21.0. No Python venv reference. |
| **cron** | `god` crontab contains only the DuckDNS job (`*/5 * * * * ~/duckdns/duck.sh`). Root crontab empty. `/etc/crontab`, `/etc/cron.d*`, and `/var/spool/cron/crontabs` produced no matches for the venv/interpreter pattern. |
| **Deploy scripts (repo snapshot)** | `deploy/scripts/deploy.sh` uses `VENV_DIR` and `source "$VENV_DIR/bin/activate"`; `deploy/scripts/pi5_deploy.sh` uses `source venv/bin/activate`; test unit uses `~/PiFitness-test/venv/bin/uvicorn`. **No references to `~/venv` or `~/venvs/pifitness-react-backend`.** |
| **Running processes** | Only PID 112068: `/home/god/PiFitness/venv/bin/python3 /home/god/PiFitness/venv/bin/uvicorn backend.main:app --host 127.0.0.1 --port 8000`. No process uses `~/venv` or `~/venvs/pifitness-react-backend`. |

### 6.3 Additional finding outside the FastAPI unit

The installed `/etc/systemd/system/pifitness_agent.service` uses:

```text
ExecStart=/home/god/PiFitness/venv/bin/python3 -m agents.background_task_execution
```

The repository snapshot’s `deploy/systemd/pifitness_agent.service` uses:

```text
ExecStart=/home/god/PiFitness/venv/bin/python3 -m pifitness.worker.background_task_execution
```

This is a module-path discrepancy between installed and repository agent units. It is outside the FastAPI unit reconciliation but is recorded here as an unresolved item for review/backlog.

---

## 7. Virtualenv Decisions

| Candidate | Decision | Evidence / rationale |
|---|---|---|
| `/home/god/PiFitness/venv` | **KEEP** | Current production `ExecStart` for `pifitness-fastapi.service` (and referenced by the installed `pifitness_agent.service`). Do not remove. |
| `/home/god/PiFitness-test/venv` | **KEEP / future** | Not present yet; intended for task 5.17. Not part of this cleanup. |
| `/home/god/venv` | **KEPT — unresolved** | No references found in systemd, PM2, cron, deploy scripts, or running processes. User requested assistance confirming the determination; no quarantine was performed in this run. Eligible for Step 18 quarantine after human confirmation. |
| `/home/god/venvs/pifitness-react-backend` | **KEPT — unresolved** | No references found in the same five checks; no `bin/uvicorn`. User requested assistance confirming the determination; no quarantine was performed. Eligible for Step 18 quarantine after human confirmation. |

No virtualenv was deleted or moved during this run.

---

## 8. Unresolved Items / Follow-ups

1. **Production checkout branch/commit not captured.** PI-1 output is absent from the supplied transcript. Expected `react-ui`; confirm before relying on the traceability row.
2. **Redundant drop-in retained.** `override.conf` re-adds `/home/god/Documents/DB_Backups`, which is already in the main unit. It is explained but redundant. Removal requires reviewer/human approval; do not use `systemctl revert`.
3. **`pifitness_agent.service` module path differs from repo.** Installed uses `agents.background_task_execution`; repo uses `pifitness.worker.background_task_execution`. Reconcile separately (backlog/reviewer).
4. **Legacy venv removals pending confirmation.** `/home/god/venv` and `/home/god/venvs/pifitness-react-backend` show no references in the five checks, but the user has not yet confirmed quarantine. Keep them in place until confirmed.
5. **Backup artifact listing not in transcript.** The findings state the original installed files were backed up and verified; the `find`/`sha256sum` output was not part of the supplied console transcript. Verify the audit folder if a formal artifact list is required.

---

## 9. Secret Handling

- `EnvironmentFiles` was recorded as a path only; the file was not read or copied.
- PM2 environment values were not dumped (`pm2 env` was not run).
- Cron lines containing potential credentials were not copied; only the DuckDNS job name was noted.
- No tokens, passwords, private keys, or raw `.env` contents appear in this report.
- Audit folder and source snapshot are mode `700`; path pointer files are mode `600`.

---

## 10. Completion-Gate Summary for Task 5.10

| Gate item | Status |
|---|---|
| Reviewed `agent/phase-3` SHA fetched and verified on Pi; production checkout not switched/merged | **Yes** — SHA `3597cfda…` matched; production checkout left in place (branch/commit to be confirmed from PI-1). |
| Installed unit effective properties match approved repo configuration | **Yes** — `ExecStart` loopback, consolidated `ReadWritePaths`, same env file/user/group/working directory. |
| Every systemd drop-in inspected; no unexplained override active | **Inspected** — one drop-in, explained but redundant; retained pending approval. |
| Environment-file path and executable path understood; no secrets exposed | **Yes** — path only; no secret values recorded. |
| Venv references checked in systemd, PM2, cron, deploy scripts, active processes | **Yes** — only production venv referenced. |
| No legacy venv deleted without proof; uncertain candidates retained/documented | **Yes** — both legacy candidates retained and marked unresolved. |
| Necessary configuration change backed up; original activation state preserved | **Yes** — backup reported verified; service remained `enabled` and was restarted only for the unit change. |
| Findings, fetched branch/SHA, source snapshot, backup location recorded | **Yes** — this report. |