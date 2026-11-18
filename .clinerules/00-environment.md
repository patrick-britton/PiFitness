# PiFitness Environment

Snapshot last verified: 2026-11-02. These are supplied facts, not a guarantee. Re-check a fact only when the current step depends on it.

## Machine and stack

- OS: Windows PC, Windows PowerShell 5.1.
- Repository: `C:\users\rickb\vsprojects\pifitness`.
- Backend: FastAPI in `backend/` (`backend/main.py`).
- Frontend: Next.js in `frontend/pifitness`. Node and npm are installed.
- DEV database: Docker container `pifitness-dev-db`, host `localhost`, port `15432`, database `pifitness_dev`, user `dev`.
- `backend/.env` holds the DEV settings: `PG_HOST=localhost`, `PG_PORT=15432`, `PG_DB=pifitness_dev`, `PG_USER=dev`, `PGSSLMODE=disable`, `BOX=local`.

Never print or log secret values from `.env` files. Read only the one non-secret setting you need.

## Command forms

Run from the repository root unless a working directory is stated.

**Interpreter (PY):** `C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe`. Never use plain `python`, `python3`, or another environment.

**Workflow commands that start with `python` run with PY.** Workflows write `python tools/workflow/fd.py hot <id> --brief`. You run:

```powershell
& 'C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe' tools/workflow/fd.py hot <id> --brief
$LASTEXITCODE
```

This applies to `fd.py`, `lineage.py`, `lint_comments.py`, `check_docs.py`, and `tools/verify.py`.

| Purpose | Command |
|---|---|
| Start backend | `& PY -m uvicorn main:app --app-dir backend --port 8000` |
| Backend health | `Invoke-RestMethod http://localhost:8000/api/health` |
| Start frontend (cwd `frontend\pifitness`) | `node node_modules\.bin\next dev -p 3000` |
| Frontend type check (cwd `frontend\pifitness`) | `npx tsc --noEmit` |
| Backend tests | `& PY -m pytest tests/ -m "not slow and not integration and not live" -q` |
| Review: comment lint | `& PY tools/lint_comments.py` |
| Review: doc citations | `& PY tools/check_docs.py cites` |
| Lineage | `& PY tools/data_dictionary/lineage.py describe\|search\|trace <target>` |
| Fast DEV DB reset | `& PY tools/db/dev_reset.py` |
| Refresh DEV context (human) | `powershell -ExecutionPolicy Bypass -File tools/refresh_dev.ps1 [-SyncPrd]` |

`lineage.py build` is run by the human only (or automated via `tools/refresh_dev.ps1`). Tests live under `tests/`.

## Architectural Principles ("Holy Grail" Documents)

Reference these documents according to the surface being designed, planned, or executed:

- **System Architecture & Flow:** [`docs/ARCHITECTURE.md`](file:///C:/users/rickb/vsprojects/pifitness/docs/ARCHITECTURE.md) — layers, FastAPI routers, Next.js components, and import boundaries.
- **Hardware & Performance:** [`docs/PI_SPECS.md`](file:///C:/users/rickb/vsprojects/pifitness/docs/PI_SPECS.md) — Raspberry Pi 5 hardware profile, NVMe SSD bandwidth, and compute constraints.
- **Frontend & UI:** [`docs/UI_PRINCIPLES.md`](file:///C:/users/rickb/vsprojects/pifitness/docs/UI_PRINCIPLES.md) — mobile-first ergonomics, thumb zones, 44px touch targets, safe area insets, loading/error states.
- **Database & Schemas:** [`docs/DATABASE_PRINCIPLES.md`](file:///C:/users/rickb/vsprojects/pifitness/docs/DATABASE_PRINCIPLES.md) — DEV vs PRD isolation, domain schemas, keyset pagination, parameterization.
- **APIs & Workers:** [`docs/INTEGRATION_PRINCIPLES.md`](file:///C:/users/rickb/vsprojects/pifitness/docs/INTEGRATION_PRINCIPLES.md) — Garmin 3-layer anti-429 token defense, Spotify OAuth, and worker execution queues.

## Known issues (from the snapshot; verify before relying on them)

- `npm run test` (Vitest) failed to start because of a Vite `configLoader` issue.
- `pytest` can exit 1 after tests pass because Windows temp-directory teardown raises `PermissionError` (`WinError 5`). A non-zero exit is still a failed command; report the teardown issue separately.
- `tools/db/sync-prod-to-dev.ps1` has no dry-run switch.
- `run_commands` hard-kills calls at 30,000 ms; `Start-Job` state does not persist between calls.

## Execution guardrails

- Use the DEV database only. Any database connection must target `localhost` or a verified loopback address for the DEV service. Never connect to PRD, `pfpi`, or any remote database host.
- Before any database write, inspect the effective connection target and confirm it is local DEV. Do not rely on a remembered `.env` value if the command can override it.
- Never execute `tools/db/sync-prod-to-dev.ps1`.
- Never SSH, SCP, or otherwise access the Pi or a production host. Pi-side checks are human gates.
- Never run deployment, migration, clone, restore, or destructive commands against production. A task that names a deployment script authorizes editing that file only, never running it against a live host.

## Slow commands

Launcher `tools\workflow\run_long.ps1`, poller `tools\workflow\poll_long.ps1`, presets in `tools\workflow\long_commands.json`. Procedure: `.clinerules/01-terminal-use.md`.
