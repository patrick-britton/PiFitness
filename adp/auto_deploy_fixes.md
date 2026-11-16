# PiFitness Autodeploy Root Cause Analysis & Remediation Scope

**Date:** 2026-11-16  
**Target:** `adp/auto_deploy_fixes.md`  
**Reference Specification:** `adp/autodeploy_objectives.md`  
**Latest Deployment Evidence:** `deploy/bootstrap.log` (commit `0eda60ac7ba9a0103eedef3f70af32bbf74623ec`)

---

## 1. Executive Summary & Root Cause Findings

During the full test deployment to `pfpi`, the deployment ran through Steps 1–9 and passed automated health checks, but manual testing on a mobile device at `https://pifitness.duckdns.org` showed the home page without the expected **"Coffee"** and **"Smoothie"** quick-log buttons.

The investigation confirmed that:
1. **The buttons exist in the commit:** In `frontend/pifitness/src/app/page.tsx` (lines 42–50), `QUICK_LOG_BUTTONS` ("Coffee" and "Smoothie") are imported and rendered.
2. **The buttons did not appear because the new frontend was neither built nor started, and the old production frontend was never stopped:**
   - `autodeploy` never executed `npm run build` (`next build`) in `/home/god/PiFitness-test/frontend/pifitness`.
   - `deploy/systemd/` contains **no systemd unit for the frontend / Next.js server** (only FastAPI backend and two timers).
   - In legacy deployments, Next.js was run on port 3000 via PM2 (`pifitness-next` running from `/home/god/PiFitness`). Because `step_stop_services.py` only halts systemd units matching `UNIT_NAME_GLOB="pifitness*"`, the old production Next.js server on port 3000 remained running throughout the entire run.
   - Nginx forwarded `/` traffic to `http://127.0.0.1:3000`, which was still serving the **old production build** ("Wow, such empty" without buttons).
3. **Browser caching was not the primary cause:** Nginx explicitly serves `/` with `Cache-Control: no-cache, no-store, must-revalidate`. The browser was accurately displaying the HTML being returned by the old server.
4. **Heartbeat interference in Step 10 (`manual_gate`):** `run_step.py`'s 15-second heartbeat timer (`... still running: manual_gate (elapsed Xs)`) continuously writes to `/dev/tty`, interrupting developer input at the prompt.
5. **Nginx registration is absent:** No step in `autodeploy` verifies or reloads Nginx configuration (`/etc/nginx/sites-available/pifitness`).

---

## 2. Detailed Technical Breakdown

### Issue 1: Missing Next.js Build Step (`npm run build`)
* **Evidence:** In `deploy/bootstrap.log` (lines 180–233), Step 3 (`dependencies`) executed:
  ```bash
  npm ci --no-audit --no-fund --include=dev
  ```
  This installed dependencies into `node_modules`, but `npm run build` (`next build`) was never called in Step 3, Step 8, or anywhere else.
* **Specification Departure:** `adp/autodeploy_objectives.md` Section 333 (line 341) explicitly states:
  > *TEST Step 8: Start TEST services — Required behavior:*  
  > `5. Run npm build`
* **Root Cause:** In the transition to `autodeploy`, the compilation step for Next.js was omitted from `step_start_services.py` (or `step_dependencies.py`). Without `.next/` being built, Next.js cannot run in production mode.

### Issue 2: Missing Frontend Systemd Unit (`pifitness-frontend.service`)
* **Evidence:**
  `deploy/systemd/` contains only:
  - `pifitness-fastapi.service`
  - `pifitness-hardware-health.service` & `.timer`
  - `pifitness_agent.service` & `.timer`
* **Root Cause:**
  In the legacy setup, Next.js was managed via PM2 (`pifitness-next` on port 3000). The new `autodeploy` design replaced custom process managers with canonical systemd units. However, while FastAPI and the timers were migrated to systemd, no `pifitness-frontend.service` was created.
  Because `step_stop_services.py` and `step_start_services.py` operate strictly on systemd units matching `pifitness*`:
  - They never stopped whatever was listening on port 3000 (the old PM2 process).
  - They never started a Next.js service pointing to `/home/god/PiFitness-test`.
  - The old PM2 process kept serving the old build on port 3000 through Nginx.

### Issue 3: Step 10 (`manual_gate`) Heartbeat Clutter
* **Evidence:** In `deploy/scripts/lib/run_step.py` (lines 186–189):
  ```python
  def on_quiet():
      console("  ... still running: %s (elapsed %ds)"
              % (step, int(time.monotonic() - started)))
  ```
  While `step_manual_gate.py` is waiting on `/dev/tty` for user input, `run_step.py` detects silence every `HEARTBEAT_SECONDS = 15` and writes to the console.
* **Root Cause:** Every 15 seconds, lines like `... still running: manual_gate (elapsed 30s)` are printed directly into the user's terminal prompt, interrupting review and input.

### Issue 4: Nginx Site Registration & Port Cleanliness
* **Evidence:** In legacy `deploy.sh`, Nginx was explicitly updated:
  ```bash
  sudo cp "$PROJECT_DIR/deploy/nginx/nginx-react.conf" /etc/nginx/sites-available/pifitness
  sudo ln -sf /etc/nginx/sites-available/pifitness /etc/nginx/sites-enabled/pifitness
  sudo nginx -t && sudo systemctl reload nginx
  ```
  In `autodeploy.sh`, Nginx is never checked, copied, or reloaded.
* **Root Cause:** If Nginx configuration changes or needs a reload after cutover, autodeploy does not ensure the site configuration matches the repo or verify that ports 8000 and 3000 are cleanly mapped. Additionally, legacy PM2 instances must be decommissioned on the Pi so they do not compete with systemd for port 3000.

---

## 3. Extensiveness & Risk Assessment

| Component | Nature of Change | Files Touched | Est. Lines Changed / Added | Risk Level |
|---|---|---|---|---|
| **1. Heartbeat Poller Fix** | Localized logic tweak in `run_step.py` to suppress output when `step == "manual_gate"`. | `deploy/scripts/lib/run_step.py` | ~4 lines | **Very Low** |
| **2. Frontend Systemd Unit** | Create standard systemd service file with `@APP_DIR@` placeholders. | `deploy/systemd/pifitness-frontend.service` | ~25 lines (new file) | **Low** |
| **3. Next.js Build Step** | Invoke `npm run build` during build/dependency or start phase. | `deploy/scripts/lib/step_start_services.py` (or `step_dependencies.py`) | ~10 lines | **Low–Medium** (timing on Pi hardware ~30–60s) |
| **4. Pi Host Hygiene (One-off manual/script)** | Clean up legacy PM2 process on the Pi (`pm2 delete all && pm2 save --force`) so port 3000 is owned exclusively by systemd. | Pi terminal / environment | 1 command | **Very Low** |
| **5. Nginx Verification (Optional/Harden)** | Ensure `nginx -t` and reload if template changes. | `deploy/scripts/lib/step_start_services.py` | ~15 lines | **Low** |

### Complexity Evaluation
* **Total Code Changes:** Under 60 lines of code across 3 files, plus 1 new systemd unit.
* **Architectural Risk:** **Low**. The existing `unitctl.py`, `step_stop_services.py`, and `step_start_services.py` are already completely dynamic: they automatically discover, template (`@APP_DIR@`), install, enable, start, stop, and roll back any unit placed in `deploy/systemd/`. Once `pifitness-frontend.service` is added to `deploy/systemd/`, the current orchestrator handles it automatically with zero custom branch logic.
* **Token Impact:** **Low**. All required fixes are compact, targeted, and follow established patterns in the repo.

---

## 4. Implemented Remediation & Legacy Transition Bridge

The codebase changes have been implemented to guarantee clean shutdown, start, and rollback regardless of current or future PRD states:

1. **Frontend Service Unit (`deploy/systemd/pifitness-frontend.service`):**
   - Created systemd unit with `@APP_DIR@/frontend/pifitness` working directory, starting Next.js via `/usr/bin/npm run start -- --port 3000 --hostname 127.0.0.1`.
   - Automatically included in `SERVICES_LIST` by `step_preflight.py` and managed dynamically by `unitctl.py`.

2. **Next.js Production Build (`step_dependencies.py` & `step_start_services.py`):**
   - Added `npm run build` right after `npm ci` in `step_dependencies.py` so `.next` is built before regression tests and services start.
   - Added defensive build check in `step_start_services.py` if `.next` is missing before starting `pifitness-frontend.service`.

3. **Legacy PM2 Transition Bridge & Port Verification (`unitctl.py`, `step_stop_services.py`, `undo_units.py`):**
   - **Shutdown:** `step_stop_services.py` checks for active legacy PM2 process (`pifitness-next`). If present, safely stops it and records `legacy_pm2_stopped=True` in `units.json`.
   - **Port Checks:** After stopping services, `step_stop_services.py` probes ports 8000 and 3000 via socket connections to verify they are free before new services start.
   - **Rollback (Revert):** If `legacy_pm2_stopped=True`, `undo_units.py` restores and restarts `pifitness-next` in PM2 so legacy PRD is never left without a frontend on rollback.
   - **PRD Success (Keep):** On successful PRD deploy, `undo_units.py` deletes `pifitness-next` from PM2 and saves PM2 state, completing the permanent transition to systemd.

4. **Heartbeat Noise Fix (`run_step.py`):**
   - In `on_quiet()`, added `if step == "manual_gate": return` to silence 15-second heartbeat logs while waiting for user input at the GO / NO-GO prompt.

---

## 5. Next Steps: Pi Execution & Verification

Launch the test deployment using the verified runner:
1. Run `powershell -File deploy\run-bootstrap.ps1`.
2. Observe automated Steps 1–9. Verify Next.js builds, ports are verified free, and both FastAPI and frontend start under systemd.
3. In Step 10 (`manual_gate`), verify the prompt is quiet and uninterrupted.
4. On mobile device in Chrome Incognito mode (`https://pifitness.duckdns.org`), verify the "Coffee" and "Smoothie" buttons appear.
5. If testing completes, type `NO-GO` to test rollback (confirming PM2 or systemd restores cleanly) or `GO` to promote.

---

## 6. Mobile Browser Cache-Clearing Protocol

When testing the next deployment, follow these instructions to rule out client-side caching:

### Option A: Mobile Chrome Incognito (Recommended)
1. In Chrome on Android/iOS, tap the **three dots menu (⋮)**.
2. Select **New Incognito tab**.
3. Navigate to `https://pifitness.duckdns.org`.
4. Incognito guarantees zero cached assets, service workers, or cookies.

### Option B: Delete Cached Files
1. In Chrome, tap **Menu (⋮)** -> **Settings** -> **Privacy and security** -> **Delete browsing data**.
2. Select Time range: **Last hour** (or **All time**).
3. Check only **Cached images and files**.
4. Tap **Delete data**.
5. Re-open Chrome and load `https://pifitness.duckdns.org`.
