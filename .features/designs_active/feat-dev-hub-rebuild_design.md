---
feature-id: feat-dev-hub-rebuild
title: dev-hub-rebuild
category: Enhancement
status: draft
created: 2026-11-22
last-updated: 2026-11-22
surfaces: tools/workflow/dev_hub.py, tools/workflow/dev_hub.html, dev_hub.bat, deploy/run-bootstrap.ps1
points:
request: .features/descriptions/feat-dev-hub-rebuild.md
---

# Design: dev-hub-rebuild

## Resume Here
**progress:** 0/0 tasks complete
**next:** review
**blocked-by:** OQ-1, OQ-2, OQ-3, OQ-4

## Architecture

### Surfaces
- `tools/workflow/dev_hub.py`: Local-only Python stdlib HTTP server (port 8760); `DevHubHandler` handles GET/POST; `archive_feature_files()` uses `shutil.move()` but files remain in source (bug: files stay behind — likely the function is called correctly but the HTML js-side `isRunning` stale flag prevents the action from ever firing, OR `shutil.move` copies across filesystem boundaries and the original is not removed); `_handle_action_stream()` dispatches actions; `launch_local_env` spawns `Start-Process powershell -NoExit` (new windows, violates B2); `launch_bootstrapper` similarly opens a new console window; pattern file: `tools/workflow/request_form.py` (same stdlib HTTP server pattern)
- `tools/workflow/dev_hub.html`: Single-page app; sidebar with 7 nav buttons; `triggerAction()` posts to `/api/action` and streams SSE; `isRunning` boolean guards concurrent actions — if the SSE stream ends without a `done` event (server-side exception, disconnect) `isRunning` stays `true` and all further actions are blocked; no tab widget for switching between DevHub and the app at :3000; pattern: existing badge/terminal layout
- `dev_hub.bat`: 10-line launcher; calls `tools\workflow\dev_hub.py`; `pause` at end keeps the console open; "immediately closes" bug is likely the server failing to start (exception), which exits the script before `pause` runs
- `deploy/run-bootstrap.ps1`: SSHes to `pfpi` as `god`, runs `pi_bootstrap_shell.sh` interactively with `-t`; script on the Pi prompts keep/wipe interactively; no mechanism to pass a non-interactive flag; no test vs prd distinction; `run-bootstrap.ps1` accepts params `HostName`, `User`, `RemoteLauncher`, `RemoteLogPath`, `LocalPath`

### Approach
- Fix archive bug: audit `archive_feature_files()` for why originals remain; likely fix is ensuring the `shutil.move` path resolves correctly and the function returns errors rather than silently skipping.
- Fix stale `isRunning` lock: ensure every server-side code path sends a `done` SSE event (even on exception); add a `reset_lock` action or auto-clear on SSE stream `done`.
- Fix subprocess window spawning (B2): replace `Start-Process powershell -NoExit` calls for backend/frontend with `subprocess.Popen` piped to the SSE log; output streams into the DevHub terminal.
- Bootstrap non-interactive (B4): add params to `run-bootstrap.ps1` and new DevHub action buttons for DEV and PRD; pass `--auto-keep` and `--env` to the remote shell (pending OQ-4 for exact mechanism).
- Tab-toggle (B3): add tab strip to DevHub header; approach pending OQ-2.
- Taskbar pinning (B5): approach pending OQ-3.

### Data and Migrations
- None

### API Changes
- New action keys on POST `/api/action`: `launch_bootstrapper_dev`, `launch_bootstrapper_prd` (pending OQ-4)
- New action key: `reset_lock` to clear `isRunning` on the HTML side via SSE `done` event

## Acceptance Criteria
- [ ] AC-1: Click "Complete Feature" and confirm → all files from `.features/descriptions/` and `.features/designs_active/` appear in `zz_feature_archive/` and are absent from their source folders (covers B1)
  - auto: python -m py_compile tools/workflow/dev_hub.py
- [ ] AC-2: Trigger any action, let it finish or error, then immediately trigger another → second action runs without "Another action is currently in progress" blocking it (covers B1, B2)
  - auto: python -m py_compile tools/workflow/dev_hub.py
- [ ] AC-3: Click "Launch Local Environment" → backend and frontend start without opening new PowerShell windows; output lines appear in the DevHub terminal (covers B2)
  - auto: none (subprocess launch behavior not automatable from a compile check; human verifies in UAT)
- [ ] AC-4: The DevHub shows a tab strip; clicking between tabs switches the visible pane without launching a new service or opening a new window (covers B3) (pending OQ-2)
  - auto: none (UI layout requires UAT)
- [ ] AC-5: Clicking "Launch Bootstrapper (DEV)" starts the Pi bootstrap without a keep/wipe prompt and targets the dev environment; "Launch Bootstrapper (PRD)" targets production (covers B4) (pending OQ-4)
  - auto: none (SSH to Pi; human verifies in UAT)
- [ ] AC-6: Running the pinning helper makes `dev_hub.bat` available to pin to the Windows taskbar (covers B5) (pending OQ-3)
  - auto: none (Windows shell feature; human verifies in UAT)

## Open Questions
- **OQ-1** Confirm understanding
  - **Question:** You have a local dev hub at `http://localhost:8760` that serves a browser GUI for running workflow commands (request form, UAT form, prep env, launch app, bootstrapper, merge to PRD, complete feature). Three bugs exist: (1) "Complete Feature" leaves files behind in their source folders after archiving; (2) triggering a command while another is finishing shows an "already running" or "process has not completed" error that blocks further actions; (3) clicking "Launch Bootstrapper" opens a window that immediately closes. Five improvements are desired: B1 (fix archive bug so files are truly moved), B2 (subprocess output stays in the DevHub terminal, no new windows), B3 (tab-toggle between DevHub and the app at :3000 without launching new services), B4 (bootstrapper non-interactive with separate DEV/PRD buttons), B5 (pinnable to taskbar). I am assuming the existing Python stdlib server and single HTML file are the only files to change (no new framework). Is this right? Default: yes, this is right.
  - **Answer:** I'm open to an entirely new framework if it simplifies development and gives a better user experience. I just don't want the core functionality of the commands this executes to change.
  - **Decision:** (to be recorded)
  - **Rationale:** Scope; if a new tool or framework is desired, the approach changes substantially.
  - **Status:** open

- **OQ-2** Tab-toggle approach for B3
  - **Question:** For switching between the DevHub dashboard and the Next.js app (:3000), should I: (A) embed an iframe in the DevHub page so the app renders inside the same window and a tab strip toggles it, or (B) only add browser-tab links (user opens :3000 in one tab, :8760 in another) with no new code? Default: A — iframe embedded in the DevHub page with a tab strip toggle.
  - **Answer:** I think whatever is more stable for the user without sacrificing functionality. 
  - **Decision:** (to be recorded)
  - **Rationale:** Option A is fully in-app and needs HTML changes. Option B needs nothing coded.
  - **Status:** open

- **OQ-3** Taskbar pinning approach for B5
  - **Question:** Batch files cannot be pinned to the Windows taskbar directly — a `.lnk` shortcut is required. Should I: (A) add `tools/create-devhub-shortcut.ps1` that creates `DevHub.lnk` on the Desktop pointing to `dev_hub.bat`, which the user then pins manually, or (B) attempt programmatic pinning via Shell COM object? Default: A — create-shortcut script, user pins manually.
  - **Answer:** We should consider alternatives that don't necessarily rely on the existing tools.
  - **Decision:** (to be recorded)
  - **Rationale:** Option B is fragile and unreliable on modern Windows. Option A is one human step.
  - **Status:** open

- **OQ-4** Bootstrap non-interactive mechanism for B4
  - **Question:** The Pi bootstrap script (`pi_bootstrap_shell.sh`) is not in this repo. To run it non-interactively with DEV/PRD separation, should I: (A) update `run-bootstrap.ps1` to pass CLI args to the remote script (e.g. `bash -l pi_bootstrap_shell.sh --auto-keep --env dev`) and document the args the Pi script must accept, or (B) set an env var over SSH that the existing Pi script already reads (or will be updated to read)? Default: A — pass `--auto-keep --env <dev|prd>` as args and document what the Pi script must accept.
  - **Answer:** Now added at `deploy/scripts/pi_bootstrap_shell.sh` -- if altered, I can manually deploy to the correct location on the Pi.
  - **Decision:** (to be recorded)
  - **Rationale:** Affects what we change in this repo and what the human must separately update on the Pi.
  - **Status:** open

## Task Plan
Filled by /feature-plan.
