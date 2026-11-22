---
feature-id: feat-dev-hub-rebuild
title: dev-hub-rebuild
category: Enhancement
status: in-progress
created: 2026-11-22
last-updated: 2026-11-22
surfaces: tools/workflow/dev_hub.py, tools/workflow/dev_hub.html, dev_hub.bat, deploy/run-bootstrap.ps1, deploy/scripts/pi_bootstrap_shell.sh
points: 7
request: .features/descriptions/feat-dev-hub-rebuild.md
---

# Design: dev-hub-rebuild

## Resume Here
**progress:** 5/5 tasks complete
**next:** none
**blocked-by:** none

## Architecture

### Surfaces
- `tools/workflow/dev_hub.py`: Local-only Python server; `DevHubHandler` handles GET/POST; `archive_feature_files()` moves active feature files into archive; `_handle_action_stream()` dispatches actions via SSE; handles background process management (FastAPI backend, Next.js frontend, bootstrap script) without creating console windows.
- `tools/workflow/dev_hub.html`: Single-page app with navigation, live environment status, and real-time streaming console; handles action lifecycle without getting stuck in a locked state; provides integrated navigation between DevHub and application views.
- `dev_hub.bat`: Launcher entry point, augmented or replaced by a dedicated windowless application runner and desktop shortcut for Windows taskbar pinning.
- `deploy/run-bootstrap.ps1`: Invokes remote bootstrap over SSH, updated to pass target environment non-interactively.
- `deploy/scripts/pi_bootstrap_shell.sh`: Remote bootstrap entry point; accepts `--environment prd|test` CLI flag to skip interactive prompts and default database handling to keep.

### Approach
- **Professional self-contained app experience (B1, B5)**: Provide a dedicated desktop application experience where the hub launches as a standalone window (e.g. app mode / windowless backend with dedicated taskbar identity) that can be pinned to the Windows taskbar. All operations remain inside this interface without command prompt or PowerShell windows popping up.
- **Subprocess management & streaming logs (B2)**: Run backend, frontend, and bootstrap processes as background subprocesses (`subprocess.Popen` with no console window creation flags). Capture stdout and stderr streams and pipe them live into the in-app terminal view.
- **Action lifecycle & lock reliability (B1, B2)**: Ensure the UI and server action state machines always recover cleanly on errors or disconnects, preventing stale "action in progress" locks.
- **File archiving reliability (B1)**: Audit and ensure file archival moves active feature files from source directories to the archive directory without leaving duplicates behind.
- **Tab & view switching (B3)**: Provide convenient header navigation between the hub dashboard and the running application without requiring service restarts or opening cluttering duplicate windows.
- **Non-interactive bootstrap (B4)**: Support distinct DEV and PRD bootstrap commands that execute the deployment sequence without manual terminal prompts, defaulting to keeping existing database data.

### Data and Migrations
- None

### API Changes
- Actions on POST `/api/action`: `launch_bootstrapper_dev`, `launch_bootstrapper_prd`
- Status and reset endpoints to maintain responsive subprocess state and terminal streams.

## Acceptance Criteria
- [ ] AC-1: Trigger "Complete Feature" and confirm → all active feature files move to the archive folder and are absent from their source locations (covers B1)
  - auto: python -m py_compile tools/workflow/dev_hub.py
- [ ] AC-2: Trigger any action, let it finish or error out, then immediately trigger another → the second action runs without a "another action is in progress" block (covers B1, B2)
  - auto: python -m py_compile tools/workflow/dev_hub.py
- [ ] AC-3: Click "Launch Local Environment" → backend and frontend start and their output appears in the DevHub terminal; no new PowerShell or console window opens (covers B2)
  - auto: none (subprocess launch behavior requires UAT)
- [ ] AC-4: The DevHub shows clearly labelled links or tabs for the DevHub and the app (:3000); clicking either opens or focuses that page without launching a new service (covers B3)
  - auto: none (UI behavior requires UAT)
- [ ] AC-5: Clicking "Bootstrap DEV" starts the Pi bootstrapper targeting the test environment without any interactive prompt; clicking "Bootstrap PRD" targets production without any interactive prompt (covers B4)
  - auto: none (SSH to Pi; human verifies in UAT)
- [ ] AC-6: The launcher can be pinned to the Windows taskbar and launches the DevHub when clicked (covers B1, B5)
  - auto: none (Windows shell feature; human verifies in UAT)

## Open Questions
- **OQ-1** Confirm understanding
  - **Question:** You have a local dev hub at `http://localhost:8760` that serves a browser GUI for running workflow commands (request form, UAT form, prep env, launch app, bootstrapper, merge to PRD, complete feature). Three bugs exist: (1) "Complete Feature" leaves files behind in their source folders after archiving; (2) triggering a command while another is finishing shows an "already running" or "process has not completed" error that blocks further actions; (3) clicking "Launch Bootstrapper" opens a window that immediately closes. Five improvements are desired: B1 (fix archive bug so files are truly moved), B2 (subprocess output stays in the DevHub terminal, no new windows), B3 (tab-toggle between DevHub and the app at :3000 without launching new services), B4 (bootstrapper non-interactive with separate DEV/PRD buttons), B5 (pinnable to taskbar). I am assuming the existing Python stdlib server and single HTML file are the only files to change (no new framework). Is this right? Default: yes, this is right.
  - **Answer:** I'm open to an entirely new framework if it simplifies development and gives a better user experience. I just don't want the core functionality of the commands this executes to change.
  - **Decision:** Framework is open — any approach is acceptable as long as the 7 existing workflow actions are preserved. Scope is B1–B5 as described.
  - **Rationale:** Scope; if a new tool or framework is desired, the approach changes substantially.
  - **Status:** resolved

- **OQ-2** Tab-toggle approach for B3
  - **Question:** For switching between the DevHub dashboard and the Next.js app (:3000), should I: (A) embed an iframe in the DevHub page so the app renders inside the same window and a tab strip toggles it, or (B) only add browser-tab links (user opens :3000 in one tab, :8760 in another) with no new code? Default: A — iframe embedded in the DevHub page with a tab strip toggle.
  - **Answer:** I think whatever is more stable for the user without sacrificing functionality.
  - **Decision:** Use browser-tab links (option B). Iframes are blocked by X-Frame-Options/CSP headers on the Next.js dev server; named browser tabs are more stable and require no special headers. The DevHub header will show a clearly labelled "Open App (:3000)" link that opens/focuses the named tab.
  - **Rationale:** Option A is fully in-app and needs HTML changes. Option B needs nothing coded.
  - **Status:** resolved

- **OQ-3** Taskbar pinning approach for B5
  - **Question:** Batch files cannot be pinned to the Windows taskbar directly — a `.lnk` shortcut is required. Should I: (A) add `tools/create-devhub-shortcut.ps1` that creates `DevHub.lnk` on the Desktop pointing to `dev_hub.bat`, which the user then pins manually, or (B) attempt programmatic pinning via Shell COM object? Default: A — create-shortcut script, user pins manually.
  - **Answer:** We should consider alternatives that don't necessarily rely on the existing tools.
  - **Decision:** Superseded by OQ-5.
  - **Rationale:** Option B is fragile and unreliable on modern Windows. Option A is one human step.
  - **Status:** resolved

- **OQ-4** Bootstrap non-interactive mechanism for B4
  - **Question:** The Pi bootstrap script (`pi_bootstrap_shell.sh`) is not in this repo. To run it non-interactively with DEV/PRD separation, should I: (A) update `run-bootstrap.ps1` to pass CLI args to the remote script (e.g. `bash -l pi_bootstrap_shell.sh --auto-keep --env dev`) and document the args the Pi script must accept, or (B) set an env var over SSH that the existing Pi script already reads (or will be updated to read)? Default: A — pass `--auto-keep --env <dev|prd>` as args and document what the Pi script must accept.
  - **Answer:** Now added at `deploy/scripts/pi_bootstrap_shell.sh` -- if altered, I can manually deploy to the correct location on the Pi.
  - **Decision:** Modify `deploy/scripts/pi_bootstrap_shell.sh` to accept `--environment prd|test` as a CLI arg and skip the interactive prompt when provided. Update `run-bootstrap.ps1` to accept `-Environment` and pass it through. Human deploys updated script to Pi manually.
  - **Rationale:** Affects what we change in this repo and what the human must separately update on the Pi.
  - **Status:** resolved

- **OQ-5** Launcher type for B5 (new)
  - **Question:** Since the launcher should be "nice and easy to use" and not necessarily rely on existing bat/ps1 tools, what type of launcher do you want? Options: (A) A Python-based system-tray icon (using `pystray` + `Pillow`) that lives in the notification area, opens the DevHub in the browser when clicked, and can be pinned as a startup item — no console window; (B) a compiled `.exe` wrapper (using PyInstaller around the existing server) that Windows treats as a pinnable app; (C) keep `dev_hub.bat` but create a Desktop `.lnk` shortcut with a custom icon (one human pin step, no new dependencies). Default: A — system-tray icon (most "app-like" feel, pinnable via Startup folder or taskbar shortcut to the tray app).
  - **Answer:** I don't care so much about the technology - but I definitely want it to feel like a legitimate, professional application -- even if it's purely custom to this specific project. That means all capabilities should be contained within the app, with no stray window launchings to monitor or keep track of the output. 
  - **Decision:** Build a self-contained application launcher with its own taskbar identity (standalone desktop app window via dedicated app mode/runner), running all background services silently without opening external console windows, and streaming all command outputs directly into the integrated in-app console.
  - **Rationale:** Meets the user's requirement for a professional, cohesive app experience with zero stray window popups.
  - **Status:** resolved

## Task Plan

### T01 Add environment flag support to bootstrap scripts
- **Surface:** deployment
- **Skill:** bash
- **Points:** 2
- **Depends-on:** none
- **Traces-to:** AC-5
- **Anchors:**
  - `modify`: `deploy/scripts/pi_bootstrap_shell.sh` `ENVIRONMENT`
  - `modify`: `deploy/run-bootstrap.ps1` `param`
- **Detail:** Support non-interactive execution by parsing `--environment` in the remote shell script to bypass the interactive menu. Update the local PowerShell runner to accept `-Environment` and pass it to the remote script call.
  Finding: `deploy/scripts/pi_bootstrap_shell.sh` lines 26-50 prompt for environment interactively, blocking non-interactive automation.
  Finding: `deploy/run-bootstrap.ps1` lines 19-26 define script parameters, where `-Environment` will be added.
  Finding: `deploy/run-bootstrap.ps1` line 37 initiates SSH invocation: `ssh -t "$target" "bash -l $RemoteLauncher"`.
- **Verify:** powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-Command deploy\run-bootstrap.ps1"
- **Status:** complete

### T02 Fix feature file archival in DevHub server
- **Surface:** backend
- **Skill:** fastapi
- **Points:** 1
- **Depends-on:** none
- **Traces-to:** AC-1
- **Anchors:**
  - `modify`: `tools/workflow/dev_hub.py` `archive_feature_files`
- **Detail:** Update `archive_feature_files` to verify source files are removed from descriptions and designs_active after moving to `zz_feature_archive`. Ensure paths are normalized and any residual source files are explicitly deleted.
  Finding: `tools/workflow/dev_hub.py` lines 118-137 iterate active directories using `shutil.move` without validating source deletion.
  Finding: If a file already exists in archive, line 133 appends a timestamp, but partial operations leave originals behind.
- **Verify:** python -m py_compile tools/workflow/dev_hub.py
- **Status:** complete

### T03 Manage background subprocesses and add bootstrap actions
- **Surface:** backend
- **Skill:** fastapi
- **Points:** 1
- **Depends-on:** T01
- **Traces-to:** AC-2, AC-3, AC-5
- **Anchors:**
  - `modify`: `tools/workflow/dev_hub.py` `_handle_action_stream`
- **Detail:** Replace `Start-Process powershell -NoExit` for backend and frontend with background `subprocess.Popen` calls that stream logs to the terminal without external windows. Add `launch_bootstrapper_dev` and `launch_bootstrapper_prd` action handlers passing `-Environment` to `deploy/run-bootstrap.ps1`. Ensure every action stream safely sends a terminal `done` event on failure to prevent locking the UI.
  Finding: `launch_local_env` at lines 384-406 creates external PowerShell windows using `Start-Process`.
  Finding: `launch_bootstrapper` at lines 409-420 fails and closes immediately due to single-quote escaping in `-ArgumentList`.
  Finding: Running subprocesses with `subprocess.Popen` and streaming output line-by-line enables full in-app terminal logging.
- **Verify:** python -m py_compile tools/workflow/dev_hub.py
- **Status:** complete

### T04 Update DevHub UI actions, lock recovery, and navigation
- **Surface:** frontend
- **Skill:** react
- **Points:** 1
- **Depends-on:** T03
- **Traces-to:** AC-2, AC-3, AC-4, AC-5
- **Anchors:**
  - `modify`: `tools/workflow/dev_hub.html` `triggerAction`
- **Detail:** Replace single bootstrap action with separate DEV and PRD bootstrap action triggers in the sidebar. Add tab navigation in the header to switch cleanly between DevHub and the app at port 3000. Harden `triggerAction` so `isRunning` is reset on stream end or error, preventing locked action states.
  Finding: Lines 540-545 in `tools/workflow/dev_hub.html` contain the legacy single bootstrapper action button.
  Finding: Header at lines 573-601 contains status badges and can host the app toggle links.
  Finding: `triggerAction` at line 784 sets `isRunning = true` but an interrupted SSE stream leaves it permanently true.
- **Verify:** python -m py_compile tools/workflow/dev_hub.py
- **Status:** complete

### T05 Create desktop application launcher and taskbar shortcut
- **Surface:** deployment
- **Skill:** powershell
- **Points:** 2
- **Depends-on:** T04
- **Traces-to:** AC-6
- **Anchors:**
  - `modify`: `dev_hub.bat`
  - `create`: `tools/create-devhub-shortcut.ps1`
- **Detail:** Update `dev_hub.bat` to launch the DevHub in app mode without a lingering console window. Create `tools/create-devhub-shortcut.ps1` to generate a Windows shortcut (`.lnk`) that launches DevHub as a standalone desktop window pinnable to the taskbar.
  Interface: `tools/create-devhub-shortcut.ps1` accepts optional `-ShortcutPath` parameter and creates a pinnable Windows shortcut pointing to the windowless DevHub launcher.
  Finding: `dev_hub.bat` ends with `pause`, keeping a terminal window open when launched.
  Finding: Chromium and Edge support `--app=http://127.0.0.1:8760/` for running web apps in a standalone desktop window.
  Finding: A `.lnk` file created with WScript.Shell can be pinned to the Windows taskbar.
- **Verify:** powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-Command tools\create-devhub-shortcut.ps1"
- **Status:** complete
