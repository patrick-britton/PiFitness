# PiFitness — Agent & Developer Instructions (`GEMINI.md`)

This file defines the project environment, architecture, commands, guardrails, and operating standards for AI agents and developers working in `C:\users\rickb\vsprojects\pifitness`.

---

## 1. Project Overview & Tech Stack

* **Application:** PiFitness — a health, fitness, and music analytics platform integrating Spotify and Garmin APIs.
* **Backend:** FastAPI in `backend/` (`backend/main.py`), running under Uvicorn.
* **Frontend:** Next.js in `frontend/pifitness/`.
* **Database (DEV):** PostgreSQL running in Docker container `pifitness-dev-db` on `localhost:15432`, database `pifitness_dev`, user `dev`.
* **Platform & Shell:** Windows PC running Windows PowerShell 5.1.

---

## 2. Environment & Verified Command Paths

Always execute commands from the repository root unless a subfolder is explicitly specified.

### Interpreters & Runtimes
* **Python interpreter:**  
  `C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe`  
  *(Always invoke via the full path; do not substitute generic `python` or `python3`.)*
* **Node & npm:** Installed for frontend development and type checking.

### Key Commands
* **Start Backend:**
  ```powershell
  & "C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe" -m uvicorn main:app --app-dir backend --port 8000
  ```
* **Backend Health Check:**
  ```powershell
  Invoke-RestMethod http://localhost:8000/api/health
  ```
* **Run Backend Tests (fast local suite):**
  ```powershell
  & "C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe" -m pytest tests/ -m "not slow and not integration and not live" -q
  ```
* **Start Frontend** *(working directory `frontend\pifitness`)*:
  ```powershell
  node node_modules\.bin\next dev -p 3000
  ```
* **Frontend Type Check** *(working directory `frontend\pifitness`)*:
  ```powershell
  npx tsc --noEmit
  ```

---

## 3. Strict Guardrails & Security

* **DEV Database Only:** All database connections must target `localhost:15432` (`pifitness_dev`). Never connect to PRD, `pfpi`, or any remote host.
* **Prohibited Database Scripts:** Never execute `tools/db/sync-prod-to-dev.ps1`.
* **No Remote / Pi Access:** Never SSH, SCP, rsync, or connect directly to the Raspberry Pi or production systems. Pi-side requirements are strictly manual/human acceptance gates.
* **No Production Deployments or Migrations:** Never execute deployment or migration scripts against live/production environments. Editing deployment scripts authorizes inspecting or modifying files only, never executing them against PRD.
* **Protect Secrets:** Secrets and credentials reside in `backend/.env` and related `.env` files. Never output, log, or commit `.env` files or secret values into transcripts, documentation, or git.

---

## 4. Data Lineage — Mandatory First-Action Precedence

For any `/lineage` request or question involving database tables, views, ETL pipelines, API transformations, or data flow:

1. **Mandatory First Tool Command:** Run the compact lineage description before any broad codebase search, file read, or schema inspection:
   ```powershell
   & "C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe" tools\data_dictionary\lineage.py describe <target>
   # Example:
   & "C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe" tools\data_dictionary\lineage.py describe table:activities.activity_details_distance
   ```
2. **Selective Evidence Extraction:** Use `lineage.py evidence` to retrieve specific writer/consumer snippets rather than reading entire large modules.
3. **No Redundant Searching:** Do not run repository-wide searches to rediscover relationships already documented in the lineage graph.

---

## 5. Terminal & Execution Rules (Windows PowerShell 5.1)

* **Syntax:** Use PowerShell 5.1 syntax. Chain statements with `;` and explicit `if` blocks; **do not use `&&` or `||`**.
* **Filtering:** Use `Select-Object -First N` / `-Last N` instead of `head`/`tail`; use `Select-String` instead of `grep`.
* **Process Isolation:** Each command execution starts a fresh shell process. Working directory changes, environment variables, and job states do not persist across calls.
* **Long-Running Commands:** For commands exceeding 30 seconds (full test suites, migrations, bulk lineage builds):
  * Use the launcher: `powershell -NoProfile -ExecutionPolicy Bypass -File tools\workflow\run_long.ps1 -Preset <name>`
  * Use the poller: `powershell -NoProfile -ExecutionPolicy Bypass -File tools\workflow\poll_long.ps1 -Name <name> -Tail 20`
  * Presets are configured in `tools\workflow\long_commands.json`.

---

## 6. Git Safety & Commit Discipline

* Stage only, never commit.

---

## 7. File Writing & Quality Protocol

1. **Targeted Edits:** Prefer localized replacements over rewriting entire files.
2. **Read Bounds:** For files over 200 lines, read only the target regions and their direct dependencies using line ranges rather than loading entire files into context.
3. **Verification After Edits:** Always run syntax and compilation checks after edits:
   * Python: verify via target test or `python -m py_compile <file>`.
   * TypeScript: `npx tsc --noEmit` from `frontend\pifitness`.
4. **No Shell Source Generation:** Do not use `Set-Content`, `Add-Content`, or redirections to generate source code. Use dedicated file tools.

