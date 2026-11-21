# Terminal Rules: Windows PowerShell 5.1

## 1. Shell basics

- The shell behind `run_commands` is Windows PowerShell 5.1, not Bash.
- Chain statements with `;` only inside one safe, understood operation. Never use `&&` or `||`.
- Python is always `C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe` (see `.clinerules/00-environment.md`).
- Use backslashes in PowerShell paths and forward slashes inside Python string literals.
- Each `run_commands` call is a separate process with a hard 30,000 ms ceiling.
- Keep independent verification commands in separate calls. Never append a `psql` or service probe to a test command.

## 2. Tested patterns

Git Bash syntax check (the call operator is required for the quoted path):

```powershell
& 'C:\Program Files\Git\bin\bash.exe' -n deploy/scripts/test-up.sh
$LASTEXITCODE
```

Python command:

```powershell
& 'C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe' -m pytest tests/test_test_db.py -q
$LASTEXITCODE
```

PowerShell script (never through Python):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File <script.ps1> <args>
$LASTEXITCODE
```

Frontend build with an environment variable (PowerShell syntax, not Bash):

```powershell
$env:NEXT_PUBLIC_APP_ENV = 'test'
npm run build
$buildExit = $LASTEXITCODE
Remove-Item Env:NEXT_PUBLIC_APP_ENV -ErrorAction SilentlyContinue
$buildExit
```

Read-only diagnostic batch: `git status`, `git branch --show-current`, and `Test-Path <file>` may share one labelled call. Read each native exit code right after its command.

The paths and arguments above are examples. Substitute the real values.

## 3. Is a command slow?

A command is slow if any of these hold:

- Its name is a key in `tools\workflow\long_commands.json`, or it is a variant of one.
- It runs the full test suite, a repository-wide scan, a migration, or a large lineage operation.
- It has already timed out once at 30,000 ms.

Run every slow command through the launcher. Run everything else in the foreground.

## 4. Slow-command procedure

Launch a preset:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools\workflow\run_long.ps1 -Preset <name>
```

Poll it:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools\workflow\poll_long.ps1 -Name <name> -Tail 20
```

- Each poll waits up to 20 seconds. Keep polling while `STATE: RUNNING`; that is not a failure.
- `STATE: FINISHED`: the `EXIT:` line is the command's exit code. Keep the stdout tail and the code as evidence.
- `STATE: DEAD`: treat as a failed run and stop as blocked unless a safe, evidence-based recovery exists.
- `-List` lists jobs; `-Stop` stops one. Logs are in `$env:TEMP\agent-jobs\<name>\`.
- Do not reuse a job name while its job is running.
- If a slow command has no preset, add a narrowly named preset to `long_commands.json` and run `python tools/workflow/fd.py backlog add "preset added: <name>"`. Use `-Command` only for ad-hoc commands with no double quotes.

## 5. After a timeout

1. Never retry the timed-out command in the foreground.
2. If the check is still required and safe locally, the next action is to add a preset if none exists and run it through the launcher. Do not run unrelated diagnostics first, and do not treat the check as passed.
3. If the check is prohibited or inapplicable here (for example a Pi-side operation), do not run it. Record why and name the manual gate that remains.
4. A timeout is never a pass. An unresolved required check means the work is blocked, unless the criterion is an explicit human gate.
5. If the launcher fails twice with no new information, stop as blocked. Do not invent another background method.

## 6. Prohibited

- Running a slow command in the foreground, including a retry after a timeout.
- `Start-Job`, `Wait-Job`, ad-hoc `Start-Process`, or `Start-Sleep` over 20 seconds inside `run_commands`.
- Judging whether a job finished from `Get-Process`.
- Assuming a process, variable, directory, or environment setting survives between calls.
- Using shell tricks to get around the file-writing protocol.

## 7. Git lock errors

If git reports an index lock, do not delete `.git/index.lock`. Stop and tell the human.
