# Block INT-0 - Tooling bootstrap + baseline capture

First block in the integrations refactor. Creates the `pifitness.integrations` package scaffold and captures baseline state so subsequent blocks can prove they changed the right things.

## Scope

1. Create `backend/pifitness/integrations/` package scaffold (empty `__init__.py` + README)
2. Capture baseline evidence (pytest count, current pirate_garmin references, deploy wiring, import sanity)
3. Write report to `docs/reorg/blocks/INT-0-report.md`
4. Commit

## Files you may touch

- `backend/pifitness/integrations/__init__.py` (create, empty)
- `backend/pifitness/integrations/README.md` (create)
- `docs/reorg/blocks/INT-0-report.md` (create, the report)

## Files you must NOT touch

- Any file under `backend/backend_functions/`
- Any file under `backend/pifitness/` except `integrations/`
- `backend/main.py`
- `pirate-garmin_clone/`
- `deploy/` (capture only, no modifications)
- `docs/reorg/blocks/README.md` (pre-created by user)

## Preconditions (verify first, STOP if fail)

```powershell
$head = git log -1 --format='%h'
$dirty = (git status --short | Measure-Object).Count
$branch = git branch --show-current
Write-Host "HEAD: $head"
Write-Host "dirty: $dirty"
Write-Host "branch: $branch"

Write-Host "`n--- required pre-created files (all must be True) ---" -ForegroundColor Cyan
Write-Host "blocks/README.md:        $((Test-Path docs\reorg\blocks\README.md))"
Write-Host "blocks/INT-0.md:         $((Test-Path docs\reorg\blocks\INT-0.md))"

Write-Host "`n--- must NOT exist yet (all must be False) ---" -ForegroundColor Cyan
Write-Host "integrations/:           $((Test-Path backend\pifitness\integrations))"
Write-Host "blocks/INT-0-report.md:  $((Test-Path docs\reorg\blocks\INT-0-report.md))"
```

Expected: HEAD `674d5b9`, dirty 0, branch `reorg/02-moves`. Two required files True. Both must-not-exist False.

If any required file is missing, STOP and report "precondition fail: user must create X before running this block."

## Actions

### Action 1 - capture pytest baseline

```powershell
& ".venv\Scripts\python.exe" -m pytest -q 2>&1 | Select-Object -Last 5
Write-Host "pytest exit: $LASTEXITCODE"
```

Record last 5 lines + exit code.

### Action 2 - capture current pirate_garmin references

```powershell
Write-Host "--- service_logins.py refs ---" -ForegroundColor Cyan
Select-String -Path backend\backend_functions\service_logins.py -Pattern "pirate_garmin|pirate-garmin" |
    ForEach-Object { "L$($_.LineNumber): $($_.Line.Trim())" }

Write-Host "`n--- json_extractors.py refs ---" -ForegroundColor Cyan
Select-String -Path backend\backend_functions\json_extractors.py -Pattern "pirate_garmin|pirate-garmin" |
    ForEach-Object { "L$($_.LineNumber): $($_.Line.Trim())" }

Write-Host "`n--- repo-wide count ---" -ForegroundColor Cyan
(Select-String -Path backend\*.py,backend\*\*.py,backend\*\*\*.py,backend\*\*\*\*.py,tests\*.py -Pattern "pirate_garmin|pirate-garmin" -ErrorAction SilentlyContinue | Measure-Object).Count
```

Record all output.

### Action 3 - capture deploy wiring

```powershell
Write-Host "--- pi5_deploy.sh pirate/playwright lines ---" -ForegroundColor Cyan
Select-String -Path deploy\scripts\pi5_deploy.sh -Pattern "pirate|playwright" |
    ForEach-Object { "L$($_.LineNumber): $($_.Line.Trim())" }

Write-Host "`n--- .pth files in venv ---" -ForegroundColor Cyan
Get-ChildItem .venv\Lib\site-packages -Filter "*.pth" -File -ErrorAction SilentlyContinue |
    ForEach-Object { "$($_.Name): $((Get-Content $_.FullName) -join '; ')" }

Write-Host "`n--- pip show pirate_garmin ---" -ForegroundColor Cyan
& ".venv\Scripts\python.exe" -m pip show pirate_garmin 2>&1 | Select-Object -First 6
```

Record all output.

### Action 4 - capture import baseline

```powershell
Write-Host "--- import smoke tests ---" -ForegroundColor Cyan
& ".venv\Scripts\python.exe" -c "import backend.main; print('backend.main OK')"
& ".venv\Scripts\python.exe" -c "import backend_functions.service_logins; print('service_logins OK')"
& ".venv\Scripts\python.exe" -c "import backend_functions.json_extractors; print('json_extractors OK')"
```

Record each line.

### Action 5 - create integrations scaffold

```powershell
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)

New-Item -ItemType Directory -Force backend\pifitness\integrations | Out-Null
[System.IO.File]::WriteAllText("$PWD\backend\pifitness\integrations\__init__.py", "", $utf8NoBom)

$readme = @'
# pifitness.integrations

Per-service login and credential-management integrations.

Each service (Garmin, Spotify, future additions) gets its own subpackage here
implementing a shared interface. Centralizes:

- Token/session lifecycle (reuse if valid -> refresh if expired -> re-auth if refresh fails)
- Provider-change isolation (Spotify API changes touch spotify/ only; Garmin changes touch garmin/ only)
- Rate-limit backoff (per-service, so a 429 from one does not stall the other)

## Not yet populated

Scaffolded in INT-0. Subpackages land in later blocks. See docs/reorg/blocks/ for the plan.
'@
[System.IO.File]::WriteAllText("$PWD\backend\pifitness\integrations\README.md", $readme, $utf8NoBom)

Write-Host "scaffold created:"
Write-Host "  __init__.py: $((Test-Path backend\pifitness\integrations\__init__.py))"
Write-Host "  README.md:   $((Test-Path backend\pifitness\integrations\README.md))"
```

### Action 6 - write report to file AND print

Use the report format from `.agents/skills/block-executor/SKILL.md`. Write the completed report to `docs/reorg/blocks/INT-0-report.md` using `[System.IO.File]::WriteAllText` with UTF-8 no BOM. Also print the report in chat.

## Verification

1. `Test-Path backend\pifitness\integrations\__init__.py` -> True
2. `Test-Path backend\pifitness\integrations\README.md` -> True
3. `Test-Path docs\reorg\blocks\INT-0-report.md` -> True
4. Both scaffold files are ASCII-clean:
   ```powershell
   $bytes1 = [System.IO.File]::ReadAllBytes("$PWD\backend\pifitness\integrations\__init__.py")
   $bytes2 = [System.IO.File]::ReadAllBytes("$PWD\backend\pifitness\integrations\README.md")
   Write-Host "init non-ASCII:   $(($bytes1 | Where-Object { $_ -gt 127 }).Count)  (expected: 0)"
   Write-Host "README non-ASCII: $(($bytes2 | Where-Object { $_ -gt 127 }).Count)  (expected: 0)"
   ```
5. Rerun `pytest -q` -> exit 0, same pass count as baseline
6. `git status --short` shows:
   - `A backend/pifitness/integrations/__init__.py`
   - `A backend/pifitness/integrations/README.md`
   - `A docs/reorg/blocks/INT-0-report.md`

## Commit

```powershell
git add backend/pifitness/integrations docs/reorg/blocks/INT-0-report.md
git status --short
git commit -m "reorg(int-0): scaffold pifitness.integrations + capture baseline [INT-0]"
git log -1 --format='%h %s'
git status --short
```

One commit. Record the sha in the report's COMMIT field.

## Report

Per the skill's report format. Write to `docs/reorg/blocks/INT-0-report.md` and print in chat.

## STOP

Do not proceed to INT-1. Wait for human relay.