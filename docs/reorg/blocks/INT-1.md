# INT-1 - Vendor Garmin integration modules

## Block metadata

- Block: INT-1
- Branch: `reorg/02-moves`
- Rollback anchor: `505761d` (INT-0 commit)
- Commit expected: yes, one commit after verification passes
- DB impact: none. Do not touch DB rows.
- Deploy impact: none in this block. Do not edit deploy scripts.

## Mission

Vendor the Garmin integration modules from `pirate-garmin_clone/src/pirate_garmin/` into
`backend/pifitness/integrations/garmin/`.

Keep:
- `auth.py`
- `client.py`
- `endpoints.py`
- `browser_login.py`

Drop:
- `cli.py`
- `render_endpoint.py`

Do not wire this into `service_logins.py` yet. Do not modify `pirate-garmin_clone/`.
This block only creates an importable vendored package.

## Files you may touch

Exactly these files:

- `backend/pifitness/integrations/garmin/__init__.py` (new)
- `backend/pifitness/integrations/garmin/auth.py` (new, copied from source)
- `backend/pifitness/integrations/garmin/client.py` (new, copied from source)
- `backend/pifitness/integrations/garmin/endpoints.py` (new, copied from source)
- `backend/pifitness/integrations/garmin/browser_login.py` (new, copied from source)
- `docs/reorg/blocks/INT-1-report.md` (new report, written after commit)

Do not modify any other file.

## Preconditions

Run this first. Do not proceed if any precondition fails.

```powershell
$failed = $false
$src = "pirate-garmin_clone\src\pirate_garmin"
$dst = "backend\pifitness\integrations\garmin"

$branch = (git rev-parse --abbrev-ref HEAD).Trim()
if ($branch -ne "reorg/02-moves") {
    Write-Host "FAIL: branch is $branch, expected reorg/02-moves"
    $failed = $true
}

$head = (git rev-parse --short HEAD).Trim()
if ($head -ne "505761d") {
    Write-Host "FAIL: HEAD is $head, expected 505761d"
    $failed = $true
}

$dirty = git status --porcelain
if ($dirty) {
    Write-Host "FAIL: working tree is dirty"
    Write-Host $dirty
    $failed = $true
}

if (-not (Test-Path $src)) {
    Write-Host "FAIL: source dir missing: $src"
    $failed = $true
}

foreach ($f in @("auth.py", "client.py", "endpoints.py", "browser_login.py")) {
    if (-not (Test-Path (Join-Path $src $f))) {
        Write-Host "FAIL: source file missing: $f"
        $failed = $true
    }
}

if (Test-Path $dst) {
    Write-Host "FAIL: target dir already exists: $dst"
    $failed = $true
}

if ($failed) {
    Write-Host "PRECONDITION FAILED - STOP"
} else {
    Write-Host "Preconditions OK"
}
```

## Actions

Only run this if preconditions passed.

```powershell
if (-not $failed) {
    try {
        New-Item -ItemType Directory -Path $dst -Force -ErrorAction Stop | Out-Null

        $files = @("auth.py", "client.py", "endpoints.py", "browser_login.py")
        foreach ($f in $files) {
            Copy-Item -LiteralPath (Join-Path $src $f) -Destination (Join-Path $dst $f) -Force -ErrorAction Stop
        }

        $initText = '"""Garmin integration package."""' + [Environment]::NewLine
        [System.IO.File]::WriteAllText(
            (Join-Path $dst "__init__.py"),
            $initText,
            [System.Text.UTF8Encoding]::new($false)
        )

        foreach ($file in (Get-ChildItem -LiteralPath $dst -Filter *.py -File)) {
            $text = [System.IO.File]::ReadAllText($file.FullName)
            $new = $text

            $new = $new -replace '(?m)^(\s*)from pirate_garmin import ', '$1from pifitness.integrations.garmin import '
            $new = $new -replace '(?m)^(\s*)from pirate_garmin\.', '$1from pifitness.integrations.garmin.'
            $new = $new -replace '(?m)^(\s*)import pirate_garmin\.', '$1import pifitness.integrations.garmin.'
            $new = $new -replace '(?m)^(\s*)import pirate_garmin(\s|$)', '$1import pifitness.integrations.garmin$2'

            if ($new -ne $text) {
                [System.IO.File]::WriteAllText(
                    $file.FullName,
                    $new,
                    [System.Text.UTF8Encoding]::new($false)
                )
            }
        }

        Write-Host "Actions done"
    } catch {
        Write-Host "FAIL: $_"
        $failed = $true
    }
}
```

## Verification

Run this after actions. Do not commit unless every check passes.

```powershell
$failed = $false
$dst = "backend\pifitness\integrations\garmin"

foreach ($f in @("__init__.py", "auth.py", "client.py", "endpoints.py", "browser_login.py")) {
    if (-not (Test-Path (Join-Path $dst $f))) {
        Write-Host "FAIL: expected file missing: $f"
        $failed = $true
    }
}

if (Test-Path (Join-Path $dst "cli.py")) {
    Write-Host "FAIL: cli.py must not be copied"
    $failed = $true
}

if (Test-Path (Join-Path $dst "render_endpoint.py")) {
    Write-Host "FAIL: render_endpoint.py must not be copied"
    $failed = $true
}

$bad = 0
foreach ($file in (Get-ChildItem -LiteralPath $dst -Filter *.py -File)) {
    $text = [System.IO.File]::ReadAllText($file.FullName)
    $count = ($text.ToCharArray() | Where-Object { [int]$_ -gt 127 }).Count
    if ($count -gt 0) {
        Write-Host "FAIL: non-ASCII found in $($file.Name): $count"
        $bad = $bad + 1
    }
}
if ($bad -ne 0) {
    $failed = $true
}

$refs = Select-String -Path (Join-Path $dst "*.py") -Pattern "from pirate_garmin|import pirate_garmin|from \.cli|from \.render_endpoint|import cli|import render_endpoint" -AllMatches
if ($refs) {
    Write-Host "FAIL: stale imports or dropped-module references found"
    $refs | ForEach-Object { Write-Host $_.Line }
    $failed = $true
}

& .\.venv\Scripts\python.exe -c "import pifitness.integrations.garmin; import pifitness.integrations.garmin.auth; import pifitness.integrations.garmin.client; import pifitness.integrations.garmin.endpoints; import pifitness.integrations.garmin.browser_login; print('garmin-import-ok')"
if ($LASTEXITCODE -ne 0) {
    Write-Host "FAIL: garmin import smoke test failed"
    $failed = $true
}

& .\.venv\Scripts\python.exe -m pytest -q
if ($LASTEXITCODE -ne 0) {
    Write-Host "FAIL: pytest failed"
    $failed = $true
}

$status = git status --short
Write-Host "git status --short:"
Write-Host $status

if ($failed) {
    Write-Host "VERIFICATION FAILED - DO NOT COMMIT"
} else {
    Write-Host "Verification OK - expected pytest baseline: 210 passed"
}
```

## Commit

Only run this if verification passed.

```powershell
if (-not $failed) {
    git add backend/pifitness/integrations/garmin
    git commit -m "reorg(int-1): vendor garmin integration modules [INT-1]"
    git log -1 --oneline
}
```

Do not add `docs/reorg/blocks/INT-1-report.md` to the commit. Write the report after committing.

## Report

After commit, write `docs/reorg/blocks/INT-1-report.md` using the standard block report format and print it. Include:

- HEAD before
- Dirty count before
- Branch
- ACTIONS status
- VERIFICATION command results
- DEVIATIONS
- FILES CHANGED raw `git status --short`
- COMMIT sha
- PYTEST result
- OBSERVATIONS
- OPEN QUESTIONS

## Rollback

If this block fails before commit:

```powershell
git reset --hard 505761d
Remove-Item -Recurse -Force backend\pifitness\integrations\garmin -ErrorAction SilentlyContinue
```

If this block fails after commit, report the commit sha and do not attempt automatic rollback.

## Do not

- Do not run `pip install`.
- Do not touch DB rows.
- Do not modify `backend/backend_functions/service_logins.py`.
- Do not modify `deploy/scripts/pi5_deploy.sh`.
- Do not modify `pirate-garmin_clone/`.
- Do not copy `cli.py` or `render_endpoint.py`.
- Do not wire the new package into runtime imports yet.