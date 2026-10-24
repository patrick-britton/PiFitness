<#
.SYNOPSIS
    Regenerates the .audit/ folder with fresh structural analysis.

.DESCRIPTION
    Rebuilds every derived audit file from the current working tree.
    Useful after any major change (moves, deletions, additions) to give
    the free IDE and Cline fresh context for planning.

    Preserves hand-maintained files (free-pass-analysis.md is never touched).

    Tier 1 (always runs, ~30-60s):
        tree, tree-filtered, tree-summary, source-files, file-count-by-dir,
        python-imports, js-imports, import-boundaries, streamlit-footprint,
        sql-files, q1/q2/q3 verification files, and cloc (if installed).

    Tier 2 (optional via -WithTools, ~2-5 min):
        culler (or vulture), knip, jscpd. Requires venv active for culler,
        npm available for knip and jscpd. Vendored packages are excluded.

.PARAMETER WithTools
    Also run Tier 2 dead-code detectors. Slower.

.PARAMETER PreserveCurrent
    Copy the current .audit/ into .audit/_backup-<timestamp>/ before
    regenerating. Useful for before/after comparison.

.EXAMPLE
    .\scripts\regenerate-audit.ps1
    Quick regeneration. ~30-60 seconds.

.EXAMPLE
    .\scripts\regenerate-audit.ps1 -WithTools
    Full regeneration including dead-code detectors. Requires venv active.

.EXAMPLE
    .\scripts\regenerate-audit.ps1 -WithTools -PreserveCurrent
    Full regeneration, keeps the prior .audit/ for comparison.
#>
[CmdletBinding()]
param(
    [switch]$WithTools,
    [switch]$PreserveCurrent
)

$ErrorActionPreference = 'Stop'

# ---- Paths ----
$RepoRoot = Split-Path -Parent $PSScriptRoot
$AuditDir = Join-Path $RepoRoot '.audit'
$LogFile  = Join-Path $AuditDir 'regenerate.log'

# ---- Ensure .audit exists ----
if (-not (Test-Path -LiteralPath $AuditDir)) {
    New-Item -ItemType Directory -Path $AuditDir -Force | Out-Null
}

# ---- Optional backup ----
if ($PreserveCurrent) {
    $ts = Get-Date -Format 'yyyyMMdd-HHmmss'
    $backupDir = Join-Path $AuditDir "_backup-$ts"
    Copy-Item -Path (Join-Path $AuditDir '*') -Destination $backupDir -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "Preserved current audit to $backupDir"
}

# ---- Transcript ----
Start-Transcript -Path $LogFile -Force | Out-Null

function Write-Step($msg) { Write-Host ''; Write-Host "==> $msg" -ForegroundColor Cyan }
function Write-Info($msg) { Write-Host "    $msg" }
function Write-Warn2($msg){ Write-Host "    WARNING: $msg" -ForegroundColor Yellow }
function Write-Ok($msg)   { Write-Host "    $msg" -ForegroundColor Green }

$TreeExcludes = 'node_modules|\.venv|\.next|\.git\\|__pycache__|dist\\|build\\|\.streamlit'

try {

# ============================== Prerequisites ==============================
Write-Step 'Prerequisites'

if (-not (Get-Command git -ErrorAction SilentlyContinue)) { throw 'git is not on PATH' }
Write-Ok 'git available'

$hasTree = [bool](Get-Command tree -ErrorAction SilentlyContinue)
if (-not $hasTree) { Write-Warn2 'tree not found - tree files will be skipped' } else { Write-Ok 'tree available' }

$hasCloc = [bool](Get-Command cloc -ErrorAction SilentlyContinue)
if (-not $hasCloc) { Write-Info 'cloc not found (install: choco install cloc) - skipping language counts' } else { Write-Ok 'cloc available' }

$venvActive    = ($env:VIRTUAL_ENV -ne $null) -or ($env:VIRTUAL_ENV_PROMPT -ne $null)
$cullerOnPath  = [bool](Get-Command culler  -ErrorAction SilentlyContinue)
$vultureOnPath = [bool](Get-Command vulture -ErrorAction SilentlyContinue)
if ($WithTools -and -not ($venvActive -or $cullerOnPath -or $vultureOnPath)) {
    Write-Warn2 'Neither culler nor vulture found, and venv is not active. Install culler globally (pipx install culler) or activate the venv.'
}

# ---- Record state ----
$gitHead   = (& git -C $RepoRoot rev-parse HEAD).Trim()
$gitBranch = (& git -C $RepoRoot rev-parse --abbrev-ref HEAD).Trim()
Write-Info "Branch: $gitBranch"
Write-Info "HEAD:   $gitHead"

# ============================== Tier 1 ==============================
Write-Step 'Tier 1 - Core structural audit'

if ($hasTree) {
    & tree /F /A | Out-File -Encoding utf8 (Join-Path $AuditDir 'tree.txt')
    Write-Ok "tree.txt"

    & tree /F /A | Select-String -NotMatch $TreeExcludes | Out-File -Encoding utf8 (Join-Path $AuditDir 'tree-filtered.txt')
    Write-Ok "tree-filtered.txt"

    & tree /F /A | Select-String -NotMatch $TreeExcludes | Select-Object -First 400 | Out-File -Encoding utf8 (Join-Path $AuditDir 'tree-summary.txt')
    Write-Ok "tree-summary.txt"
}

& git -C $RepoRoot ls-files |
    Where-Object { $_ -match '\.(py|ts|tsx|js|jsx|sql)$' } |
    Sort-Object |
    Out-File -Encoding utf8 (Join-Path $AuditDir 'source-files.txt')
Write-Ok "source-files.txt"

& git -C $RepoRoot ls-files |
    Where-Object { $_ -match '\.(py|ts|tsx|js|jsx)$' } |
    ForEach-Object { ($_ -split '/')[0..1] -join '/' } |
    Group-Object |
    Sort-Object Count -Descending |
    Format-Table Count, Name -AutoSize |
    Out-File -Encoding utf8 (Join-Path $AuditDir 'file-count-by-dir.txt')
Write-Ok "file-count-by-dir.txt"

Get-ChildItem -Path $RepoRoot -Recurse -Include *.py |
    Where-Object { $_.FullName -notmatch '\\(\.venv|node_modules|__pycache__)\\' } |
    ForEach-Object {
        $rel = $_.FullName.Replace($RepoRoot + '\', '')
        $imports = Select-String -Path $_.FullName -Pattern '^\s*(from\s+[\w\.]+\s+import|import\s+[\w\.]+)' |
            ForEach-Object { $_.Line.Trim() }
        "### $rel"
        if ($imports) { $imports | ForEach-Object { "  $_" } } else { "  (no imports)" }
        ""
    } | Out-File -Encoding utf8 (Join-Path $AuditDir 'python-imports.txt')
Write-Ok "python-imports.txt"

Get-ChildItem -Path $RepoRoot -Recurse -Include *.ts,*.tsx,*.js,*.jsx |
    Where-Object { $_.FullName -notmatch '\\(node_modules|\.next|dist|build)\\' } |
    ForEach-Object {
        $rel = $_.FullName.Replace($RepoRoot + '\', '')
        $imports = Select-String -Path $_.FullName -Pattern '^\s*import\s+' |
            ForEach-Object { $_.Line.Trim() }
        "### $rel"
        if ($imports) { $imports | ForEach-Object { "  $_" } } else { "  (no imports)" }
        ""
    } | Out-File -Encoding utf8 (Join-Path $AuditDir 'js-imports.txt')
Write-Ok "js-imports.txt"

$rows = @()
Get-ChildItem -Path $RepoRoot -Recurse -Include *.py |
    Where-Object { $_.FullName -notmatch '\\(\.venv|node_modules|__pycache__)\\' } |
    ForEach-Object {
        $rel = $_.FullName.Replace($RepoRoot + '\', '')
        $fromDir = ($rel -split '\\')[0..1] -join '/'
        Select-String -Path $_.FullName -Pattern '^\s*(from|import)\s+([\w\.]+)' |
            ForEach-Object {
                if ($_.Line -match '^\s*(?:from|import)\s+([\w\.]+)') {
                    $rows += [PSCustomObject]@{ From = $fromDir; To = $matches[1].Split('.')[0] }
                }
            }
    }
$rows | Group-Object From, To |
    ForEach-Object { [PSCustomObject]@{ Edges = $_.Count; Pair = $_.Name } } |
    Sort-Object Edges -Descending |
    Format-Table -AutoSize |
    Out-File -Encoding utf8 (Join-Path $AuditDir 'import-boundaries.txt')
Write-Ok "import-boundaries.txt"

Get-ChildItem -Path $RepoRoot -Recurse -Include *.py |
    Where-Object { $_.FullName -notmatch '\\(\.venv|node_modules|__pycache__)\\' } |
    ForEach-Object {
        $hits = Select-String -Path $_.FullName -Pattern '\bimport\s+streamlit|\bfrom\s+streamlit|\bst\.'
        if ($hits) {
            [PSCustomObject]@{
                File = $_.FullName.Replace($RepoRoot + '\', '')
                StreamlitRefs = $hits.Count
            }
        }
    } |
    Sort-Object StreamlitRefs -Descending |
    Format-Table -AutoSize |
    Out-File -Encoding utf8 (Join-Path $AuditDir 'streamlit-footprint.txt')
Write-Ok "streamlit-footprint.txt"

Get-ChildItem -Path $RepoRoot -Recurse -Include *.sql |
    Where-Object { $_.FullName -notmatch '\\(\.venv|node_modules|__pycache__)\\' } |
    Select-Object @{N='Path';E={$_.FullName.Replace($RepoRoot + '\', '')}}, Length, LastWriteTime |
    Sort-Object Path |
    Format-Table -AutoSize |
    Out-File -Encoding utf8 (Join-Path $AuditDir 'sql-files.txt')
Write-Ok "sql-files.txt"

# ============================== Phase 1 verification files ==============================
Write-Step 'Phase 1 verification files (q1 / q2 / q3)'

Get-ChildItem -Path $RepoRoot -Recurse -Include *.py |
    Where-Object { $_.FullName -notmatch '\\(\.venv|node_modules|__pycache__)\\' } |
    Select-String -Pattern 'viz_factory' |
    Select-Object Path, LineNumber, Line |
    Out-File -Encoding utf8 (Join-Path $AuditDir 'q1-viz-factory-usage.txt')
Write-Ok "q1-viz-factory-usage.txt"

# q2: POST-Phase-1, this should be EMPTY except for docstring/comment hits.
Get-ChildItem -Path (Join-Path $RepoRoot 'backend_functions') -Recurse -Include *.py |
    Select-String -Pattern '^\s*(from|import)\s+backend\.' |
    Select-Object Path, LineNumber, Line |
    Out-File -Encoding utf8 (Join-Path $AuditDir 'q2-backend-functions-imports-backend.txt')
Write-Ok "q2-backend-functions-imports-backend.txt"

Get-ChildItem -Path (Join-Path $RepoRoot 'backend_functions') -Recurse -Include *.py |
    Where-Object { $_.Name -match 'task_execution' } |
    Select-String -Pattern 'importlib|import_module|__import__' |
    Select-Object Path, LineNumber, Line |
    Out-File -Encoding utf8 (Join-Path $AuditDir 'q3-importlib-loaders.txt')
Write-Ok "q3-importlib-loaders.txt"

# ---- cloc ----
if ($hasCloc) {
    Write-Info 'Running cloc...'
    $clocTxt  = Join-Path $AuditDir 'cloc.txt'
    $clocJson = Join-Path $AuditDir 'cloc.json'
    & cloc --quiet --exclude-dir=node_modules,.venv,.next,.git,__pycache__,dist,build "--out=$clocTxt" $RepoRoot | Out-Null
    & cloc --quiet --json --exclude-dir=node_modules,.venv,.next,.git,__pycache__,dist,build $RepoRoot | Out-File -Encoding utf8 $clocJson
    Write-Ok "cloc.txt + cloc.json"
}

# ============================== Tier 2 - Dead-code ==============================
if ($WithTools) {
    Write-Step 'Tier 2 - Dead-code detectors'

    $cullerFound  = [bool](Get-Command culler  -ErrorAction SilentlyContinue)
    $vultureFound = [bool](Get-Command vulture -ErrorAction SilentlyContinue)

    # External tools write warnings to stderr; don't let that halt the script.
    $prevEAP = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'

    # Exclude vendored and generated directories from dead-code analysis.
    $cullerSources = @('--src', 'backend', '--src', 'backend_functions', '--src', 'agents', '--src', 'tests', '--src', 'scripts')

    try {
        if ($cullerFound) {
            Write-Info 'Running culler...'
            $cullerOut = Join-Path $AuditDir 'culler-report.txt'
            & culler check $RepoRoot @cullerSources 2>&1 | Out-File -Encoding utf8 $cullerOut
            Write-Ok "culler-report.txt"
        } elseif ($vultureFound) {
            Write-Info 'culler not found, running vulture...'
            $vultureOut = Join-Path $AuditDir 'vulture-report.txt'
            & vulture $RepoRoot 2>&1 | Out-File -Encoding utf8 $vultureOut
            Write-Ok "vulture-report.txt"
        } else {
            Write-Warn2 'Neither culler nor vulture found (pip install culler)'
        }
    } catch {
        Write-Warn2 "Dead-code scan failed: $_"
    } finally {
        $ErrorActionPreference = $prevEAP
    }

    $prevEAP2 = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'

    try {
        if (Get-Command knip -ErrorAction SilentlyContinue) {
            Write-Info 'Running knip...'
            $knipOut = Join-Path $AuditDir 'knip-report.txt'
            Push-Location $RepoRoot
            try { & knip 2>&1 | Out-File -Encoding utf8 $knipOut }
            finally { Pop-Location }
            Write-Ok "knip-report.txt"
        } else {
            Write-Info 'knip not on PATH (npm install -D knip) - skipping'
        }
    } catch {
        Write-Warn2 "knip failed: $_"
    }

    try {
        if (Get-Command jscpd -ErrorAction SilentlyContinue) {
            Write-Info 'Running jscpd...'
            $jscpdDir = Join-Path $AuditDir 'jscpd-report'
            if (-not (Test-Path $jscpdDir)) { New-Item -ItemType Directory -Path $jscpdDir -Force | Out-Null }
            Push-Location $RepoRoot
            try { & jscpd --min-lines 10 --output $jscpdDir --reporters json . 2>&1 | Out-Null }
            finally { Pop-Location }
            Write-Ok "jscpd-report/"
        } else {
            Write-Info 'jscpd not on PATH (npm install -g jscpd) - skipping'
        }
    } catch {
        Write-Warn2 "jscpd failed: $_"
    } finally {
        $ErrorActionPreference = $prevEAP2
    }
}

# ============================== Manifest ==============================
Write-Step 'Writing manifest'

$generatedFiles = Get-ChildItem $AuditDir -Recurse -File |
    Where-Object {
        $_.Name -ne 'regenerate.log' -and
        $_.Name -ne '_manifest.txt' -and
        $_.Name -notmatch '^free-pass-analysis\.md$' -and
        $_.FullName -notmatch '\\_backup-'
    } |
    Sort-Object FullName

$manifestPath = Join-Path $AuditDir '_manifest.txt'
$lines = @(
    "Audit manifest",
    "Generated:  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "Branch:     $gitBranch",
    "HEAD:       $gitHead",
    "",
    "Files produced by this run ($($generatedFiles.Count)):"
)
foreach ($f in $generatedFiles) {
    $rel = $f.FullName.Replace($AuditDir + '\', '')
    $lines += ("  {0,-45} {1,12:N0} bytes" -f $rel, $f.Length)
}

$lines += ""
$lines += "Preserved (not regenerated):"
$fpa = Join-Path $AuditDir 'free-pass-analysis.md'
if (Test-Path $fpa) {
    $lines += "  free-pass-analysis.md                     present"
} else {
    $lines += "  free-pass-analysis.md                     MISSING - re-paste from prior conversation if needed"
}

$lines += ""
$lines += "Phase 1 status check:"
$q2Content = Get-Content (Join-Path $AuditDir 'q2-backend-functions-imports-backend.txt') -ErrorAction SilentlyContinue |
    Where-Object { $_.Trim() -ne '' -and $_ -notmatch '^Path|^----|^C:\\' }
if ($q2Content) {
    $lines += "  Cycle status:    SEE q2 file - $($q2Content.Count) line(s) matched."
    $lines += "                   Post-Phase-1, these should only be docstring/comment references."
} else {
    $lines += "  Cycle status:    CLEAN - no reverse edges from backend_functions to backend"
}

$lines += ""
$lines += "Phase 2 targets (present in this snapshot):"
foreach ($target in @('frontend_functions', 'backend_functions\viz_factory', '.streamlit', 'pi_fitness.py')) {
    $fullPath = Join-Path $RepoRoot $target
    $present = Test-Path -LiteralPath $fullPath
    $status = if ($present) { 'PRESENT (Phase 2 delete candidate)' } else { 'absent' }
    $lines += ("  {0,-40} {1}" -f $target, $status)
}

$lines | Out-File -Encoding utf8 $manifestPath
Write-Ok "_manifest.txt"

Write-Step 'Done'
Write-Host "Audit regenerated in: $AuditDir" -ForegroundColor Green
Write-Host "Manifest:             $manifestPath" -ForegroundColor Green
Write-Host "Log:                  $LogFile" -ForegroundColor Green

} catch {
    Write-Host ''
    Write-Host "FAILED: $_" -ForegroundColor Red
    Write-Host "Log:    $LogFile" -ForegroundColor Red
    exit 1
} finally {
    Stop-Transcript | Out-Null
}