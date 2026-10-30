$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path "$PSScriptRoot\..\..\..").Path
$py = "$repoRoot\.venv\Scripts\python.exe"
$fixture = "$repoRoot\tools\refactor\tests\fixture"
$tool = "$repoRoot\tools\refactor\move_python.py"

$tmp = New-Item -ItemType Directory -Path "$env:TEMP\move_python_test_$((Get-Random).ToString())"
$originalDir = (Get-Location).Path

try {
    Copy-Item -Path "$fixture\*" -Destination $tmp.FullName -Recurse -Force
    Set-Location $tmp.FullName

    # Preconditions
    if (-not (Test-Path "pkg_a\foo.py")) { throw "SETUP FAIL: pkg_a\foo.py missing" }
    if (Test-Path "pkg_b\foo.py")        { throw "SETUP FAIL: pkg_b\foo.py already exists" }

    # Dry-run first (should NOT move)
    $dryOut = & $py $tool manifest.json --dry-run 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "dry-run exited $LASTEXITCODE`n$dryOut" }
    if (-not (Test-Path "pkg_a\foo.py")) { throw "FAIL: dry-run moved pkg_a\foo.py" }

    # Apply
    $applyOut = & $py $tool manifest.json --apply 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "apply exited $LASTEXITCODE`n$applyOut" }

    # Assertions
    if (Test-Path "pkg_a\foo.py")     { throw "FAIL: pkg_a\foo.py still exists" }
    if (-not (Test-Path "pkg_b\foo.py")) { throw "FAIL: pkg_b\foo.py missing" }

    $userContent = Get-Content "user.py" -Raw
    if ($userContent -notmatch "from pkg_b\.foo import hello") {
        throw "FAIL: user.py import not rewritten.`n---user.py---`n$userContent"
    }

    $result = & $py -c "import sys; sys.path.insert(0,'.'); import user; print(user.hello())" 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "FAIL: import user failed`n$result" }
    if ($result.Trim() -ne "hi") { throw "FAIL: import user returned '$($result.Trim())', expected 'hi'" }

    Write-Host "PASS: all assertions"
    exit 0
}
finally {
    Set-Location $originalDir
    Remove-Item -Recurse -Force $tmp.FullName -ErrorAction SilentlyContinue
}
