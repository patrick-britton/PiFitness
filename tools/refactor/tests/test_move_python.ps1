# ASCII-only file. Windows PowerShell 5.1 reads .ps1 via -File using
# the system codepage when no BOM is present, so any non-ASCII character
# corrupts parsing. Use "-" instead of em/en dashes, "->" instead of arrows.

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path "$PSScriptRoot\..\..\..").Path
$py = "$repoRoot\.venv\Scripts\python.exe"
$fixture = "$repoRoot\tools\refactor\tests\fixture"
$tool = "$repoRoot\tools\refactor\move_python.py"
$pkgMap = "pkg_a=pkg_a,pkg_b=pkg_b"

$tmp = New-Item -ItemType Directory -Path "$env:TEMP\move_python_test_$((Get-Random).ToString())"
$originalDir = (Get-Location).Path

try {
    Copy-Item -Path "$fixture\*" -Destination $tmp.FullName -Recurse -Force
    Set-Location $tmp.FullName

    if (-not (Test-Path "pkg_a\foo.py")) { throw "SETUP FAIL: pkg_a\foo.py missing" }
    if (-not (Test-Path "pkg_a\bar.py")) { throw "SETUP FAIL: pkg_a\bar.py missing" }
    if (-not (Test-Path "pkg_a\qux.py")) { throw "SETUP FAIL: pkg_a\qux.py missing" }
    if (-not (Test-Path "user3.py"))     { throw "SETUP FAIL: user3.py missing" }
    if (-not (Test-Path "user4.py"))     { throw "SETUP FAIL: user4.py missing" }

    $applyOut = & $py $tool manifest.json --pkg-map $pkgMap --apply 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "apply exited $LASTEXITCODE`n$applyOut" }

    # CASE 1: move only - multi-name import preserved
    if (Test-Path "pkg_a\foo.py")        { throw "FAIL: pkg_a\foo.py still exists" }
    if (-not (Test-Path "pkg_b\foo.py")) { throw "FAIL: pkg_b\foo.py missing" }
    $u1 = Get-Content "user.py" -Raw
    if ($u1 -notmatch "from pkg_b\.foo import hello, other") {
        throw "FAIL: multi-name import not rewritten.`n$u1"
    }
    $r1 = & $py -c "import sys; sys.path.insert(0,'.'); import user; print(user.hello(), user.other())" 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0 -or $r1.Trim() -ne "hi other") {
        throw "FAIL: user import returned '$($r1.Trim())', expected 'hi other'"
    }

    # CASE 2: move + rename
    if (Test-Path "pkg_a\bar.py")        { throw "FAIL: pkg_a\bar.py still exists" }
    if (-not (Test-Path "pkg_b\baz.py")) { throw "FAIL: pkg_b\baz.py missing" }
    $u2 = Get-Content "user2.py" -Raw
    if ($u2 -notmatch "from pkg_b\.baz import bar") {
        throw "FAIL: user2.py not rewritten.`n$u2"
    }
    $r2 = & $py -c "import sys; sys.path.insert(0,'.'); import user2; print(user2.bar())" 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0 -or $r2.Trim() -ne "bar") {
        throw "FAIL: user2 import returned '$($r2.Trim())', expected 'bar'"
    }

    # CASE 3: move into new subfolder - auto-create __init__.py + import rewrite
    if (Test-Path "pkg_a\qux.py")                  { throw "FAIL: pkg_a\qux.py still exists" }
    if (-not (Test-Path "pkg_b\sub\__init__.py"))  { throw "FAIL: __init__.py not created" }
    if (-not (Test-Path "pkg_b\sub\qux.py"))       { throw "FAIL: pkg_b\sub\qux.py missing" }
    $u3 = Get-Content "user3.py" -Raw
    if ($u3 -notmatch "from pkg_b\.sub\.qux import qux") {
        throw "FAIL: user3.py not rewritten.`n$u3"
    }
    $r3 = & $py -c "import sys; sys.path.insert(0,'.'); import user3; print(user3.qux())" 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0 -or $r3.Trim() -ne "qux") {
        throw "FAIL: user3 import returned '$($r3.Trim())', expected 'qux'"
    }

    # CASE 4: alias + comment + string preservation (libcst-specific)
    if (-not (Test-Path "pkg_b\foo.py")) { throw "FAIL: CASE 4 precondition - no pkg_b/foo.py" }
    $u4 = Get-Content "user4.py" -Raw
    if ($u4 -notmatch "from pkg_b\.foo import hello as h") {
        throw "FAIL: alias import not rewritten.`n$u4"
    }
    if ($u4 -notmatch "# This is a comment mentioning from pkg_a\.foo import hello") {
        throw "FAIL: comment was incorrectly rewritten (libcst regression!).`n$u4"
    }
    if ($u4 -notmatch 'MSG = "from pkg_a\.foo import hello"') {
        throw "FAIL: string was incorrectly rewritten (libcst regression!).`n$u4"
    }
    $r4 = & $py -c "import sys; sys.path.insert(0,'.'); import user4; user4.h()" 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "FAIL: user4 import failed`n$r4" }

    Write-Host "PASS: all assertions"
    exit 0
}
finally {
    Set-Location $originalDir
    Remove-Item -Recurse -Force $tmp.FullName -ErrorAction SilentlyContinue
}