# Cline PreToolUse hook - .clineignore guard, v2 (PowerShell)
#
# What changed vs. v1 (see the write-up for reasons):
#   * Blocks carry the exact rule (.clineignore line + pattern) in BOTH errorMessage (user) and
#     contextModification (the only field that reaches the model).
#   * Every call is logged to %TEMP%\cline-clineignore-hook.log (override: $env:CLINE_HOOK_LOG).
#   * Shell commands are parsed into segments/words instead of "every token is a path":
#       - the program word is never an operand (so .venv\Scripts\python.exe is fine)
#       - assignments ($py = ".venv\...") and prose (commit messages, log text) are not operands
#       - path-only operations (git rm/mv/add/commit/ls-files/..., Remove-Item, Move-Item, ...)
#         on ignored paths are ALLOWED: they never put file contents in front of the model
#       - readers (cat/type/Get-Content/Select-String/git diff/...) on ignored paths are blocked
#       - unknown programs are blocked only if an operand is ignored AND exists on disk
#       - inline code (python -c, pwsh -Command, cmd /c, ...) is scanned, not ignored
#   * .clineignore is protected against WRITES only (reading/mentioning/committing it is fine).
#   * One git process per call (check-ignore --stdin -z -v), persistent scratch repo, no git init per call.
#   * Directory operands match directory-only patterns (".venv/", ".audit/").
#   * Always prints a JSON verdict. Internal errors fail OPEN but are logged.
#   * Dry-run: set CLINE_HOOK_MODE=warn (or edit $DefaultMode) to log + warn instead of cancel.

$ErrorActionPreference = 'Continue'
$DefaultMode = 'block'     # 'block' | 'warn'
$script:Mode    = if ($env:CLINE_HOOK_MODE) { $env:CLINE_HOOK_MODE.ToLowerInvariant() } else { $DefaultMode }
$script:LogPath = if ($env:CLINE_HOOK_LOG)  { $env:CLINE_HOOK_LOG } else { Join-Path ([IO.Path]::GetTempPath()) 'cline-clineignore-hook.log' }
$script:Cands   = New-Object System.Collections.ArrayList   # path operands to test against .clineignore
$script:CiWrites = New-Object System.Collections.ArrayList  # attempted writes to .clineignore (text notes)

$FILE_TOOLS    = @('read_files','read_file','editor','apply_patch','write_to_file','replace_in_file','list_files')
$SHELL_TOOLS   = @('run_commands','execute_command')
$READ_ONLY_TOOLS = @('read_files','read_file','list_files')

# Programs whose operands are read/copied/displayed -> ignored operand = block.
$READERS = @('cat','type','gc','get-content','more','less','head','tail','bat','sls','select-string','findstr',
    'grep','rg','ag','ack','sed','awk','od','xxd','hexdump','format-hex','strings','tac','nl','jq','yq',
    'copy','cp','copy-item','xcopy','robocopy','code','notepad','start','start-process','ii','invoke-item',
    'explorer','import-csv','certutil','tar','zip','7z','compress-archive','scp','curl','wget')
# Programs that write their operands.
$WRITERS = @('set-content','sc','add-content','ac','out-file','tee-object','tee','clear-content')
# Pure file-management / navigation: they never emit file contents -> operands are not checked.
$PATHONLY = @('rm','del','erase','rd','rmdir','remove-item','ri','mv','move','move-item','mi','ren','rename',
    'rename-item','rni','mkdir','md','new-item','ni','test-path','ls','dir','gci','get-childitem','get-item','gi',
    'cd','chdir','set-location','sl','push-location','pushd','pop-location','popd',
    'echo','write-output','write-host','get-location','pwd')
# Programs that can mutate a file given as operand (for .clineignore protection).
$MUTATORS = @('rm','del','erase','remove-item','ri','mv','move','move-item','mi','ren','rename','rename-item',
    'rni','set-content','sc','add-content','ac','out-file','clear-content','tee-object','tee','new-item','ni',
    'copy','cp','copy-item')
$GIT_READERS  = @('diff','show','blame','annotate','cat-file','grep','difftool','archive','format-patch')
$GIT_MUTATORS = @('rm','mv','checkout','restore','clean','reset')

function Write-HookLog([string]$line) {
    try {
        $fi = Get-Item -LiteralPath $script:LogPath -ErrorAction SilentlyContinue
        if ($fi -and $fi.Length -gt 1MB) { Move-Item -LiteralPath $script:LogPath -Destination ($script:LogPath + '.1') -Force }
        Add-Content -LiteralPath $script:LogPath -Value ("{0} {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff'), $line) -Encoding UTF8
    } catch {}
}

function Emit([bool]$cancel, [string]$errorMessage, [string]$context) {
    $o = [ordered]@{ cancel = $cancel }
    if ($context)      { $o.contextModification = $context }
    if ($errorMessage) { $o.errorMessage = $errorMessage }
    [Console]::Out.Write(($o | ConvertTo-Json -Compress))
}

function ConvertFrom-MaybeJson($value) {
    if ($value -is [string]) {
        $t = $value.Trim()
        if ($t.Length -gt 1 -and ($t[0] -eq '"' -or $t[0] -eq '[' -or $t[0] -eq '{')) {
            try { return ($t | ConvertFrom-Json) } catch { return $value }
        }
    }
    return $value
}

function Add-PathsFromValue($value, $acc) {
    if ($null -eq $value) { return }
    $value = ConvertFrom-MaybeJson $value
    if ($value -is [string]) { if (-not [string]::IsNullOrWhiteSpace($value)) { [void]$acc.Add($value) }; return }
    if ($value -is [System.Collections.IEnumerable] -and -not ($value -is [string])) {
        foreach ($item in $value) { Add-PathsFromValue $item $acc }
        return
    }
    if ($value -is [System.Management.Automation.PSCustomObject]) {
        foreach ($name in @('path','file_path','filePath','file','files','paths','file_paths')) {
            if ($value.PSObject.Properties.Name -contains $name) { Add-PathsFromValue $value.$name $acc }
        }
    }
}

function Get-CommandStrings($value) {
    $value = ConvertFrom-MaybeJson $value
    if ($null -eq $value) { return @() }
    if ($value -is [string]) { return @($value) }
    if ($value -is [System.Collections.IEnumerable] -and -not ($value -is [string])) {
        $acc = @(); foreach ($item in $value) { $acc += Get-CommandStrings $item }; return $acc
    }
    if ($value -is [System.Management.Automation.PSCustomObject]) {
        $acc = @()
        foreach ($name in @('command','cmd','commands')) {
            if ($value.PSObject.Properties.Name -contains $name) { $acc += Get-CommandStrings $value.$name }
        }
        if ($value.PSObject.Properties.Name -contains 'args') { $acc += Get-CommandStrings $value.args }
        return $acc
    }
    return @()
}

# ---- path helpers (pure string logic, backslash-normalised) -------------------------------------
function Normalize-AbsPath([string]$path, [string]$root) {
    if ([string]::IsNullOrWhiteSpace($path)) { return $null }
    $p = $path.Trim().Trim('"').Trim("'")
    if ($p -notmatch '^([A-Za-z]:|[\\/])') { $p = $root.TrimEnd('\','/') + '\' + $p }
    $p = $p -replace '/', '\'
    $drive = ''
    if ($p -match '^([A-Za-z]):') { $drive = $matches[1] + ':' }
    $parts = New-Object System.Collections.Generic.List[string]
    foreach ($seg in $p.Split('\')) {
        if ($seg -eq '' -or $seg -eq '.' -or $seg -match '^[A-Za-z]:$') { continue }
        if ($seg -eq '..') { if ($parts.Count -gt 0) { $parts.RemoveAt($parts.Count - 1) }; continue }
        $parts.Add($seg)
    }
    if ($drive) { if ($parts.Count -eq 0) { return $drive + '\' }; return $drive + '\' + ($parts -join '\') }
    if ($parts.Count -eq 0) { return '\' }
    return '\' + ($parts -join '\')
}

function Get-RelPath([string]$token) {   # relative path with '/', '' for the root itself, $null if outside root
    $abs = Normalize-AbsPath $token $script:Root
    if (-not $abs) { return $null }
    if ($abs.Equals($script:RootNorm, [StringComparison]::OrdinalIgnoreCase)) { return '' }
    if (-not $abs.StartsWith($script:RootNorm + '\', [StringComparison]::OrdinalIgnoreCase)) { return $null }
    return ($abs.Substring($script:RootNorm.Length + 1) -replace '\\', '/')
}

function Test-OnDisk([string]$rel) {   # 'dir' | 'file' | $null
    try {
        $fs = ($script:RootNorm + '\' + ($rel -replace '/', '\')) -replace '\\', [string][IO.Path]::DirectorySeparatorChar
        if (Test-Path -LiteralPath $fs -PathType Container) { return 'dir' }
        if (Test-Path -LiteralPath $fs -PathType Leaf)      { return 'file' }
    } catch {}
    return $null
}

function Test-Skippable([string]$w) {   # variables, env refs, illegal chars, pathspec magic, absurd length
    if ([string]::IsNullOrWhiteSpace($w) -or $w.Length -gt 400) { return $true }
    if ($w -match '[\$%<>|"\x00-\x1f]') { return $true }
    if ($w.StartsWith(':') -or $w.StartsWith('!')) { return $true }   # pathspec magic, glob negation
    return $false
}

# ---- tiny shell tokenizer: segments -> words {T=text; Q=was quoted; Redir=redirect target} -------
function Split-Command([string]$text) {
    $segments = New-Object System.Collections.ArrayList
    $words    = New-Object System.Collections.ArrayList
    $sb = New-Object System.Text.StringBuilder
    $q = ''; $has = $false; $quoted = $false; $redir = $false
    $endWord = {
        if ($has) { [void]$words.Add([pscustomobject]@{ T = $sb.ToString(); Q = $quoted; Redir = $redir }); $redir = $false }
        [void]$sb.Clear(); $has = $false; $quoted = $false
    }
    $endSeg = {
        . $endWord
        if ($words.Count -gt 0) { [void]$segments.Add($words.ToArray()); $words.Clear() }
        $redir = $false
    }
    $n = $text.Length
    for ($i = 0; $i -lt $n; $i++) {
        $c = $text[$i]; $cs = [string]$c
        if ($q) {
            if ($cs -ceq $q) {
                if ($i + 1 -lt $n -and $text[$i + 1] -eq $c) { [void]$sb.Append($c); $i++ } else { $q = '' }
            }
            elseif ($c -eq '\' -and $q -eq '"' -and $i + 1 -lt $n -and $text[$i + 1] -eq '"') { [void]$sb.Append('"'); $i++ }
            elseif ($c -eq '`' -and $q -eq '"' -and $i + 1 -lt $n) { [void]$sb.Append($text[$i + 1]); $i++ }
            else { [void]$sb.Append($c) }
            continue
        }
        if ($c -eq "'" -or $c -eq '"') { $q = $cs; $has = $true; $quoted = $true; continue }
        if ($c -eq '`' -and $i + 1 -lt $n) { [void]$sb.Append($text[$i + 1]); $has = $true; $i++; continue }
        if ([char]::IsWhiteSpace($c)) { if ($c -eq "`n" -or $c -eq "`r") { . $endSeg } else { . $endWord }; continue }
        if ($c -eq ';' -or $c -eq '|' -or $c -eq '&' -or $c -eq '(' -or $c -eq ')' -or $c -eq '{' -or $c -eq '}') { . $endSeg; continue }
        if ($c -eq '>' -or $c -eq '<') {
            if ($has -and -not $quoted -and $sb.ToString() -match '^\d+$') { [void]$sb.Clear(); $has = $false }   # 2> prefix
            . $endWord
            $isWrite = ($c -eq '>')
            if ($i + 1 -lt $n -and $text[$i + 1] -eq '>') { $i++ }
            if ($i + 1 -lt $n -and $text[$i + 1] -eq '&') {   # 2>&1
                $i++; while ($i + 1 -lt $n -and [char]::IsDigit($text[$i + 1])) { $i++ }
                continue
            }
            if ($isWrite) { $redir = $true }
            continue
        }
        [void]$sb.Append($c); $has = $true
    }
    . $endSeg
    return ,$segments
}

function Get-ProgName([string]$w) {
    $x = ($w -replace '\\', '/').TrimEnd('/')
    $x = $x.Substring($x.LastIndexOf('/') + 1).ToLowerInvariant()
    return ($x -replace '\.(exe|cmd|bat|ps1|com)$', '')
}

function Add-Cand([string]$raw, [string]$mode, [bool]$needExist, [string]$prog) {
    [void]$script:Cands.Add([pscustomobject]@{ Raw = $raw; Mode = $mode; NeedExist = $needExist; Prog = $prog })
}

function Test-ExecutableName([string]$w) {   # no extension, or an executable/script extension
    return ($w -notmatch '\.[A-Za-z0-9_]+$' -or $w -match '(?i)\.(exe|cmd|bat|com|ps1|sh)$')
}

function Expand-Wildcard([string]$rel) {     # wildcard in the LAST path segment only; max 200 hits
    if (-not $rel.Contains('/')) { $dirRel = ''; $leaf = $rel }
    else { $dirRel = $rel.Substring(0, $rel.LastIndexOf('/')); $leaf = $rel.Substring($rel.LastIndexOf('/') + 1) }
    if ($leaf -notmatch '[\*\?]' -or $dirRel -match '[\*\?]') { return @() }
    $dirFs = $script:RootNorm
    if ($dirRel) { $dirFs += '\' + ($dirRel -replace '/', '\') }
    $dirFs = $dirFs -replace '\\', [string][IO.Path]::DirectorySeparatorChar
    $prefix = ''; if ($dirRel) { $prefix = $dirRel + '/' }
    try {
        if (-not [IO.Directory]::Exists($dirFs)) { return @() }
        return @([IO.Directory]::GetFileSystemEntries($dirFs, $leaf) | Select-Object -First 200 |
                 ForEach-Object { $prefix + [IO.Path]::GetFileName($_) })
    } catch { return @() }
}

function Test-IsClineignorePath([string]$token) {
    if (Test-Skippable $token) { return $false }
    $rel = Get-RelPath $token
    return ($null -ne $rel -and ($rel -ieq '.clineignore' -or $rel.EndsWith('/.clineignore', [StringComparison]::OrdinalIgnoreCase)))
}

function Invoke-CodeScan([string]$code) {   # inline python/node/... : only operands that exist on disk count
    foreach ($t in ($code -split '[\s;|&()<>,=\[\]{}''"]+')) {
        if (-not $t -or $t.StartsWith('-') -or (Test-Skippable $t)) { continue }
        Add-Cand $t 'unknown' $true 'inline-code'
    }
    if ($code -match '\.clineignore' -and $code -match '(?i)\b(write|write_text|write_bytes|unlink|remove|rename|replace|rmtree|truncate)\b|open\([^)]*,\s*[''"][wax+]') {
        [void]$script:CiWrites.Add('inline code that appears to write .clineignore')
    }
}

function Invoke-SegmentAnalysis($seg, [int]$depth) {
    $w = @($seg)
    if ($w.Count -eq 0) { return }
    $first = $w[0].T
    # assignment statements ($py = "...", $x += ...) never read anything
    if ($first -match '^\$[\w:\.]+(=.*)?$' -and ($w.Count -eq 1 -or $first -match '=' -or $w[1].T -match '^[+\-*/]?=$')) {
        $vals = @(); if ($first -match '=(.+)$') { $vals += $matches[1] }
        for ($j = 1; $j -lt $w.Count; $j++) { if ($w[$j].T -notmatch '^[+\-*/]?=$') { $vals += $w[$j].T } }
        foreach ($v in $vals) { if (-not (Test-Skippable $v) -and -not (Test-ExecutableName $v)) { Add-Cand $v 'unknown' $true 'assignment' } }
        return
    }
    $prog = Get-ProgName $first
    # a "program" word that is really a data file, e.g. [IO.File]::ReadAllText('x.json') splits into a bare 'x.json' segment
    if (-not (Test-Skippable $first) -and -not (Test-ExecutableName $first)) { Add-Cand $first 'unknown' $true 'quoted-word' }
    $rest = @(); if ($w.Count -gt 1) { $rest = @($w[1..($w.Count - 1)]) }

    # inline code / nested shells
    $flags = $null; $kind = $null
    if     ($prog -match '^(python[\d.]*|py|pythonw)$') { $flags = @('-c'); $kind = 'code' }
    elseif ($prog -match '^(node|nodejs)$')             { $flags = @('-e','-p','--eval','--print'); $kind = 'code' }
    elseif ($prog -match '^(perl|ruby)$')               { $flags = @('-e'); $kind = 'code' }
    elseif ($prog -match '^(powershell|pwsh)$')         { $flags = @('-c','-command'); $kind = 'shell' }
    elseif ($prog -match '^(bash|sh|zsh)$')             { $flags = @('-c'); $kind = 'shell' }
    elseif ($prog -eq 'cmd')                            { $flags = @('/c','/k'); $kind = 'shell' }
    if ($flags) {
        for ($i = 0; $i -lt $rest.Count; $i++) {
            if (-not $rest[$i].Q -and ($flags -contains $rest[$i].T.ToLowerInvariant())) {
                $after = @(); if ($i + 1 -lt $rest.Count) { $after = @($rest[($i + 1)..($rest.Count - 1)]) }
                $text = (($after | ForEach-Object { $_.T }) -join ' ')
                if ($kind -eq 'shell') { Invoke-CommandAnalysis $text ($depth + 1) } else { Invoke-CodeScan $text }
                return
            }
        }
    }

    $mode = 'unknown'; $sub = $null; $mutator = ($MUTATORS -contains $prog)
    if ($prog -eq 'git') {
        $i = 0
        while ($i -lt $rest.Count) {
            $t = $rest[$i].T
            if ($t -ceq '-C' -or $t -ceq '-c' -or $t -eq '--git-dir' -or $t -eq '--work-tree') { $i += 2; continue }
            if ($t.StartsWith('-')) { $i++; continue }
            $sub = $t.ToLowerInvariant(); break
        }
        $patchFlag = $false
        foreach ($r in $rest) { if ($r.T -match '^(-p|-u|--patch|--cc|-S.*|-G.*|--textconv)$') { $patchFlag = $true } }
        if (($GIT_READERS -contains $sub) -or (($sub -eq 'log' -or $sub -eq 'stash' -or $sub -eq 'reflog') -and $patchFlag)) { $mode = 'read' }
        else { $mode = 'pathonly' }
        $mutator = ($GIT_MUTATORS -contains $sub)
    }
    elseif ($READERS   -contains $prog) { $mode = 'read' }
    elseif ($WRITERS   -contains $prog) { $mode = 'write' }
    elseif ($PATHONLY  -contains $prog) { $mode = 'pathonly' }
    if ($prog -eq 'sed' -and (@($rest | Where-Object { $_.T -match '^-i' }).Count -gt 0)) { $mutator = $true }

    $grepLike = ($prog -match '^(grep|egrep|fgrep|rg|ag|ack|findstr|sls|select-string)$')
    $patFlagSeen = $false; $firstPosSkipped = $false; $skipNext = $false
    if ($grepLike) {
        foreach ($r in $rest) { if (-not $r.Q -and $r.T.ToLowerInvariant() -in @('-pattern','-e','--regexp','-regexp')) { $patFlagSeen = $true } }
    }
    foreach ($word in $rest) {
        $t = $word.T
        if ($grepLike -and -not $word.Q -and $t.ToLowerInvariant() -in @('-pattern','-e','--regexp','-regexp','-g','--glob','--iglob','-t','--type','-T','--type-not','--exclude','--include','--exclude-dir','-A','-B','-C','-m','--max-count','-context')) { $skipNext = $true; continue }
        if (-not $word.Q -and $t.StartsWith('-')) {                                # flags; -Path:x / --file=x carry a value
            # only PowerShell's -Path:value form on read/write verbs; --exclude-dir=.venv / --ignore=.venv are exclusions
            if (($mode -eq 'read' -or $mode -eq 'write') -and $t -match '^-[A-Za-z]\w*:(.+)$') { $t = $matches[1] } else { continue }
        }
        elseif (-not $word.Q -and $t -match '^/[A-Za-z?]+(:.*)?$') { continue }    # cmd-style /s /i /c:text
        if ($grepLike -and -not $word.Redir) {                                      # the search pattern is not a path
            if ($skipNext) { $skipNext = $false; continue }
            if (-not $patFlagSeen -and -not $firstPosSkipped) { $firstPosSkipped = $true; continue }
        }
        if (-not $word.Q -and $t -match '^[^=\\/]+=(.+)$') { $t = $matches[1] }   # name=value -> value
        if ($word.Redir) {
            if (-not (Test-Skippable $t)) {
                if (Test-IsClineignorePath $t) { [void]$script:CiWrites.Add("redirect into $t") }
                Add-Cand $t 'write' $false 'redirect'
            }
            continue
        }
        if ($t -match '\s') {                                                      # prose or a quoted path with spaces
            if ($mode -ne 'pathonly' -and $t.Length -lt 300 -and $t -notmatch '[<>|"\x00-\x1f\$]') { Add-Cand $t $mode $true $prog }
            continue
        }
        if (Test-Skippable $t) { continue }
        if ($mutator -and (Test-IsClineignorePath $t)) { [void]$script:CiWrites.Add("$prog $sub on $t".Replace('  ', ' ')) }
        if ($mode -eq 'pathonly') { continue }
        Add-Cand $t $mode ($mode -eq 'unknown') $prog
    }
}

function Invoke-CommandAnalysis([string]$text, [int]$depth) {
    if ($depth -gt 3 -or [string]::IsNullOrWhiteSpace($text)) { return }
    foreach ($seg in (Split-Command $text)) { Invoke-SegmentAnalysis $seg $depth }
}

# ---- git check-ignore (single process, NUL-delimited, verbose) --------------------------------
function Invoke-Native([string]$exe, [string]$arguments, [byte[]]$stdin) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $exe; $psi.Arguments = $arguments
    $psi.UseShellExecute = $false; $psi.CreateNoWindow = $true
    $psi.RedirectStandardInput = $true; $psi.RedirectStandardOutput = $true; $psi.RedirectStandardError = $true
    $psi.StandardOutputEncoding = New-Object System.Text.UTF8Encoding($false)
    $psi.StandardErrorEncoding  = New-Object System.Text.UTF8Encoding($false)
    $p = [System.Diagnostics.Process]::Start($psi)
    $errTask = $p.StandardError.ReadToEndAsync()
    if ($stdin) { $p.StandardInput.BaseStream.Write($stdin, 0, $stdin.Length); $p.StandardInput.BaseStream.Flush() }
    $p.StandardInput.Close()
    $out = $p.StandardOutput.ReadToEnd()
    if (-not $p.WaitForExit(15000)) { try { $p.Kill() } catch {}; throw "timeout: $exe $arguments" }
    return [pscustomobject]@{ Out = $out; Err = $errTask.Result; Code = $p.ExitCode }
}

function Get-IgnoreHits([string[]]$probes, [string]$ignoreFile) {   # probe path -> 'line N: pattern'
    $git = (Get-Command git -ErrorAction SilentlyContinue | Select-Object -First 1).Source
    if (-not $git) { throw 'git not found on PATH (hook cannot evaluate .clineignore)' }
    $scratch = Join-Path ([IO.Path]::GetTempPath()) 'cline-clineignore-hook-scratch'
    if (-not (Test-Path -LiteralPath (Join-Path $scratch '.git\HEAD'))) {
        New-Item -ItemType Directory -Path $scratch -Force | Out-Null
        [void](Invoke-Native $git ('init -q "{0}"' -f $scratch) $null)
    }
    $ig = $ignoreFile -replace '\\', '/'
    $args1 = '-C "{0}" -c "core.excludesFile={1}" check-ignore --no-index -z -v --stdin' -f $scratch, $ig
    $bytes = (New-Object System.Text.UTF8Encoding($false)).GetBytes((($probes -join "`0") + "`0"))
    $r = Invoke-Native $git $args1 $bytes
    if ($r.Code -gt 1) { throw "git check-ignore failed ($($r.Code)): $($r.Err.Trim())" }
    $hits = @{}
    $f = $r.Out.Split([char]0)
    for ($i = 0; $i + 3 -lt $f.Count; $i += 4) {
        $pattern = $f[$i + 2]
        if ($pattern.StartsWith('!')) { continue }          # negated pattern = explicitly NOT ignored
        $hits[$f[$i + 3]] = ('line {0}: {1}' -f $f[$i + 1], $pattern)
    }
    return $hits
}

# =============================================================================================
$sw = [Diagnostics.Stopwatch]::StartNew()
$tool = '?'
try {
    $stdin = New-Object System.IO.StreamReader([Console]::OpenStandardInput(), (New-Object System.Text.UTF8Encoding($false)))
    $raw = $stdin.ReadToEnd()
    if ([string]::IsNullOrWhiteSpace($raw)) { Emit $false '' ''; exit 0 }
    $payload = $raw | ConvertFrom-Json

    $params = $null
    if ($payload.PSObject.Properties.Name -contains 'tool_call' -and $payload.tool_call) {
        $tc = $payload.tool_call; $tool = ''
        if ($tc.PSObject.Properties.Name -contains 'name')  { $tool = [string]$tc.name }
        if ($tc.PSObject.Properties.Name -contains 'input') { $params = $tc.input }
    }
    if (($tool -eq '?' -or [string]::IsNullOrWhiteSpace($tool)) -and
        $payload.PSObject.Properties.Name -contains 'preToolUse' -and $payload.preToolUse) {
        $pre = $payload.preToolUse; $tool = ''
        foreach ($k in @('toolName','tool','name')) {
            if ($pre.PSObject.Properties.Name -contains $k -and $pre.$k) { $tool = [string]$pre.$k; break }
        }
        foreach ($k in @('parameters','input')) {
            if ($pre.PSObject.Properties.Name -contains $k -and $null -ne $pre.$k) { $params = $pre.$k; break }
        }
    }
    if ([string]::IsNullOrWhiteSpace($tool) -or $tool -eq '?' -or (($FILE_TOOLS + $SHELL_TOOLS) -notcontains $tool)) { Emit $false '' ''; exit 0 }

    $root = $null; $wr = $null
    if ($payload.PSObject.Properties.Name -contains 'workspaceRoots') { $wr = $payload.workspaceRoots }
    if ($wr -is [string]) { $root = $wr } elseif ($wr -and @($wr).Count -gt 0) { $root = [string]@($wr)[0] }
    if ([string]::IsNullOrWhiteSpace($root)) { $root = (Get-Location).Path }
    $script:Root = $root
    $script:RootNorm = Normalize-AbsPath $root $root
    $ignoreFile = Join-Path $root '.clineignore'
    if (-not (Test-Path -LiteralPath $ignoreFile)) { Emit $false '' ''; exit 0 }

    # ---- collect candidates -------------------------------------------------------------
    $cmdText = ''
    if ($SHELL_TOOLS -contains $tool) {
        $cmdText = (Get-CommandStrings $params) -join "`n"
        Invoke-CommandAnalysis $cmdText 0
    } else {
        $paths = New-Object System.Collections.ArrayList
        if ($tool -eq 'apply_patch') {
            $body = ''
            if ($params -is [string]) { $body = $params }
            elseif ($params -and $params.PSObject.Properties.Name -contains 'input') { $body = [string](ConvertFrom-MaybeJson $params.input) }
            foreach ($line in ($body -split "`n")) {
                if ($line -match '^\*\*\* (Add File|Update File|Delete File|Move to):\s*(.+)$') { [void]$paths.Add($matches[2].Trim()) }
            }
        } else { Add-PathsFromValue $params $paths }
        foreach ($p in $paths) {
            if ($p -match '[<>|"\x00-\x1f]' -or $p.StartsWith(':')) { continue }
            if (($READ_ONLY_TOOLS -notcontains $tool) -and (Test-IsClineignorePath $p)) { [void]$script:CiWrites.Add("$tool on $p") }
            Add-Cand $p 'file' $false $tool
        }
    }

    # ---- verdict 1: .clineignore must not be modified --------------------------------------
    $reason = $null
    if ($script:CiWrites.Count -gt 0) {
        $reason = "modifying .clineignore is not allowed ($($script:CiWrites[0])). Reading it, listing it, or committing it is fine."
    }

    # ---- verdict 2: ignored operands ------------------------------------------------------
    if (-not $reason -and $script:Cands.Count -gt 0) {
        $items = New-Object System.Collections.ArrayList
        foreach ($c in $script:Cands) {
            $rel0 = Get-RelPath $c.Raw
            if ($null -eq $rel0 -or $rel0 -eq '') { continue }
            $rels = @($rel0)
            if ($rel0 -match '[\*\?]') { $rels = @(Expand-Wildcard $rel0) + @($rel0) }
            foreach ($rel in $rels) {
                $kindOnDisk = Test-OnDisk $rel
                if ($c.NeedExist -and -not $kindOnDisk) { continue }
                $probe = if ($kindOnDisk -eq 'dir') { $rel.TrimEnd('/') + '/_' } else { $rel }
                [void]$items.Add([pscustomobject]@{ C = $c; Rel = $rel; Probe = $probe })
            }
        }
        if ($items.Count -gt 0) {
            $hits = Get-IgnoreHits @($items | ForEach-Object { $_.Probe } | Select-Object -Unique) $ignoreFile
            foreach ($it in $items) {
                if ($hits.ContainsKey($it.Probe)) {
                    $rule = $hits[$it.Probe]
                    $what = switch ($it.C.Prog) {
                        'inline-code' { 'inline code references' }
                        'assignment'  { 'a variable assignment references' }
                        'quoted-word' { 'the command references' }
                        'redirect'    { 'a redirect would write' }
                        default {
                            switch ($it.C.Mode) {
                                'file'  { "$tool on" }
                                'read'  { "'$($it.C.Prog)' would read" }
                                'write' { "'$($it.C.Prog)' would write" }
                                default { "'$($it.C.Prog)' was given" }
                            }
                        }
                    }
                    $reason = "$what '$($it.Rel)', which is excluded by .clineignore ($rule)."
                    break
                }
            }
        }
    }
    $sw.Stop()

    if (-not $reason) {
        Write-HookLog ("ALLOW tool={0} cands={1} ms={2}" -f $tool, $script:Cands.Count, $sw.ElapsedMilliseconds)
        Emit $false '' ''
        exit 0
    }

    $snippet = if ($cmdText) { ($cmdText -replace '\s+', ' '); } else { '' }
    if ($snippet.Length -gt 240) { $snippet = $snippet.Substring(0, 240) + '...' }
    Write-HookLog ("{0} tool={1} ms={2} reason={3} cmd={4}" -f $(if ($script:Mode -eq 'warn') { 'WARN ' } else { 'BLOCK' }), $tool, $sw.ElapsedMilliseconds, $reason, $snippet)

    $guide = "Hook policy: path-only operations (git rm/mv/add/commit/status/ls-files/check-ignore, Remove-Item, Move-Item) on ignored paths are allowed; reading, editing, or passing ignored files to other programs is not. Do not retry the same call. If this is required by the plan, stop and ask the user to adjust .clineignore or the hook."
    if ($script:Mode -eq 'warn') {
        Emit $false '' "[clineignore-hook WARN-ONLY] Would have blocked $tool`: $reason"
    } else {
        Emit $true "Blocked $tool`: $reason" "BLOCKED by clineignore-hook: $reason $guide"
    }
    exit 0
}
catch {
    Write-HookLog ("ERROR tool={0} {1} @ line {2}" -f $tool, $_.Exception.Message, $_.InvocationInfo.ScriptLineNumber)
    Emit $false '' ''
    exit 0
}