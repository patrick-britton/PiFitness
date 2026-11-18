<#
.SYNOPSIS
    tools/workflow/append_progress.ps1 - Legacy progress logging script.

.ROLE IN LIFECYCLE
    - Why it exists: Historical helper used to log task completion events to `docs/workflow/PROGRESS.md`.
    - Current Status: OBSOLETE / LEGACY. Modern feature development tracks task progress and logs
      directly via `python tools/workflow/fd.py log <id> ...`, `fd.py start`, and `fd.py done`.
    - Recommendation: Retained for reference, but safe to retire.
#>
param(
    [Parameter(Mandatory)][string]$Task,
    [Parameter(Mandatory)][string]$Model,
    [Parameter(Mandatory)][ValidateSet('START','DONE','DONE (human review)','BLOCKED','FORCED','REVIEW')][string]$Event,
    [Parameter(Mandatory)][string]$Evidence
)

$date = Get-Date -Format 'yyyy-MM-dd'
$line = "$date | $Task | $Model | $Event | $Evidence"
Add-Content -Path 'docs\workflow\PROGRESS.md' -Value $line -Encoding UTF8
Write-Output $line