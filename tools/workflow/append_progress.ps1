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