param(
    [string]$BaseUrl = "http://127.0.0.1:8787"
)

$ErrorActionPreference = "Stop"

if ($BaseUrl.EndsWith("/")) {
    $BaseUrl = $BaseUrl.Substring(0, $BaseUrl.Length - 1)
}

$health = Invoke-RestMethod -Method Get -Uri "$BaseUrl/health" -TimeoutSec 10
$source = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/source" -TimeoutSec 10
$snapshot = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/snapshot?range=1d" -TimeoutSec 20

Write-Host "=== /health ==="
$health | ConvertTo-Json -Depth 4

Write-Host "=== /v1/source ==="
$source | ConvertTo-Json -Depth 4

Write-Host "=== /v1/snapshot summary ==="
[PSCustomObject]@{
    account = $snapshot.accountMeta.login
    server = $snapshot.accountMeta.server
    source = $snapshot.accountMeta.source
    curvePoints = @($snapshot.curvePoints).Count
    positions = @($snapshot.positions).Count
    trades = @($snapshot.trades).Count
} | ConvertTo-Json -Depth 3
