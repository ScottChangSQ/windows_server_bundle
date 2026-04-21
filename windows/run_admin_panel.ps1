param(
    [string]$RepoRoot = "",
    [string]$BundleRoot = "",
    [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Stop"

# 解析管理面板所在目录，兼容“完整仓库根目录”和“部署包根目录”。
function Resolve-GatewayDir {
    param(
        [string]$RepoRootValue,
        [string]$BundleRootValue
    )

    $candidates = @()
    if (-not [string]::IsNullOrWhiteSpace($BundleRootValue)) {
        $candidates += [PSCustomObject]@{ Type = "bundle"; Root = $BundleRootValue }
    }
    if (-not [string]::IsNullOrWhiteSpace($RepoRootValue)) {
        $candidates += [PSCustomObject]@{ Type = "repo"; Root = $RepoRootValue }
    }
    if ($candidates.Count -eq 0) {
        $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
        $bundleCandidate = Split-Path -Parent $scriptDir
        $candidates += [PSCustomObject]@{ Type = "bundle"; Root = $bundleCandidate }
        $repoCandidate = (Resolve-Path (Join-Path $scriptDir "..\..\..")).Path
        $candidates += [PSCustomObject]@{ Type = "repo"; Root = $repoCandidate }
    }

    foreach ($candidate in $candidates) {
        if (-not (Test-Path $candidate.Root)) {
            continue
        }
        $resolvedRoot = (Resolve-Path $candidate.Root).Path
        if ($candidate.Type -eq "bundle") {
            $gatewayDir = Join-Path $resolvedRoot "mt5_gateway"
        }
        else {
            $gatewayDir = Join-Path $resolvedRoot "bridge\mt5_gateway"
        }
        if (Test-Path $gatewayDir) {
            return $gatewayDir
        }
    }

    throw "Gateway directory not found. Provide -RepoRoot <repo> or -BundleRoot <bundle>."
}

$gatewayDir = Resolve-GatewayDir -RepoRootValue $RepoRoot -BundleRootValue $BundleRoot
$startScript = Join-Path $gatewayDir "start_admin_panel.ps1"
if (-not (Test-Path $startScript)) {
    throw "start_admin_panel.ps1 not found: $startScript"
}

$logsDir = Join-Path $gatewayDir "logs"
New-Item -Path $logsDir -ItemType Directory -Force | Out-Null

while ($true) {
    $ts = Get-Date -Format "yyyyMMdd-HHmmss"
    $logFile = Join-Path $logsDir "admin-panel-$ts.log"
    try {
        Set-Location $gatewayDir
        & $startScript -EnvFile $EnvFile *> $logFile
    } catch {
        $_ | Out-String | Tee-Object -FilePath $logFile -Append
    }
    Start-Sleep -Seconds 5
}
