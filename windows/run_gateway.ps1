param(
    [string]$RepoRoot = "",
    [string]$BundleRoot = "",
    [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Stop"

# 解析网关目录，兼容“完整仓库根目录”和“部署包根目录”。
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
$startScript = Join-Path $gatewayDir "start_gateway.ps1"
if (-not (Test-Path $startScript)) {
    throw "start_gateway.ps1 not found: $startScript"
}

# 显式把当前 bundle 的 manifest 路径传给网关进程，避免运行时按目录层级误判来源。
$bundleManifestPath = Join-Path (Split-Path -Parent $gatewayDir) "bundle_manifest.json"
if (Test-Path $bundleManifestPath) {
    $env:MT5_BUNDLE_MANIFEST_PATH = (Resolve-Path $bundleManifestPath).Path
}
else {
    Remove-Item Env:MT5_BUNDLE_MANIFEST_PATH -ErrorAction SilentlyContinue
}

$logsDir = Join-Path $gatewayDir "logs"
New-Item -Path $logsDir -ItemType Directory -Force | Out-Null

while ($true) {
    $ts = Get-Date -Format "yyyyMMdd-HHmmss"
    $logFile = Join-Path $logsDir "gateway-$ts.log"
    try {
        Set-Location $gatewayDir
        & $startScript -EnvFile $EnvFile *>&1 | Tee-Object -FilePath $logFile
    } catch {
        $_ | Out-String | Tee-Object -FilePath $logFile -Append
    }
    Start-Sleep -Seconds 5
}
