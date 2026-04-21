param(
    [string]$RepoRoot = "",
    [string]$BundleRoot = "",
    [string]$TaskName = "MT5AdminPanelAutoStart",
    [switch]$Force
)

$ErrorActionPreference = "Stop"

function Resolve-TaskLayout {
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
            $runner = Join-Path $resolvedRoot "windows\run_admin_panel.ps1"
        }
        else {
            $runner = Join-Path $resolvedRoot "deploy\tencent\windows\run_admin_panel.ps1"
        }
        if (Test-Path $runner) {
            return [PSCustomObject]@{
                Root = $resolvedRoot
                Runner = $runner
                Layout = $candidate.Type
            }
        }
    }

    throw "Runner script not found. Provide -RepoRoot <repo> or -BundleRoot <bundle>."
}

function Resolve-InteractiveTaskUser {
    $userName = [System.Environment]::UserName
    $userDomain = [System.Environment]::UserDomainName
    if ([string]::IsNullOrWhiteSpace($userName)) {
        return ""
    }

    $candidates = @(
        [string]::Concat($userDomain, "\", $userName),
        $userName
    )
    foreach ($candidate in $candidates) {
        $normalized = ""
        if ($null -ne $candidate) {
            $normalized = $candidate.ToString().Trim().ToUpperInvariant()
        }
        if ([string]::IsNullOrWhiteSpace($normalized)) {
            continue
        }
        if ($normalized -in @("SYSTEM", "NT AUTHORITY\\SYSTEM", "LOCAL SERVICE", "NETWORK SERVICE")) {
            continue
        }
        return $candidate.Trim()
    }

    return ""
}

function Resolve-TaskRegistrationProfile {
    $interactiveUser = Resolve-InteractiveTaskUser
    if (-not [string]::IsNullOrWhiteSpace($interactiveUser)) {
        return [PSCustomObject]@{
            Trigger = New-ScheduledTaskTrigger -AtLogOn -User $interactiveUser
            Principal = New-ScheduledTaskPrincipal -UserId $interactiveUser -LogonType Interactive -RunLevel Highest
            LaunchMode = "interactive_user"
            UserId = $interactiveUser
            TriggerType = "AtLogOn"
        }
    }

    return [PSCustomObject]@{
        Trigger = New-ScheduledTaskTrigger -AtStartup
        Principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
        LaunchMode = "service_account"
        UserId = "SYSTEM"
        TriggerType = "AtStartup"
    }
}

$layout = Resolve-TaskLayout -RepoRootValue $RepoRoot -BundleRootValue $BundleRoot
$runner = $layout.Runner

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    if (-not $Force) {
        throw "Task '$TaskName' already exists. Use -Force to replace."
    }
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

$rootArgName = "RepoRoot"
if ($layout.Layout -eq "bundle") {
    $rootArgName = "BundleRoot"
}
$args = "-NoProfile -ExecutionPolicy Bypass -File `"$runner`" -" + $rootArgName + " `"" + $layout.Root + "`""
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $args
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
$registrationProfile = Resolve-TaskRegistrationProfile
$trigger = $registrationProfile.Trigger
$principal = $registrationProfile.Principal

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal | Out-Null

Start-ScheduledTask -TaskName $TaskName
Write-Host ("Task '" + $TaskName + "' is registered and started. launchMode=" + $registrationProfile.LaunchMode + "; userId=" + $registrationProfile.UserId + "; trigger=" + $registrationProfile.TriggerType)
