# 在 Windows 服务器上执行“一键停服释放端口 + 重新部署”，并显示可关闭的单窗口状态面板。

param(
    [ValidateSet("Gui", "Worker")]
    [string]$Mode = "Gui",
    [string]$RunId = "",
    [string]$BundleRoot = "",
    [string]$StatusFile = "",
    [string]$LogFile = ""
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$script:DeployScriptPath = $PSCommandPath

# 用共享读写方式访问状态和日志文件，避免 GUI 读文件时阻塞 worker 写文件。
function Write-TextFileShared {
    param(
        [string]$Path,
        [string]$Text,
        [switch]$Append
    )

    $directory = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($directory)) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }

    $mode = if ($Append) {
        [System.IO.FileMode]::OpenOrCreate
    }
    else {
        [System.IO.FileMode]::Create
    }

    $stream = [System.IO.File]::Open(
        $Path,
        $mode,
        [System.IO.FileAccess]::Write,
        [System.IO.FileShare]::ReadWrite
    )
    try {
        if ($Append) {
            $stream.Seek(0, [System.IO.SeekOrigin]::End) | Out-Null
        }

        $writer = New-Object System.IO.StreamWriter($stream, [System.Text.UTF8Encoding]::new($false))
        try {
            $writer.Write($Text)
            $writer.Flush()
        }
        finally {
            $writer.Dispose()
        }
    }
    finally {
        $stream.Dispose()
    }
}

# 用共享读写方式读取文件，避免状态窗口与 worker 互相抢占文件句柄。
function Read-TextFileShared {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return ""
    }

    $stream = [System.IO.File]::Open(
        $Path,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::ReadWrite
    )
    try {
        $reader = New-Object System.IO.StreamReader($stream, [System.Text.UTF8Encoding]::new($false), $true)
        try {
            return $reader.ReadToEnd()
        }
        finally {
            $reader.Dispose()
        }
    }
    finally {
        $stream.Dispose()
    }
}

function Resolve-BundleRootPath {
    param([string]$BundleRootValue)

    if (-not [string]::IsNullOrWhiteSpace($BundleRootValue)) {
        return (Resolve-Path $BundleRootValue).Path
    }
    return (Split-Path -Parent $script:DeployScriptPath)
}

function Read-BundleManifest {
    param([string]$ResolvedBundleRoot)

    $manifestPath = Join-Path $ResolvedBundleRoot "bundle_manifest.json"
    if (-not (Test-Path $manifestPath)) {
        throw "缺少部署指纹文件: $manifestPath"
    }

    $manifestText = Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($manifestText)) {
        throw "部署指纹文件为空: $manifestPath"
    }

    $manifest = $manifestText | ConvertFrom-Json
    $bundleFingerprint = [string]$manifest.bundleFingerprint
    $generatedAt = [string]$manifest.generatedAt
    if ([string]::IsNullOrWhiteSpace($bundleFingerprint)) {
        throw "部署指纹文件缺少 bundleFingerprint: $manifestPath"
    }

    return [PSCustomObject]@{
        Path = $manifestPath
        BundleFingerprint = $bundleFingerprint
        GeneratedAt = $generatedAt
    }
}

function New-DeployContext {
    param(
        [string]$ResolvedBundleRoot,
        [string]$ResolvedStatusFile,
        [string]$ResolvedLogFile,
        [string]$DeployRunId
    )

    $bundleManifest = Read-BundleManifest -ResolvedBundleRoot $ResolvedBundleRoot
    $windowsDir = Join-Path $ResolvedBundleRoot "windows"
    return @{
        RunId = $DeployRunId
        BundleRoot = $ResolvedBundleRoot
        BundleParent = Split-Path -Parent $ResolvedBundleRoot
        BundleManifestPath = $bundleManifest.Path
        ExpectedBundleFingerprint = $bundleManifest.BundleFingerprint
        ExpectedBundleGeneratedAt = $bundleManifest.GeneratedAt
        GatewayDir = Join-Path $ResolvedBundleRoot "mt5_gateway"
        WindowsDir = $windowsDir
        StatusFile = $ResolvedStatusFile
        LogFile = $ResolvedLogFile
        GatewayTaskName = "MT5GatewayAutoStart"
        AdminTaskName = "MT5AdminPanelAutoStart"
        State = [ordered]@{
            runId = $DeployRunId
            bundleRoot = $ResolvedBundleRoot
            startedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
            finishedAt = ""
            overallStatus = "running"
            message = "部署准备中"
            currentStep = ""
            steps = @()
            artifacts = [ordered]@{
                logFile = $ResolvedLogFile
                statusFile = $ResolvedStatusFile
                bundleManifest = $bundleManifest.Path
                bundleFingerprint = $bundleManifest.BundleFingerprint
            }
        }
    }
}

function Save-DeployState {
    param($Context)

    $json = $Context.State | ConvertTo-Json -Depth 8
    Write-TextFileShared -Path $Context.StatusFile -Text $json
}

function Write-DeployLog {
    param(
        $Context,
        [string]$Message
    )

    $line = "[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $Message
    Write-TextFileShared -Path $Context.LogFile -Text ($line + "`r`n") -Append
}

function Find-StepIndex {
    param(
        $Context,
        [string]$Name
    )

    for ($i = 0; $i -lt $Context.State.steps.Count; $i++) {
        if ($Context.State.steps[$i].name -eq $Name) {
            return $i
        }
    }
    return -1
}

function Begin-Step {
    param(
        $Context,
        [string]$Name,
        [string]$Message
    )

    $step = [ordered]@{
        name = $Name
        status = "running"
        message = $Message
        startedAt = (Get-Date).ToString("HH:mm:ss")
        finishedAt = ""
        details = @()
    }
    $Context.State.steps += $step
    $Context.State.currentStep = $Name
    $Context.State.message = $Message
    Save-DeployState -Context $Context
    Write-DeployLog -Context $Context -Message ("开始: " + $Name + " - " + $Message)
}

function Complete-Step {
    param(
        $Context,
        [string]$Name,
        [string]$Status,
        [string]$Message,
        [object[]]$Details = @()
    )

    $index = Find-StepIndex -Context $Context -Name $Name
    if ($index -lt 0) {
        return
    }

    $Context.State.steps[$index].status = $Status
    $Context.State.steps[$index].message = $Message
    $Context.State.steps[$index].finishedAt = (Get-Date).ToString("HH:mm:ss")
    $Context.State.steps[$index].details = @($Details)
    $Context.State.message = $Message
    Save-DeployState -Context $Context
    Write-DeployLog -Context $Context -Message ("完成: " + $Name + " - " + $Status + " - " + $Message)
    foreach ($detail in $Details) {
        if ($null -eq $detail) {
            continue
        }
        Write-DeployLog -Context $Context -Message ("  " + $detail.ToString())
    }
}

function Fail-Deploy {
    param(
        $Context,
        [string]$Name,
        [string]$Message
    )

    Complete-Step -Context $Context -Name $Name -Status "failed" -Message $Message
    $Context.State.overallStatus = "failed"
    $Context.State.finishedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $Context.State.currentStep = $Name
    $Context.State.message = $Message
    Save-DeployState -Context $Context
}

function Complete-Deploy {
    param(
        $Context,
        [string]$Message,
        [hashtable]$Artifacts = @{}
    )

    $Context.State.overallStatus = "success"
    $Context.State.finishedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $Context.State.currentStep = ""
    $Context.State.message = $Message
    foreach ($key in $Artifacts.Keys) {
        $Context.State.artifacts[$key] = $Artifacts[$key]
    }
    Save-DeployState -Context $Context
    Write-DeployLog -Context $Context -Message $Message
}

function Test-PsScriptSyntax {
    param([string]$Path)

    $tokens = $null
    $errors = $null
    [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors) | Out-Null
    if ($errors -and $errors.Count -gt 0) {
        $messages = @()
        foreach ($err in $errors) {
            $messages += ("行 {0}: {1}" -f $err.Extent.StartLineNumber, $err.Message)
        }
        throw ("脚本语法检查失败: " + $Path + " | " + ($messages -join " ; "))
    }
}

function Get-WebRequestFailureMessage {
    param(
        [System.Management.Automation.ErrorRecord]$ErrorRecord
    )

    if ($null -eq $ErrorRecord) {
        return "未知错误"
    }

    $exception = $ErrorRecord.Exception
    if ($null -eq $exception) {
        return $ErrorRecord.ToString()
    }

    $response = $exception.Response
    if ($null -ne $response) {
        try {
            $statusCode = ""
            $statusDescription = ""
            if ($response.PSObject.Properties.Name -contains "StatusCode") {
                $statusCode = [string]$response.StatusCode
            }
            if ($response.PSObject.Properties.Name -contains "StatusDescription") {
                $statusDescription = [string]$response.StatusDescription
            }

            $body = ""
            if ($response.PSObject.Methods.Name -contains "GetResponseStream") {
                $stream = $response.GetResponseStream()
                if ($null -ne $stream) {
                    try {
                        $reader = New-Object System.IO.StreamReader($stream, [System.Text.UTF8Encoding]::new($false))
                        try {
                            $body = $reader.ReadToEnd().Trim()
                        }
                        finally {
                            $reader.Dispose()
                        }
                    }
                    finally {
                        $stream.Dispose()
                    }
                }
            }

            $message = "HTTP $statusCode"
            if (-not [string]::IsNullOrWhiteSpace($statusDescription)) {
                $message += " $statusDescription"
            }
            if (-not [string]::IsNullOrWhiteSpace($body)) {
                $message += " | $body"
            }
            return $message
        }
        catch {
        }
    }

    if ($exception.InnerException -and -not [string]::IsNullOrWhiteSpace($exception.InnerException.Message)) {
        return $exception.InnerException.Message
    }
    if (-not [string]::IsNullOrWhiteSpace($exception.Message)) {
        return $exception.Message
    }
    return $ErrorRecord.ToString()
}

function Start-HealthProbe {
    param(
        $Context,
        [string]$Label
    )

    Write-DeployLog -Context $Context -Message ("健康检查开始: " + $Label)
}

function Wait-HttpOk {
    param(
        [string]$Url,
        [int]$MaxSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($MaxSeconds)
    $lastFailure = ""
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest $Url -UseBasicParsing -TimeoutSec 5
            if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300) {
                return $resp
            }
            $lastFailure = "HTTP $($resp.StatusCode)"
        }
        catch {
            $lastFailure = Get-WebRequestFailureMessage -ErrorRecord $_
        }
        Start-Sleep -Seconds 2
    }

    if ([string]::IsNullOrWhiteSpace($lastFailure)) {
        throw "等待接口超时: $Url"
    }
    throw "等待接口超时: $Url | 最后错误: $lastFailure"
}

function Wait-HttpJsonFieldMatch {
    param(
        [string]$Url,
        [string]$FieldName,
        [string]$ExpectedValue,
        [int]$MaxSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($MaxSeconds)
    $lastFailure = ""
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest $Url -UseBasicParsing -TimeoutSec 5
            if ($resp.StatusCode -lt 200 -or $resp.StatusCode -ge 300) {
                $lastFailure = "HTTP $($resp.StatusCode)"
                throw "HTTP $($resp.StatusCode)"
            }
            $payload = $resp.Content | ConvertFrom-Json
            $actualValue = [string]$payload.$FieldName
            if ($actualValue -eq $ExpectedValue) {
                return [pscustomobject]@{
                    StatusCode = [int]$resp.StatusCode
                    FieldName = $FieldName
                    FieldValue = $actualValue
                }
            }
            $lastFailure = ("字段 {0} 实际值为 {1}，期望 {2}" -f $FieldName, $actualValue, $ExpectedValue)
        }
        catch {
            $failureMessage = Get-WebRequestFailureMessage -ErrorRecord $_
            if (-not [string]::IsNullOrWhiteSpace($failureMessage)) {
                $lastFailure = $failureMessage
            }
        }
        Start-Sleep -Seconds 2
    }

    if ([string]::IsNullOrWhiteSpace($lastFailure)) {
        throw "等待接口字段匹配超时: $Url -> $FieldName"
    }
    throw "等待接口字段匹配超时: $Url -> $FieldName | 最后错误: $lastFailure"
}

function Invoke-HttpsLoopbackRequest {
    param(
        [string]$HostName,
        [string]$Path = "/",
        [int]$Port = 443,
        [int]$TimeoutSeconds = 5
    )

    $tcpClient = New-Object System.Net.Sockets.TcpClient
    try {
        $connectResult = $tcpClient.BeginConnect([System.Net.IPAddress]::Loopback, $Port, $null, $null)
        if (-not $connectResult.AsyncWaitHandle.WaitOne($TimeoutSeconds * 1000)) {
            throw "连接超时: 127.0.0.1:$Port"
        }
        $tcpClient.EndConnect($connectResult)

        $networkStream = $tcpClient.GetStream()
        $networkStream.ReadTimeout = $TimeoutSeconds * 1000
        $networkStream.WriteTimeout = $TimeoutSeconds * 1000

        $sslStream = New-Object System.Net.Security.SslStream($networkStream, $false)
        try {
            $sslStream.AuthenticateAsClient($HostName)

            $requestText = "GET $Path HTTP/1.1`r`nHost: $HostName`r`nConnection: close`r`n`r`n"
            $requestBytes = [System.Text.Encoding]::ASCII.GetBytes($requestText)
            $sslStream.Write($requestBytes, 0, $requestBytes.Length)
            $sslStream.Flush()

            $reader = New-Object System.IO.StreamReader($sslStream, [System.Text.Encoding]::ASCII, $false, 1024, $true)
            try {
                $statusLine = $reader.ReadLine()
            }
            finally {
                $reader.Dispose()
            }
        }
        finally {
            $sslStream.Dispose()
        }
    }
    finally {
        $tcpClient.Dispose()
    }

    if ([string]::IsNullOrWhiteSpace($statusLine)) {
        throw "未收到 HTTPS 响应状态行: $HostName$Path"
    }

    if ($statusLine -notmatch '^HTTP/\d+\.\d+\s+(?<code>\d{3})\b') {
        throw "无法解析 HTTPS 响应状态行: $statusLine"
    }

    return [pscustomobject]@{
        StatusCode = [int]$Matches["code"]
        StatusLine = $statusLine
    }
}

function Wait-HttpsLoopbackOk {
    param(
        [string]$HostName,
        [string]$Path = "/",
        [int]$MaxSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($MaxSeconds)
    $lastFailure = ""
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-HttpsLoopbackRequest -HostName $HostName -Path $Path
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                return $response
            }
            $lastFailure = "HTTP $($response.StatusCode)"
        }
        catch {
            if ($_.Exception -and -not [string]::IsNullOrWhiteSpace($_.Exception.Message)) {
                $lastFailure = $_.Exception.Message
            }
        }
        Start-Sleep -Seconds 2
    }

    if ([string]::IsNullOrWhiteSpace($lastFailure)) {
        throw "等待本机 HTTPS SNI 接口超时: https://$HostName$Path"
    }
    throw "等待本机 HTTPS SNI 接口超时: https://$HostName$Path | 最后错误: $lastFailure"
}

function Wait-WebSocketMessage {
    param(
        [string]$Url,
        [int]$MaxSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($MaxSeconds)
    $lastFailure = ""

    while ((Get-Date) -lt $deadline) {
        $client = [System.Net.WebSockets.ClientWebSocket]::new()
        $cts = [System.Threading.CancellationTokenSource]::new()
        try {
            $remainingSeconds = [Math]::Max(1, [int][Math]::Ceiling(($deadline - (Get-Date)).TotalSeconds))
            $attemptSeconds = [Math]::Min($remainingSeconds, 10)
            $cts.CancelAfter([TimeSpan]::FromSeconds($attemptSeconds))
            $client.ConnectAsync([Uri]$Url, $cts.Token).GetAwaiter().GetResult()
            $buffer = New-Object byte[] 65536
            $segment = [ArraySegment[byte]]::new($buffer)
            $builder = New-Object System.Text.StringBuilder
            do {
                $result = $client.ReceiveAsync($segment, $cts.Token).GetAwaiter().GetResult()
                if ($result.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Close) {
                    throw "WebSocket 在收到首条消息前被关闭"
                }
                $null = $builder.Append([System.Text.Encoding]::UTF8.GetString($buffer, 0, $result.Count))
            } while (-not $result.EndOfMessage)

            $message = $builder.ToString()
            if ([string]::IsNullOrWhiteSpace($message)) {
                throw "WebSocket 首条消息为空"
            }
            return [pscustomobject]@{
                Url = $Url
                Preview = $message.Substring(0, [Math]::Min(160, $message.Length))
            }
        }
        catch {
            if ($_.Exception -and -not [string]::IsNullOrWhiteSpace($_.Exception.Message)) {
                $lastFailure = $_.Exception.Message
            }
            elseif ([string]::IsNullOrWhiteSpace($lastFailure)) {
                $lastFailure = "未知错误"
            }
        }
        finally {
            try {
                if ($client.State -eq [System.Net.WebSockets.WebSocketState]::Open) {
                    $client.CloseAsync(
                        [System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure,
                        "deploy-check",
                        [System.Threading.CancellationToken]::None
                    ).GetAwaiter().GetResult() | Out-Null
                }
            }
            catch {
            }
            $cts.Dispose()
            $client.Dispose()
        }
        if ((Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 2
        }
    }

    if ([string]::IsNullOrWhiteSpace($lastFailure)) {
        $lastFailure = "未知错误"
    }
    throw "等待 WebSocket 首条消息超时: $Url | 最后错误: $lastFailure"
}

function Resolve-CaddyExecutablePath {
    param($Context)

    $candidates = @(
        (Join-Path $Context.WindowsDir "caddy.exe"),
        (Join-Path $Context.BundleRoot "caddy.exe"),
        (Join-Path $Context.BundleParent "caddy.exe")
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    throw "找不到 caddy.exe。已检查: $($candidates -join ', ')"
}

function Get-TargetListeners {
    Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in 80, 443, 2019, 8787, 8788 } |
        Sort-Object LocalPort, OwningProcess
}

function Get-ListenerProcessDetails {
    param(
        [int[]]$Ports
    )

    $listeners = Get-TargetListeners
    if ($Ports -and $Ports.Count -gt 0) {
        $listeners = $listeners | Where-Object { $_.LocalPort -in $Ports }
    }

    $details = @()
    foreach ($listener in @($listeners)) {
        $pidValue = [int]$listener.OwningProcess
        $process = Get-CimInstance Win32_Process -Filter ("ProcessId = " + $pidValue) -ErrorAction SilentlyContinue
        $commandLine = ""
        if ($process) {
            $commandLine = [string]$process.CommandLine
        }
        if ([string]::IsNullOrWhiteSpace($commandLine)) {
            $commandLine = (Get-Process -Id $pidValue -ErrorAction SilentlyContinue).Path
        }
        $details += ("{0}:{1}/PID{2} -> {3}" -f $listener.LocalAddress, $listener.LocalPort, $pidValue, $commandLine)
    }
    return $details
}

function Test-GatewayEnvContract {
    param($Context)

    $envPath = Join-Path $Context.GatewayDir ".env"
    if (-not (Test-Path $envPath)) {
        throw "网关环境文件缺失: $envPath"
    }

    $requiredKeys = @(
        "MT5_LOGIN",
        "MT5_PASSWORD",
        "MT5_SERVER",
        "MT5_SERVER_TIMEZONE"
    )
    $values = @{}
    foreach ($line in Get-Content -LiteralPath $envPath -Encoding UTF8) {
        if ($null -eq $line) {
            continue
        }
        $trimmed = $line.ToString().Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) {
            continue
        }
        $separatorIndex = $trimmed.IndexOf("=")
        if ($separatorIndex -lt 1) {
            continue
        }
        $key = $trimmed.Substring(0, $separatorIndex).Trim()
        $value = $trimmed.Substring($separatorIndex + 1).Trim()
        if (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        $values[$key] = $value
    }

    $missingKeys = @()
    foreach ($key in $requiredKeys) {
        if (-not $values.ContainsKey($key) -or [string]::IsNullOrWhiteSpace([string]$values[$key])) {
            $missingKeys += $key
        }
    }
    if ($missingKeys.Count -gt 0) {
        throw ("网关环境文件缺少必填项: " + ($missingKeys -join ", ") + " | 文件: " + $envPath)
    }
}

function Get-ServiceLogTail {
    param(
        [string]$LogsDir,
        [string]$Pattern,
        [int]$MaxLines = 20
    )

    if (-not (Test-Path $LogsDir)) {
        return ""
    }
    $latestLog = Get-ChildItem -Path $LogsDir -Filter $Pattern -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
    if ($null -eq $latestLog) {
        return ""
    }
    $lines = Get-Content -LiteralPath $latestLog.FullName -Encoding UTF8 -ErrorAction SilentlyContinue |
        Select-Object -Last $MaxLines
    if ($null -eq $lines -or $lines.Count -eq 0) {
        return ""
    }
    $joined = (($lines | ForEach-Object { $_.ToString().Trim() }) -join " || ").Trim()
    if ([string]::IsNullOrWhiteSpace($joined)) {
        return ""
    }
    return ($latestLog.Name + ": " + $joined)
}

function Test-BundleManagedProcess {
    param(
        $Process,
        $Context
    )

    if ($null -eq $Process) {
        return $false
    }

    $name = [string]$Process.Name
    $commandLine = [string]$Process.CommandLine
    $executablePath = [string]$Process.ExecutablePath
    $gatewayDir = [string]$Context.GatewayDir
    $windowsDir = [string]$Context.WindowsDir
    $bundleRoot = [string]$Context.BundleRoot

    if ($null -eq $commandLine) { $commandLine = "" }
    if ($null -eq $executablePath) { $executablePath = "" }
    if ($null -eq $gatewayDir) { $gatewayDir = "" }
    if ($null -eq $windowsDir) { $windowsDir = "" }
    if ($null -eq $bundleRoot) { $bundleRoot = "" }

    $normalizedCommandLine = $commandLine.Replace("/", "\").ToLowerInvariant()
    $normalizedExecutablePath = $executablePath.Replace("/", "\").ToLowerInvariant()
    $normalizedGatewayDir = $gatewayDir.Replace("/", "\").ToLowerInvariant()
    $normalizedWindowsDir = $windowsDir.Replace("/", "\").ToLowerInvariant()
    $normalizedBundleRoot = $bundleRoot.Replace("/", "\").ToLowerInvariant()
    $serverScriptPath = ([System.IO.Path]::Combine($gatewayDir, "server_v2.py")).Replace("/", "\").ToLowerInvariant()
    $adminScriptPath = ([System.IO.Path]::Combine($gatewayDir, "admin_panel.py")).Replace("/", "\").ToLowerInvariant()
    $serverScriptName = [System.IO.Path]::GetFileName($serverScriptPath).ToLowerInvariant()
    $adminScriptName = [System.IO.Path]::GetFileName($adminScriptPath).ToLowerInvariant()
    $pythonExecutableManagedByBundle = $normalizedExecutablePath.StartsWith($normalizedGatewayDir)
    if (-not $pythonExecutableManagedByBundle) {
        $pythonExecutableManagedByBundle = $normalizedExecutablePath.StartsWith($normalizedBundleRoot)
    }

    if ($name -match '^pythonw?\.exe$') {
        return $normalizedCommandLine.Contains($serverScriptPath) `
            -or $normalizedCommandLine.Contains($adminScriptPath) `
            -or (
                $pythonExecutableManagedByBundle -and (
                    $normalizedCommandLine.Contains($serverScriptName) `
                        -or $normalizedCommandLine.Contains($adminScriptName)
                )
            )
    }

    if ($name -ieq "caddy.exe" -or $name -ieq "nginx.exe") {
        return $normalizedExecutablePath.StartsWith($normalizedBundleRoot) `
            -or $normalizedCommandLine.Contains($normalizedWindowsDir) `
            -or $normalizedCommandLine.Contains($normalizedBundleRoot) `
            -or $normalizedCommandLine.Contains($normalizedGatewayDir)
    }

    return $false
}

function Stop-TargetProcessesAndPorts {
    param($Context)

    $details = New-Object System.Collections.Generic.List[string]

    foreach ($taskName in @($Context.GatewayTaskName, $Context.AdminTaskName)) {
        try {
            Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue | Out-Null
            $details.Add("已停止计划任务: $taskName")
        }
        catch {
            $details.Add("计划任务停止跳过: $taskName")
        }
    }

    foreach ($serviceName in @("nginx", "caddy")) {
        try {
            $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
            if ($service -and $service.Status -ne "Stopped") {
                Stop-Service -Name $serviceName -Force -ErrorAction SilentlyContinue
                $details.Add("已停止服务: $serviceName")
            }
        }
        catch {
            $details.Add("服务停止失败: $serviceName")
        }
    }

    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            Test-BundleManagedProcess -Process $_ -Context $Context
        } |
        ForEach-Object {
            try {
                Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
                $details.Add(("已停止进程 PID {0}: {1}" -f $_.ProcessId, $_.Name))
            }
            catch {
                $details.Add(("停止进程失败 PID {0}: {1}" -f $_.ProcessId, $_.Name))
            }
        }

    $listenerPids = Get-TargetListeners |
        Select-Object -ExpandProperty OwningProcess -Unique |
        Where-Object { $_ -gt 0 -and $_ -ne 4 }

    foreach ($listenerPid in $listenerPids) {
        try {
            $process = Get-Process -Id $listenerPid -ErrorAction SilentlyContinue
            if ($process) {
                Stop-Process -Id $listenerPid -Force -ErrorAction SilentlyContinue
                $details.Add(("已释放占口 PID {0}: {1}" -f $listenerPid, $process.ProcessName))
            }
        }
        catch {
            $details.Add(("释放占口失败 PID {0}" -f $listenerPid))
        }
    }

    Start-Sleep -Seconds 2

    $remaining = Get-TargetListeners |
        Where-Object { $_.OwningProcess -gt 0 -and $_.OwningProcess -ne 4 }

    if ($remaining) {
        $ports = $remaining |
            ForEach-Object { ("{0}:{1}/PID{2}" -f $_.LocalAddress, $_.LocalPort, $_.OwningProcess) }
        throw ("仍有端口未释放: " + ($ports -join ", "))
    }

    return $details.ToArray()
}

function Invoke-DeployWorker {
    param($Context)

    $gatewayDir = $Context.GatewayDir
    $windowsDir = $Context.WindowsDir
    $bootstrapScript = Join-Path $windowsDir "01_bootstrap_gateway.ps1"
    $registerGatewayTaskScript = Join-Path $windowsDir "02_register_startup_task.ps1"
    $registerAdminTaskScript = Join-Path $windowsDir "04_register_admin_panel_task.ps1"

    if (-not (Test-Path $Context.BundleRoot)) { throw "找不到目录: $($Context.BundleRoot)" }
    if (-not (Test-Path $gatewayDir)) { throw "找不到目录: $gatewayDir" }
    if (-not (Test-Path $windowsDir)) { throw "找不到目录: $windowsDir" }

    try {
        Begin-Step -Context $Context -Name "停止旧服务" -Message "停止旧任务、旧面板、旧网关并释放关键端口"
        $stopDetails = Stop-TargetProcessesAndPorts -Context $Context
        Complete-Step -Context $Context -Name "停止旧服务" -Status "success" -Message "旧服务和占口已释放" -Details $stopDetails

        Begin-Step -Context $Context -Name "脚本校验" -Message "检查部署脚本与启动脚本语法"
        $requiredScripts = @(
            (Join-Path $windowsDir "01_bootstrap_gateway.ps1"),
            (Join-Path $windowsDir "02_register_startup_task.ps1"),
            (Join-Path $windowsDir "04_register_admin_panel_task.ps1"),
            (Join-Path $windowsDir "run_gateway.ps1"),
            (Join-Path $windowsDir "run_admin_panel.ps1"),
            (Join-Path $gatewayDir "start_gateway.ps1"),
            (Join-Path $gatewayDir "start_admin_panel.ps1")
        )
        foreach ($script in $requiredScripts) {
            if (-not (Test-Path $script)) {
                throw "缺少脚本: $script"
            }
            Test-PsScriptSyntax -Path $script
        }
        Complete-Step -Context $Context -Name "脚本校验" -Status "success" -Message "部署脚本语法检查通过"

        Begin-Step -Context $Context -Name "初始化环境" -Message "创建虚拟环境并安装依赖"
        & $bootstrapScript -BundleRoot $Context.BundleRoot
        Test-GatewayEnvContract -Context $Context
        Complete-Step -Context $Context -Name "初始化环境" -Status "success" -Message "Python 环境初始化完成"

        Begin-Step -Context $Context -Name "注册任务" -Message "重新注册网关与管理面板开机自启任务"
        & $registerGatewayTaskScript -BundleRoot $Context.BundleRoot -TaskName $Context.GatewayTaskName -Force
        & $registerAdminTaskScript -BundleRoot $Context.BundleRoot -TaskName $Context.AdminTaskName -Force
        Enable-ScheduledTask -TaskName $Context.GatewayTaskName -ErrorAction SilentlyContinue | Out-Null
        Enable-ScheduledTask -TaskName $Context.AdminTaskName -ErrorAction SilentlyContinue | Out-Null
        Complete-Step -Context $Context -Name "注册任务" -Status "success" -Message "计划任务已更新"

        Begin-Step -Context $Context -Name "启动后台服务" -Message "启动网关、管理面板与 Caddy"
        Start-ScheduledTask -TaskName $Context.GatewayTaskName
        Start-ScheduledTask -TaskName $Context.AdminTaskName

        $caddyExe = Resolve-CaddyExecutablePath -Context $Context
        $caddyConfig = Join-Path $windowsDir "Caddyfile"
        if (-not (Test-Path $caddyConfig)) {
            throw "找不到 Caddyfile: $caddyConfig"
        }
        $caddyLogDir = Join-Path $windowsDir "logs"
        New-Item -ItemType Directory -Force -Path $caddyLogDir | Out-Null
        $caddyOut = Join-Path $caddyLogDir "caddy-out.log"
        $caddyErr = Join-Path $caddyLogDir "caddy-err.log"

        Start-Process `
            -WindowStyle Hidden `
            -FilePath $caddyExe `
            -ArgumentList @("run", "--config", $caddyConfig) `
            -WorkingDirectory $windowsDir `
            -RedirectStandardOutput $caddyOut `
            -RedirectStandardError $caddyErr | Out-Null

        Complete-Step `
            -Context $Context `
            -Name "启动后台服务" `
            -Status "success" `
            -Message "网关、面板与 Caddy 已启动" `
            -Details @(
                "计划任务: $($Context.GatewayTaskName)",
                "计划任务: $($Context.AdminTaskName)",
                "Caddy: $caddyExe",
                "BundleFingerprint: $($Context.ExpectedBundleFingerprint)"
            )

        Begin-Step -Context $Context -Name "健康检查" -Message "验证端口监听和接口可用性"
        Start-HealthProbe -Context $Context -Label "8787 /health"
        try {
            $directGateway = Wait-HttpJsonFieldMatch `
                -Url "http://127.0.0.1:8787/health" `
                -FieldName "bundleFingerprint" `
                -ExpectedValue $Context.ExpectedBundleFingerprint `
                -MaxSeconds 90
        }
        catch {
            $gatewayLogTail = Get-ServiceLogTail -LogsDir (Join-Path $Context.GatewayDir "logs") -Pattern "gateway-*.log"
            if ([string]::IsNullOrWhiteSpace($gatewayLogTail)) {
                throw
            }
            throw ($_.Exception.Message + " | 最近网关日志: " + $gatewayLogTail)
        }
        Write-DeployLog -Context $Context -Message ("健康检查通过: 8787 /health -> " + $directGateway.StatusCode + " | bundleFingerprint=" + $directGateway.FieldValue)
        Start-HealthProbe -Context $Context -Label "8788 /"
        $directAdmin = Wait-HttpOk -Url "http://127.0.0.1:8788/" -MaxSeconds 90
        Write-DeployLog -Context $Context -Message ("健康检查通过: 8788 / -> " + $directAdmin.StatusCode)
        Start-HealthProbe -Context $Context -Label "80 /health"
        $proxyGateway = Wait-HttpJsonFieldMatch `
            -Url "http://127.0.0.1/health" `
            -FieldName "bundleFingerprint" `
            -ExpectedValue $Context.ExpectedBundleFingerprint `
            -MaxSeconds 90
        Write-DeployLog -Context $Context -Message ("健康检查通过: 80 /health -> " + $proxyGateway.StatusCode + " | bundleFingerprint=" + $proxyGateway.FieldValue)
        Start-HealthProbe -Context $Context -Label "443 loopback tradeapp.ltd/health"
        $loopbackHttpsGateway = Wait-HttpsLoopbackOk -HostName "tradeapp.ltd" -Path "/health" -MaxSeconds 90
        Write-DeployLog -Context $Context -Message ("健康检查通过: 443 loopback tradeapp.ltd/health -> " + $loopbackHttpsGateway.StatusCode)
        Start-HealthProbe -Context $Context -Label "443 tradeapp.ltd/health"
        $publicHttpsGateway = Wait-HttpJsonFieldMatch `
            -Url "https://tradeapp.ltd/health" `
            -FieldName "bundleFingerprint" `
            -ExpectedValue $Context.ExpectedBundleFingerprint `
            -MaxSeconds 180
        Write-DeployLog -Context $Context -Message ("健康检查通过: 443 tradeapp.ltd/health -> " + $publicHttpsGateway.StatusCode + " | bundleFingerprint=" + $publicHttpsGateway.FieldValue)
        Start-HealthProbe -Context $Context -Label "8787 /v2/account/snapshot"
        $directAccountSnapshot = Wait-HttpOk -Url "http://127.0.0.1:8787/v2/account/snapshot" -MaxSeconds 90
        Write-DeployLog -Context $Context -Message ("健康检查通过: 8787 /v2/account/snapshot -> " + $directAccountSnapshot.StatusCode)
        Start-HealthProbe -Context $Context -Label "8787 /v2/account/history?range=all"
        $directAccountHistory = Wait-HttpOk -Url "http://127.0.0.1:8787/v2/account/history?range=all" -MaxSeconds 90
        Write-DeployLog -Context $Context -Message ("健康检查通过: 8787 /v2/account/history?range=all -> " + $directAccountHistory.StatusCode)
        Start-HealthProbe -Context $Context -Label "8787 /v2/account/full (diagnostic)"
        $directAccountFull = Wait-HttpOk -Url "http://127.0.0.1:8787/v2/account/full" -MaxSeconds 180
        Write-DeployLog -Context $Context -Message ("健康检查通过: 8787 /v2/account/full (diagnostic) -> " + $directAccountFull.StatusCode)
        Start-HealthProbe -Context $Context -Label "443 tradeapp.ltd/v2/account/snapshot"
        $publicAccountSnapshot = Wait-HttpOk -Url "https://tradeapp.ltd/v2/account/snapshot" -MaxSeconds 180
        Write-DeployLog -Context $Context -Message ("健康检查通过: 443 tradeapp.ltd/v2/account/snapshot -> " + $publicAccountSnapshot.StatusCode)
        Start-HealthProbe -Context $Context -Label "443 tradeapp.ltd/v2/account/history?range=all"
        $publicAccountHistory = Wait-HttpOk -Url "https://tradeapp.ltd/v2/account/history?range=all" -MaxSeconds 180
        Write-DeployLog -Context $Context -Message ("健康检查通过: 443 tradeapp.ltd/v2/account/history?range=all -> " + $publicAccountHistory.StatusCode)
        Start-HealthProbe -Context $Context -Label "wss://tradeapp.ltd/v2/stream"
        $streamCheck = Wait-WebSocketMessage -Url "wss://tradeapp.ltd/v2/stream" -MaxSeconds 90
        Write-DeployLog -Context $Context -Message ("健康检查通过: wss://tradeapp.ltd/v2/stream -> " + $streamCheck.Preview)

        $adminProxyStatus = $null
        try {
            $adminResp = Invoke-WebRequest "http://127.0.0.1/admin/" -UseBasicParsing -TimeoutSec 5
            $adminProxyStatus = [int]$adminResp.StatusCode
        }
        catch {
            if ($_.Exception.Response) {
                $adminProxyStatus = [int]$_.Exception.Response.StatusCode.value__
            }
            else {
                throw
            }
        }
        Write-DeployLog -Context $Context -Message ("健康检查通过: /admin/ -> " + $adminProxyStatus)

        $listeners = Get-TargetListeners | ForEach-Object {
            "{0}:{1}/PID{2}" -f $_.LocalAddress, $_.LocalPort, $_.OwningProcess
        }
        $listenerDetails = Get-ListenerProcessDetails -Ports @(8787, 8788)
        Complete-Step `
            -Context $Context `
            -Name "健康检查" `
            -Status "success" `
            -Message "关键接口检查通过" `
            -Details @(
                ("8787 /health -> " + $directGateway.StatusCode + " | bundleFingerprint=" + $directGateway.FieldValue),
                ("8788 / -> " + $directAdmin.StatusCode),
                ("80 /health -> " + $proxyGateway.StatusCode + " | bundleFingerprint=" + $proxyGateway.FieldValue),
                ("443 loopback tradeapp.ltd/health -> " + $loopbackHttpsGateway.StatusCode),
                ("443 tradeapp.ltd/health -> " + $publicHttpsGateway.StatusCode + " | bundleFingerprint=" + $publicHttpsGateway.FieldValue),
                ("8787 /v2/account/snapshot -> " + $directAccountSnapshot.StatusCode),
                ("8787 /v2/account/history?range=all -> " + $directAccountHistory.StatusCode),
                ("8787 /v2/account/full (diagnostic) -> " + $directAccountFull.StatusCode),
                ("443 tradeapp.ltd/v2/account/snapshot -> " + $publicAccountSnapshot.StatusCode),
                ("443 tradeapp.ltd/v2/account/history?range=all -> " + $publicAccountHistory.StatusCode),
                ("wss://tradeapp.ltd/v2/stream -> " + $streamCheck.Preview),
                ("/admin/ -> " + $adminProxyStatus),
                ("监听端口: " + ($listeners -join " ; ")),
                ("监听来源: " + ($listenerDetails -join " ; "))
            )

        Complete-Deploy `
            -Context $Context `
            -Message "部署完成，可关闭此窗口；关闭不会影响已启动的后台服务。" `
            -Artifacts @{
                gatewayHealth = "http://127.0.0.1:8787/health"
                publicGatewayHealth = "https://tradeapp.ltd/health"
                gatewayStream = "wss://tradeapp.ltd/v2/stream"
                adminPanel = "http://127.0.0.1/admin/"
                bundleRoot = $Context.BundleRoot
                bundleFingerprint = $Context.ExpectedBundleFingerprint
            }
    }
    catch {
        $stepName = $Context.State.currentStep
        if ([string]::IsNullOrWhiteSpace($stepName)) {
            $stepName = "部署异常"
            Begin-Step -Context $Context -Name $stepName -Message "部署发生未捕获错误"
        }
        Fail-Deploy -Context $Context -Name $stepName -Message $_.Exception.Message
        throw
    }
}

function Show-DeployWindow {
    param(
        [string]$ResolvedBundleRoot,
        [string]$ResolvedStatusFile,
        [string]$ResolvedLogFile,
        [string]$DeployRunId
    )

    Add-Type -AssemblyName System.Windows.Forms
    Add-Type -AssemblyName System.Drawing

    $worker = Start-Process `
        -WindowStyle Hidden `
        -FilePath "powershell.exe" `
        -ArgumentList @(
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-File", $script:DeployScriptPath,
            "-Mode", "Worker",
            "-RunId", $DeployRunId,
            "-BundleRoot", $ResolvedBundleRoot,
            "-StatusFile", $ResolvedStatusFile,
            "-LogFile", $ResolvedLogFile
        ) `
        -PassThru

    $form = New-Object System.Windows.Forms.Form
    $form.Text = "MT5 部署状态"
    $form.StartPosition = "CenterScreen"
    $form.Size = New-Object System.Drawing.Size(1000, 720)
    $form.MinimumSize = New-Object System.Drawing.Size(900, 640)
    $form.TopMost = $true

    $titleLabel = New-Object System.Windows.Forms.Label
    $titleLabel.Text = "MT5 / 面板 / Caddy 一键重部署"
    $titleLabel.Location = New-Object System.Drawing.Point(16, 14)
    $titleLabel.Size = New-Object System.Drawing.Size(620, 26)
    $titleLabel.Font = New-Object System.Drawing.Font("Microsoft YaHei UI", 13, [System.Drawing.FontStyle]::Bold)
    $form.Controls.Add($titleLabel)

    $statusLabel = New-Object System.Windows.Forms.Label
    $statusLabel.Text = "状态：启动中"
    $statusLabel.Location = New-Object System.Drawing.Point(16, 46)
    $statusLabel.Size = New-Object System.Drawing.Size(940, 22)
    $form.Controls.Add($statusLabel)

    $bundleLabel = New-Object System.Windows.Forms.Label
    $bundleLabel.Text = "部署目录：" + $ResolvedBundleRoot
    $bundleLabel.Location = New-Object System.Drawing.Point(16, 70)
    $bundleLabel.Size = New-Object System.Drawing.Size(940, 22)
    $form.Controls.Add($bundleLabel)

    $stepList = New-Object System.Windows.Forms.ListView
    $stepList.Location = New-Object System.Drawing.Point(16, 104)
    $stepList.Size = New-Object System.Drawing.Size(950, 260)
    $stepList.View = [System.Windows.Forms.View]::Details
    $stepList.FullRowSelect = $true
    $stepList.GridLines = $true
    $stepList.Columns.Add("步骤", 190) | Out-Null
    $stepList.Columns.Add("状态", 90) | Out-Null
    $stepList.Columns.Add("说明", 630) | Out-Null
    $form.Controls.Add($stepList)

    $detailLabel = New-Object System.Windows.Forms.Label
    $detailLabel.Text = "执行日志"
    $detailLabel.Location = New-Object System.Drawing.Point(16, 376)
    $detailLabel.Size = New-Object System.Drawing.Size(120, 22)
    $form.Controls.Add($detailLabel)

    $logBox = New-Object System.Windows.Forms.TextBox
    $logBox.Location = New-Object System.Drawing.Point(16, 402)
    $logBox.Size = New-Object System.Drawing.Size(950, 230)
    $logBox.Multiline = $true
    $logBox.ScrollBars = "Vertical"
    $logBox.ReadOnly = $true
    $logBox.Font = New-Object System.Drawing.Font("Consolas", 10)
    $form.Controls.Add($logBox)

    $closeButton = New-Object System.Windows.Forms.Button
    $closeButton.Text = "关闭窗口"
    $closeButton.Location = New-Object System.Drawing.Point(836, 644)
    $closeButton.Size = New-Object System.Drawing.Size(130, 30)
    $closeButton.Add_Click({ $form.Close() })
    $form.Controls.Add($closeButton)

    $timer = New-Object System.Windows.Forms.Timer
    $timer.Interval = 1000
    $timer.Add_Tick({
        if (Test-Path $ResolvedStatusFile) {
            try {
                $stateText = Read-TextFileShared -Path $ResolvedStatusFile
                if (-not [string]::IsNullOrWhiteSpace($stateText)) {
                    $state = $stateText | ConvertFrom-Json
                }
                else {
                    $state = $null
                }
                if ($state) {
                    $statusLabel.Text = "状态：" + $state.overallStatus + " | " + $state.message
                    $stepList.BeginUpdate()
                    $stepList.Items.Clear()
                    foreach ($step in @($state.steps)) {
                        $item = New-Object System.Windows.Forms.ListViewItem($step.name)
                        $null = $item.SubItems.Add([string]$step.status)
                        $null = $item.SubItems.Add([string]$step.message)
                        $stepList.Items.Add($item) | Out-Null
                    }
                    $stepList.EndUpdate()

                    if ($state.overallStatus -ne "running") {
                        $statusLabel.Text = "状态：" + $state.overallStatus + " | " + $state.message
                    }
                }
            }
            catch {
            }
        }

        if (Test-Path $ResolvedLogFile) {
            try {
                $logBox.Text = Read-TextFileShared -Path $ResolvedLogFile
                $logBox.SelectionStart = $logBox.TextLength
                $logBox.ScrollToCaret()
            }
            catch {
            }
        }

        if ($worker.HasExited -and (Test-Path $ResolvedStatusFile)) {
            try {
                $stateText = Read-TextFileShared -Path $ResolvedStatusFile
                if (-not [string]::IsNullOrWhiteSpace($stateText)) {
                    $state = $stateText | ConvertFrom-Json
                }
                else {
                    $state = $null
                }
                if ($state -and $state.overallStatus -eq "running") {
                    $statusLabel.Text = "状态：worker 已退出，请检查日志"
                }
            }
            catch {
            }
        }
    })

    $form.Add_Shown({
        $timer.Start()
    })
    $form.Add_FormClosed({
        $timer.Stop()
        $timer.Dispose()
    })

    [void]$form.ShowDialog()
}

$resolvedBundleRoot = Resolve-BundleRootPath -BundleRootValue $BundleRoot
$runToken = if ([string]::IsNullOrWhiteSpace($RunId)) {
    Get-Date -Format "yyyyMMdd-HHmmss"
}
else {
    $RunId
}
$windowsLogDir = Join-Path $resolvedBundleRoot "windows\logs"
New-Item -ItemType Directory -Force -Path $windowsLogDir | Out-Null
$resolvedStatusFile = if ([string]::IsNullOrWhiteSpace($StatusFile)) {
    Join-Path $windowsLogDir ("deploy-" + $runToken + ".status.json")
}
else {
    $StatusFile
}
$resolvedLogFile = if ([string]::IsNullOrWhiteSpace($LogFile)) {
    Join-Path $windowsLogDir ("deploy-" + $runToken + ".log")
}
else {
    $LogFile
}

if ($Mode -eq "Gui") {
    $initialContext = New-DeployContext `
        -ResolvedBundleRoot $resolvedBundleRoot `
        -ResolvedStatusFile $resolvedStatusFile `
        -ResolvedLogFile $resolvedLogFile `
        -DeployRunId $runToken
    Save-DeployState -Context $initialContext
    Show-DeployWindow `
        -ResolvedBundleRoot $resolvedBundleRoot `
        -ResolvedStatusFile $resolvedStatusFile `
        -ResolvedLogFile $resolvedLogFile `
        -DeployRunId $runToken
    exit 0
}

$context = New-DeployContext `
    -ResolvedBundleRoot $resolvedBundleRoot `
    -ResolvedStatusFile $resolvedStatusFile `
    -ResolvedLogFile $resolvedLogFile `
    -DeployRunId $runToken
Save-DeployState -Context $context

try {
    Invoke-DeployWorker -Context $context
    exit 0
}
catch {
    Write-DeployLog -Context $context -Message ("部署失败: " + $_.Exception.Message)
    exit 1
}
