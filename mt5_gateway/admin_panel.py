"""轻量管理面板服务，负责聚合网关状态、日志、配置与常用运维操作。"""

from __future__ import annotations

import json
import os
import asyncio
import socket
import subprocess
import urllib.error
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import uvicorn
except Exception:  # pragma: no cover - 测试环境可缺省运行依赖
    uvicorn = None

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - 测试环境可缺省运行依赖
    def load_dotenv(*args, **kwargs):
        return None

try:
    from fastapi import FastAPI, HTTPException, Query, Response
    from fastapi.responses import HTMLResponse, PlainTextResponse
except Exception:  # pragma: no cover - 测试环境可缺省运行依赖
    class FastAPI:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

        def post(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    class HTTPException(Exception):
        def __init__(self, status_code: int = 500, detail: Any = ""):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    def Query(default=None, **kwargs):  # type: ignore[override]
        return default

    class HTMLResponse(str):
        pass

    class PlainTextResponse(str):
        pass

    class Response:  # type: ignore[override]
        def __init__(self, content: str = "", media_type: str = "text/plain"):
            self.content = content
            self.media_type = media_type

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
LOGS_DIR = BASE_DIR / "logs"
STATIC_DIR = BASE_DIR / "static" / "admin"


# 兼容“仓库源码目录”和“部署包目录”两种运行布局。
def resolve_project_root() -> Path:
    bundle_root = BASE_DIR.parent
    if (bundle_root / "windows").exists():
        return bundle_root
    return BASE_DIR.parent.parent


REPO_ROOT = resolve_project_root()

load_dotenv(ENV_PATH)

app = FastAPI(title="MT5 Gateway Admin Panel", version="1.0.0")


# 管理面板也运行在同一台 Windows 主机上，统一切回 Selector 事件循环，避免 Proactor 断链异常污染进程。
def _configure_windows_event_loop_policy() -> None:
    if os.name != "nt":
        return
    selector_policy = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if selector_policy is None:
        return
    try:
        current_policy = asyncio.get_event_loop_policy()
        if isinstance(current_policy, selector_policy):
            return
    except Exception:
        pass
    try:
        asyncio.set_event_loop_policy(selector_policy())
    except Exception:
        pass


# 统一按 UTF-8 读取文本文件。
def read_text_file_utf8(path: Path) -> str:
    if not path.exists():
        return ""
    for encoding, errors in (("utf-8", "strict"), ("gbk", "strict"), ("utf-8", "replace"), ("gbk", "replace")):
        try:
            return path.read_text(encoding=encoding, errors=errors).lstrip("\ufeff\ufffe")
        except Exception:
            continue
    return path.read_text(encoding="utf-8", errors="replace").lstrip("\ufeff\ufffe")


# 统一按 UTF-8 写回文本文件。
def write_text_file_utf8(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content or "", encoding="utf-8")


# 把 .env 文本解析为简单字典，供状态卡片和命令构建复用。
def parse_env_map(content: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for raw_line in (content or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip().strip('"').strip("'")
    return result


# 读取当前 .env 的键值映射。
def load_env_map() -> Dict[str, str]:
    return parse_env_map(read_text_file_utf8(ENV_PATH))


# 管理面板优先使用显式网关地址，否则退回本机 127.0.0.1。
def resolve_gateway_url(env_map: Dict[str, str]) -> str:
    explicit = str((env_map or {}).get("ADMIN_GATEWAY_URL", "") or "").strip()
    if explicit:
        return explicit.rstrip("/")
    host = str((env_map or {}).get("GATEWAY_HOST", "127.0.0.1") or "127.0.0.1").strip()
    port = str((env_map or {}).get("GATEWAY_PORT", "8787") or "8787").strip()
    normalized_host = "127.0.0.1" if host in ("0.0.0.0", "", "::") else host
    return f"http://{normalized_host}:{port}".rstrip("/")


# PowerShell 单引号安全转义，避免路径和命令参数被截断。
def ps_quote(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


# 生成“停止后再启动”的重启命令。
def compose_restart_command(stop_command: str, start_command: str) -> str:
    stop_part = stop_command or ""
    start_part = start_command or ""
    if not stop_part:
        return start_part
    if not start_part:
        return stop_part
    return f"{stop_part}; Start-Sleep -Seconds 2; {start_part}"


# 按可执行文件路径精确匹配当前组件进程，避免误伤同机其他同名进程。
def build_managed_process_stop_command(exe_path: str) -> str:
    safe_exe_path = str(exe_path or "").strip()
    if not safe_exe_path:
        return "throw 'Executable path is not configured.'"
    normalized_exe_path = safe_exe_path.replace("\\", "/").replace("'", "''").lower()
    return (
        "Get-CimInstance Win32_Process | Where-Object { "
        + "$normalizedExecutablePath = (([string]$_.ExecutablePath) -replace '\\\\', '/').ToLowerInvariant(); "
        + "$normalizedExecutablePath -eq "
        + ps_quote(normalized_exe_path)
        + " } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
    )


# 按可执行文件路径精确读取组件进程状态，避免把其他部署实例算进来。
def build_managed_process_status_command(exe_path: str) -> str:
    safe_exe_path = str(exe_path or "").strip()
    if not safe_exe_path:
        return ""
    normalized_exe_path = safe_exe_path.replace("\\", "/").replace("'", "''").lower()
    return (
        "Get-CimInstance Win32_Process | Where-Object { "
        + "$normalizedExecutablePath = (([string]$_.ExecutablePath) -replace '\\\\', '/').ToLowerInvariant(); "
        + "$normalizedExecutablePath -eq "
        + ps_quote(normalized_exe_path)
        + " } | Select-Object Name,ProcessId,ExecutablePath | ConvertTo-Json -Compress"
    )


# 解析当前管理面板所处的部署布局。
def resolve_runtime_layout(project_root: str) -> Dict[str, Any]:
    safe_root = Path(str(project_root or REPO_ROOT))
    bundle_windows_dir = safe_root / "windows"
    if bundle_windows_dir.exists():
        return {
            "mode": "bundle",
            "root": safe_root,
            "windows_dir": bundle_windows_dir,
            "gateway_runner": bundle_windows_dir / "run_gateway.ps1",
            "gateway_arg_name": "BundleRoot",
        }
    repo_windows_dir = safe_root / "deploy" / "tencent" / "windows"
    return {
        "mode": "repo",
        "root": safe_root,
        "windows_dir": repo_windows_dir,
        "gateway_runner": repo_windows_dir / "run_gateway.ps1",
        "gateway_arg_name": "RepoRoot",
    }


# 生成隐藏启动 Caddy 的默认命令，避免弹出独立命令窗口。
def build_hidden_caddy_start_command(exe_path: str, windows_dir: Path, config_path: str) -> str:
    if not exe_path:
        return ""
    stop_same_caddy = build_managed_process_stop_command(exe_path)
    safe_exe = ps_quote(exe_path)
    safe_windows_dir = ps_quote(str(windows_dir))
    safe_config = ps_quote(config_path)
    safe_log_dir = ps_quote(str(windows_dir / "logs"))
    safe_stdout = ps_quote(str(windows_dir / "logs" / "caddy-out.log"))
    safe_stderr = ps_quote(str(windows_dir / "logs" / "caddy-err.log"))
    return (
        f"if (Test-Path {safe_exe}) "
        + "{ "
        + f"New-Item -ItemType Directory -Force -Path {safe_log_dir} | Out-Null; "
        + stop_same_caddy
        + "; "
        + "Start-Process -WindowStyle Hidden -FilePath "
        + safe_exe
        + " -ArgumentList @('run','--config',"
        + safe_config
        + ") -WorkingDirectory "
        + safe_windows_dir
        + " -RedirectStandardOutput "
        + safe_stdout
        + " -RedirectStandardError "
        + safe_stderr
        + " } else { throw 'Executable path is not configured.' }"
    )


# 统一把组件管理能力整理成页面可直接消费的结构。
def build_component_registry(env_map: Dict[str, str], repo_root: str) -> Dict[str, Dict[str, Any]]:
    safe_env = env_map or {}
    safe_repo_root = str(repo_root or REPO_ROOT)
    safe_repo_root_pattern = safe_repo_root.replace("'", "''")
    safe_gateway_dir = str(BASE_DIR)
    safe_gateway_dir_pattern = safe_gateway_dir.replace("\\", "/").replace("'", "''").lower()
    runtime_layout = resolve_runtime_layout(safe_repo_root)
    gateway_task_name = str(safe_env.get("ADMIN_GATEWAY_TASK_NAME", "MT5GatewayAutoStart") or "MT5GatewayAutoStart").strip()
    gateway_runner = runtime_layout["gateway_runner"]
    gateway_arg_name = str(runtime_layout["gateway_arg_name"] or "RepoRoot")
    windows_dir = runtime_layout["windows_dir"]
    mt5_path = str(safe_env.get("ADMIN_MT5_PATH") or safe_env.get("MT5_PATH") or "").strip()
    caddy_path = str(safe_env.get("ADMIN_CADDY_PATH") or safe_env.get("CADDY_PATH") or "").strip()
    nginx_path = str(safe_env.get("ADMIN_NGINX_PATH") or safe_env.get("NGINX_PATH") or "").strip()
    caddy_config_path = str(
        safe_env.get("ADMIN_CADDY_CONFIG_PATH")
        or safe_env.get("CADDY_CONFIG_PATH")
        or (windows_dir / "Caddyfile")
    ).strip()

    gateway_start = str(safe_env.get("ADMIN_GATEWAY_START_CMD", "") or "").strip()
    if not gateway_start:
        gateway_start = (
            f"if (Get-ScheduledTask -TaskName {ps_quote(gateway_task_name)} -ErrorAction SilentlyContinue) "
            + "{ Start-ScheduledTask -TaskName "
            + ps_quote(gateway_task_name)
            + " } elseif (Test-Path "
            + ps_quote(str(gateway_runner))
            + ") { Start-Process powershell.exe -ArgumentList "
            + "@('-NoProfile','-ExecutionPolicy','Bypass','-File',"
            + ps_quote(str(gateway_runner))
            + ",'-"
            + gateway_arg_name
            + "',"
            + ps_quote(safe_repo_root)
            + ") } else { throw 'Gateway runner not found.' }"
        )

    gateway_stop = str(safe_env.get("ADMIN_GATEWAY_STOP_CMD", "") or "").strip()
    if not gateway_stop:
        gateway_stop = (
            f"if (Get-ScheduledTask -TaskName {ps_quote(gateway_task_name)} -ErrorAction SilentlyContinue) "
            + "{ Stop-ScheduledTask -TaskName "
            + ps_quote(gateway_task_name)
            + " -ErrorAction SilentlyContinue }; "
            + "Get-CimInstance Win32_Process | Where-Object { "
            + "$normalizedExecutablePath = (([string]$_.ExecutablePath) -replace '\\\\', '/').ToLowerInvariant(); "
            + "$normalizedCommandLine = (([string]$_.CommandLine) -replace '\\\\', '/').ToLowerInvariant(); "
            + "$pythonExecutableManagedByBundle = $normalizedExecutablePath.StartsWith("
            + ps_quote(safe_gateway_dir_pattern)
            + "); "
            + "($normalizedCommandLine.Contains('server_v2.py')) -and ("
            + "$pythonExecutableManagedByBundle -or "
            + f"($normalizedCommandLine.Contains({ps_quote(safe_repo_root_pattern.lower())}))"
            + ")"
            + " } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
        )

    registry = {
        "gateway": {
            "label": "MT5 网关",
            "statusMode": "gateway",
            "taskName": gateway_task_name,
            "processNames": ["python", "pythonw"],
            "actions": {
                "start": gateway_start,
                "stop": gateway_stop,
                "restart": str(safe_env.get("ADMIN_GATEWAY_RESTART_CMD", "") or "").strip()
                or compose_restart_command(gateway_stop, gateway_start),
            },
        },
        "mt5": build_process_component(
            label="MT5 客户端",
            process_names=["terminal64"],
            start_cmd=str(safe_env.get("ADMIN_MT5_START_CMD", "") or "").strip(),
            stop_cmd=str(safe_env.get("ADMIN_MT5_STOP_CMD", "") or "").strip(),
            restart_cmd=str(safe_env.get("ADMIN_MT5_RESTART_CMD", "") or "").strip(),
            exe_path=mt5_path,
        ),
        "caddy": build_service_or_process_component(
            label="Caddy",
            process_names=["caddy"],
            service_name=str(safe_env.get("ADMIN_CADDY_SERVICE_NAME", "") or "").strip(),
            start_cmd=str(safe_env.get("ADMIN_CADDY_START_CMD", "") or "").strip(),
            stop_cmd=str(safe_env.get("ADMIN_CADDY_STOP_CMD", "") or "").strip(),
            restart_cmd=str(safe_env.get("ADMIN_CADDY_RESTART_CMD", "") or "").strip(),
            exe_path=caddy_path,
            stop_signal_arg="",
            windows_dir=windows_dir,
            config_path=caddy_config_path,
        ),
        "nginx": build_service_or_process_component(
            label="Nginx",
            process_names=["nginx"],
            service_name=str(safe_env.get("ADMIN_NGINX_SERVICE_NAME", "") or "").strip(),
            start_cmd=str(safe_env.get("ADMIN_NGINX_START_CMD", "") or "").strip(),
            stop_cmd=str(safe_env.get("ADMIN_NGINX_STOP_CMD", "") or "").strip(),
            restart_cmd=str(safe_env.get("ADMIN_NGINX_RESTART_CMD", "") or "").strip(),
            exe_path=nginx_path,
            stop_signal_arg="-s stop",
            windows_dir=windows_dir,
            config_path="",
        ),
    }
    return registry


# 构建基于进程的组件默认启停命令。
def build_process_component(label: str,
                            process_names: List[str],
                            start_cmd: str,
                            stop_cmd: str,
                            restart_cmd: str,
                            exe_path: str) -> Dict[str, Any]:
    effective_start = start_cmd or (
        f"if (Test-Path {ps_quote(exe_path)}) "
        + "{ Start-Process -FilePath "
        + ps_quote(exe_path)
        + " } else { throw 'Executable path is not configured.' }"
        if exe_path
        else ""
    )
    effective_stop = stop_cmd or build_managed_process_stop_command(exe_path)
    effective_restart = restart_cmd or compose_restart_command(effective_stop, effective_start)
    return {
        "label": label,
        "statusMode": "process",
        "exePath": exe_path,
        "processNames": process_names,
        "actions": {
            "start": effective_start,
            "stop": effective_stop,
            "restart": effective_restart,
        },
    }


# 优先支持 Windows 服务，其次退回自定义命令或进程默认命令。
def build_service_or_process_component(label: str,
                                       process_names: List[str],
                                       service_name: str,
                                       start_cmd: str,
                                       stop_cmd: str,
                                       restart_cmd: str,
                                       exe_path: str,
                                       stop_signal_arg: str,
                                       windows_dir: Path,
                                       config_path: str) -> Dict[str, Any]:
    if service_name:
        service_start = start_cmd or f"Start-Service -Name {ps_quote(service_name)}"
        service_stop = stop_cmd or f"Stop-Service -Name {ps_quote(service_name)} -Force"
        service_restart = restart_cmd or f"Restart-Service -Name {ps_quote(service_name)} -Force"
        return {
            "label": label,
            "statusMode": "service",
            "serviceName": service_name,
            "processNames": process_names,
            "actions": {
                "start": service_start,
                "stop": service_stop,
                "restart": service_restart,
            },
        }

    effective_start = start_cmd or (
        build_hidden_caddy_start_command(exe_path, windows_dir, config_path)
        if label == "Caddy"
        else (
            f"if (Test-Path {ps_quote(exe_path)}) "
            + "{ Start-Process -FilePath "
            + ps_quote(exe_path)
            + " } else { throw 'Executable path is not configured.' }"
            if exe_path
            else ""
        )
    )
    effective_stop = stop_cmd
    if not effective_stop:
        if exe_path and stop_signal_arg:
            effective_stop = f"& {ps_quote(exe_path)} {stop_signal_arg}"
        else:
            effective_stop = build_managed_process_stop_command(exe_path)
    effective_restart = restart_cmd or compose_restart_command(effective_stop, effective_start)
    return {
        "label": label,
        "statusMode": "process",
        "exePath": exe_path,
        "processNames": process_names,
        "actions": {
            "start": effective_start,
            "stop": effective_stop,
            "restart": effective_restart,
        },
    }


# 调用 PowerShell 执行本机管理命令。
def run_powershell_command(command: str) -> Dict[str, Any]:
    completed = subprocess.run(
        ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return {
        "ok": completed.returncode == 0,
        "code": completed.returncode,
        "stdout": (completed.stdout or "").strip(),
        "stderr": (completed.stderr or "").strip(),
        "command": command,
    }


# 读取指定 Windows 服务的状态。
def read_service_status(service_name: str) -> Dict[str, Any]:
    if not service_name:
        return {"running": False, "statusText": "未配置服务名"}
    command = (
        "Get-Service -Name "
        + ps_quote(service_name)
        + " -ErrorAction SilentlyContinue | Select-Object Name,Status | ConvertTo-Json -Compress"
    )
    result = run_powershell_command(command)
    if not result["ok"] or not result["stdout"]:
        return {"running": False, "statusText": "未运行", "details": result}
    payload = json.loads(result["stdout"])
    status_text = str(payload.get("Status", "") or "")
    return {
        "running": status_text.lower() == "running",
        "statusText": status_text or "未知",
        "details": payload,
    }


# 读取指定进程组的运行状态。
def read_process_status(process_names: List[str], exe_path: str = "") -> Dict[str, Any]:
    safe_names = [name for name in (process_names or []) if name]
    status_command = build_managed_process_status_command(exe_path)
    if status_command:
        command = status_command
    elif safe_names:
        command = (
            "Get-Process -Name @("
            + ",".join(ps_quote(name) for name in safe_names)
            + ") -ErrorAction SilentlyContinue | "
            + "Select-Object ProcessName,Id | ConvertTo-Json -Compress"
        )
    else:
        return {"running": False, "statusText": "未配置进程名"}
    result = run_powershell_command(command)
    if not result["ok"] or not result["stdout"]:
        return {"running": False, "statusText": "未运行", "details": result}
    payload = json.loads(result["stdout"])
    items = payload if isinstance(payload, list) else [payload]
    return {
        "running": len(items) > 0,
        "statusText": f"运行中 ({len(items)})",
        "details": items,
    }


# 从网关拉取 JSON，失败时统一抛出可读错误。
def request_gateway_json(base_url: str,
                         path: str,
                         method: str = "GET",
                         payload: Optional[Dict[str, Any]] = None,
                         timeout: int = 12) -> Dict[str, Any]:
    url = f"{str(base_url or '').rstrip('/')}{path}"
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=max(1, int(timeout or 12))) as response:
        text = response.read().decode("utf-8")
    return json.loads(text or "{}")


# 读取本机 TCP 端口是否已监听，供网关健康检查失败时兜底判断。
def is_tcp_endpoint_open(url: str, timeout: float = 1.0) -> bool:
    parsed = urllib.parse.urlparse(str(url or ""))
    host = str(parsed.hostname or "").strip()
    if not host:
        return False
    if host in ("0.0.0.0", "::"):
        host = "127.0.0.1"
    port = int(parsed.port or (443 if parsed.scheme == "https" else 80))
    try:
        with socket.create_connection((host, port), timeout=max(0.2, float(timeout or 1.0))):
            return True
    except Exception:
        return False


# 读取管理面板右侧日志区的最近内容。
def read_recent_logs(logs_dir: Path, limit: int) -> List[Dict[str, Any]]:
    if not logs_dir.exists():
        return []
    safe_limit = max(1, int(limit or 1))
    files = sorted(
        [path for path in logs_dir.glob("*.log") if path.is_file()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    entries: List[Dict[str, Any]] = []
    for path in files:
        lines = read_text_file_utf8(path).splitlines()
        if not lines:
            continue
        remaining = safe_limit - len(entries)
        if remaining <= 0:
            break
        tail = lines[-remaining:]
        for line in tail:
            entries.append({"file": path.name, "line": line})
        if len(entries) >= safe_limit:
            break
    return entries[:safe_limit]


# 汇总单个组件的当前状态，供状态卡片与操作按钮复用。
def inspect_component(target: str, spec: Dict[str, Any], gateway_url: str) -> Dict[str, Any]:
    if target == "gateway":
        try:
            health = request_gateway_json(gateway_url, "/health", timeout=6)
            return {
                "running": bool(health.get("ok", False)),
                "statusText": "在线" if health.get("ok", False) else "异常",
                "details": health,
            }
        except Exception as exc:
            if is_tcp_endpoint_open(gateway_url, timeout=1.0):
                return {
                    "running": True,
                    "statusText": "端口在线",
                    "details": {"warning": f"健康检查超时或失败：{exc}"},
                }
            return {"running": False, "statusText": "离线", "details": {"error": str(exc)}}

    if spec.get("statusMode") == "service":
        return read_service_status(str(spec.get("serviceName", "") or ""))
    return read_process_status(
        list(spec.get("processNames") or []),
        str(spec.get("exePath", "") or ""),
    )


def decorate_gateway_payload(payload: Dict[str, Any], fallback_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    safe_payload = dict(payload or {})
    session_state = safe_payload.get("session") or {}
    safe_payload["session"] = {
        "activeAccount": dict(session_state.get("activeAccount") or {}),
        "savedAccountCount": int(session_state.get("savedAccountCount") or 0),
    }
    offset_minutes = int(safe_payload.get("mt5TimeOffsetMinutes", 0) or 0)
    server_timezone = str(safe_payload.get("mt5ServerTimezone", "") or "").strip()
    if not server_timezone:
        safe_payload["mt5ServerTimezoneWarning"] = "当前未配置 MT5_SERVER_TIMEZONE，MT5 历史时间归一化将无法成立。"
    elif offset_minutes != 0:
        safe_payload["mt5TimeOffsetWarning"] = (
            f"当前 MT5_TIME_OFFSET_MINUTES={offset_minutes}；严格时间链只认 MT5_SERVER_TIMEZONE，该分钟偏移不应再作为真值。"
        )
    if fallback_state and str(safe_payload.get("error", "") or "").strip():
        safe_payload.setdefault("portOnline", bool(fallback_state.get("running", False)))
        safe_payload.setdefault("statusText", str(fallback_state.get("statusText", "") or ""))
        fallback_details = fallback_state.get("details") or {}
        warning = str(fallback_details.get("warning", "") or "")
        if warning:
            safe_payload["warning"] = warning
    return safe_payload


# 读取管理面板总状态。
def build_admin_state() -> Dict[str, Any]:
    env_map = load_env_map()
    gateway_url = resolve_gateway_url(env_map)
    registry = build_component_registry(env_map, str(REPO_ROOT))
    gateway_component_state = inspect_component("gateway", registry.get("gateway") or {}, gateway_url)
    gateway_health: Dict[str, Any]
    gateway_source: Dict[str, Any]
    try:
        gateway_health = request_gateway_json(gateway_url, "/health", timeout=6)
    except Exception as exc:
        gateway_health = {"ok": False, "error": f"健康检查超时或失败：{exc}"}
    try:
        gateway_source = request_gateway_json(gateway_url, "/v1/source", timeout=6)
    except Exception as exc:
        gateway_source = {"ok": False, "error": f"来源检查超时或失败：{exc}"}
    gateway_health = decorate_gateway_payload(gateway_health, gateway_component_state)
    gateway_source = decorate_gateway_payload(gateway_source, gateway_component_state)

    components = {}
    for target, spec in registry.items():
        state = gateway_component_state if target == "gateway" else inspect_component(target, spec, gateway_url)
        components[target] = {
            "label": spec.get("label"),
            "state": state,
            "actions": spec.get("actions") or {},
        }
    return {
        "gatewayUrl": gateway_url,
        "envPath": str(ENV_PATH),
        "components": components,
        "gatewayHealth": gateway_health,
        "gatewaySource": gateway_source,
        "logsDir": str(LOGS_DIR),
    }


# 提供管理面板主页。
@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return read_text_file_utf8(STATIC_DIR / "index.html")


# 提供管理面板脚本文件。
@app.get("/app.js", response_class=PlainTextResponse)
def app_js() -> Response:
    return Response(
        content=read_text_file_utf8(STATIC_DIR / "app.js"),
        media_type="application/javascript",
    )


# 提供管理面板样式文件。
@app.get("/styles.css", response_class=PlainTextResponse)
def styles_css() -> Response:
    return Response(
        content=read_text_file_utf8(STATIC_DIR / "styles.css"),
        media_type="text/css",
    )


# 返回统一状态卡片需要的数据。
@app.get("/api/state")
def api_state() -> Dict[str, Any]:
    return build_admin_state()


# 返回当前 .env 原文，供面板直接编辑。
@app.get("/api/env")
def api_env() -> Dict[str, Any]:
    return {"path": str(ENV_PATH), "content": read_text_file_utf8(ENV_PATH)}


# 保存新的 .env 原文。
@app.post("/api/env")
def api_save_env(payload: Dict[str, Any]) -> Dict[str, Any]:
    content = str((payload or {}).get("content", "") or "")
    write_text_file_utf8(ENV_PATH, content)
    return {"ok": True, "path": str(ENV_PATH), "size": len(content)}


# 返回最近日志内容。
@app.get("/api/logs")
def api_logs(limit: int = Query(default=200, ge=20, le=2000)) -> Dict[str, Any]:
    return {"entries": read_recent_logs(LOGS_DIR, limit)}


# 读取当前异常规则配置。
@app.get("/api/abnormal-config")
def api_abnormal_config() -> Dict[str, Any]:
    gateway_url = resolve_gateway_url(load_env_map())
    try:
        return request_gateway_json(gateway_url, "/v1/abnormal")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"读取异常规则失败: {exc}")


# 保存新的异常规则配置。
@app.post("/api/abnormal-config")
def api_save_abnormal_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    gateway_url = resolve_gateway_url(load_env_map())
    try:
        return request_gateway_json(gateway_url, "/v1/abnormal/config", method="POST", payload=payload or {})
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"保存异常规则失败: {exc}")


# 请求网关清空运行时缓存。
@app.post("/api/cache/clear")
def api_clear_cache() -> Dict[str, Any]:
    gateway_url = resolve_gateway_url(load_env_map())
    try:
        return request_gateway_json(gateway_url, "/internal/admin/cache/clear", method="POST", payload={})
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"清缓存失败: {exc}")


# 执行本机组件的启动、停止或重启。
@app.post("/api/process")
def api_process_action(payload: Dict[str, Any]) -> Dict[str, Any]:
    safe_payload = payload or {}
    target = str(safe_payload.get("target", "") or "").strip().lower()
    action = str(safe_payload.get("action", "") or "").strip().lower()
    registry = build_component_registry(load_env_map(), str(REPO_ROOT))
    if target not in registry:
        raise HTTPException(status_code=404, detail=f"未知组件: {target}")
    if action not in ("start", "stop", "restart"):
        raise HTTPException(status_code=400, detail=f"未知操作: {action}")
    command = str((registry[target].get("actions") or {}).get(action, "") or "").strip()
    if not command:
        raise HTTPException(status_code=400, detail=f"{target} 未配置 {action} 命令")
    result = run_powershell_command(command)
    if not result["ok"]:
        raise HTTPException(status_code=500, detail=result)
    gateway_url = resolve_gateway_url(load_env_map())
    return {
        "ok": True,
        "target": target,
        "action": action,
        "result": result,
        "state": inspect_component(target, registry[target], gateway_url),
    }


if __name__ == "__main__":
    env_map = load_env_map()
    host = str(env_map.get("ADMIN_PANEL_HOST", "0.0.0.0") or "0.0.0.0").strip()
    port = int(str(env_map.get("ADMIN_PANEL_PORT", "8788") or "8788").strip() or "8788")
    _configure_windows_event_loop_policy()
    uvicorn.run("admin_panel:app", host=host, port=port, reload=False)
