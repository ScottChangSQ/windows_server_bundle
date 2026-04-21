"""独立执行 MT5 登录探针，避免主进程被 MT5 调用永久阻塞。"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict


_TRACE_FILE_PATH = ""


def _read_payload() -> Dict[str, Any]:
    """从标准输入读取登录探针参数。"""
    raw = sys.stdin.buffer.read()
    if not raw:
        raise ValueError("probe payload is empty")
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("probe payload must be an object")
    return payload


def _build_error(message: str) -> int:
    """输出统一错误结构并返回失败退出码。"""
    sys.stdout.write(json.dumps({"ok": False, "error": str(message or "")}, ensure_ascii=False))
    sys.stdout.flush()
    return 1


def _now_ms() -> int:
    """返回当前毫秒时间戳。"""
    return int(time.time() * 1000)


def _flush_trace(trace: list[Dict[str, Any]]) -> None:
    """把当前阶段轨迹立即写入 trace 文件，保证超时场景仍可回收。"""
    global _TRACE_FILE_PATH
    trace_file_path = str(_TRACE_FILE_PATH or "").strip()
    if not trace_file_path:
        return
    try:
        trace_file = Path(trace_file_path)
        temp_file = trace_file.with_suffix(trace_file.suffix + ".tmp")
        temp_file.write_text(json.dumps({"trace": trace}, ensure_ascii=False), encoding="utf-8")
        temp_file.replace(trace_file)
    except Exception:
        pass


def _append_trace(trace: list[Dict[str, Any]],
                  stage: str,
                  status: str,
                  message: str,
                  **detail: Any) -> None:
    """向探针结果里追加一条阶段轨迹。"""
    item = {
        "stage": str(stage or "").strip(),
        "status": str(status or "").strip(),
        "message": str(message or "").strip(),
        "serverTime": _now_ms(),
    }
    safe_detail = {str(key): value for key, value in detail.items() if value not in (None, "", [])}
    if safe_detail:
        item["detail"] = safe_detail
    trace.append(item)
    _flush_trace(trace)


def _initialize_base_session(mt5_module: Any,
                             path_value: str,
                             timeout_ms: int,
                             trace: list[Dict[str, Any]]) -> tuple[bool, str]:
    """先建立基础 MT5 会话，兼容不接受 timeout 的旧版 SDK。"""
    _append_trace(trace, "base_initialize_start", "pending", "开始建立基础终端连接")
    kwargs = {"timeout": timeout_ms}
    if path_value:
        kwargs["path"] = path_value
    try:
        initialized = bool(mt5_module.initialize(**kwargs))
    except TypeError:
        _append_trace(trace, "base_initialize_retry_without_timeout", "pending", "当前 SDK 不支持 timeout，改用旧版 initialize")
        legacy_kwargs = dict(kwargs)
        legacy_kwargs.pop("timeout", None)
        try:
            initialized = bool(mt5_module.initialize(**legacy_kwargs))
        except Exception as exc:
            _append_trace(trace, "base_initialize_failed", "failed", f"基础终端连接失败: {exc}")
            return False, f"MetaTrader5 initialize failed: {exc}"
    except Exception as exc:
        _append_trace(trace, "base_initialize_failed", "failed", f"基础终端连接失败: {exc}")
        return False, f"MetaTrader5 initialize failed: {exc}"
    if not initialized:
        _append_trace(trace, "base_initialize_failed", "failed", f"基础终端连接失败: {mt5_module.last_error()}")
        return False, f"MetaTrader5 initialize failed: {mt5_module.last_error()}"
    _append_trace(trace, "base_initialize_ok", "ok", "已建立基础终端连接")
    return True, ""


def _login_via_legacy_flow(mt5_module: Any,
                           login_value: int,
                           password_value: str,
                           server_value: str,
                           timeout_ms: int,
                           trace: list[Dict[str, Any]]) -> tuple[bool, str]:
    """当 authenticated initialize 不可用时，改用 initialize + login 正式完成鉴权。"""
    _append_trace(trace, "legacy_login_start", "pending", "开始执行 legacy initialize + login 登录")
    try:
        logged_in = bool(
            mt5_module.login(
                login_value,
                password=password_value,
                server=server_value,
                timeout=timeout_ms,
            )
        )
    except TypeError:
        _append_trace(trace, "legacy_login_retry_without_timeout", "pending", "当前 SDK 不支持 login(timeout)，改用旧版 login")
        try:
            logged_in = bool(
                mt5_module.login(
                    login_value,
                    password=password_value,
                    server=server_value,
                )
            )
        except Exception as exc:
            _append_trace(trace, "legacy_login_failed", "failed", f"legacy login 失败: {exc}")
            return False, f"MetaTrader5 login failed: {exc}"
    except Exception as exc:
        _append_trace(trace, "legacy_login_failed", "failed", f"legacy login 失败: {exc}")
        return False, f"MetaTrader5 login failed: {exc}"
    if not logged_in:
        _append_trace(trace, "legacy_login_failed", "failed", f"legacy login 失败: {mt5_module.last_error()}")
        return False, f"MetaTrader5 login failed: {mt5_module.last_error()}"
    _append_trace(trace, "legacy_login_ok", "ok", "legacy login 完成")
    return True, ""


def _resolve_current_account_identity(mt5_module: Any) -> tuple[str, str]:
    """读取当前终端已经激活的 canonical 账号身份。"""
    account = mt5_module.account_info()
    return (
        str(getattr(account, "login", "") or "").strip(),
        str(getattr(account, "server", "") or "").strip(),
    )


def _is_current_account_match(mt5_module: Any,
                              login_value: int,
                              server_value: str,
                              trace: list[Dict[str, Any]]) -> tuple[bool, str, str]:
    """判断当前终端是否已经就是目标账号。"""
    current_login, current_server = _resolve_current_account_identity(mt5_module)
    _append_trace(
        trace,
        "current_terminal_identity",
        "ok",
        "已读取当前终端账号身份",
        currentTerminalLogin=current_login,
        currentTerminalServer=current_server,
    )
    if current_login != str(int(login_value)):
        return False, current_login, current_server
    if current_server and _normalize_server_identity(current_server) != _normalize_server_identity(server_value):
        return False, current_login, current_server
    return True, current_login, current_server


def _normalize_server_identity(server_value: Any) -> str:
    """统一规范化服务器名，避免仅分隔符差异就错过现有会话复用。"""
    raw = str(server_value or "").strip().lower()
    if not raw:
        return ""
    return "".join(ch for ch in raw if ch.isalnum())


def _initialize_authenticated_session(mt5_module: Any,
                                      login_value: int,
                                      password_value: str,
                                      server_value: str,
                                      path_value: str,
                                      timeout_ms: int,
                                      trace: list[Dict[str, Any]]) -> tuple[bool, str]:
    """优先走带鉴权初始化；若 SDK 不支持，则退回同样严格的 legacy 登录主链。"""
    _append_trace(trace, "authenticated_initialize_start", "pending", "开始执行 authenticated initialize 登录")
    kwargs = {
        "timeout": timeout_ms,
        "login": login_value,
        "password": password_value,
        "server": server_value,
    }
    if path_value:
        kwargs["path"] = path_value
    try:
        initialized = bool(mt5_module.initialize(**kwargs))
    except TypeError:
        _append_trace(trace, "authenticated_initialize_unsupported", "pending", "当前 SDK 不支持 authenticated initialize，切到 legacy 登录")
        initialized, error = _initialize_base_session(mt5_module, path_value, timeout_ms, trace)
        if not initialized:
            return False, error
        return _login_via_legacy_flow(
            mt5_module,
            login_value=login_value,
            password_value=password_value,
            server_value=server_value,
            timeout_ms=timeout_ms,
            trace=trace,
        )
    except Exception as exc:
        _append_trace(trace, "authenticated_initialize_failed", "failed", f"authenticated initialize 失败: {exc}")
        return False, f"MetaTrader5 initialize failed: {exc}"
    if not initialized:
        _append_trace(trace, "authenticated_initialize_failed", "failed", f"authenticated initialize 失败: {mt5_module.last_error()}")
        return False, f"MetaTrader5 initialize/login failed: {mt5_module.last_error()}"
    _append_trace(trace, "authenticated_initialize_ok", "ok", "authenticated initialize 完成")
    return True, ""


def _resolve_existing_session_before_relogin(mt5_module: Any,
                                             login_value: int,
                                             server_value: str,
                                             path_value: str,
                                             timeout_ms: int,
                                             trace: list[Dict[str, Any]]) -> tuple[bool, str, str]:
    """若当前终端已经是目标账号，则直接复用现有已登录会话。"""
    initialized, _ = _initialize_base_session(mt5_module, path_value, timeout_ms, trace)
    if not initialized:
        return False, "", ""
    matched, current_login, current_server = _is_current_account_match(
        mt5_module,
        login_value=login_value,
        server_value=server_value,
        trace=trace,
    )
    if not matched:
        _append_trace(
            trace,
            "existing_session_reuse_miss",
            "ok",
            "当前终端账号不是目标账号，需要继续执行正式登录",
            currentTerminalLogin=current_login,
            currentTerminalServer=current_server,
        )
        return False, "", ""
    _append_trace(
        trace,
        "existing_session_reused",
        "ok",
        "当前终端已是目标账号，直接复用现有登录会话",
        currentTerminalLogin=current_login,
        currentTerminalServer=current_server,
    )
    return True, current_login, current_server


def main() -> int:
    """执行带鉴权初始化并返回 canonical 账号身份。"""
    global _TRACE_FILE_PATH
    trace: list[Dict[str, Any]] = []
    try:
        payload = _read_payload()
        _TRACE_FILE_PATH = str(payload.get("traceFile") or "").strip()
        _append_trace(trace, "probe_payload_received", "ok", "已收到登录探针参数")
        login_value = int(payload.get("login") or 0)
        password_value = str(payload.get("password") or "")
        server_value = str(payload.get("server") or "").strip()
        path_value = str(payload.get("path") or "").strip()
        timeout_ms = int(payload.get("timeoutMs") or 0)
        if login_value <= 0 or not password_value or not server_value or timeout_ms <= 0:
            sys.stdout.write(json.dumps({"ok": False, "error": "probe payload missing login/password/server/timeoutMs", "trace": trace}, ensure_ascii=False))
            sys.stdout.flush()
            return 1

        try:
            import MetaTrader5 as mt5  # type: ignore
        except Exception as exc:
            _append_trace(trace, "mt5_import_failed", "failed", f"MetaTrader5 导入失败: {exc}")
            sys.stdout.write(json.dumps({"ok": False, "error": f"MetaTrader5 import failed: {exc}", "trace": trace}, ensure_ascii=False))
            sys.stdout.flush()
            return 1
        reused, reused_login, reused_server = _resolve_existing_session_before_relogin(
            mt5,
            login_value=login_value,
            server_value=server_value,
            path_value=path_value,
            timeout_ms=timeout_ms,
            trace=trace,
        )
        if reused:
            _append_trace(trace, "canonical_identity_ready", "ok", "已确认复用会话的 canonical identity", login=reused_login, server=reused_server)
            sys.stdout.write(
                json.dumps(
                    {
                        "ok": True,
                        "login": reused_login,
                        "server": reused_server,
                        "trace": trace,
                    },
                    ensure_ascii=False,
                )
            )
            sys.stdout.flush()
            return 0
        initialized, error_message = _initialize_authenticated_session(
            mt5,
            login_value=login_value,
            password_value=password_value,
            server_value=server_value,
            path_value=path_value,
            timeout_ms=timeout_ms,
            trace=trace,
        )
        if not initialized:
            sys.stdout.write(json.dumps({"ok": False, "error": error_message, "trace": trace}, ensure_ascii=False))
            sys.stdout.flush()
            return 1

        account = mt5.account_info()
        canonical_login = str(getattr(account, "login", "") or "").strip()
        canonical_server = str(getattr(account, "server", "") or "").strip()
        if not canonical_login or not canonical_server:
            _append_trace(trace, "canonical_identity_missing", "failed", f"account_info 未返回 canonical identity: {mt5.last_error()}")
            sys.stdout.write(
                json.dumps(
                    {
                        "ok": False,
                        "error": f"MetaTrader5 account_info missing canonical identity: {mt5.last_error()}",
                        "trace": trace,
                    },
                    ensure_ascii=False,
                )
            )
            sys.stdout.flush()
            return 1

        _append_trace(trace, "canonical_identity_ready", "ok", "已确认登录后的 canonical identity", login=canonical_login, server=canonical_server)

        sys.stdout.write(
            json.dumps(
                {
                    "ok": True,
                    "login": canonical_login,
                    "server": canonical_server,
                    "trace": trace,
                },
                ensure_ascii=False,
            )
        )
        sys.stdout.flush()
        return 0
    except Exception as exc:  # pragma: no cover - 兜底保护仅用于返回明确错误
        _append_trace(trace, "probe_unexpected_failed", "failed", f"探针发生未预期错误: {exc}")
        sys.stdout.write(
            json.dumps(
                {
                    "ok": False,
                    "error": f"unexpected probe failure: {exc}",
                    "trace": trace,
                },
                ensure_ascii=False,
            )
        )
        sys.stdout.flush()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
