"""隔离进程 MT5 直登脚本，按官方顺序 initialize -> login -> shutdown。"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict


_TRACE_FILE_PATH = ""


def _now_ms() -> int:
    """返回当前毫秒时间戳。"""
    return int(time.time() * 1000)


def _read_payload() -> Dict[str, Any]:
    """从标准输入读取直登参数。"""
    raw = sys.stdin.buffer.read()
    if not raw:
        raise ValueError("direct login payload is empty")
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("direct login payload must be an object")
    return payload


def _flush_trace(trace: list[Dict[str, Any]]) -> None:
    """把当前阶段轨迹立即写入 trace 文件，保证超时场景仍可回收。"""
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
                  error_code: str = "",
                  **detail: Any) -> None:
    """向结果里追加一条阶段事实。"""
    item = {
        "stage": str(stage or "").strip(),
        "status": str(status or "").strip(),
        "message": str(message or "").strip(),
        "serverTime": _now_ms(),
    }
    safe_error_code = str(error_code or "").strip()
    if safe_error_code:
        item["errorCode"] = safe_error_code
    safe_detail = {str(key): value for key, value in detail.items() if value not in (None, "", [])}
    if safe_detail:
        item["detail"] = safe_detail
    trace.append(item)
    _flush_trace(trace)


def _normalize_server_identity(server_value: Any) -> str:
    """统一规范化服务器名，避免仅分隔符差异造成误判。"""
    raw = str(server_value or "").strip().lower()
    if not raw:
        return ""
    return "".join(ch for ch in raw if ch.isalnum())


def _emit_result(payload: Dict[str, Any]) -> int:
    """输出统一 JSON 结果。"""
    sys.stdout.write(json.dumps(payload, ensure_ascii=False))
    sys.stdout.flush()
    return 0 if bool(payload.get("ok", False)) else 1


def _initialize_terminal_connection(mt5_module: Any,
                                    path_value: str,
                                    timeout_ms: int,
                                    trace: list[Dict[str, Any]]) -> tuple[bool, str]:
    """先连接 MT5 终端程序，再进入正式账号登录。"""
    kwargs = {"timeout": timeout_ms}
    if path_value:
        kwargs["path"] = path_value
    _append_trace(
        trace,
        "direct_initialize_start",
        "pending",
        "开始建立 MT5 基础终端连接",
        path=path_value,
    )
    try:
        initialized = bool(mt5_module.initialize(**kwargs))
    except TypeError:
        legacy_kwargs = dict(kwargs)
        legacy_kwargs.pop("timeout", None)
        try:
            initialized = bool(mt5_module.initialize(**legacy_kwargs))
        except Exception as exc:
            return False, f"MT5 initialize failed: {exc}"
    except Exception as exc:
        return False, f"MT5 initialize failed: {exc}"
    if not initialized:
        return False, f"MT5 initialize failed: {mt5_module.last_error()}"
    _append_trace(trace, "direct_initialize_ok", "ok", "MT5 基础终端连接已建立")
    return True, ""


def _login_account(mt5_module: Any,
                   login_value: int,
                   password_value: str,
                   server_value: str,
                   timeout_ms: int,
                   trace: list[Dict[str, Any]]) -> tuple[bool, str]:
    """在已连接终端上登录本次输入的账号。"""
    _append_trace(
        trace,
        "direct_login_start",
        "pending",
        "开始登录本次输入的 MT5 账号",
        login=str(login_value),
        server=server_value,
    )
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
        try:
            logged_in = bool(
                mt5_module.login(
                    login_value,
                    password=password_value,
                    server=server_value,
                )
            )
        except Exception as exc:
            return False, f"MT5 login failed: {exc}"
    except Exception as exc:
        return False, f"MT5 login failed: {exc}"
    if not logged_in:
        return False, f"MT5 login failed: {mt5_module.last_error()}"
    _append_trace(trace, "direct_login_ok", "ok", "MT5 账号登录成功")
    return True, ""


def main() -> int:
    """执行隔离进程 MT5 单路径直登。"""
    global _TRACE_FILE_PATH
    trace: list[Dict[str, Any]] = []
    mt5_module: Any = None
    try:
        payload = _read_payload()
        _TRACE_FILE_PATH = str(payload.get("traceFile") or "").strip()
        login_value = int(payload.get("login") or 0)
        password_value = str(payload.get("password") or "")
        server_value = str(payload.get("server") or "").strip()
        path_value = str(payload.get("path") or "").strip()
        timeout_ms = int(payload.get("timeoutMs") or 0)
        if login_value <= 0 or not password_value or not server_value or timeout_ms <= 0:
            _append_trace(trace, "direct_payload_invalid", "failed", "直登参数不完整", error_code="SESSION_DIRECT_INVALID_PAYLOAD")
            return _emit_result({"ok": False, "error": "direct login payload missing login/password/server/timeoutMs", "trace": trace})

        try:
            import MetaTrader5 as mt5  # type: ignore
            mt5_module = mt5
        except Exception as exc:
            _append_trace(trace, "direct_import_failed", "failed", f"MetaTrader5 导入失败: {exc}", error_code="SESSION_DIRECT_IMPORT_FAILED")
            return _emit_result({"ok": False, "error": f"MetaTrader5 import failed: {exc}", "trace": trace})

        initialized, initialize_error = _initialize_terminal_connection(
            mt5_module=mt5_module,
            path_value=path_value,
            timeout_ms=timeout_ms,
            trace=trace,
        )
        if not initialized:
            _append_trace(
                trace,
                "direct_initialize_failed",
                "failed",
                f"MT5 直登初始化失败: {initialize_error}",
                error_code="SESSION_DIRECT_INITIALIZE_FAILED",
            )
            return _emit_result({"ok": False, "error": initialize_error, "trace": trace})

        logged_in, login_error = _login_account(
            mt5_module=mt5_module,
            login_value=login_value,
            password_value=password_value,
            server_value=server_value,
            timeout_ms=timeout_ms,
            trace=trace,
        )
        if not logged_in:
            _append_trace(
                trace,
                "direct_login_failed",
                "failed",
                f"MT5 账号登录失败: {login_error}",
                error_code="SESSION_DIRECT_LOGIN_FAILED",
            )
            return _emit_result({"ok": False, "error": login_error, "trace": trace})

        try:
            account = mt5_module.account_info()
        except Exception as exc:
            _append_trace(
                trace,
                "direct_identity_failed",
                "failed",
                f"登录后读取 MT5 当前账号身份失败: {exc}",
                error_code="SESSION_DIRECT_IDENTITY_FAILED",
            )
            return _emit_result({"ok": False, "error": f"MT5 canonical account identity missing after direct login: {exc}", "trace": trace})

        canonical_login = str(getattr(account, "login", "") or "").strip() if account is not None else ""
        canonical_server = str(getattr(account, "server", "") or "").strip() if account is not None else ""
        if not canonical_login or not canonical_server:
            _append_trace(
                trace,
                "direct_identity_failed",
                "failed",
                "登录后未能读取到完整的 MT5 当前账号身份",
                error_code="SESSION_DIRECT_IDENTITY_FAILED",
            )
            return _emit_result({"ok": False, "error": "MT5 canonical account identity missing after direct login", "trace": trace})
        if canonical_login != str(login_value):
            _append_trace(
                trace,
                "direct_identity_failed",
                "failed",
                f"登录后 MT5 当前账号与输入账号不一致: expected={login_value}, actual={canonical_login}",
                error_code="SESSION_DIRECT_IDENTITY_MISMATCH",
                expectedLogin=str(login_value),
                actualLogin=canonical_login,
                expectedServer=server_value,
                actualServer=canonical_server,
            )
            return _emit_result({"ok": False, "error": f"MT5 canonical account identity mismatch after direct login: expected={login_value}, actual={canonical_login}", "trace": trace})
        if _normalize_server_identity(canonical_server) != _normalize_server_identity(server_value):
            _append_trace(
                trace,
                "direct_identity_failed",
                "failed",
                f"登录后 MT5 当前服务器与输入服务器不一致: expected={server_value}, actual={canonical_server}",
                error_code="SESSION_DIRECT_IDENTITY_MISMATCH",
                expectedLogin=str(login_value),
                actualLogin=canonical_login,
                expectedServer=server_value,
                actualServer=canonical_server,
            )
            return _emit_result({"ok": False, "error": f"MT5 canonical account identity mismatch after direct login: expectedServer={server_value}, actualServer={canonical_server}", "trace": trace})

        _append_trace(
            trace,
            "direct_identity_confirmed",
            "ok",
            "已确认当前 MT5 终端就是本次输入的账号",
            login=canonical_login,
            server=canonical_server,
        )
        return _emit_result({"ok": True, "login": canonical_login, "server": canonical_server, "trace": trace})
    except Exception as exc:  # pragma: no cover
        _append_trace(trace, "direct_unexpected_failed", "failed", f"隔离直登发生未预期错误: {exc}", error_code="SESSION_DIRECT_UNEXPECTED_FAILED")
        return _emit_result({"ok": False, "error": f"unexpected direct login failure: {exc}", "trace": trace})
    finally:
        if mt5_module is not None:
            try:
                mt5_module.shutdown()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
