"""MT5 GUI 切号主链，只负责窗口检测、附着、切号和最终账号确认。"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable, Dict, Optional


AccountReader = Callable[[], Optional[Dict[str, Any]]]
AttachTerminal = Callable[[int], Any]
PerformSwitch = Callable[[str, str, str, int], Dict[str, Any]]
StageReporter = Callable[[str, str, str, Optional[Dict[str, Any]]], None]


def _normalize_account(account: Any) -> Optional[Dict[str, str]]:
    """把账号对象收口成稳定字典，只认 login/server。"""
    if isinstance(account, dict):
        login_value = str(account.get("login") or "").strip()
        server_value = str(account.get("server") or "").strip()
    else:
        login_value = str(getattr(account, "login", "") or "").strip()
        server_value = str(getattr(account, "server", "") or "").strip()
    if not login_value:
        return None
    return {
        "login": login_value,
        "server": server_value,
    }


def _build_timeout_message(
    *,
    login_error: str,
    last_account: Optional[Dict[str, str]],
    poll_timeout_ms: int,
) -> str:
    """构造切号超时摘要，保留原始 login 错误和最后一次真实账号。"""
    timeout_seconds = max(1, int(round(max(0, int(poll_timeout_ms)) / 1000.0)))
    message = f"{timeout_seconds}s 内未切换到目标账号"
    if login_error:
        message += f"；mt5.login 原始错误={login_error}"
    if isinstance(last_account, dict) and last_account:
        message += (
            "；最后实际账号="
            + str(last_account.get("login") or "").strip()
            + " / "
            + str(last_account.get("server") or "").strip()
        )
    return message


@dataclass
class Mt5GuiController:
    """可注入依赖的 MT5 GUI 切号执行器。"""

    detect_window: Callable[[], bool]
    launch_terminal: Callable[[], None]
    wait_window_ready: Callable[[int], bool]
    attach_terminal: AttachTerminal
    read_account: AccountReader
    perform_switch: PerformSwitch
    monotonic_ms: Callable[[], int]
    sleep: Callable[[float], None] = time.sleep
    window_ready_timeout_ms: int = 15000
    attach_retry_count: int = 2
    attach_retry_interval_seconds: float = 3.0
    final_poll_timeout_ms: int = 30000
    final_poll_interval_seconds: float = 2.0
    total_timeout_ms: int = 120000
    attach_attempt_timeout_ms: int = 20000
    report_stage: Optional[StageReporter] = None

    def switch_account(self, *, login: str, password: str, server: str) -> Dict[str, Any]:
        """执行完整 GUI 切号主链。"""
        target_login = str(login or "").strip()
        target_server = str(server or "").strip()
        started_at = int(self.monotonic_ms())
        deadline = started_at + max(1000, int(self.total_timeout_ms))
        self._emit_stage(
            stage="switch_flow_started",
            status="pending",
            message="开始执行 MT5 GUI 切号链",
            detail={
                "targetLogin": target_login,
                "targetServer": target_server,
                "totalTimeoutMs": int(self.total_timeout_ms),
                "remainingBudgetMs": self._remaining_ms(deadline),
            },
        )
        if not self.detect_window():
            self._emit_stage(
                stage="terminal_launch_start",
                status="pending",
                message="当前未发现 MT5 终端进程，开始按 MT5_PATH 拉起",
                detail={"remainingBudgetMs": self._remaining_ms(deadline)},
            )
            try:
                self.launch_terminal()
            except Exception as exc:
                return self._failure(
                    stage="window_not_found_then_launch_failed",
                    message=f"当前未发现 MT5 主窗口，按 MT5_PATH 拉起失败: {exc}",
                    started_at=started_at,
                )
            wait_timeout_ms = min(int(self.window_ready_timeout_ms), self._remaining_ms(deadline))
            if wait_timeout_ms <= 0:
                return self._failure(
                    stage="window_not_found_then_window_ready_timeout",
                    message="当前未发现 MT5 主窗口，且切号总预算已在等待窗口前耗尽",
                    started_at=started_at,
                )
            self._emit_stage(
                stage="window_ready_wait_start",
                status="pending",
                message="已发起 MT5 拉起，开始等待终端进程就绪",
                detail={
                    "timeoutMs": wait_timeout_ms,
                    "remainingBudgetMs": self._remaining_ms(deadline),
                },
            )
            if not self.wait_window_ready(wait_timeout_ms):
                return self._failure(
                    stage="window_not_found_then_window_ready_timeout",
                    message="当前未发现 MT5 主窗口，拉起后 15s 内仍未等到窗口就绪",
                    started_at=started_at,
                )
            self._emit_stage(
                stage="window_ready",
                status="ok",
                message="MT5 终端进程已就绪",
                detail={"remainingBudgetMs": self._remaining_ms(deadline)},
            )
        else:
            self._emit_stage(
                stage="window_detected",
                status="ok",
                message="已发现现有 MT5 终端进程",
                detail={"remainingBudgetMs": self._remaining_ms(deadline)},
            )

        attach_result = self._attach_with_retry(deadline=deadline)
        if not bool(attach_result.get("ok")):
            return self._failure(
                stage="attach_failed",
                message=str(attach_result.get("message") or "MT5 主窗口已存在，但附着/初始化失败"),
                started_at=started_at,
            )
        self._emit_stage(
            stage="attach_ready",
            status="ok",
            message="已附着当前 MT5 终端",
            detail={
                "attempt": int(attach_result.get("attempt") or 0),
                "attemptCount": int(attach_result.get("attemptCount") or 0),
                "remainingBudgetMs": self._remaining_ms(deadline),
            },
        )

        self._emit_stage(
            stage="baseline_account_read_start",
            status="pending",
            message="开始读取切号前基线账号",
            detail={"remainingBudgetMs": self._remaining_ms(deadline)},
        )
        baseline_account = _normalize_account(self.read_account())
        if baseline_account is None:
            return self._failure(
                stage="baseline_account_read_failed",
                message="已附着 MT5，但读取切号前基线账号失败",
                started_at=started_at,
            )
        self._emit_stage(
            stage="baseline_account_ready",
            status="ok",
            message="已读取切号前基线账号",
            detail={
                "baselineAccount": baseline_account,
                "remainingBudgetMs": self._remaining_ms(deadline),
            },
        )

        login_error = ""
        switch_timeout_ms = self._remaining_ms(deadline)
        if switch_timeout_ms <= 0:
            return self._failure(
                stage="switch_timeout_account_not_changed",
                message="切号总预算已耗尽，尚未执行 mt5.login(...)",
                started_at=started_at,
                baseline_account=baseline_account,
            )
        self._emit_stage(
            stage="switch_call_start",
            status="pending",
            message="开始调用 mt5.login(...) 执行切号",
            detail={
                "timeoutMs": switch_timeout_ms,
                "baselineAccount": baseline_account,
                "remainingBudgetMs": switch_timeout_ms,
            },
        )
        try:
            switch_result = dict(self.perform_switch(target_login, password, target_server, switch_timeout_ms) or {})
            login_error = str(switch_result.get("error") or switch_result.get("message") or "").strip()
        except Exception as exc:
            return self._failure(
                stage="switch_call_exception",
                message=f"mt5.login(...) 调用抛异常: {exc}",
                started_at=started_at,
                baseline_account=baseline_account,
            )
        self._emit_stage(
            stage="switch_call_returned",
            status="ok",
            message="mt5.login(...) 已返回，开始确认最终真实账号",
            detail={
                "loginError": login_error,
                "remainingBudgetMs": self._remaining_ms(deadline),
            },
        )

        poll_timeout_ms = min(int(self.final_poll_timeout_ms), self._remaining_ms(deadline))
        if poll_timeout_ms <= 0:
            return self._failure(
                stage="switch_timeout_account_not_changed",
                message="mt5.login(...) 已返回，但切号总预算已耗尽，未能继续确认最终真实账号",
                started_at=started_at,
                baseline_account=baseline_account,
                login_error=login_error,
                last_observed_account=baseline_account,
            )
        self._emit_stage(
            stage="final_account_poll_start",
            status="pending",
            message="开始轮询最终真实账号",
            detail={
                "timeoutMs": poll_timeout_ms,
                "loginError": login_error,
                "remainingBudgetMs": self._remaining_ms(deadline),
            },
        )
        poll_deadline = min(deadline, int(self.monotonic_ms()) + poll_timeout_ms)
        last_observed_account = baseline_account
        while int(self.monotonic_ms()) <= poll_deadline:
            observed_account = _normalize_account(self.read_account())
            if observed_account is None:
                return self._failure(
                    stage="final_account_read_failed",
                    message="切号后轮询真实账号时读取失败",
                    started_at=started_at,
                    baseline_account=baseline_account,
                    login_error=login_error,
                    last_observed_account=last_observed_account,
                )
            last_observed_account = observed_account
            if str(observed_account.get("login") or "").strip() == target_login:
                elapsed_ms = int(self.monotonic_ms()) - started_at
                self._emit_stage(
                    stage="final_account_confirmed",
                    status="ok",
                    message="已确认 MT5 终端切换到目标账号",
                    detail={
                        "finalAccount": observed_account,
                        "elapsedMs": elapsed_ms,
                    },
                )
                return {
                    "ok": True,
                    "stage": "switch_succeeded",
                    "message": (
                        "切换前="
                        + str(baseline_account.get("login") or "").strip()
                        + " / "
                        + str(baseline_account.get("server") or "").strip()
                        + "；切换后="
                        + str(observed_account.get("login") or "").strip()
                        + " / "
                        + str(observed_account.get("server") or "").strip()
                        + f"；耗时={elapsed_ms}ms"
                    ),
                    "elapsedMs": elapsed_ms,
                    "baselineAccount": baseline_account,
                    "finalAccount": observed_account,
                    "loginError": login_error,
                    "lastObservedAccount": observed_account,
                    "login": str(observed_account.get("login") or "").strip(),
                    "server": str(observed_account.get("server") or "").strip(),
                }
            if int(self.monotonic_ms()) >= poll_deadline:
                break
            self.sleep(float(self.final_poll_interval_seconds))

        return self._failure(
            stage="switch_timeout_account_not_changed",
            message=_build_timeout_message(
                login_error=login_error,
                last_account=last_observed_account,
                poll_timeout_ms=poll_timeout_ms,
            ),
            started_at=started_at,
            baseline_account=baseline_account,
            login_error=login_error,
            last_observed_account=last_observed_account,
        )

    def _attach_with_retry(self, *, deadline: int) -> Dict[str, Any]:
        """按固定次数重试附着当前 MT5 终端。"""
        attempts = max(0, int(self.attach_retry_count)) + 1
        for index in range(attempts):
            attempt = index + 1
            remaining_attempts = attempts - index
            timeout_ms = self._build_attach_attempt_timeout(deadline=deadline, remaining_attempts=remaining_attempts)
            if timeout_ms <= 0:
                return {
                    "ok": False,
                    "attempt": attempt,
                    "attemptCount": attempts,
                    "message": "MT5 主窗口已存在，但切号总预算已在附着前耗尽",
                }
            self._emit_stage(
                stage="attach_attempt_start",
                status="pending",
                message=f"开始第 {attempt}/{attempts} 次附着 MT5 终端",
                detail={
                    "attempt": attempt,
                    "attemptCount": attempts,
                    "timeoutMs": timeout_ms,
                    "remainingBudgetMs": timeout_ms,
                },
            )
            attach_result = self._normalize_attach_result(self.attach_terminal(timeout_ms))
            if bool(attach_result.get("ok")):
                return {
                    "ok": True,
                    "attempt": attempt,
                    "attemptCount": attempts,
                    "detail": attach_result.get("detail"),
                    "message": str(attach_result.get("message") or "").strip(),
                }
            self._emit_stage(
                stage="attach_attempt_failed",
                status="failed",
                message=str(attach_result.get("message") or f"第 {attempt}/{attempts} 次附着 MT5 终端失败"),
                detail={
                    "attempt": attempt,
                    "attemptCount": attempts,
                    "timeoutMs": timeout_ms,
                    "remainingBudgetMs": self._remaining_ms(deadline),
                    **dict(attach_result.get("detail") or {}),
                },
            )
            if index + 1 < attempts:
                retry_sleep_seconds = min(
                    float(self.attach_retry_interval_seconds),
                    max(0.0, float(self._remaining_ms(deadline)) / 1000.0),
                )
                if retry_sleep_seconds <= 0:
                    break
                self._emit_stage(
                    stage="attach_retry_wait",
                    status="pending",
                    message="附着未成功，准备重试",
                    detail={
                        "attempt": attempt,
                        "attemptCount": attempts,
                        "waitSeconds": retry_sleep_seconds,
                        "remainingBudgetMs": self._remaining_ms(deadline),
                    },
                )
                self.sleep(retry_sleep_seconds)
        return {
            "ok": False,
            "attempt": attempts,
            "attemptCount": attempts,
            "message": str(attach_result.get("message") or "MT5 主窗口已存在，但附着/初始化失败"),
            "detail": attach_result.get("detail") if isinstance(attach_result, dict) else None,
        }

    def _remaining_ms(self, deadline: int) -> int:
        """返回距离总预算截止还剩多少毫秒。"""
        return max(0, int(deadline) - int(self.monotonic_ms()))

    def _build_attach_attempt_timeout(self, *, deadline: int, remaining_attempts: int) -> int:
        """按剩余总预算和剩余尝试次数分配单次附着预算。"""
        remaining_budget_ms = self._remaining_ms(deadline)
        if remaining_budget_ms <= 0:
            return 0
        attempts_left = max(1, int(remaining_attempts))
        fair_share_ms = max(1000, int(remaining_budget_ms / attempts_left))
        return min(remaining_budget_ms, max(1000, int(self.attach_attempt_timeout_ms)), fair_share_ms)

    def _normalize_attach_result(self, result: Any) -> Dict[str, Any]:
        """兼容旧 bool 和新结构化附着结果。"""
        if isinstance(result, dict):
            return {
                "ok": bool(result.get("ok")),
                "message": str(result.get("message") or "").strip(),
                "detail": dict(result.get("detail") or {}),
            }
        return {
            "ok": bool(result),
            "message": "",
            "detail": {},
        }

    def _emit_stage(self, *, stage: str, status: str, message: str, detail: Optional[Dict[str, Any]] = None) -> None:
        """把运行中阶段实时回传给诊断回调。"""
        if self.report_stage is None:
            return
        self.report_stage(str(stage or "").strip(), str(status or "").strip(), str(message or "").strip(), dict(detail or {}))

    def _failure(
        self,
        *,
        stage: str,
        message: str,
        started_at: int,
        baseline_account: Optional[Dict[str, str]] = None,
        login_error: str = "",
        last_observed_account: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """统一失败结果结构。"""
        return {
            "ok": False,
            "stage": str(stage or "").strip(),
            "message": str(message or "").strip(),
            "elapsedMs": max(0, int(self.monotonic_ms()) - int(started_at)),
            "baselineAccount": baseline_account,
            "finalAccount": None,
            "loginError": str(login_error or "").strip(),
            "lastObservedAccount": last_observed_account,
        }
