"""v2 会话管理器，负责登录、状态读取与退出流程。"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

try:
    from bridge.mt5_gateway import v2_session_crypto
    from bridge.mt5_gateway import v2_session_models
except Exception:  # pragma: no cover
    import v2_session_crypto  # type: ignore
    import v2_session_models  # type: ignore


def _now_ms() -> int:
    """返回当前 UTC 毫秒时间戳。"""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _mask_login(login: str) -> str:
    """把账号加工成尾号掩码。"""
    login_text = str(login or "")
    return f"****{login_text[-4:]}" if login_text else "****"


def _build_profile_id(login: str, server: str) -> str:
    """按账号和服务器生成稳定 profileId。"""
    server_text = str(server or "").strip().lower().replace(" ", "_")
    normalized = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in server_text)
    return f"acct_{str(login or '').strip()}_{normalized}"


class AccountSessionManager:
    """会话主流程管理器。"""

    def __init__(
        self,
        store: Any,
        gateway: Any,
        now_ms_provider: Optional[Callable[[], int]] = None,
        on_session_changed: Optional[Callable[[str, Optional[Dict[str, Any]]], None]] = None,
    ):
        """注入存储层、网关适配层和时间函数。"""
        self.store = store
        self.gateway = gateway
        self._now_ms_provider = now_ms_provider or _now_ms
        self._on_session_changed = on_session_changed
        self._audit_logs: List[Dict[str, Any]] = []

    def _now_ms(self) -> int:
        """读取当前毫秒时间。"""
        return int(self._now_ms_provider())

    def _append_audit_log(self, action: str, request_id: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """记录简化审计日志，供后续任务扩展。"""
        self._audit_logs.append(
            {
                "action": str(action or ""),
                "requestId": str(request_id or ""),
                "timestampMs": self._now_ms(),
                "extra": dict(extra or {}),
            }
        )
        if len(self._audit_logs) > 200:
            self._audit_logs = self._audit_logs[-200:]

    def _notify_session_changed(self, action: str, profile: Optional[Dict[str, Any]]) -> None:
        """触发会话变更回调，供网关清理运行时状态。"""
        if self._on_session_changed is None:
            return
        self._on_session_changed(str(action or ""), None if profile is None else dict(profile))

    def _build_profile(self, login: str, server: str, active: bool) -> Dict[str, Any]:
        """构造账号摘要结构。"""
        login_text = str(login or "").strip()
        server_text = str(server or "").strip()
        masked = _mask_login(login_text)
        state = "activated" if active else ""
        payload = v2_session_models.SessionAccountSummary(
            profile_id=_build_profile_id(login_text, server_text),
            login=login_text,
            login_masked=masked,
            server=server_text,
            display_name=f"{server_text} {masked}",
            active=bool(active),
            state=state,
        ).to_dict()
        payload["lastSeenMs"] = self._now_ms()
        return payload

    def _resolve_gateway_identity(self, account_meta: Any) -> tuple[str, str]:
        """从网关返回中提取完整账号身份。"""
        if not isinstance(account_meta, dict):
            raise ValueError("gateway canonical account identity missing")
        login_text = str(account_meta.get("login") or "").strip()
        server_text = str(account_meta.get("server") or "").strip()
        if not login_text or not server_text:
            raise ValueError("gateway canonical account identity missing")
        return login_text, server_text

    def _build_active_profile_from_gateway_meta(self, account_meta: Any) -> Dict[str, Any]:
        """只按网关确认的完整身份构造当前激活账号摘要。"""
        login_text, server_text = self._resolve_gateway_identity(account_meta)
        return self._build_profile(login_text, server_text, active=True)

    def _to_account_receipt(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """把档案对象裁剪成接口返回结构。"""
        summary = v2_session_models.SessionAccountSummary.from_mapping(profile)
        if summary is None:
            return {}
        return summary.to_dict()

    def _build_switch_flow_receipt_kwargs(self, account_meta: Any) -> Dict[str, Any]:
        """把网关切号结果映射成统一回执字段。"""
        payload = dict(account_meta or {})
        return {
            "stage": str(payload.get("stage") or "").strip(),
            "elapsed_ms": int(payload.get("elapsedMs") or 0),
            "baseline_account": v2_session_models.SessionAccountSummary.from_mapping(payload.get("baselineAccount")),
            "final_account": v2_session_models.SessionAccountSummary.from_mapping(payload.get("finalAccount")),
            "login_error": str(payload.get("loginError") or "").strip(),
            "last_observed_account": v2_session_models.SessionAccountSummary.from_mapping(payload.get("lastObservedAccount")),
        }

    def _to_active_account_summary(self, profile: Optional[Dict[str, Any]]) -> Optional[v2_session_models.SessionAccountSummary]:
        """把运行态会话收口成完整 activeAccount 摘要。"""
        summary = v2_session_models.SessionAccountSummary.from_mapping(profile)
        if summary is None:
            return None
        if not summary.profile_id or not summary.login or not summary.server:
            return None
        summary.active = True
        summary.state = "activated"
        return summary

    def _to_saved_account_summary(self, profile: Optional[Dict[str, Any]]) -> Optional[v2_session_models.SessionAccountSummary]:
        """把已保存账号收口成静态摘要，剥离任何运行态字段。"""
        if not isinstance(profile, dict) or not profile:
            return None
        sanitized = dict(profile)
        sanitized["active"] = False
        sanitized["state"] = ""
        summary = v2_session_models.SessionAccountSummary.from_mapping(sanitized)
        if summary is None:
            return None
        if not summary.profile_id or not summary.login or not summary.server:
            return None
        summary.active = False
        summary.state = ""
        return summary

    def _load_profile_record_snapshot(self, profile_id: str) -> Optional[Dict[str, Any]]:
        """读取某个已保存账号档案的原始记录快照，用于失败回滚恢复。"""
        profile_key = str(profile_id or "").strip()
        if not profile_key or not hasattr(self.store, "load_profile"):
            return None
        record = self.store.load_profile(profile_key)
        if not isinstance(record, dict) or not record:
            return None
        return dict(record)

    def _restore_saved_profile_record(self, profile_id: str, record: Optional[Dict[str, Any]]) -> None:
        """恢复或删除某个已保存账号档案，确保失败登录不会污染账号库。"""
        profile_key = str(profile_id or "").strip()
        if not profile_key:
            return
        restore_fn = getattr(self.store, "restore_profile_record", None)
        if callable(restore_fn):
            restore_fn(profile_key, record)
            return
        if isinstance(record, dict):
            raise RuntimeError("store.restore_profile_record is required to restore saved profile record")
        delete_fn = getattr(self.store, "delete_profile", None)
        if callable(delete_fn):
            delete_fn(profile_key)
            return
        raise RuntimeError("store.delete_profile is required to remove saved profile record")

    def _rollback_login_after_commit_failed(
        self,
        active_profile: Dict[str, Any],
        request_id: str,
        commit_error: Exception,
        saved_profile_snapshot: Optional[Dict[str, Any]],
        should_restore_saved_profile: bool,
    ) -> None:
        """登录链路在提交阶段失败时执行补偿，避免留下半成功状态。"""
        rollback_errors: List[str] = []
        if should_restore_saved_profile:
            try:
                self._restore_saved_profile_record(
                    str(active_profile.get("profileId") or ""),
                    saved_profile_snapshot,
                )
            except Exception as exc:  # pragma: no cover - 回滚失败只做兜底记录
                rollback_errors.append(f"restore_saved_profile failed: {exc}")
        try:
            self.store.clear_active_session()
        except Exception as exc:  # pragma: no cover - 回滚失败只做兜底记录
            rollback_errors.append(f"clear_active_session failed: {exc}")
        try:
            self.gateway.logout_mt5()
        except Exception as exc:  # pragma: no cover - 回滚失败只做兜底记录
            rollback_errors.append(f"logout_mt5 failed: {exc}")
        try:
            # 回滚后主动通知会话变更，让上游有机会清理缓存。
            self._notify_session_changed("logout", active_profile)
        except Exception as exc:  # pragma: no cover - 回调失败只做兜底记录
            rollback_errors.append(f"notify_session_changed failed: {exc}")

        self._append_audit_log(
            "login_rollback",
            request_id,
            {
                "profileId": str(active_profile.get("profileId") or ""),
                "error": str(commit_error),
                "rollbackErrors": rollback_errors,
            },
        )

        if rollback_errors:
            raise RuntimeError(
                f"登录提交失败，且补偿失败：{'; '.join(rollback_errors)}"
            ) from commit_error
        raise commit_error

    def _settle_logged_out_after_login_failure(
        self,
        previous_active_profile: Optional[Dict[str, Any]],
        request_id: str,
        login_error: Exception,
    ) -> None:
        """登录阶段失败时，把旧文件态收口到 logged_out，避免运行态与文件态分裂。"""
        cleanup_errors: List[str] = []
        try:
            self.store.clear_active_session()
        except Exception as exc:
            cleanup_errors.append(f"clear_active_session failed: {exc}")
        try:
            self._notify_session_changed("logout", previous_active_profile)
        except Exception as exc:
            cleanup_errors.append(f"notify_session_changed failed: {exc}")

        self._append_audit_log(
            "login_failed_before_commit",
            request_id,
            {
                "error": str(login_error),
                "hadPreviousActive": bool(previous_active_profile),
                "cleanupErrors": cleanup_errors,
            },
        )

        if cleanup_errors:
            raise RuntimeError(
                f"登录失败，且收口失败：{'; '.join(cleanup_errors)}"
            ) from login_error
        raise login_error

    def _decrypt_saved_password(self, record: Dict[str, Any]) -> str:
        """从账号档案解密密码。"""
        encrypted_password = str((record or {}).get("encryptedPassword") or "").strip()
        if not encrypted_password:
            raise ValueError("saved profile missing encryptedPassword")
        try:
            cipher = base64.b64decode(encrypted_password, validate=True)
        except Exception as exc:
            raise ValueError("saved profile encryptedPassword is invalid base64") from exc
        secret = v2_session_crypto.unprotect_secret_for_machine(cipher)
        return secret.decode("utf-8")

    def _force_gateway_consistency_refresh(self) -> None:
        """触发网关缓存清理和强一致刷新。"""
        clear_fn = getattr(self.gateway, "clear_account_caches", None)
        refresh_fn = getattr(self.gateway, "force_account_resync", None)
        if not callable(clear_fn):
            raise RuntimeError("gateway.clear_account_caches is required")
        if not callable(refresh_fn):
            raise RuntimeError("gateway.force_account_resync is required")
        clear_fn()
        refresh_fn()

    def _load_active_profile_snapshot(self) -> Optional[Dict[str, Any]]:
        """读取当前激活账号快照。"""
        if not hasattr(self.store, "load_active_session"):
            return None
        loaded = self.store.load_active_session()
        if not isinstance(loaded, dict) or not loaded:
            return None
        return dict(loaded)

    def _load_saved_profile_credentials(self, profile_id: str) -> Optional[Dict[str, Any]]:
        """读取已保存账号的完整登录凭据。"""
        profile_key = str(profile_id or "").strip()
        if not profile_key:
            return None
        if not hasattr(self.store, "load_profile"):
            return None
        record = self.store.load_profile(profile_key)
        if not isinstance(record, dict) or not record:
            return None
        raw_profile = record.get("profile")
        if not isinstance(raw_profile, dict) or not raw_profile:
            return None
        profile = dict(raw_profile)
        login = str(profile.get("login") or "").strip()
        server = str(profile.get("server") or "").strip()
        if not login or not server:
            return None
        try:
            password = self._decrypt_saved_password(record)
        except Exception:
            return None
        return {
            "profileId": profile_key,
            "profile": profile,
            "login": login,
            "server": server,
            "password": password,
        }

    def _build_restore_credentials(self, old_active_profile: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """构建回滚旧账号所需的登录凭据。"""
        if not isinstance(old_active_profile, dict) or not old_active_profile:
            return None
        profile_id = str(old_active_profile.get("profileId") or "").strip()
        if not profile_id:
            return None
        return self._load_saved_profile_credentials(profile_id)

    def _rollback_switch_failure(
        self,
        request_id: str,
        old_active_profile: Optional[Dict[str, Any]],
        restore_credentials: Optional[Dict[str, Any]],
        switch_error: Exception,
        failure_stage: str,
    ) -> None:
        """切换失败后的统一补偿：优先恢复旧账号，否则进入安全登出态。"""
        rollback_errors: List[str] = []
        restored = False

        if restore_credentials:
            try:
                restored_meta = self.gateway.switch_mt5_account(
                    login=str(restore_credentials.get("login") or ""),
                    password=str(restore_credentials.get("password") or ""),
                    server=str(restore_credentials.get("server") or ""),
                )
                restored_profile = self._build_active_profile_from_gateway_meta(restored_meta)
                self.store.save_active_session(restored_profile)
                self._notify_session_changed("switch", restored_profile)
                self._force_gateway_consistency_refresh()
                restored = True
            except Exception as restore_error:
                rollback_errors.append(f"restore_previous_account failed: {restore_error}")

        if not restored:
            try:
                self.store.clear_active_session()
            except Exception as clear_error:
                rollback_errors.append(f"clear_active_session failed: {clear_error}")
            try:
                self.gateway.logout_mt5()
            except Exception as logout_error:
                rollback_errors.append(f"logout_mt5 failed: {logout_error}")
            try:
                self._notify_session_changed("logout", old_active_profile)
            except Exception as notify_error:
                rollback_errors.append(f"notify_session_changed failed: {notify_error}")
            try:
                self._force_gateway_consistency_refresh()
            except Exception as refresh_error:
                rollback_errors.append(f"force_consistency_refresh failed: {refresh_error}")

        self._append_audit_log(
            "switch_rollback",
            request_id,
            {
                "failureStage": str(failure_stage or ""),
                "error": str(switch_error),
                "restoredPreviousAccount": restored,
                "rollbackErrors": rollback_errors,
            },
        )

        if rollback_errors:
            raise RuntimeError(
                f"账号切换失败，且补偿失败：{'; '.join(rollback_errors)}"
            ) from switch_error

    def login_new_account(
        self,
        login: str,
        password: str,
        server: str,
        remember: bool,
        request_id: str = "",
    ) -> Dict[str, Any]:
        """登录新账号并切为当前激活会话。"""
        previous_active_profile = self._load_active_profile_snapshot()
        try:
            account_meta = self.gateway.login_mt5(
                login=login,
                password=password,
                server=server,
                request_id=str(request_id or ""),
            )
            active_profile = self._build_active_profile_from_gateway_meta(account_meta)
        except Exception as login_error:
            self._settle_logged_out_after_login_failure(previous_active_profile, request_id, login_error)
            raise AssertionError("unreachable")
        saved_profile_snapshot = None
        should_restore_saved_profile = False

        try:
            if bool(remember):
                saved_profile_snapshot = self._load_profile_record_snapshot(active_profile["profileId"])
                should_restore_saved_profile = True
                self.store.save_profile(active_profile, password)
            self.store.save_active_session(active_profile)
            self._append_audit_log("login", request_id, {"profileId": active_profile["profileId"], "remember": bool(remember)})
            self._notify_session_changed("login", active_profile)
            self._force_gateway_consistency_refresh()
        except Exception as commit_error:
            self._rollback_login_after_commit_failed(
                active_profile,
                request_id,
                commit_error,
                saved_profile_snapshot=saved_profile_snapshot,
                should_restore_saved_profile=should_restore_saved_profile,
            )

        return v2_session_models.SessionReceipt(
            state="activated",
            request_id=str(request_id or ""),
            active_account=v2_session_models.SessionAccountSummary.from_mapping(active_profile),
            message=str(account_meta.get("message") or "登录成功"),
            **self._build_switch_flow_receipt_kwargs(account_meta),
        ).to_dict()

    def logout_current_session(self, request_id: str = "") -> Dict[str, Any]:
        """退出当前激活会话并清理激活状态。"""
        active_profile = self._load_active_profile_snapshot()
        self.store.clear_active_session()
        try:
            self.gateway.logout_mt5()
        except Exception as logout_error:
            # 运行态登出失败时，把文件态恢复到原激活账号，避免状态分裂。
            if isinstance(active_profile, dict) and active_profile:
                try:
                    self.store.save_active_session(active_profile)
                except Exception as restore_error:
                    raise RuntimeError(
                        f"MT5 登出失败且会话恢复失败: {restore_error}"
                    ) from logout_error
            raise logout_error
        self._append_audit_log("logout", request_id, {})
        self._notify_session_changed("logout", active_profile)
        payload = v2_session_models.SessionReceipt(
            state="logged_out",
            request_id=str(request_id or ""),
            active_account=None,
            message="已退出当前账号",
        ).to_dict()
        return payload

    def build_status_payload(self) -> Dict[str, Any]:
        """构建当前会话状态结构。"""
        active = self.store.load_active_session()
        saved_accounts = [
            item
            for item in (
                self._to_saved_account_summary(profile)
                for profile in self.store.list_profiles()
            )
            if item is not None
        ]
        active_account = self._to_active_account_summary(active)
        payload = v2_session_models.SessionStatusPayload(
            state="activated" if active_account else "logged_out",
            active_account=active_account,
            saved_accounts=saved_accounts,
        )
        return payload.to_dict()

    def switch_saved_account(self, profile_id: str, request_id: str = "") -> Dict[str, Any]:
        """按 profileId 切换到已保存账号。"""
        profile_key = str(profile_id or "").strip()
        if not profile_key:
            raise ValueError("profile_id is required")
        target_credentials = self._load_saved_profile_credentials(profile_key)
        if not target_credentials:
            raise ValueError("saved profile not found")
        old_active_profile = self._load_active_profile_snapshot()
        restore_credentials = self._build_restore_credentials(old_active_profile)

        try:
            account_meta = self.gateway.switch_mt5_account(
                login=str(target_credentials.get("login") or ""),
                password=str(target_credentials.get("password") or ""),
                server=str(target_credentials.get("server") or ""),
                request_id=str(request_id or ""),
            )
        except Exception as switch_error:
            self._rollback_switch_failure(
                request_id=request_id,
                old_active_profile=old_active_profile,
                restore_credentials=restore_credentials,
                switch_error=switch_error,
                failure_stage="switch_target_account",
            )
            raise switch_error

        active_profile = self._build_active_profile_from_gateway_meta(account_meta)
        try:
            self.store.save_active_session(active_profile)
            self._notify_session_changed("switch", active_profile)
            self._force_gateway_consistency_refresh()
        except Exception as commit_error:
            self._rollback_switch_failure(
                request_id=request_id,
                old_active_profile=old_active_profile,
                restore_credentials=restore_credentials,
                switch_error=commit_error,
                failure_stage="commit_after_switch",
            )
            raise commit_error

        self._append_audit_log("switch", request_id, {"profileId": active_profile["profileId"]})
        return v2_session_models.SessionReceipt(
            state="activated",
            request_id=str(request_id or ""),
            active_account=v2_session_models.SessionAccountSummary.from_mapping(active_profile),
            message=str(account_meta.get("message") or "切换成功"),
            **self._build_switch_flow_receipt_kwargs(account_meta),
        ).to_dict()
