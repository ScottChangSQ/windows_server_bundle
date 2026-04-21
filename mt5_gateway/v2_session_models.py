"""v2 会话层领域模型，统一承载公钥、账号摘要、状态与动作回执结构。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def _current_timestamp_ms() -> int:
    """获取当前时间戳（毫秒）。"""
    return int(datetime.utcnow().timestamp() * 1000)


def _safe_text(value: Any) -> str:
    """把任意输入转成字符串，避免字段值为空时出现 None。"""
    return "" if value is None else str(value)


def _safe_bool(value: Any) -> bool:
    """严格解析布尔值，避免把 'false' 这类字符串当成 True。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = _safe_text(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "off", ""}:
        return False
    return False


@dataclass
class SessionAccountSummary:
    """远程会话账号摘要，覆盖 active/saved account 的统一字段。"""

    profile_id: str
    login: str
    login_masked: str
    server: str
    display_name: str
    active: bool = False
    state: str = ""

    @classmethod
    def from_mapping(cls, payload: Optional[Dict[str, Any]], default_state: str = "") -> Optional["SessionAccountSummary"]:
        """从接口字典恢复账号摘要模型。"""
        if not isinstance(payload, dict) or not payload:
            return None
        state = _safe_text(payload.get("state") or default_state)
        active = _safe_bool(payload.get("active"))
        return cls(
            profile_id=_safe_text(payload.get("profileId")),
            login=_safe_text(payload.get("login")),
            login_masked=_safe_text(payload.get("loginMasked")),
            server=_safe_text(payload.get("server")),
            display_name=_safe_text(payload.get("displayName")),
            active=active,
            state=state,
        )

    def to_dict(self) -> Dict[str, Any]:
        """输出给接口和持久化层复用的统一账号摘要结构。"""
        return {
            "profileId": _safe_text(self.profile_id),
            "login": _safe_text(self.login),
            "loginMasked": _safe_text(self.login_masked),
            "server": _safe_text(self.server),
            "displayName": _safe_text(self.display_name),
            "active": bool(self.active),
            "state": _safe_text(self.state),
        }


@dataclass
class SessionPublicKeyPayload:
    """public-key 接口模型，承载公钥和当前账号摘要。"""

    key_id: str
    algorithm: str
    public_key_pem: str
    expires_at: int
    active_account: Optional[SessionAccountSummary] = None
    saved_accounts: List[SessionAccountSummary] = field(default_factory=list)
    ok: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """输出 public-key 接口约定字段。"""
        saved_accounts = [item.to_dict() for item in self.saved_accounts]
        return {
            "ok": bool(self.ok),
            "keyId": _safe_text(self.key_id),
            "algorithm": _safe_text(self.algorithm),
            "publicKeyPem": _safe_text(self.public_key_pem),
            "expiresAt": int(self.expires_at or 0),
            "activeAccount": None if self.active_account is None else self.active_account.to_dict(),
            "savedAccounts": saved_accounts,
            "savedAccountCount": len(saved_accounts),
        }


@dataclass
class SessionStatusPayload:
    """status 接口模型，承载当前激活账号和已保存账号列表。"""

    state: str
    active_account: Optional[SessionAccountSummary] = None
    saved_accounts: List[SessionAccountSummary] = field(default_factory=list)
    ok: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """输出 status 接口约定字段。"""
        saved_accounts = [item.to_dict() for item in self.saved_accounts]
        return {
            "ok": bool(self.ok),
            "state": _safe_text(self.state),
            "activeAccount": None if self.active_account is None else self.active_account.to_dict(),
            "savedAccounts": saved_accounts,
            "savedAccountCount": len(saved_accounts),
        }


@dataclass
class SessionReceipt:
    """login/switch/logout 的统一动作回执模型。"""

    state: str
    request_id: str = ""
    active_account: Optional[SessionAccountSummary] = None
    message: str = ""
    error_code: str = ""
    retryable: bool = False
    ok: bool = True
    stage: str = ""
    elapsed_ms: int = 0
    baseline_account: Optional[SessionAccountSummary] = None
    final_account: Optional[SessionAccountSummary] = None
    login_error: str = ""
    last_observed_account: Optional[SessionAccountSummary] = None

    def to_dict(self) -> Dict[str, Any]:
        """输出动作回执的统一结构。"""
        return {
            "ok": bool(self.ok),
            "state": _safe_text(self.state),
            "requestId": _safe_text(self.request_id),
            "activeAccount": None if self.active_account is None else self.active_account.to_dict(),
            "message": _safe_text(self.message),
            "errorCode": _safe_text(self.error_code),
            "retryable": bool(self.retryable),
            "stage": _safe_text(self.stage),
            "elapsedMs": int(self.elapsed_ms or 0),
            "baselineAccount": None if self.baseline_account is None else self.baseline_account.to_dict(),
            "finalAccount": None if self.final_account is None else self.final_account.to_dict(),
            "loginError": _safe_text(self.login_error),
            "lastObservedAccount": None if self.last_observed_account is None else self.last_observed_account.to_dict(),
        }


@dataclass
class LoginEnvelope:
    """登录信封模型，描述 App 发送到服务端的加密请求结构。"""

    request_id: str
    key_id: str
    algorithm: str
    encrypted_key: str
    encrypted_payload: str
    iv: str
    save_account: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """输出统一登录信封字段。"""
        return {
            "requestId": _safe_text(self.request_id),
            "keyId": _safe_text(self.key_id),
            "algorithm": _safe_text(self.algorithm),
            "encryptedKey": _safe_text(self.encrypted_key),
            "encryptedPayload": _safe_text(self.encrypted_payload),
            "iv": _safe_text(self.iv),
            "saveAccount": bool(self.save_account),
        }


@dataclass
class PublicKeyInfo:
    """公钥信息的标准表示，供认证与加密链路复用。"""

    key_id: str
    public_key_pem: str
    algorithm: str = "rsa-oaep+aes-gcm"
    created_at_ms: int = field(default_factory=_current_timestamp_ms)
    metadata: Dict[str, str] = field(default_factory=dict)
    expires_at_ms: Optional[int] = None


@dataclass
class AccountProfile:
    """维护远程 MT5 账号档案时的核心字段。"""

    account_id: str
    mt5_login: int
    nickname: str
    mode: str
    active: bool = False
    owner: str = "unknown"
    public_key: Optional[PublicKeyInfo] = None
    tags: List[str] = field(default_factory=list)
    last_seen_ms: Optional[int] = None


@dataclass
class EncryptedAccountRecord:
    """代表服务端保护后的账号凭据记录。"""

    profile: AccountProfile
    encrypted_payload: bytes
    protected_at_ms: int = field(default_factory=_current_timestamp_ms)
    entropy: Optional[bytes] = None


def summarize_account(profile: AccountProfile) -> Dict[str, str]:
    """生成便于日志与审计的账号摘要。"""
    public_key_id = profile.public_key.key_id if profile.public_key else "none"
    return {
        "accountId": profile.account_id,
        "mt5Login": str(profile.mt5_login),
        "mode": profile.mode,
        "active": str(profile.active),
        "publicKeyId": public_key_id,
    }
