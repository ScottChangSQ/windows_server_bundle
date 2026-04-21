"""v2 会话层需要的加密与时间校验工具。"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import platform
import sys
import threading
import uuid
from datetime import datetime, timezone
from typing import Iterable

_IS_WINDOWS = sys.platform.startswith("win")

try:
    from bridge.mt5_gateway import v2_session_models
except Exception:  # pragma: no cover
    import v2_session_models  # type: ignore

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding, rsa
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    _HAS_CRYPTOGRAPHY = True
except Exception:  # pragma: no cover - 依赖缺失时由调用方感知
    hashes = None  # type: ignore[assignment]
    serialization = None  # type: ignore[assignment]
    padding = None  # type: ignore[assignment]
    rsa = None  # type: ignore[assignment]
    AESGCM = None  # type: ignore[assignment]
    _HAS_CRYPTOGRAPHY = False


if _IS_WINDOWS:
    import ctypes
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]

    _CRYPTPROTECT_LOCAL_MACHINE = 0x4
    _CRYPTPROTECT_UI_FORBIDDEN = 0x1

    _crypt32 = ctypes.windll.crypt32
    _kernel32 = ctypes.windll.kernel32
    _kernel32.LocalFree.argtypes = [wintypes.HLOCAL]
    _kernel32.LocalFree.restype = wintypes.HLOCAL

    def _create_blob(source: bytes) -> tuple[DATA_BLOB, ctypes.Array]:
        buffer = ctypes.create_string_buffer(source, len(source))
        blob = DATA_BLOB()
        blob.cbData = len(source)
        blob.pbData = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte))
        return blob, buffer

    def _crypt_protect_data(data: bytes) -> bytes:
        in_blob, _in_buffer = _create_blob(data)
        out_blob = DATA_BLOB()
        flags = _CRYPTPROTECT_LOCAL_MACHINE | _CRYPTPROTECT_UI_FORBIDDEN
        if not _crypt32.CryptProtectData(
            ctypes.byref(in_blob),
            None,
            None,
            None,
            None,
            flags,
            ctypes.byref(out_blob),
        ):
            raise ctypes.WinError()
        try:
            return ctypes.string_at(out_blob.pbData, out_blob.cbData)
        finally:
            _kernel32.LocalFree(out_blob.pbData)

    def _crypt_unprotect_data(data: bytes) -> bytes:
        in_blob, _in_buffer = _create_blob(data)
        out_blob = DATA_BLOB()
        flags = _CRYPTPROTECT_UI_FORBIDDEN
        if not _crypt32.CryptUnprotectData(
            ctypes.byref(in_blob),
            None,
            None,
            None,
            None,
            flags,
            ctypes.byref(out_blob),
        ):
            raise ctypes.WinError()
        try:
            return ctypes.string_at(out_blob.pbData, out_blob.cbData)
        finally:
            _kernel32.LocalFree(out_blob.pbData)


def _normalize_bytes(value: bytes | bytearray) -> bytes:
    """把输入转成不可变字节串。"""
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, bytes):
        return value
    raise TypeError("secret must be bytes or bytearray")


def _now_ms() -> int:
    """返回当前 UTC 毫秒时间戳。"""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _decode_b64_field(value: str, field_name: str) -> bytes:
    """把 base64 字段解码成字节串。"""
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    try:
        return base64.b64decode(text, validate=True)
    except Exception as exc:
        raise ValueError(f"{field_name} is not valid base64") from exc


def _parse_bool_flag(value: object, default: bool = False) -> bool:
    """严格解析布尔值，避免字符串 'false' 被当成真值。"""
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "off", ""}:
        return False
    return bool(default)


def _normalize_public_key_active_account(active_account: dict | None) -> dict | None:
    """public-key 接口对当前激活账号补齐 canonical active 标记。"""
    if not isinstance(active_account, dict) or not active_account:
        return None
    normalized = dict(active_account)
    state = str(normalized.get("state") or "").strip().lower()
    if state == "activated":
        normalized["active"] = True
    return normalized


class LoginEnvelopeCrypto:
    """管理登录信封公钥与解密流程。"""

    def __init__(
        self,
        now_ms_provider=None,
        key_ttl_ms: int = 10 * 60 * 1000,
        allowed_skew_ms: int = 5 * 60 * 1000,
    ):
        """初始化密钥轮换与时间窗口配置。"""
        if not _HAS_CRYPTOGRAPHY:
            raise RuntimeError("cryptography is required for rsa-oaep+aes-gcm")
        self._now_ms_provider = now_ms_provider or _now_ms
        self._key_ttl_ms = max(60_000, int(key_ttl_ms or 0))
        self._allowed_skew_ms = max(0, int(allowed_skew_ms or 0))
        self._algorithm = "rsa-oaep+aes-gcm"
        self._key_id = ""
        self._expires_at_ms = 0
        self._private_key = None
        self._public_key_pem = ""
        # 记录已处理 nonce，阻断时间窗内重放。
        self._nonce_seen_at: dict[str, int] = {}
        self._state_lock = threading.RLock()

    def _now_ms(self) -> int:
        """读取当前毫秒时间。"""
        return int(self._now_ms_provider())

    def _rotate_keypair(self) -> None:
        """生成新的 RSA 密钥对并刷新过期时间。"""
        with self._state_lock:
            private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            public_key = private_key.public_key()
            public_pem = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            ).decode("utf-8")
            now_ms = self._now_ms()
            self._private_key = private_key
            self._public_key_pem = public_pem
            self._expires_at_ms = now_ms + self._key_ttl_ms
            self._key_id = f"key-{now_ms}-{fingerprint_public_key(public_pem)[:12]}"

    def _ensure_key_for_public_read(self) -> None:
        """确保 public-key 接口总能拿到可用密钥。"""
        with self._state_lock:
            if self._private_key is None:
                self._rotate_keypair()
                return
            if self._now_ms() >= int(self._expires_at_ms or 0):
                self._rotate_keypair()

    def _resolve_private_key_for_decrypt(self, key_id: str):
        """校验 keyId 与密钥有效期，并返回私钥快照。"""
        with self._state_lock:
            if self._private_key is None:
                raise ValueError("keyId invalid")
            if self._now_ms() >= int(self._expires_at_ms or 0):
                raise ValueError("keyId expired")
            if str(key_id or "").strip() != str(self._key_id or ""):
                raise ValueError("keyId invalid")
            return self._private_key

    def _nonce_window_ms(self) -> int:
        """返回 nonce 去重窗口，至少 1 分钟。"""
        return max(60_000, int(self._allowed_skew_ms or 0))

    def _prune_nonce_cache(self, now_ms: int) -> None:
        """清理过期 nonce，控制内存占用。"""
        window_ms = self._nonce_window_ms()
        expired_keys = [key for key, seen_at in self._nonce_seen_at.items() if (now_ms - int(seen_at or 0)) > window_ms]
        for key in expired_keys:
            self._nonce_seen_at.pop(key, None)
        if len(self._nonce_seen_at) <= 4096:
            return
        keep_items = sorted(self._nonce_seen_at.items(), key=lambda item: int(item[1] or 0), reverse=True)[:2048]
        self._nonce_seen_at = {str(key): int(value) for key, value in keep_items}

    def _assert_and_register_nonce(self, nonce: str, now_ms: int) -> None:
        """校验 nonce 未重放并注册当前请求。"""
        nonce_text = str(nonce or "").strip()
        if not nonce_text:
            raise ValueError("nonce is required")
        with self._state_lock:
            self._prune_nonce_cache(now_ms)
            if nonce_text in self._nonce_seen_at:
                raise ValueError("nonce replay detected")
            self._nonce_seen_at[nonce_text] = int(now_ms)

    def build_public_key_payload(
        self,
        active_account: dict | None = None,
        saved_accounts: list | None = None,
    ) -> dict:
        """构建 public-key 接口的稳定返回结构。"""
        self._ensure_key_for_public_read()
        safe_saved_accounts = [dict(item or {}) for item in (saved_accounts or [])]
        with self._state_lock:
            key_id = str(self._key_id or "")
            algorithm = str(self._algorithm)
            public_key_pem = str(self._public_key_pem or "")
            expires_at = int(self._expires_at_ms or 0)
        payload = v2_session_models.SessionPublicKeyPayload(
            key_id=key_id,
            algorithm=algorithm,
            public_key_pem=public_key_pem,
            expires_at=expires_at,
            active_account=v2_session_models.SessionAccountSummary.from_mapping(
                _normalize_public_key_active_account(active_account)
            ),
            saved_accounts=[
                item
                for item in (
                    v2_session_models.SessionAccountSummary.from_mapping(account)
                    for account in safe_saved_accounts
                )
                if item is not None
            ],
        )
        return payload.to_dict()

    def decrypt_login_envelope(self, payload: dict) -> dict:
        """解密登录信封并返回业务载荷。"""
        safe_payload = dict(payload or {})
        key_id = str(safe_payload.get("keyId") or "").strip()
        algorithm = str(safe_payload.get("algorithm") or "").strip().lower()
        if algorithm != self._algorithm:
            raise ValueError("algorithm not supported")
        private_key = self._resolve_private_key_for_decrypt(key_id)

        iv = _decode_b64_field(str(safe_payload.get("iv") or ""), "iv")
        encrypted_key = _decode_b64_field(str(safe_payload.get("encryptedKey") or ""), "encryptedKey")
        encrypted_payload = _decode_b64_field(str(safe_payload.get("encryptedPayload") or ""), "encryptedPayload")
        try:
            aes_key = private_key.decrypt(
                encrypted_key,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None,
                ),
            )
            plain_bytes = AESGCM(aes_key).decrypt(iv, encrypted_payload, None)
            plain_payload = json.loads(plain_bytes.decode("utf-8"))
        except ValueError:
            raise
        except Exception as exc:
            raise ValueError("decrypt envelope failed") from exc

        login = str(plain_payload.get("login") or "").strip()
        password = str(plain_payload.get("password") or "")
        server = str(plain_payload.get("server") or "").strip()
        remember = _parse_bool_flag(
            plain_payload.get("remember"),
            default=_parse_bool_flag(safe_payload.get("saveAccount")),
        )
        nonce = str(plain_payload.get("nonce") or "")
        client_time = int(plain_payload.get("clientTime") or 0)
        if not login:
            raise ValueError("login is required")
        if not password:
            raise ValueError("password is required")
        if not server:
            raise ValueError("server is required")
        now_ms = self._now_ms()
        validate_request_time(client_time, now_ms, self._allowed_skew_ms)
        self._assert_and_register_nonce(nonce, now_ms)
        return {
            "requestId": str(safe_payload.get("requestId") or ""),
            "login": login,
            "password": password,
            "server": server,
            "remember": remember,
            "nonce": nonce,
            "clientTime": client_time,
        }


_DEV_FALLBACK_KEY: bytes | None = None


def _dev_fallback_entropy_key() -> bytes:
    """为测试/开发环境生成的机器特征 key（非生产）。"""
    global _DEV_FALLBACK_KEY
    if _DEV_FALLBACK_KEY is not None:
        return _DEV_FALLBACK_KEY
    components: Iterable[str] = [
        platform.node(),
        str(uuid.getnode()),
        os.environ.get("USER") or os.environ.get("USERNAME") or "",
    ]
    secret_source = "|".join(filter(None, components))
    if not secret_source:
        secret_source = "mt5-session-fallback"
    _DEV_FALLBACK_KEY = hashlib.sha256(secret_source.encode("utf-8")).digest()
    return _DEV_FALLBACK_KEY


def _dev_fallback_transform(data: bytes) -> bytes:
    """仅作为测试/开发用回退，加密强度远低于 DPAPI。"""
    key = _dev_fallback_entropy_key()
    if not key:
        return data
    return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


def validate_request_time(client_time_ms: int, now_ms: int, allowed_skew_ms: int) -> bool:
    """校验请求时间戳是否在允许的漂移范围内，否则抛出 ValueError。"""
    now = int(now_ms)
    client = int(client_time_ms)
    allowed = int(allowed_skew_ms)
    if allowed < 0:
        raise ValueError("allowed_skew_ms must be non-negative")
    skew = abs(now - client)
    if skew > allowed:
        raise ValueError("request time outside allowed skew")
    return True


def protect_secret_for_machine(secret: bytes | bytearray) -> bytes:
    """为当前机器保护一段秘密数据，返回密文；非 Windows 平台仅用测试兼容回退。"""
    secret_bytes = _normalize_bytes(secret)
    if _IS_WINDOWS:
        return _crypt_protect_data(secret_bytes)
    return _dev_fallback_transform(secret_bytes)


def unprotect_secret_for_machine(ciphertext: bytes | bytearray) -> bytes:
    """恢复 protect_secret_for_machine 的密文，非 Windows 依赖回退转换。"""
    data = _normalize_bytes(ciphertext)
    if _IS_WINDOWS:
        return _crypt_unprotect_data(data)
    return _dev_fallback_transform(data)


def fingerprint_public_key(public_key_pem: str) -> str:
    """根据 PEM 数据生成简洁 fingerprint 供会话层引用。"""
    canonical = public_key_pem.replace("\r", "").replace("\n", "").strip().encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()
