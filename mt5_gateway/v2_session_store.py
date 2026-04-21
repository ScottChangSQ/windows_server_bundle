"""v2 会话存储层，负责当前激活会话与账号档案落盘。"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from bridge.mt5_gateway import v2_session_crypto
except Exception:  # pragma: no cover
    import v2_session_crypto  # type: ignore


def _now_ms() -> int:
    """返回当前 UTC 毫秒时间戳。"""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


class FileSessionStore:
    """使用文件系统维护会话状态与账号档案。"""

    def __init__(self, session_root: Path | str):
        """初始化会话目录与文件路径。"""
        self.session_root = Path(session_root)
        self.active_session_path = self.session_root / "active_session.json"
        self.accounts_dir = self.session_root / "accounts"

    def _ensure_dirs(self) -> None:
        """确保会话目录存在。"""
        self.accounts_dir.mkdir(parents=True, exist_ok=True)

    def _read_json_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """读取 JSON 文件，不存在时返回 None。"""
        if not file_path.exists():
            return None
        try:
            with file_path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _write_json_file(self, file_path: Path, payload: Dict[str, Any]) -> None:
        """按 UTF-8 写 JSON 文件。"""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)

    def _extract_profile_payload(self, record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """从完整档案中提取 profile 字段，结构非法时返回 None。"""
        if not isinstance(record, dict):
            return None
        profile = record.get("profile")
        if not isinstance(profile, dict) or not profile:
            return None
        return dict(profile)

    def save_active_session(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """保存当前激活账号摘要。"""
        self._ensure_dirs()
        payload = dict(profile or {})
        payload["active"] = True
        payload["updatedAtMs"] = _now_ms()
        self._write_json_file(self.active_session_path, payload)
        return payload

    def load_active_session(self) -> Optional[Dict[str, Any]]:
        """读取当前激活账号摘要。"""
        return self._read_json_file(self.active_session_path)

    def clear_active_session(self) -> None:
        """清除当前激活账号摘要。"""
        if self.active_session_path.exists():
            self.active_session_path.unlink()

    def save_profile(self, profile: Dict[str, Any], password: str) -> Dict[str, Any]:
        """保存加密后的账号档案。"""
        profile_id = str((profile or {}).get("profileId") or "").strip()
        if not profile_id:
            raise ValueError("profileId is required")
        self._ensure_dirs()
        cipher = v2_session_crypto.protect_secret_for_machine(str(password or "").encode("utf-8"))
        # 已保存账号档案只保存静态摘要，不把当前运行态直接落盘。
        safe_profile = dict(profile or {})
        safe_profile["active"] = False
        safe_profile["state"] = ""
        record = {
            "profile": safe_profile,
            "encryptedPassword": base64.b64encode(cipher).decode("ascii"),
            "updatedAtMs": _now_ms(),
        }
        self._write_json_file(self.accounts_dir / f"{profile_id}.json", record)
        return record

    def restore_profile_record(self, profile_id: str, record: Optional[Dict[str, Any]]) -> None:
        """按原始记录恢复账号档案；record 为空时删除档案。"""
        profile_key = str(profile_id or "").strip()
        if not profile_key:
            raise ValueError("profileId is required")
        record_path = self.accounts_dir / f"{profile_key}.json"
        if not isinstance(record, dict):
            if record_path.exists():
                record_path.unlink()
            return
        self._ensure_dirs()
        self._write_json_file(record_path, dict(record))

    def load_profile(self, profile_id: str) -> Optional[Dict[str, Any]]:
        """按 profileId 读取完整账号档案。"""
        profile_key = str(profile_id or "").strip()
        if not profile_key:
            return None
        return self._read_json_file(self.accounts_dir / f"{profile_key}.json")

    def delete_profile(self, profile_id: str) -> bool:
        """按 profileId 删除账号档案，存在返回 True。"""
        profile_key = str(profile_id or "").strip()
        if not profile_key:
            return False
        record_path = self.accounts_dir / f"{profile_key}.json"
        if not record_path.exists():
            return False
        record_path.unlink()
        return True

    def list_profiles(self) -> List[Dict[str, Any]]:
        """列出已保存账号的摘要。"""
        if not self.accounts_dir.exists():
            return []
        profiles: List[Dict[str, Any]] = []
        for record_path in sorted(self.accounts_dir.glob("*.json")):
            record = self._read_json_file(record_path)
            profile = self._extract_profile_payload(record)
            if profile is not None:
                profiles.append(profile)
        return profiles
