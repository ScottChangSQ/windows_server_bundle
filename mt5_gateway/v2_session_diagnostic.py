"""v2 会话登录诊断缓存。

负责保存最近会话登录链的关键阶段，供 latest/lookup 查询复用。
"""

from __future__ import annotations

from collections import deque
from threading import Lock
from typing import Any, Deque, Dict, List, Optional


class SessionDiagnosticStore:
    """线程安全的会话诊断缓存。"""

    def __init__(self, max_entries: int = 2000) -> None:
        self._max_entries = max(10, int(max_entries or 2000))
        self._items: Deque[Dict[str, Any]] = deque()
        self._lock = Lock()

    def clear(self) -> None:
        """清空缓存。"""
        with self._lock:
            self._items.clear()

    def append(
        self,
        *,
        request_id: str,
        action: str,
        stage: str,
        status: str,
        message: str,
        server_time: int,
        error_code: str = "",
        detail: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """追加一条诊断事实，最近项始终在前。"""
        item = {
            "requestId": str(request_id or "").strip(),
            "action": str(action or "").strip(),
            "stage": str(stage or "").strip(),
            "status": str(status or "").strip(),
            "message": str(message or "").strip(),
            "errorCode": str(error_code or "").strip(),
            "serverTime": int(server_time or 0),
            "detail": dict(detail or {}),
        }
        with self._lock:
            self._items.appendleft(item)
            while len(self._items) > self._max_entries:
                self._items.pop()
        return dict(item)

    def recent(self, limit: int) -> List[Dict[str, Any]]:
        """返回最近记录，按最新优先。"""
        safe_limit = max(1, int(limit or 1))
        with self._lock:
            return [dict(item) for item in list(self._items)[:safe_limit]]

    def lookup(self, request_id: str) -> List[Dict[str, Any]]:
        """按 requestId 查询完整时间线，按时间正序返回。"""
        key = str(request_id or "").strip()
        if not key:
            return []
        with self._lock:
            matched = [dict(item) for item in self._items if str(item.get("requestId") or "") == key]
        matched.reverse()
        return matched

    def latest_request_id(self) -> str:
        """返回最近一条诊断对应的 requestId。"""
        with self._lock:
            if not self._items:
                return ""
            return str(self._items[0].get("requestId") or "").strip()

    def latest_timeline(self, request_id: str = "") -> List[Dict[str, Any]]:
        """返回最近一次或指定 requestId 的完整时间线。"""
        key = str(request_id or "").strip() or self.latest_request_id()
        if not key:
            return []
        return self.lookup(key)
