"""v2 交易审计缓存。

负责保存最近交易链的关键事实，供 recent/lookup 查询复用。
"""

from __future__ import annotations

from collections import deque
from threading import Lock
from typing import Any, Deque, Dict, List, Optional


class TradeAuditStore:
    """线程安全的最近交易审计缓存。"""

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
        trace_id: str,
        trace_type: str,
        action: str,
        symbol: str,
        account_mode: str,
        stage: str,
        status: str,
        error_code: str,
        message: str,
        server_time: int,
        action_summary: str = "",
        created_at: int = 0,
    ) -> Dict[str, Any]:
        """追加一条交易审计事实，最近项始终在前。"""
        item = {
            "traceId": str(trace_id or "").strip(),
            "traceType": str(trace_type or "").strip(),
            "action": str(action or "").strip(),
            "symbol": str(symbol or "").strip(),
            "accountMode": str(account_mode or "").strip(),
            "stage": str(stage or "").strip(),
            "status": str(status or "").strip(),
            "errorCode": str(error_code or "").strip(),
            "message": str(message or "").strip(),
            "actionSummary": str(action_summary or "").strip(),
            "serverTime": int(server_time or 0),
            "createdAt": int(created_at or server_time or 0),
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

    def lookup(self, trace_id: str) -> List[Dict[str, Any]]:
        """按 traceId 查询完整时间线，按时间正序返回。"""
        key = str(trace_id or "").strip()
        if not key:
            return []
        with self._lock:
            matched = [dict(item) for item in self._items if str(item.get("traceId") or "") == key]
        matched.reverse()
        return matched

    def latest(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """返回某条交易链最新一条记录。"""
        key = str(trace_id or "").strip()
        if not key:
            return None
        with self._lock:
            for item in self._items:
                if str(item.get("traceId") or "") == key:
                    return dict(item)
        return None
