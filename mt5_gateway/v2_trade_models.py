"""v2 交易网关的响应模型与错误码定义。

该模块只放纯函数和常量，供 server_v2 与 v2_trade 复用。
"""

from __future__ import annotations

from typing import Any, Dict, Optional

STATUS_EXECUTABLE = "EXECUTABLE"
STATUS_NOT_EXECUTABLE = "NOT_EXECUTABLE"
STATUS_ACCEPTED = "ACCEPTED"
STATUS_FAILED = "FAILED"
STATUS_DUPLICATE = "DUPLICATE"
STATUS_PARTIAL = "PARTIAL"
STATUS_REJECTED = "REJECTED"

STRATEGY_BEST_EFFORT = "BEST_EFFORT"
STRATEGY_ALL_OR_NONE = "ALL_OR_NONE"
STRATEGY_GROUPED = "GROUPED"
SUPPORTED_BATCH_STRATEGIES = {
    STRATEGY_BEST_EFFORT,
    STRATEGY_ALL_OR_NONE,
    STRATEGY_GROUPED,
}

ERROR_INVALID_ACTION = "TRADE_INVALID_ACTION"
ERROR_INVALID_SYMBOL = "TRADE_INVALID_SYMBOL"
ERROR_INVALID_SIDE = "TRADE_INVALID_SIDE"
ERROR_INVALID_VOLUME = "TRADE_INVALID_VOLUME"
ERROR_INVALID_VOLUME_STEP = "TRADE_INVALID_VOLUME_STEP"
ERROR_INVALID_STOPS_DISTANCE = "TRADE_INVALID_STOPS_DISTANCE"
ERROR_INVALID_POSITION = "TRADE_INVALID_POSITION"
ERROR_INVALID_ORDER = "TRADE_INVALID_ORDER"
ERROR_INVALID_PARAMS = "TRADE_INVALID_PARAMS"
ERROR_INSUFFICIENT_MARGIN = "TRADE_INSUFFICIENT_MARGIN"
ERROR_MARKET_CLOSED = "TRADE_MARKET_CLOSED"
ERROR_REQUOTE = "TRADE_REQUOTE"
ERROR_TIMEOUT = "TRADE_TIMEOUT"
ERROR_DUPLICATE = "TRADE_DUPLICATE_SUBMISSION"
ERROR_DUPLICATE_PAYLOAD_MISMATCH = "TRADE_DUPLICATE_PAYLOAD_MISMATCH"
ERROR_UNSAFE_ACCOUNT_MODE = "TRADE_UNSAFE_ACCOUNT_MODE"
ERROR_RESULT_UNKNOWN = "TRADE_RESULT_UNKNOWN"
ERROR_EXECUTION_FAILED = "TRADE_EXECUTION_FAILED"
ERROR_REQUEST_NOT_FOUND = "TRADE_REQUEST_NOT_FOUND"
ERROR_BATCH_INVALID_ID = "TRADE_BATCH_INVALID_ID"
ERROR_BATCH_INVALID_STRATEGY = "TRADE_BATCH_INVALID_STRATEGY"
ERROR_BATCH_EMPTY_ITEMS = "TRADE_BATCH_EMPTY_ITEMS"
ERROR_BATCH_UNSUPPORTED_ACTION = "TRADE_BATCH_UNSUPPORTED_ACTION"
ERROR_BATCH_ITEM_INVALID_ID = "TRADE_BATCH_ITEM_INVALID_ID"
ERROR_BATCH_NOT_FOUND = "TRADE_BATCH_NOT_FOUND"

SUCCESS_RETCODES = {0, 10008, 10009, 10010}

RETCODE_ERROR_MAP = {
    10004: ERROR_REQUOTE,
    10012: ERROR_TIMEOUT,
    10014: ERROR_INVALID_VOLUME,
    10016: ERROR_INVALID_STOPS_DISTANCE,
    10018: ERROR_MARKET_CLOSED,
    10019: ERROR_INSUFFICIENT_MARGIN,
    10020: ERROR_REQUOTE,
}


def _to_int(value: Any, default: int = 0) -> int:
    """把任意值安全转成整数。"""
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    """把任意值安全转成浮点。"""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def build_error(code: str, message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """构建统一错误对象。"""
    return {
        "code": str(code or ERROR_EXECUTION_FAILED),
        "message": str(message or ""),
        "details": dict(details or {}),
    }


def mt5_result_to_dict(result: Any) -> Dict[str, Any]:
    """把 MT5 返回对象转换为可序列化结构。"""
    if result is None:
        return {
            "retcode": -1,
            "comment": "",
            "order": 0,
            "deal": 0,
            "volume": 0.0,
            "price": 0.0,
            "requestId": "",
        }
    return {
        "retcode": _to_int(getattr(result, "retcode", -1), -1),
        "comment": str(getattr(result, "comment", "") or ""),
        "order": _to_int(getattr(result, "order", 0), 0),
        "deal": _to_int(getattr(result, "deal", 0), 0),
        "volume": _to_float(getattr(result, "volume", 0.0), 0.0),
        "price": _to_float(getattr(result, "price", 0.0), 0.0),
        "requestId": str(getattr(result, "request_id", "") or ""),
    }


def is_success_retcode(retcode: int) -> bool:
    """判断 retcode 是否代表已通过。"""
    return _to_int(retcode, -1) in SUCCESS_RETCODES


def error_from_retcode(retcode: int, comment: str = "") -> Optional[Dict[str, Any]]:
    """把 MT5 retcode 映射成统一错误码。"""
    normalized_code = _to_int(retcode, -1)
    if is_success_retcode(normalized_code):
        return None
    mapped = RETCODE_ERROR_MAP.get(normalized_code, ERROR_EXECUTION_FAILED)
    message = comment or f"MT5 retcode={normalized_code}"
    return build_error(mapped, message, {"retcode": normalized_code})


def build_trade_check_response(
    *,
    request_id: str,
    action: str,
    account_mode: str,
    status: str,
    error: Optional[Dict[str, Any]],
    check: Optional[Dict[str, Any]],
    server_time: int,
) -> Dict[str, Any]:
    """构建 `/v2/trade/check` 的标准返回。"""
    return {
        "requestId": str(request_id or ""),
        "action": str(action or ""),
        "accountMode": str(account_mode or "unknown"),
        "status": str(status or STATUS_NOT_EXECUTABLE),
        "error": None if error is None else dict(error),
        "check": None if check is None else dict(check),
        "serverTime": int(server_time or 0),
    }


def build_trade_submit_response(
    *,
    request_id: str,
    action: str,
    account_mode: str,
    status: str,
    error: Optional[Dict[str, Any]],
    check: Optional[Dict[str, Any]],
    result: Optional[Dict[str, Any]],
    server_time: int,
    idempotent: bool,
) -> Dict[str, Any]:
    """构建 `/v2/trade/submit` 的标准返回。"""
    return {
        "requestId": str(request_id or ""),
        "action": str(action or ""),
        "accountMode": str(account_mode or "unknown"),
        "status": str(status or STATUS_FAILED),
        "error": None if error is None else dict(error),
        "check": None if check is None else dict(check),
        "result": None if result is None else dict(result),
        "idempotent": bool(idempotent),
        "serverTime": int(server_time or 0),
    }


def build_trade_batch_item_response(
    *,
    item_id: str,
    action: str,
    status: str,
    error: Optional[Dict[str, Any]],
    check: Optional[Dict[str, Any]],
    result: Optional[Dict[str, Any]],
    group_key: str = "",
) -> Dict[str, Any]:
    """构建批量交易的单项返回。"""
    payload = {
        "itemId": str(item_id or ""),
        "action": str(action or ""),
        "status": str(status or STATUS_FAILED),
        "error": None if error is None else dict(error),
        "check": None if check is None else dict(check),
        "result": None if result is None else dict(result),
    }
    if str(group_key or "").strip():
        payload["groupKey"] = str(group_key or "").strip()
    return payload


def build_trade_batch_submit_response(
    *,
    batch_id: str,
    strategy: str,
    account_mode: str,
    status: str,
    error: Optional[Dict[str, Any]],
    items: Optional[list],
    server_time: int,
) -> Dict[str, Any]:
    """构建 `/v2/trade/batch/submit` 的标准返回。"""
    return {
        "batchId": str(batch_id or ""),
        "strategy": str(strategy or STRATEGY_BEST_EFFORT),
        "accountMode": str(account_mode or "unknown"),
        "status": str(status or STATUS_FAILED),
        "error": None if error is None else dict(error),
        "items": list(items or []),
        "serverTime": int(server_time or 0),
    }
