"""v2 交易命令解析与 MT5 请求构建。

该模块负责把 APP 交易命令统一成可执行请求，并做前置校验。
"""

from __future__ import annotations

import uuid
from typing import Any, Callable, Dict, Mapping, Optional

import v2_trade_models

ACTION_OPEN_MARKET = "OPEN_MARKET"
ACTION_CLOSE_POSITION = "CLOSE_POSITION"
ACTION_PENDING_ADD = "PENDING_ADD"
ACTION_PENDING_MODIFY = "PENDING_MODIFY"
ACTION_PENDING_CANCEL = "PENDING_CANCEL"
ACTION_MODIFY_TPSL = "MODIFY_TPSL"
ACTION_CLOSE_BY = "CLOSE_BY"

SUPPORTED_ACTIONS = {
    ACTION_OPEN_MARKET,
    ACTION_CLOSE_POSITION,
    ACTION_PENDING_ADD,
    ACTION_PENDING_MODIFY,
    ACTION_PENDING_CANCEL,
    ACTION_MODIFY_TPSL,
    ACTION_CLOSE_BY,
}


def _to_float(value: Any, default: float = 0.0) -> float:
    """把任意值安全转成浮点。"""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    """把任意值安全转成整数。"""
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalize_action(value: Any) -> str:
    """把 action 统一成大写枚举值。"""
    return str(value or "").strip().upper()


def _normalize_side(value: Any) -> str:
    """把方向统一成 buy/sell。"""
    side = str(value or "").strip().lower()
    if side in {"buy", "long"}:
        return "buy"
    if side in {"sell", "short"}:
        return "sell"
    return ""


def _normalize_pending_order_type(value: Any) -> str:
    """规范化挂单类型字符串。"""
    return str(value or "").strip().lower()


def _volume_step_error(volume: float, step: float) -> bool:
    """判断手数是否符合步进。"""
    if step <= 0:
        return False
    ratio = round(volume / step)
    return abs(volume - ratio * step) > 1e-9


def _resolve_symbol_info(symbol: str, symbol_info_lookup: Optional[Callable[[str], Any]]) -> Any:
    """读取 symbol_info，失败时返回 None。"""
    if not symbol or symbol_info_lookup is None:
        return None
    try:
        return symbol_info_lookup(symbol)
    except Exception:
        return None


def _validate_volume(params: Mapping[str, Any], symbol_info: Any) -> Optional[Dict[str, Any]]:
    """校验手数合法性和步进。"""
    volume = _to_float(params.get("volume"), 0.0)
    if volume <= 0.0:
        return v2_trade_models.build_error(
            v2_trade_models.ERROR_INVALID_VOLUME,
            "volume 必须大于 0",
        )
    min_volume = _to_float(getattr(symbol_info, "volume_min", None), 0.0)
    max_volume = _to_float(getattr(symbol_info, "volume_max", None), 0.0)
    step = _to_float(params.get("volumeStep"), _to_float(getattr(symbol_info, "volume_step", None), 0.01))
    if min_volume > 0 and volume < min_volume:
        return v2_trade_models.build_error(
            v2_trade_models.ERROR_INVALID_VOLUME,
            f"volume 不能小于 {min_volume}",
            {"minVolume": min_volume},
        )
    if max_volume > 0 and volume > max_volume:
        return v2_trade_models.build_error(
            v2_trade_models.ERROR_INVALID_VOLUME,
            f"volume 不能大于 {max_volume}",
            {"maxVolume": max_volume},
        )
    if _volume_step_error(volume, step):
        return v2_trade_models.build_error(
            v2_trade_models.ERROR_INVALID_VOLUME_STEP,
            "volume 不满足步进要求",
            {"step": step},
        )
    return None


def _validate_stops(params: Mapping[str, Any], symbol_info: Any) -> Optional[Dict[str, Any]]:
    """校验止损止盈与参考价的最小距离。"""
    sl = _to_float(params.get("sl"), 0.0)
    tp = _to_float(params.get("tp"), 0.0)
    if sl <= 0.0 and tp <= 0.0:
        return None
    price = _to_float(params.get("price"), _to_float(params.get("entryPrice"), 0.0))
    if price <= 0.0:
        return None
    point = _to_float(getattr(symbol_info, "point", None), 0.0)
    stops_level = _to_int(getattr(symbol_info, "trade_stops_level", None), 0)
    min_distance = point * stops_level
    if min_distance <= 0.0:
        return None
    if sl > 0.0 and abs(price - sl) < min_distance:
        return v2_trade_models.build_error(
            v2_trade_models.ERROR_INVALID_STOPS_DISTANCE,
            "止损距离不足",
            {"minDistance": min_distance, "price": price, "sl": sl},
        )
    if tp > 0.0 and abs(price - tp) < min_distance:
        return v2_trade_models.build_error(
            v2_trade_models.ERROR_INVALID_STOPS_DISTANCE,
            "止盈距离不足",
            {"minDistance": min_distance, "price": price, "tp": tp},
        )
    return None


def detect_account_mode(mt5_module: Any, account_info: Optional[Any] = None) -> str:
    """识别账户是 netting 还是 hedging。"""
    info = account_info
    if info is None and mt5_module is not None:
        try:
            info = mt5_module.account_info()
        except Exception:
            info = None
    if info is None:
        return "unknown"
    margin_mode = _to_int(getattr(info, "margin_mode", None), -1)
    if margin_mode == 2:
        return "hedging"
    if margin_mode in {0, 1}:
        return "netting"
    return "unknown"


def normalize_trade_payload(payload: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """规范化交易命令基础字段。"""
    data = dict(payload or {})
    params = data.get("params")
    if not isinstance(params, Mapping):
        params = {}
    request_id = str(data.get("requestId") or "").strip()
    if not request_id:
        request_id = uuid.uuid4().hex
    return {
        "requestId": request_id,
        "action": _normalize_action(data.get("action")),
        "params": dict(params),
    }


def prepare_trade_request(
    payload: Optional[Mapping[str, Any]],
    *,
    account_mode: str,
    mt5_module: Any,
    position_lookup: Optional[Callable[[Mapping[str, Any], str], Optional[Dict[str, Any]]]] = None,
    symbol_info_lookup: Optional[Callable[[str], Any]] = None,
) -> Dict[str, Any]:
    """把交易命令转换为 MT5 请求并做校验。"""
    command = normalize_trade_payload(payload)
    action = command["action"]
    params = command["params"]
    resolved_position: Optional[Dict[str, Any]] = None

    if action not in SUPPORTED_ACTIONS:
        return {
            "command": command,
            "request": None,
            "error": v2_trade_models.build_error(
                v2_trade_models.ERROR_INVALID_ACTION,
                "action 不在第一阶段支持范围",
                {"supportedActions": sorted(SUPPORTED_ACTIONS)},
            ),
        }

    symbol = str(params.get("symbol") or "").strip().upper()

    if action in {ACTION_OPEN_MARKET, ACTION_PENDING_ADD, ACTION_PENDING_MODIFY} and not symbol:
        return {
            "command": command,
            "request": None,
            "error": v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_SYMBOL, "symbol 不能为空"),
        }

    if action in {ACTION_CLOSE_POSITION, ACTION_MODIFY_TPSL}:
        if account_mode not in {"netting", "hedging"}:
            return {
                "command": command,
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_UNSAFE_ACCOUNT_MODE,
                    "账户模式未知，拒绝执行平仓/改单",
                ),
            }
        if position_lookup is None:
            return {
                "command": command,
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_POSITION,
                    "无法定位目标持仓",
                ),
            }
        if account_mode == "hedging":
            position_ticket = _to_int(params.get("positionTicket") or params.get("positionId"), 0)
            if position_ticket <= 0:
                return {
                    "command": command,
                    "request": None,
                    "error": v2_trade_models.build_error(
                        v2_trade_models.ERROR_INVALID_POSITION,
                        "hedging 模式必须指定 positionTicket",
                    ),
                }
        resolved_position = position_lookup(params, account_mode)
        if not resolved_position:
            return {
                "command": command,
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_POSITION,
                    "未找到可确认的目标持仓",
                ),
            }
        symbol = str(resolved_position.get("symbol") or symbol).strip().upper()
    elif action == ACTION_CLOSE_BY:
        if account_mode != "hedging":
            return {
                "command": command,
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_UNSAFE_ACCOUNT_MODE,
                    "Close By 仅支持 hedging 账户",
                ),
            }
        position_ticket = _to_int(params.get("positionTicket") or params.get("positionId"), 0)
        opposite_position_ticket = _to_int(
            params.get("oppositePositionTicket") or params.get("oppositePositionId"),
            0,
        )
        if position_ticket <= 0 or opposite_position_ticket <= 0:
            return {
                "command": command,
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_POSITION,
                    "Close By 必须同时指定 positionTicket 和 oppositePositionTicket",
                ),
            }
        if not symbol:
            return {
                "command": command,
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_SYMBOL,
                    "Close By 需要 symbol",
                ),
            }

    symbol_info = _resolve_symbol_info(symbol, symbol_info_lookup)

    if action in {ACTION_OPEN_MARKET, ACTION_CLOSE_POSITION, ACTION_PENDING_ADD}:
        volume_error = _validate_volume(params, symbol_info)
        if volume_error is not None:
            return {"command": command, "request": None, "error": volume_error}

    if action in {ACTION_OPEN_MARKET, ACTION_PENDING_ADD, ACTION_PENDING_MODIFY, ACTION_MODIFY_TPSL}:
        stop_error = _validate_stops(params, symbol_info)
        if stop_error is not None:
            return {"command": command, "request": None, "error": stop_error}

    request_builder = _request_builder(
        mt5_module,
        params=params,
        action=action,
        symbol=symbol,
        resolved_position=resolved_position,
    )
    if request_builder["error"] is not None:
        return {"command": command, "request": None, "error": request_builder["error"]}
    return {"command": command, "request": request_builder["request"], "error": None}


def _request_builder(
    mt5_module: Any,
    *,
    params: Mapping[str, Any],
    action: str,
    symbol: str,
    resolved_position: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """按 action 构建 MT5 请求。"""
    trade_action_deal = _to_int(getattr(mt5_module, "TRADE_ACTION_DEAL", 1), 1)
    trade_action_pending = _to_int(getattr(mt5_module, "TRADE_ACTION_PENDING", 5), 5)
    trade_action_modify = _to_int(getattr(mt5_module, "TRADE_ACTION_MODIFY", 7), 7)
    trade_action_remove = _to_int(getattr(mt5_module, "TRADE_ACTION_REMOVE", 8), 8)
    trade_action_sltp = _to_int(getattr(mt5_module, "TRADE_ACTION_SLTP", 6), 6)
    trade_action_close_by = _to_int(getattr(mt5_module, "TRADE_ACTION_CLOSE_BY", 10), 10)
    order_type_buy = _to_int(getattr(mt5_module, "ORDER_TYPE_BUY", 0), 0)
    order_type_sell = _to_int(getattr(mt5_module, "ORDER_TYPE_SELL", 1), 1)
    order_time_gtc = _to_int(getattr(mt5_module, "ORDER_TIME_GTC", 0), 0)
    order_filling_fok = _to_int(getattr(mt5_module, "ORDER_FILLING_FOK", 0), 0)

    base = {
        "comment": str(params.get("comment") or "mt5-gateway-v2"),
        "deviation": _to_int(params.get("deviation"), 20),
        "magic": _to_int(params.get("magic"), 20260406),
        "type_time": _to_int(params.get("typeTime"), order_time_gtc),
        "type_filling": _to_int(params.get("typeFilling"), order_filling_fok),
    }

    if action == ACTION_OPEN_MARKET:
        side = _normalize_side(params.get("side"))
        if not side:
            return {
                "request": None,
                "error": v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_SIDE, "side 仅支持 buy/sell"),
            }
        request = dict(base)
        request.update(
            {
                "action": trade_action_deal,
                "symbol": symbol,
                "type": order_type_buy if side == "buy" else order_type_sell,
                "volume": _to_float(params.get("volume"), 0.0),
                "price": _to_float(params.get("price"), 0.0),
            }
        )
        sl = _to_float(params.get("sl"), 0.0)
        tp = _to_float(params.get("tp"), 0.0)
        if sl > 0.0:
            request["sl"] = sl
        if tp > 0.0:
            request["tp"] = tp
        return {"request": request, "error": None}

    if action == ACTION_CLOSE_POSITION:
        position = dict(resolved_position or {})
        symbol = str(position.get("symbol") or symbol).upper()
        if not symbol:
            return {
                "request": None,
                "error": v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_SYMBOL, "平仓需要 symbol"),
            }
        side = _normalize_side(position.get("side"))
        if not side:
            return {
                "request": None,
                "error": v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_POSITION, "目标持仓方向不可识别"),
            }
        position_ticket = _to_int(position.get("ticket") or position.get("positionTicket") or position.get("positionId"), 0)
        if position_ticket <= 0:
            return {
                "request": None,
                "error": v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_POSITION, "目标持仓 ticket 不可识别"),
            }
        request = dict(base)
        request.update(
            {
                "action": trade_action_deal,
                "symbol": symbol,
                "type": order_type_sell if side == "buy" else order_type_buy,
                "volume": _to_float(params.get("volume"), 0.0),
                "price": _to_float(params.get("price"), 0.0),
                "position": position_ticket,
            }
        )
        return {"request": request, "error": None}

    if action == ACTION_PENDING_ADD:
        order_type_name = _normalize_pending_order_type(params.get("orderType"))
        pending_type_map = {
            "buy_limit": _to_int(getattr(mt5_module, "ORDER_TYPE_BUY_LIMIT", 2), 2),
            "sell_limit": _to_int(getattr(mt5_module, "ORDER_TYPE_SELL_LIMIT", 3), 3),
            "buy_stop": _to_int(getattr(mt5_module, "ORDER_TYPE_BUY_STOP", 4), 4),
            "sell_stop": _to_int(getattr(mt5_module, "ORDER_TYPE_SELL_STOP", 5), 5),
        }
        if order_type_name not in pending_type_map:
            return {
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_PARAMS,
                    "挂单类型仅支持 buy_limit/sell_limit/buy_stop/sell_stop",
                ),
            }
        price = _to_float(params.get("price"), 0.0)
        if price <= 0.0:
            return {
                "request": None,
                "error": v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_PARAMS, "挂单 price 必须大于 0"),
            }
        request = dict(base)
        request.update(
            {
                "action": trade_action_pending,
                "symbol": symbol,
                "type": pending_type_map[order_type_name],
                "volume": _to_float(params.get("volume"), 0.0),
                "price": price,
            }
        )
        sl = _to_float(params.get("sl"), 0.0)
        tp = _to_float(params.get("tp"), 0.0)
        if sl > 0.0:
            request["sl"] = sl
        if tp > 0.0:
            request["tp"] = tp
        return {"request": request, "error": None}

    if action == ACTION_PENDING_CANCEL:
        order_ticket = _to_int(params.get("orderTicket") or params.get("orderId"), 0)
        if order_ticket <= 0:
            return {
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_ORDER,
                    "撤单需要 orderTicket",
                ),
            }
        request = dict(base)
        request.update({"action": trade_action_remove, "order": order_ticket})
        return {"request": request, "error": None}

    if action == ACTION_PENDING_MODIFY:
        order_ticket = _to_int(params.get("orderTicket") or params.get("orderId"), 0)
        price = _to_float(params.get("price"), 0.0)
        sl = _to_float(params.get("sl"), 0.0)
        tp = _to_float(params.get("tp"), 0.0)
        if order_ticket <= 0:
            return {
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_ORDER,
                    "修改挂单需要 orderTicket",
                ),
            }
        if price <= 0.0 and sl <= 0.0 and tp <= 0.0:
            return {
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_PARAMS,
                    "修改挂单至少要传一个值",
                ),
            }
        request = dict(base)
        request.update({"action": trade_action_modify, "order": order_ticket})
        if symbol:
            request["symbol"] = symbol
        if price > 0.0:
            request["price"] = price
        if sl > 0.0:
            request["sl"] = sl
        if tp > 0.0:
            request["tp"] = tp
        return {"request": request, "error": None}

    if action == ACTION_MODIFY_TPSL:
        sl = _to_float(params.get("sl"), 0.0)
        tp = _to_float(params.get("tp"), 0.0)
        if sl <= 0.0 and tp <= 0.0:
            return {
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_PARAMS,
                    "修改 TP/SL 至少要传一个值",
                ),
            }
        request = dict(base)
        request.update({"action": trade_action_sltp})
        position = dict(resolved_position or {})
        position_ticket = _to_int(position.get("ticket") or position.get("positionTicket") or position.get("positionId"), 0)
        if position_ticket <= 0:
            return {
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_POSITION,
                    "修改 TP/SL 需要可确认的目标持仓",
                ),
            }
        request["position"] = position_ticket
        resolved_symbol = str(position.get("symbol") or symbol).upper()
        if resolved_symbol:
            request["symbol"] = resolved_symbol
        if sl > 0.0:
            request["sl"] = sl
        if tp > 0.0:
            request["tp"] = tp
        return {"request": request, "error": None}

    if action == ACTION_CLOSE_BY:
        position_ticket = _to_int(params.get("positionTicket") or params.get("positionId"), 0)
        opposite_position_ticket = _to_int(
            params.get("oppositePositionTicket") or params.get("oppositePositionId"),
            0,
        )
        if position_ticket <= 0 or opposite_position_ticket <= 0:
            return {
                "request": None,
                "error": v2_trade_models.build_error(
                    v2_trade_models.ERROR_INVALID_POSITION,
                    "Close By 需要成对持仓 ticket",
                ),
            }
        request = dict(base)
        request.update(
            {
                "action": trade_action_close_by,
                "symbol": symbol,
                "position": position_ticket,
                "position_by": opposite_position_ticket,
            }
        )
        return {"request": request, "error": None}

    return {
        "request": None,
        "error": v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_ACTION, "未知 action"),
    }
