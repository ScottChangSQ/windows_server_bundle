"""MT5 网关 V2 账户模型构建工具。

该模块仅包含纯函数，负责把上游快照/历史数据转换为 V2 可用的展示模型。
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence, MutableMapping, Dict, List, Optional


def _to_int(value: Any, default: int = 0) -> int:
    """将任意值安全转换为整数，无法转换时返回默认值。"""
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    """将任意值安全转换为浮点，无法转换时返回默认值。"""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _derive_margin_level(equity: float, margin: float) -> Optional[float]:
    """根据 equity 与 margin 推算保证金率。"""
    if margin <= 0:
        return None
    return (equity / margin) * 100


def _coalesce_none(*values: Any) -> Any:
    """只在值为 None 时才回退，保留 0/0.0 这类合法真值。"""
    for value in values:
        if value is not None:
            return value
    return None


def _require_text(raw: Mapping[str, Any], key: str, context: str) -> str:
    """读取必须存在的文本字段，缺失时直接报协议错误。"""
    value = str(raw.get(key) or "").strip()
    if not value:
        raise RuntimeError(f"{context} missing {key}")
    return value


def _require_matching_product_name(raw: Mapping[str, Any], trade_symbol: str, context: str) -> str:
    """展示名必须与 canonical tradeSymbol 对齐，避免同一产品多口径并存。"""
    product_name = _require_text(raw, "productName", context)
    if product_name != trade_symbol:
        raise RuntimeError(f"{context} productName must equal tradeSymbol")
    return product_name


def _normalize_position(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """统一处理持仓对象字段，保证数值类型和字段可预测。"""
    trade_symbol = _require_text(raw, "tradeSymbol", "position")
    market_symbol = _require_text(raw, "marketSymbol", "position")
    product_id = _require_text(raw, "productId", "position")
    return {
        "productId": product_id,
        "marketSymbol": market_symbol,
        "tradeSymbol": trade_symbol,
        "productName": _require_matching_product_name(raw, trade_symbol, "position"),
        "code": trade_symbol,
        "side": str(raw.get("side") or "").strip(),
        "positionTicket": _to_int(raw.get("positionTicket")),
        "orderId": _to_int(raw.get("orderId")),
        "openTime": _to_int(raw.get("openTime")),
        "quantity": _to_float(raw.get("quantity")),
        "costPrice": _to_float(raw.get("costPrice")),
        "latestPrice": _to_float(raw.get("latestPrice")),
        "totalPnL": _to_float(raw.get("totalPnL")),
        "pendingCount": _to_int(raw.get("pendingCount")),
        "takeProfit": _to_float(raw.get("takeProfit")),
        "stopLoss": _to_float(raw.get("stopLoss")),
        "storageFee": _to_float(raw.get("storageFee")),
    }


def _normalize_order(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """统一处理挂单对象字段，保持数值以浮点形式输出。"""
    trade_symbol = _require_text(raw, "tradeSymbol", "order")
    market_symbol = _require_text(raw, "marketSymbol", "order")
    product_id = _require_text(raw, "productId", "order")
    return {
        "productId": product_id,
        "marketSymbol": market_symbol,
        "tradeSymbol": trade_symbol,
        "productName": _require_matching_product_name(raw, trade_symbol, "order"),
        "code": trade_symbol,
        "side": str(raw.get("side") or "").strip(),
        "orderId": _to_int(raw.get("orderId")),
        "openTime": _to_int(raw.get("openTime")),
        "quantity": _to_float(raw.get("quantity")),
        "pendingLots": _to_float(raw.get("pendingLots")),
        "pendingPrice": _to_float(raw.get("pendingPrice")),
        "latestPrice": _to_float(raw.get("latestPrice")),
        "pendingCount": _to_int(raw.get("pendingCount")),
        "takeProfit": _to_float(raw.get("takeProfit")),
        "stopLoss": _to_float(raw.get("stopLoss")),
        "status": raw.get("status"),
    }


def _normalize_trade(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """把 canonical 成交数据标准化为 V2 历史成交行。"""
    trade_symbol = _require_text(raw, "tradeSymbol", "trade")
    market_symbol = _require_text(raw, "marketSymbol", "trade")
    product_id = _require_text(raw, "productId", "trade")
    return {
        "timestamp": _to_int(raw.get("timestamp")),
        "productId": product_id,
        "marketSymbol": market_symbol,
        "tradeSymbol": trade_symbol,
        "productName": _require_matching_product_name(raw, trade_symbol, "trade"),
        "code": trade_symbol,
        "side": str(raw.get("side") or "").strip(),
        "price": _to_float(raw.get("price")),
        "quantity": _to_float(raw.get("quantity")),
        "profit": _to_float(raw.get("profit")),
        "fee": _to_float(raw.get("fee")),
        "storageFee": _to_float(raw.get("storageFee")),
        "openTime": _to_int(raw.get("openTime")),
        "closeTime": _to_int(raw.get("closeTime")),
        "openPrice": _to_float(raw.get("openPrice")),
        "closePrice": _to_float(raw.get("closePrice")),
        "dealTicket": _to_int(raw.get("dealTicket")),
        "orderId": _to_int(raw.get("orderId")),
        "positionId": _to_int(raw.get("positionId")),
        "entryType": _to_int(raw.get("entryType")),
        "remark": raw.get("remark") or "",
    }


def _normalize_curve_point(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """规范化 canonical 曲线点，保证 equity/balance/positionRatio 始终存在。"""
    return {
        "timestamp": _to_int(raw.get("timestamp")),
        "equity": _to_float(raw.get("equity")),
        "balance": _to_float(raw.get("balance")),
        "positionRatio": _to_float(raw.get("positionRatio")),
    }


def _normalize_sequence(
    iterable: Optional[Iterable[Mapping[str, Any]]], normalizer: Any
) -> List[Dict[str, Any]]:
    """辅助函数，遍历可选序列并应用字段标准化器。"""
    if iterable is None:
        return []
    results: List[Dict[str, Any]] = []
    for raw in iterable:
        if not isinstance(raw, Mapping):
            continue
        results.append(normalizer(raw))
    return results


def build_account_snapshot_model(raw_snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    """根据上游快照构造 V2 账户快照展示模型。"""
    metrics = (raw_snapshot.get("accountMetrics") or raw_snapshot.get("metrics") or {}) or {}

    balance = _to_float(_coalesce_none(metrics.get("balance"), raw_snapshot.get("balance")))
    equity = _to_float(_coalesce_none(metrics.get("equity"), raw_snapshot.get("equity")))
    margin = _to_float(_coalesce_none(metrics.get("margin"), raw_snapshot.get("margin")))
    free_margin = _to_float(
        _coalesce_none(metrics.get("freeMargin"), raw_snapshot.get("freeMargin"), raw_snapshot.get("free_margin"))
    )
    margin_level_source = _coalesce_none(metrics.get("marginLevel"), raw_snapshot.get("marginLevel"))
    margin_level = _to_float(margin_level_source) if margin_level_source is not None else _derive_margin_level(equity, margin)
    profit_source = _coalesce_none(metrics.get("profit"), raw_snapshot.get("profit"))
    profit = _to_float(profit_source if profit_source is not None else equity - balance)

    positions_source = raw_snapshot.get("positions")
    orders_source = raw_snapshot.get("orders")

    return {
        "balance": balance,
        "equity": equity,
        "margin": margin,
        "freeMargin": free_margin,
        "marginLevel": margin_level,
        "profit": profit,
        "positions": _normalize_sequence(positions_source, _normalize_position),
        "orders": _normalize_sequence(orders_source, _normalize_order),
    }


def build_account_history_model(raw_history: Mapping[str, Any]) -> Dict[str, Any]:
    """根据 canonical 历史数据构造 V2 账户历史展示模型。"""
    trades_source = raw_history.get("trades")
    orders_source = raw_history.get("orders")
    curve_source = raw_history.get("curvePoints")

    trades = _normalize_sequence(trades_source, _normalize_trade)
    orders = _normalize_sequence(orders_source, _normalize_order)
    curve_points = _normalize_sequence(curve_source, _normalize_curve_point)
    sorted_curve = sorted(curve_points, key=lambda point: point["timestamp"])

    return {
        "trades": trades,
        "orders": orders,
        "curvePoints": sorted_curve,
    }


def build_account_full_model(raw_full: Mapping[str, Any]) -> Dict[str, Any]:
    """把完整账户快照收口成单次强一致刷新可直接消费的完整模型。"""
    snapshot_model = build_account_snapshot_model(
        {
            "metrics": {
                "balance": ((raw_full.get("accountMeta") or {}) or {}).get("balance"),
                "equity": ((raw_full.get("accountMeta") or {}) or {}).get("equity"),
                "margin": ((raw_full.get("accountMeta") or {}) or {}).get("margin"),
                "freeMargin": ((raw_full.get("accountMeta") or {}) or {}).get("freeMargin"),
                "marginLevel": ((raw_full.get("accountMeta") or {}) or {}).get("marginLevel"),
                "profit": ((raw_full.get("accountMeta") or {}) or {}).get("profit"),
            },
            "positions": raw_full.get("positions") or [],
            "orders": raw_full.get("orders") or [],
        }
    )
    history_model = build_account_history_model(
        {
            "trades": raw_full.get("trades") or [],
            "orders": raw_full.get("orders") or [],
            "curvePoints": raw_full.get("curvePoints") or [],
        }
    )
    account_meta = (raw_full.get("accountMeta") or {}) or {}
    return {
        "account": {
            "balance": snapshot_model.get("balance"),
            "equity": snapshot_model.get("equity"),
            "margin": snapshot_model.get("margin"),
            "freeMargin": snapshot_model.get("freeMargin"),
            "marginLevel": snapshot_model.get("marginLevel"),
            "profit": snapshot_model.get("profit"),
            "leverage": _to_int(account_meta.get("leverage")),
        },
        "positions": snapshot_model.get("positions") or [],
        "orders": snapshot_model.get("orders") or [],
        "trades": history_model.get("trades") or [],
        "curvePoints": history_model.get("curvePoints") or [],
        "overviewMetrics": [dict(item) for item in (raw_full.get("overviewMetrics") or [])],
        "curveIndicators": [dict(item) for item in (raw_full.get("curveIndicators") or [])],
        "statsMetrics": [dict(item) for item in (raw_full.get("statsMetrics") or [])],
    }


def build_account_snapshot_response(
    snapshot_model: Mapping[str, Any],
    account_meta: Optional[Mapping[str, Any]] = None,
    sync_seq: Optional[int] = None,
    *,
    is_delta: bool = False,
    unchanged: bool = False,
) -> Dict[str, Any]:
    """为 /v2/account/snapshot 构造响应体，保持纯函数。"""
    meta: MutableMapping[str, Any] = dict(account_meta or {})
    if sync_seq is not None:
        meta["syncSeq"] = sync_seq

    response: Dict[str, Any] = {"accountMeta": dict(meta)}
    response.update({k: v for k, v in snapshot_model.items()})
    if is_delta:
        response["isDelta"] = True
    if unchanged:
        response["unchanged"] = True
    return response


def build_account_history_response(
    history_model: Mapping[str, Any],
    account_meta: Optional[Mapping[str, Any]] = None,
    sync_seq: Optional[int] = None,
    *,
    is_delta: bool = False,
    unchanged: bool = False,
) -> Dict[str, Any]:
    """为 /v2/account/history 构造响应体，保持纯函数。"""
    meta: MutableMapping[str, Any] = dict(account_meta or {})
    if sync_seq is not None:
        meta["syncSeq"] = sync_seq

    response: Dict[str, Any] = {"accountMeta": dict(meta)}
    response.update({k: v for k, v in history_model.items()})
    if is_delta:
        response["isDelta"] = True
    if unchanged:
        response["unchanged"] = True
    return response


def build_account_full_response(
    full_model: Mapping[str, Any],
    account_meta: Optional[Mapping[str, Any]] = None,
    sync_seq: Optional[int] = None,
    *,
    is_delta: bool = False,
    unchanged: bool = False,
) -> Dict[str, Any]:
    """为 /v2/account/full 构造响应体，保持单次强一致刷新结构稳定。"""
    meta: MutableMapping[str, Any] = dict(account_meta or {})
    if sync_seq is not None:
        meta["syncSeq"] = sync_seq

    response: Dict[str, Any] = {"accountMeta": dict(meta)}
    response.update({k: v for k, v in full_model.items()})
    if is_delta:
        response["isDelta"] = True
    if unchanged:
        response["unchanged"] = True
    return response
