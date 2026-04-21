"""Binance 市场运行时真值，统一收口 WS 1m K 线并提供服务端热路径读取。"""

from threading import RLock
from typing import Any, Dict, List, Optional, Sequence

import v2_market

MINUTE_HISTORY_LIMIT = 2048


def create_market_stream_runtime(symbols: Sequence[str]) -> Dict[str, Any]:
    """创建市场 WS 运行时；每个产品只保留一份最近闭合 candle 和当前 patch。"""
    runtime_symbols: Dict[str, Dict[str, Any]] = {}
    for symbol in symbols or []:
        descriptor = v2_market._symbol_descriptor(str(symbol or ""))
        runtime_symbols[descriptor["marketSymbol"]] = {
            "productId": descriptor["productId"],
            "marketSymbol": descriptor["marketSymbol"],
            "tradeSymbol": descriptor["tradeSymbol"],
            "latestClosedCandle": None,
            "latestPatch": None,
            "recentClosedMinutes": [],
            "updatedAt": 0,
            "lastEventTime": 0,
        }
    return {
        "lock": RLock(),
        "mode": "binance-ws",
        "connected": False,
        "connecting": False,
        "updatedAt": 0,
        "lastError": "",
        "symbols": runtime_symbols,
    }


def mark_connection_state(runtime: Dict[str, Any],
                          *,
                          connecting: bool,
                          connected: bool,
                          updated_at_ms: int,
                          last_error: str = "") -> None:
    """更新运行时连接状态，供 source/health 接口直接暴露。"""
    with runtime["lock"]:
        runtime["connecting"] = bool(connecting)
        runtime["connected"] = bool(connected)
        runtime["updatedAt"] = max(0, int(updated_at_ms or 0))
        runtime["lastError"] = str(last_error or "")


def bootstrap_symbol_from_rest_rows(runtime: Dict[str, Any],
                                    symbol: str,
                                    rows: Sequence[Any],
                                    server_time_ms: int) -> None:
    """用冷启动 REST 结果初始化某个产品的最新闭合 candle 和 patch。"""
    descriptor = v2_market._symbol_descriptor(symbol)
    closed_rows, patch_row = v2_market.separate_closed_rest_rows(rows or [], server_time_ms)
    latest_closed = None
    latest_patch = None
    latest_closed_minutes: List[Dict[str, Any]] = []
    if closed_rows:
        for row in closed_rows:
            _remember_closed_minute(
                latest_closed_minutes,
                v2_market.build_market_candle_payload(
                    descriptor["marketSymbol"],
                    "1m",
                    row=row,
                    is_closed=True,
                    source="binance-rest",
                ),
            )
        latest_closed = v2_market.build_market_candle_payload(
            descriptor["marketSymbol"],
            "1m",
            row=closed_rows[-1],
            is_closed=True,
            source="binance-rest",
        )
    if patch_row is not None:
        latest_patch = v2_market.build_market_candle_payload(
            descriptor["marketSymbol"],
            "1m",
            row=patch_row,
            is_closed=False,
            source="binance-rest",
        )
    _update_symbol_state(
        runtime,
        descriptor["marketSymbol"],
        latest_closed=latest_closed,
        latest_patch=latest_patch,
        recent_closed_minutes=latest_closed_minutes,
        updated_at_ms=max(0, int(server_time_ms or 0)),
    )


def apply_ws_kline_event(runtime: Dict[str, Any], payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """消费一条 Binance WS 1m K 线消息，并推进当前产品的最新状态。"""
    event = dict((payload or {}).get("data") or payload or {})
    kline = dict(event.get("k") or {})
    symbol = str(kline.get("s") or event.get("s") or "").strip().upper()
    if not symbol:
        return None
    descriptor = v2_market._symbol_descriptor(symbol)
    candle = v2_market.build_market_candle_payload_from_ws_event(event)
    if candle is None:
        return None
    event_time_ms = max(
        int(event.get("E", 0) or 0),
        int(candle.get("closeTime", 0) or 0),
    )
    if candle.get("isClosed"):
        _update_symbol_state(
            runtime,
            descriptor["marketSymbol"],
            latest_closed=candle,
            latest_patch=None,
            recent_closed_minutes=[candle],
            updated_at_ms=event_time_ms,
            last_event_time_ms=event_time_ms,
        )
    else:
        _update_symbol_state(
            runtime,
            descriptor["marketSymbol"],
            latest_closed=None,
            latest_patch=candle,
            updated_at_ms=event_time_ms,
            last_event_time_ms=event_time_ms,
        )
    return candle


def build_symbol_state(runtime: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """构建某个产品的当前市场真值快照，供 stream 和 snapshot 热路径复用。"""
    descriptor = v2_market._symbol_descriptor(symbol)
    with runtime["lock"]:
        bucket = runtime["symbols"].setdefault(
            descriptor["marketSymbol"],
            {
                "productId": descriptor["productId"],
                "marketSymbol": descriptor["marketSymbol"],
                "tradeSymbol": descriptor["tradeSymbol"],
                "latestClosedCandle": None,
                "latestPatch": None,
                "recentClosedMinutes": [],
                "updatedAt": 0,
                "lastEventTime": 0,
            },
        )
        latest_closed = _clone_candle(bucket.get("latestClosedCandle"))
        latest_patch = _clone_candle(bucket.get("latestPatch"))
        updated_at_ms = max(0, int(bucket.get("updatedAt", 0) or 0))
    latest_price = 0.0
    latest_open_time = 0
    latest_close_time = 0
    if latest_patch is not None:
        latest_price = float(latest_patch.get("close", 0.0) or 0.0)
        latest_open_time = int(latest_patch.get("openTime", 0) or 0)
        latest_close_time = int(latest_patch.get("closeTime", 0) or 0)
    elif latest_closed is not None:
        latest_price = float(latest_closed.get("close", 0.0) or 0.0)
        latest_open_time = int(latest_closed.get("openTime", 0) or 0)
        latest_close_time = int(latest_closed.get("closeTime", 0) or 0)
    return {
        "productId": descriptor["productId"],
        "marketSymbol": descriptor["marketSymbol"],
        "tradeSymbol": descriptor["tradeSymbol"],
        "interval": "1m",
        "latestPrice": latest_price,
        "latestOpenTime": latest_open_time,
        "latestCloseTime": latest_close_time,
        "latestClosedCandle": latest_closed,
        "latestPatch": latest_patch,
        "updatedAt": updated_at_ms,
    }


def build_interval_patch(runtime: Dict[str, Any], symbol: str, interval: str) -> Optional[Dict[str, Any]]:
    """用近期 1m 真值聚合当前长周期未闭合 patch。"""
    safe_interval = str(interval or "").strip()
    interval_ms = _resolve_fixed_interval_ms(safe_interval)
    if interval_ms <= 60_000:
        return None
    descriptor = v2_market._symbol_descriptor(symbol)
    with runtime["lock"]:
        bucket = runtime["symbols"].get(descriptor["marketSymbol"]) or {}
        latest_patch = _clone_candle(bucket.get("latestPatch"))
        recent_closed_minutes = [_clone_candle(item) for item in (bucket.get("recentClosedMinutes") or []) if item]
    if latest_patch is None:
        return None
    bucket_start = (int(latest_patch.get("openTime", 0) or 0) // interval_ms) * interval_ms
    bucket_close = bucket_start + interval_ms - 1
    inputs: List[Dict[str, Any]] = []
    for candle in recent_closed_minutes:
        open_time = int(candle.get("openTime", 0) or 0)
        if open_time < bucket_start or open_time >= int(latest_patch.get("openTime", 0) or 0):
            continue
        if (open_time // interval_ms) * interval_ms != bucket_start:
            continue
        inputs.append(candle)
    inputs.append(latest_patch)
    if not inputs:
        return None
    inputs.sort(key=lambda item: int(item.get("openTime", 0) or 0))
    first = inputs[0]
    last = inputs[-1]
    return {
        "productId": descriptor["productId"],
        "marketSymbol": descriptor["marketSymbol"],
        "tradeSymbol": descriptor["tradeSymbol"],
        "symbol": descriptor["marketSymbol"],
        "interval": safe_interval,
        "openTime": bucket_start,
        "closeTime": bucket_close,
        "open": float(first.get("open", 0.0) or 0.0),
        "high": max(float(item.get("high", 0.0) or 0.0) for item in inputs),
        "low": min(float(item.get("low", 0.0) or 0.0) for item in inputs),
        "close": float(last.get("close", 0.0) or 0.0),
        "volume": sum(float(item.get("volume", 0.0) or 0.0) for item in inputs),
        "quoteVolume": sum(float(item.get("quoteVolume", 0.0) or 0.0) for item in inputs),
        "tradeCount": sum(int(item.get("tradeCount", 0) or 0) for item in inputs),
        "source": "binance-ws",
        "isClosed": False,
        "version": bucket_start,
    }


def get_latest_patch_row(runtime: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
    """返回某个产品当前未闭合 patch 的原始映射结构，供 market/candles 直接复用。"""
    snapshot = build_symbol_state(runtime, symbol)
    latest_patch = snapshot.get("latestPatch")
    if latest_patch is None:
        return None
    return {
        "k": {
            "t": latest_patch.get("openTime"),
            "T": latest_patch.get("closeTime"),
            "s": latest_patch.get("symbol"),
            "i": latest_patch.get("interval"),
            "o": str(latest_patch.get("open", 0.0) or 0.0),
            "h": str(latest_patch.get("high", 0.0) or 0.0),
            "l": str(latest_patch.get("low", 0.0) or 0.0),
            "c": str(latest_patch.get("close", 0.0) or 0.0),
            "v": str(latest_patch.get("volume", 0.0) or 0.0),
            "q": str(latest_patch.get("quoteVolume", 0.0) or 0.0),
            "n": int(latest_patch.get("tradeCount", 0) or 0),
            "x": False,
        }
    }


def build_source_status(runtime: Dict[str, Any]) -> Dict[str, Any]:
    """输出可直接拼进 `/v1/source` 的市场运行时状态。"""
    with runtime["lock"]:
        return {
            "marketRuntimeMode": str(runtime.get("mode") or "binance-ws"),
            "marketRuntimeConnected": bool(runtime.get("connected")),
            "marketRuntimeConnecting": bool(runtime.get("connecting")),
            "marketRuntimeUpdatedAt": max(0, int(runtime.get("updatedAt", 0) or 0)),
            "marketRuntimeLastError": str(runtime.get("lastError") or ""),
        }


def _update_symbol_state(runtime: Dict[str, Any],
                         market_symbol: str,
                         *,
                         latest_closed: Optional[Dict[str, Any]],
                         latest_patch: Optional[Dict[str, Any]],
                         recent_closed_minutes: Optional[List[Dict[str, Any]]] = None,
                         updated_at_ms: int,
                         last_event_time_ms: int = 0) -> None:
    """原地更新某个产品的状态桶。"""
    with runtime["lock"]:
        bucket = runtime["symbols"].setdefault(
            market_symbol,
            {
                "productId": v2_market._symbol_descriptor(market_symbol)["productId"],
                "marketSymbol": market_symbol,
                "tradeSymbol": v2_market._symbol_descriptor(market_symbol)["tradeSymbol"],
                "latestClosedCandle": None,
                "latestPatch": None,
                "recentClosedMinutes": [],
                "updatedAt": 0,
                "lastEventTime": 0,
            },
        )
        if latest_closed is not None:
            bucket["latestClosedCandle"] = _clone_candle(latest_closed)
            _remember_closed_minute(bucket.setdefault("recentClosedMinutes", []), latest_closed)
            existing_patch = bucket.get("latestPatch")
            if existing_patch and int(existing_patch.get("openTime", 0) or 0) <= int(latest_closed.get("openTime", 0) or 0):
                bucket["latestPatch"] = None
        if latest_patch is not None:
            bucket["latestPatch"] = _clone_candle(latest_patch)
        elif latest_closed is not None:
            bucket["latestPatch"] = None
        if recent_closed_minutes:
            history = bucket.setdefault("recentClosedMinutes", [])
            for candle in recent_closed_minutes:
                _remember_closed_minute(history, candle)
        bucket["updatedAt"] = max(
            int(bucket.get("updatedAt", 0) or 0),
            max(0, int(updated_at_ms or 0)),
        )
        bucket["lastEventTime"] = max(
            int(bucket.get("lastEventTime", 0) or 0),
            max(0, int(last_event_time_ms or 0)),
        )
        runtime["updatedAt"] = max(
            int(runtime.get("updatedAt", 0) or 0),
            bucket["updatedAt"],
        )


def _clone_candle(candle: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if candle is None:
        return None
    return dict(candle)


def _remember_closed_minute(history: List[Dict[str, Any]], candle: Dict[str, Any]) -> None:
    normalized = _clone_candle(candle)
    if normalized is None:
        return
    open_time = int(normalized.get("openTime", 0) or 0)
    for index, current in enumerate(list(history)):
        current_open_time = int(current.get("openTime", 0) or 0)
        if current_open_time == open_time:
            history[index] = normalized
            break
    else:
        history.append(normalized)
    history.sort(key=lambda item: int(item.get("openTime", 0) or 0))
    if len(history) > MINUTE_HISTORY_LIMIT:
        del history[:-MINUTE_HISTORY_LIMIT]


def _resolve_fixed_interval_ms(interval: str) -> int:
    if interval == "1m":
        return 60_000
    if interval == "5m":
        return 5 * 60_000
    if interval == "15m":
        return 15 * 60_000
    if interval == "30m":
        return 30 * 60_000
    if interval == "1h":
        return 60 * 60_000
    if interval == "4h":
        return 4 * 60 * 60_000
    if interval == "1d":
        return 24 * 60 * 60_000
    return 0
