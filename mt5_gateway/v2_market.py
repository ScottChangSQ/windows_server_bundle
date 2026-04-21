"""Binance 市场数据流水线，供 server_v2 构建 v2 market 响应时复用。"""

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Tuple, Union

MarketCandleRow = Union[Sequence[Any], Mapping[str, Any]]
MarketCandle = Dict[str, Any]


def _symbol_descriptor(symbol: str) -> Dict[str, str]:
    normalized = str(symbol or "").strip().upper()
    if normalized in {"BTCUSDT", "BTCUSD", "BTC"}:
        return {
            "productId": "BTC",
            "marketSymbol": "BTCUSDT",
            "tradeSymbol": "BTCUSD",
        }
    if normalized in {"XAUUSDT", "XAUUSD", "XAU"}:
        return {
            "productId": "XAU",
            "marketSymbol": "XAUUSDT",
            "tradeSymbol": "XAUUSD",
        }
    return {
        "productId": normalized,
        "marketSymbol": normalized,
        "tradeSymbol": normalized,
    }


def build_market_candle_payload(symbol: str,
                               interval: str,
                               row: MarketCandleRow,
                               *,
                               is_closed: bool,
                               source: str,
                               version: Optional[int] = None) -> MarketCandle:
    """把一条 Binance kline 行统一转换为标准的 v2 candle 结构。"""
    parsed = _parse_market_row(row)
    descriptor = _symbol_descriptor(str(symbol or parsed.get("symbol") or ""))
    payload_symbol = descriptor["marketSymbol"]
    payload_interval = str(interval or parsed.get("interval") or "")
    payload_version = int(version if version is not None else parsed.get("openTime", 0))
    return {
        "productId": descriptor["productId"],
        "marketSymbol": descriptor["marketSymbol"],
        "tradeSymbol": descriptor["tradeSymbol"],
        "symbol": payload_symbol,
        "interval": payload_interval,
        "openTime": parsed["openTime"],
        "closeTime": parsed["closeTime"],
        "open": parsed["open"],
        "high": parsed["high"],
        "low": parsed["low"],
        "close": parsed["close"],
        "volume": parsed["volume"],
        "quoteVolume": parsed["quoteVolume"],
        "tradeCount": parsed["tradeCount"],
        "source": str(source or ""),
        "isClosed": bool(is_closed),
        "version": payload_version,
    }


def build_market_candle_payload_from_ws_event(event: Mapping[str, Any]) -> Optional[MarketCandle]:
    """把 Binance WS kline 事件转换成标准 candle payload。"""
    safe_event = dict(event or {})
    kline = dict(safe_event.get("k") or {})
    symbol = str(kline.get("s") or safe_event.get("s") or "").strip()
    interval = str(kline.get("i") or safe_event.get("i") or "").strip()
    if not symbol or not interval:
        return None
    return build_market_candle_payload(
        symbol=symbol,
        interval=interval,
        row={"k": kline},
        is_closed=bool(kline.get("x")),
        source="binance-ws",
    )


def split_market_rows(rows: List[MarketCandle]) -> Tuple[List[MarketCandle], Optional[MarketCandle]]:
    """把混合的 candle 列表拆成闭合 candles 与最新 patch。"""
    closed: List[MarketCandle] = []
    latest_patch: Optional[MarketCandle] = None
    for item in rows:
        if item.get("isClosed"):
            closed.append(item)
        else:
            latest_patch = item
    closed_sorted = sorted(closed, key=lambda cand: cand.get("openTime", 0))
    return closed_sorted, latest_patch


def separate_closed_rest_rows(rest_rows: Sequence[MarketCandleRow],
                              server_time_ms: int) -> Tuple[List[MarketCandleRow], Optional[MarketCandleRow]]:
    """把 REST 返回里的最后一根未闭合 K 线拆成 latestPatch。"""
    rows = list(rest_rows or [])
    if not rows:
        return [], None
    last_row = rows[-1]
    parsed = _parse_market_row(last_row)
    if int(parsed.get("closeTime", 0) or 0) >= int(server_time_ms or 0):
        return rows[:-1], last_row
    return rows, None


def build_market_candles_response(symbol: str,
                                  interval: str,
                                  server_time: int,
                                  rest_rows: Sequence[MarketCandleRow],
                                  latest_patch: Optional[MarketCandleRow] = None,
                                  *,
                                  rest_source: str = "binance-rest",
                                  patch_source: str = "binance-ws") -> Dict[str, Any]:
    """把 REST 和当前 patch 拼成符合 v2 market candles 的返回体。"""
    assembled: List[MarketCandle] = []
    for entry in rest_rows:
        assembled.append(
            build_market_candle_payload(
                symbol,
                interval,
                row=entry,
                is_closed=True,
                source=rest_source,
            )
        )
    if latest_patch is not None:
        patch_is_closed = _extract_bool(latest_patch, ("x", "isClosed"))
        assembled.append(
            build_market_candle_payload(
                symbol,
                interval,
                row=latest_patch,
                is_closed=patch_is_closed,
                source=patch_source,
            )
        )
    closed_candles, patch_candle = split_market_rows(assembled)
    return {
        "symbol": symbol,
        "interval": interval,
        "serverTime": int(server_time),
        "candles": closed_candles,
        "latestPatch": patch_candle,
        "nextSyncToken": build_market_candles_sync_token(
            server_time,
            symbol,
            interval,
            len(closed_candles),
        ),
    }


def build_market_candles_sync_token(server_time_ms: int,
                                    symbol: str,
                                    interval: str,
                                    candle_count: int) -> str:
    """根据 server_time 与 symbol/interval 生成与 server_v2 对齐的 sync token。"""
    revision = f"market:{symbol or ''}:{interval or ''}:{candle_count}"
    payload = f"{int(server_time_ms)}:{revision}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def _parse_market_row(row: MarketCandleRow) -> Dict[str, Any]:
    """兼容序列与映射，解析出 open/close 等标准字段。"""
    if _is_mapping(row):
        return _parse_mapping_row(row)
    if _is_sequence(row):
        return _parse_sequence_row(row)
    raise TypeError("无法识别的 kline 行结构")


def _parse_mapping_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    data = dict(row.get("k") or row)
    return {
        "symbol": _extract_str(data, ("s", "symbol")),
        "interval": _extract_str(data, ("i", "interval")),
        "openTime": _extract_int(data, ("t", "openTime", "startTime", "time")),
        "closeTime": _extract_int(data, ("T", "closeTime")),
        "open": _extract_float(data, ("o", "open")),
        "high": _extract_float(data, ("h", "high")),
        "low": _extract_float(data, ("l", "low")),
        "close": _extract_float(data, ("c", "close")),
        "volume": _extract_float(data, ("v", "volume")),
        "quoteVolume": _extract_float(data, ("q", "quoteVolume")),
        "tradeCount": _extract_int(data, ("n", "tradeCount")),
    }


def _parse_sequence_row(row: Sequence[Any]) -> Dict[str, Any]:
    normalized = list(row)
    if len(normalized) < 9:
        raise ValueError("REST kline 数据行长度不足")
    return {
        "symbol": "",
        "interval": "",
        "openTime": _to_int(normalized[0]),
        "open": _to_float(normalized[1]),
        "high": _to_float(normalized[2]),
        "low": _to_float(normalized[3]),
        "close": _to_float(normalized[4]),
        "volume": _to_float(normalized[5]),
        "closeTime": _to_int(normalized[6]),
        "quoteVolume": _to_float(normalized[7]),
        "tradeCount": _to_int(normalized[8]),
    }


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _is_mapping(value: Any) -> bool:
    return isinstance(value, Mapping)


def _extract_bool(row: MarketCandleRow, keys: Tuple[str, ...], default: bool = False) -> bool:
    if _is_mapping(row):
        for key in keys:
            value = row.get(key)
            if value is not None:
                return bool(value)
    return default


def _extract_str(data: Mapping[str, Any], keys: Tuple[str, ...]) -> str:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _extract_int(data: Mapping[str, Any], keys: Tuple[str, ...]) -> int:
    for key in keys:
        value = data.get(key)
        if value is not None:
            return _to_int(value)
    return 0


def _extract_float(data: Mapping[str, Any], keys: Tuple[str, ...]) -> float:
    for key in keys:
        value = data.get(key)
        if value is not None:
            return _to_float(value)
    return 0.0


def _to_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
