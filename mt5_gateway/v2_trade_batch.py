"""v2 批量交易命令解析与执行聚合。"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional

import v2_trade
import v2_trade_models


def _normalize_strategy(value: Any) -> str:
    return str(value or v2_trade_models.STRATEGY_BEST_EFFORT).strip().upper()


def _normalize_item(item: Optional[Mapping[str, Any]], index: int) -> Dict[str, Any]:
    safe_item = dict(item or {})
    params = safe_item.get("params")
    if not isinstance(params, Mapping):
        params = {}
    return {
        "itemId": str(safe_item.get("itemId") or "").strip(),
        "action": str(safe_item.get("action") or "").strip().upper(),
        "params": dict(params),
        "groupKey": str(safe_item.get("groupKey") or dict(params).get("groupKey") or "").strip(),
        "index": index,
    }


def normalize_batch_payload(payload: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    safe_payload = dict(payload or {})
    items = safe_payload.get("items")
    if not isinstance(items, list):
        items = []
    return {
        "batchId": str(safe_payload.get("batchId") or "").strip(),
        "strategy": _normalize_strategy(safe_payload.get("strategy")),
        "items": [_normalize_item(item, index) for index, item in enumerate(items)],
    }


def _build_failed_response(batch: Mapping[str, Any],
                           error: Dict[str, Any],
                           account_mode: str,
                           items: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    return v2_trade_models.build_trade_batch_submit_response(
        batch_id=str(batch.get("batchId") or ""),
        strategy=str(batch.get("strategy") or v2_trade_models.STRATEGY_BEST_EFFORT),
        account_mode=account_mode,
        status=v2_trade_models.STATUS_FAILED,
        error=error,
        items=items or [],
        server_time=0,
    )


def build_batch_not_found(batch_id: str) -> Dict[str, Any]:
    return v2_trade_models.build_trade_batch_submit_response(
        batch_id=batch_id,
        strategy=v2_trade_models.STRATEGY_BEST_EFFORT,
        account_mode="unknown",
        status=v2_trade_models.STATUS_FAILED,
        error=v2_trade_models.build_error(v2_trade_models.ERROR_BATCH_NOT_FOUND, "未找到对应 batchId"),
        items=[],
        server_time=0,
    )


def validate_batch_payload(batch: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    batch_id = str(batch.get("batchId") or "").strip()
    if not batch_id:
        return v2_trade_models.build_error(v2_trade_models.ERROR_BATCH_INVALID_ID, "batchId 不能为空")
    strategy = str(batch.get("strategy") or "").strip().upper()
    if strategy not in v2_trade_models.SUPPORTED_BATCH_STRATEGIES:
        return v2_trade_models.build_error(
            v2_trade_models.ERROR_BATCH_INVALID_STRATEGY,
            "strategy 不在支持范围内",
            {"supportedStrategies": sorted(v2_trade_models.SUPPORTED_BATCH_STRATEGIES)},
        )
    items = list(batch.get("items") or [])
    if not items:
        return v2_trade_models.build_error(v2_trade_models.ERROR_BATCH_EMPTY_ITEMS, "items 不能为空")
    for item in items:
        item_id = str(item.get("itemId") or "").strip()
        if not item_id:
            return v2_trade_models.build_error(v2_trade_models.ERROR_BATCH_ITEM_INVALID_ID, "itemId 不能为空")
        action = str(item.get("action") or "").strip().upper()
        if action not in v2_trade.SUPPORTED_ACTIONS:
            return v2_trade_models.build_error(
                v2_trade_models.ERROR_BATCH_UNSUPPORTED_ACTION,
                "批量动作不在支持范围内",
                {"itemId": item_id, "action": action},
            )
    return None


def _prepare_batch_item(item: Mapping[str, Any],
                        *,
                        account_mode: str,
                        mt5_module: Any,
                        position_lookup: Optional[Callable[[Mapping[str, Any], str], Optional[Dict[str, Any]]]],
                        symbol_info_lookup: Optional[Callable[[str], Any]]) -> Dict[str, Any]:
    payload = {
        "requestId": str(item.get("itemId") or ""),
        "action": str(item.get("action") or ""),
        "params": dict(item.get("params") or {}),
    }
    prepared = v2_trade.prepare_trade_request(
        payload,
        account_mode=account_mode,
        mt5_module=mt5_module,
        position_lookup=position_lookup,
        symbol_info_lookup=symbol_info_lookup,
    )
    return {
        "itemId": str(item.get("itemId") or ""),
        "action": str(item.get("action") or ""),
        "groupKey": str(item.get("groupKey") or ""),
        "prepared": prepared,
    }


def _execute_single_item(prepared_item: Mapping[str, Any],
                         *,
                         check_request: Callable[[Dict[str, Any]], Any],
                         send_request: Callable[[Dict[str, Any]], Any]) -> Dict[str, Any]:
    item_id = str(prepared_item.get("itemId") or "")
    action = str(prepared_item.get("action") or "")
    group_key = str(prepared_item.get("groupKey") or "")
    prepared = prepared_item.get("prepared") if isinstance(prepared_item.get("prepared"), dict) else {}
    prepare_error = prepared.get("error")
    if prepare_error is not None:
        return v2_trade_models.build_trade_batch_item_response(
            item_id=item_id,
            action=action,
            status=v2_trade_models.STATUS_REJECTED,
            error=prepare_error,
            check=None,
            result=None,
            group_key=group_key,
        )
    request = prepared.get("request") if isinstance(prepared.get("request"), dict) else {}
    check_result = v2_trade_models.mt5_result_to_dict(check_request(request))
    check_error = v2_trade_models.error_from_retcode(
        check_result.get("retcode", -1),
        check_result.get("comment", ""),
    )
    if check_error is not None:
        return v2_trade_models.build_trade_batch_item_response(
            item_id=item_id,
            action=action,
            status=v2_trade_models.STATUS_REJECTED,
            error=check_error,
            check=check_result,
            result=None,
            group_key=group_key,
        )
    send_result = v2_trade_models.mt5_result_to_dict(send_request(request))
    send_error = v2_trade_models.error_from_retcode(
        send_result.get("retcode", -1),
        send_result.get("comment", ""),
    )
    return v2_trade_models.build_trade_batch_item_response(
        item_id=item_id,
        action=action,
        status=v2_trade_models.STATUS_ACCEPTED if send_error is None else v2_trade_models.STATUS_REJECTED,
        error=send_error,
        check=check_result,
        result=send_result,
        group_key=group_key,
    )


def submit_trade_batch(payload: Optional[Mapping[str, Any]],
                       *,
                       account_mode: str,
                       mt5_module: Any,
                       position_lookup: Optional[Callable[[Mapping[str, Any], str], Optional[Dict[str, Any]]]] = None,
                       symbol_info_lookup: Optional[Callable[[str], Any]] = None,
                       check_request: Optional[Callable[[Dict[str, Any]], Any]] = None,
                       send_request: Optional[Callable[[Dict[str, Any]], Any]] = None,
                       server_time: int = 0) -> Dict[str, Any]:
    batch = normalize_batch_payload(payload)
    validation_error = validate_batch_payload(batch)
    if validation_error is not None:
        return v2_trade_models.build_trade_batch_submit_response(
            batch_id=batch["batchId"],
            strategy=batch["strategy"],
            account_mode=account_mode,
            status=v2_trade_models.STATUS_FAILED,
            error=validation_error,
            items=[],
            server_time=server_time,
        )

    prepared_items = [
        _prepare_batch_item(
            item,
            account_mode=account_mode,
            mt5_module=mt5_module,
            position_lookup=position_lookup,
            symbol_info_lookup=symbol_info_lookup,
        )
        for item in batch["items"]
    ]
    if batch["strategy"] == v2_trade_models.STRATEGY_ALL_OR_NONE:
        if any((prepared.get("prepared") or {}).get("error") is not None for prepared in prepared_items):
            rejected_items = [
                v2_trade_models.build_trade_batch_item_response(
                    item_id=str(item.get("itemId") or ""),
                    action=str(item.get("action") or ""),
                    status=v2_trade_models.STATUS_REJECTED,
                    error=((prepared.get("prepared") or {}).get("error") if isinstance(prepared.get("prepared"), dict) else None),
                    check=None,
                    result=None,
                    group_key=str(item.get("groupKey") or ""),
                )
                for item, prepared in zip(batch["items"], prepared_items)
            ]
            return v2_trade_models.build_trade_batch_submit_response(
                batch_id=batch["batchId"],
                strategy=batch["strategy"],
                account_mode=account_mode,
                status=v2_trade_models.STATUS_FAILED,
                error=v2_trade_models.build_error(v2_trade_models.ERROR_EXECUTION_FAILED, "ALL_OR_NONE 预校验失败"),
                items=rejected_items,
                server_time=server_time,
            )

    safe_check_request = check_request or (lambda _request: None)
    safe_send_request = send_request or (lambda _request: None)
    items = [
        _execute_single_item(
            prepared,
            check_request=safe_check_request,
            send_request=safe_send_request,
        )
        for prepared in prepared_items
    ]
    accepted_count = sum(1 for item in items if str(item.get("status") or "") == v2_trade_models.STATUS_ACCEPTED)
    rejected_count = len(items) - accepted_count
    if accepted_count > 0 and rejected_count == 0:
        status = v2_trade_models.STATUS_ACCEPTED
        error = None
    elif accepted_count > 0:
        status = v2_trade_models.STATUS_PARTIAL
        error = v2_trade_models.build_error(v2_trade_models.ERROR_EXECUTION_FAILED, "部分成功，部分失败")
    else:
        status = v2_trade_models.STATUS_FAILED
        error = v2_trade_models.build_error(v2_trade_models.ERROR_EXECUTION_FAILED, "批量执行失败")
    return v2_trade_models.build_trade_batch_submit_response(
        batch_id=batch["batchId"],
        strategy=batch["strategy"],
        account_mode=account_mode,
        status=status,
        error=error,
        items=items,
        server_time=server_time,
    )
