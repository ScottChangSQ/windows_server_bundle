# MT5 网关主入口，统一承载行情、交易与会话接口。
import base64
import hashlib
import json
import math
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone
from statistics import mean, pstdev
from threading import Condition, Event, Lock, RLock, Thread
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.gzip import GZipMiddleware
import websockets
import v2_account
import v2_market
import v2_market_runtime
import v2_session_crypto
import v2_session_diagnostic
import v2_session_manager
import v2_mt5_account_switch
import v2_session_store
import v2_trade
import v2_trade_audit
import v2_trade_batch
import v2_trade_models

try:
    import MetaTrader5 as mt5
except Exception:  # pragma: no cover
    mt5 = None

load_dotenv()


def _configure_windows_event_loop_policy() -> None:
    if os.name != "nt":
        return
    selector_policy = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if selector_policy is None:
        return
    try:
        current_policy = asyncio.get_event_loop_policy()
        if isinstance(current_policy, selector_policy):
            return
    except Exception:
        pass
    try:
        asyncio.set_event_loop_policy(selector_policy())
    except Exception:
        pass


def _read_bounded_env_int(name: str, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    """读取整数环境变量，并按契约边界收口到允许范围内。"""
    try:
        value = int(os.getenv(name, str(default)))
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return int(value)


LOGIN = os.getenv("MT5_LOGIN", "").strip()
PASSWORD = os.getenv("MT5_PASSWORD", "").strip()
SERVER = os.getenv("MT5_SERVER", "").strip()
PATH = os.getenv("MT5_PATH", "").strip() or None
SERVER_ALIASES_RAW = os.getenv("MT5_SERVER_ALIASES", "").strip()
# Android 会话客户端当前读超时为 135s；GUI 切号链总预算必须留出响应回传余量，避免手机先超时。
SESSION_CLIENT_READ_TIMEOUT_MS = 135000
MT5_GUI_SWITCH_TOTAL_TIMEOUT_MS = max(1000, SESSION_CLIENT_READ_TIMEOUT_MS - 15000)
MT5_INIT_TIMEOUT_MS = _read_bounded_env_int(
    "MT5_INIT_TIMEOUT_MS",
    default=50000,
    minimum=1000,
    # 部署脚本与 .env.example 允许更长的初始化/登录超时（例如 90s）。
    # 这里不应把用户显式配置静默截断到 50s，否则会造成“部署口径和服务端真值不一致”。
    maximum=120000,
)
try:
    MT5_TIME_OFFSET_MINUTES = int(os.getenv("MT5_TIME_OFFSET_MINUTES", "0"))
except Exception:
    MT5_TIME_OFFSET_MINUTES = 0
MT5_SERVER_TIMEZONE = os.getenv("MT5_SERVER_TIMEZONE", "").strip()
HOST = os.getenv("GATEWAY_HOST", "0.0.0.0")
PORT = int(os.getenv("GATEWAY_PORT", "8787"))
GATEWAY_MODE = os.getenv("GATEWAY_MODE", "auto").strip().lower()  # auto | pull | ea
EA_SNAPSHOT_TTL_SEC = int(os.getenv("EA_SNAPSHOT_TTL_SEC", "35"))
EA_INGEST_TOKEN = os.getenv("EA_INGEST_TOKEN", "").strip()
SNAPSHOT_BUILD_CACHE_MS = int(os.getenv("SNAPSHOT_BUILD_CACHE_MS", "1000"))
SNAPSHOT_BUILD_MAX_STALE_MS = max(
    SNAPSHOT_BUILD_CACHE_MS,
    int(os.getenv("SNAPSHOT_BUILD_MAX_STALE_MS", "30000"))
)
SNAPSHOT_BUILD_CACHE_MAX_ENTRIES = max(1, int(os.getenv("SNAPSHOT_BUILD_CACHE_MAX_ENTRIES", "6")))
SNAPSHOT_DELTA_ENABLED = os.getenv("SNAPSHOT_DELTA_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
SNAPSHOT_DELTA_FALLBACK_RATIO = float(os.getenv("SNAPSHOT_DELTA_FALLBACK_RATIO", "0.85"))
SNAPSHOT_SYNC_CACHE_MAX_ENTRIES = max(1, int(os.getenv("SNAPSHOT_SYNC_CACHE_MAX_ENTRIES", "12")))
SNAPSHOT_RANGE_ALL_DAYS = max(30, int(os.getenv("SNAPSHOT_RANGE_ALL_DAYS", "36500")))
MT5_HISTORY_LOOKAHEAD_HOURS = max(0, int(os.getenv("MT5_HISTORY_LOOKAHEAD_HOURS", "24")))
TRADE_HISTORY_TARGET_ITEMS = max(200, int(os.getenv("TRADE_HISTORY_TARGET_ITEMS", "1000")))
TRADE_REQUEST_STORE_MAX_ENTRIES = max(100, int(os.getenv("TRADE_REQUEST_STORE_MAX_ENTRIES", "2000")))
BINANCE_REST_UPSTREAM = (os.getenv("BINANCE_REST_UPSTREAM", "https://fapi.binance.com").strip().rstrip("/"))
BINANCE_WS_UPSTREAM = (os.getenv("BINANCE_WS_UPSTREAM", "wss://fstream.binance.com").strip().rstrip("/"))
HEALTH_CACHE_MS = max(1000, int(os.getenv("HEALTH_CACHE_MS", "5000")))
# 行情 stream 默认每秒推送一次；缓存寿命必须略长于推送节奏，否则每轮都会重新打上游。
MARKET_CANDLES_CACHE_MS = max(1500, int(os.getenv("MARKET_CANDLES_CACHE_MS", "2000")))
MARKET_CANDLES_CACHE_MAX_ENTRIES = max(8, int(os.getenv("MARKET_CANDLES_CACHE_MAX_ENTRIES", "120")))
MARKET_CANDLES_UPSTREAM_CHUNK_LIMIT = max(100, min(1000, int(os.getenv("MARKET_CANDLES_UPSTREAM_CHUNK_LIMIT", "500"))))
MARKET_CANDLES_UPSTREAM_RETRY = max(0, int(os.getenv("MARKET_CANDLES_UPSTREAM_RETRY", "1")))
V2_STREAM_PUSH_INTERVAL_MS = _read_bounded_env_int(
    "V2_STREAM_PUSH_INTERVAL_MS",
    default=1000,
    minimum=200,
    maximum=10000,
)
ABNORMAL_RECORD_LIMIT = max(50, int(os.getenv("ABNORMAL_RECORD_LIMIT", "5000")))
ABNORMAL_ALERT_LIMIT = max(20, int(os.getenv("ABNORMAL_ALERT_LIMIT", "120")))
ABNORMAL_KLINE_LIMIT = max(2, int(os.getenv("ABNORMAL_KLINE_LIMIT", "60")))
ABNORMAL_FETCH_CACHE_MS = max(1000, int(os.getenv("ABNORMAL_FETCH_CACHE_MS", "4000")))
ABNORMAL_DELTA_ENABLED = os.getenv("ABNORMAL_DELTA_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
MARKET_SYMBOL_BTC = "BTCUSDT"
MARKET_SYMBOL_XAU = "XAUUSDT"
ABNORMAL_SYMBOLS = (MARKET_SYMBOL_BTC, MARKET_SYMBOL_XAU)
SESSION_DATA_DIR = os.getenv("MT5_SESSION_DATA_DIR", "").strip()
if not SESSION_DATA_DIR:
    SESSION_DATA_DIR = str(Path(__file__).resolve().parent / "data" / "session")

app = FastAPI(title="MT5 Bridge Gateway", version="1.1.0")
app.add_middleware(GZipMiddleware, minimum_size=512)
state_lock = RLock()
snapshot_cache_lock = Lock()
abnormal_state_lock = Lock()
ea_snapshot_cache: Optional[Dict] = None
ea_snapshot_received_at_ms = 0
ea_snapshot_change_digest = ""
mt5_last_connected_path = ""
snapshot_build_cache: Dict[str, Dict[str, Any]] = {}
snapshot_sync_cache: Dict[str, Dict[str, Any]] = {}
# 账户运行态真值缓存：只承载 MT5 拉取快照及其投影缓存。
account_runtime_cache: Dict[str, Any] = {
    "snapshotBuildCache": snapshot_build_cache,
    "snapshotSyncCache": snapshot_sync_cache,
}
# 账户发布态：统一承载“上次发布给客户端的运行态”和 bus 序号，避免再拆成两套并行状态。
account_publish_state: Dict[str, Any] = {
    "runtimeSeq": 0,
    "runtimeToken": "",
    "runtimeDigest": "",
    "runtimeSnapshot": None,
    "previousRuntimeSeq": 0,
    "previousRuntimeToken": "",
    "previousRuntimeSnapshot": None,
    "busSeq": 0,
    "publishedAt": 0,
    "revisions": {
        "marketRevision": "",
        "accountRuntimeRevision": "",
        "accountHistoryRevision": "",
        "abnormalRevision": "",
    },
    "event": None,
    "abnormalSnapshot": None,
    "pendingEventType": "",
    "pendingReasons": [],
}
v2_sync_state: Dict[str, Any] = {}
v2_bus_producer_lock = Lock()
v2_bus_producer_started = False
v2_bus_producer_thread: Optional[Thread] = None
v2_bus_stop_event = Event()
v2_bus_wake_event = Event()
market_candles_cache: Dict[str, Dict[str, Any]] = {}
health_status_cache: Dict[str, Any] = {}
abnormal_config_state: Dict[str, Any] = {"logicAnd": False, "symbols": {}}
abnormal_record_store: List[Dict[str, Any]] = []
abnormal_alert_store: List[Dict[str, Any]] = []
abnormal_last_close_time_by_symbol: Dict[str, int] = {}
abnormal_last_notify_at: Dict[str, int] = {}
abnormal_kline_cache: Dict[str, Dict[str, Any]] = {}
abnormal_sync_state: Dict[str, Any] = {}
trade_request_lock = Lock()
trade_request_store: Dict[str, Dict[str, Any]] = {}
batch_request_store: Dict[str, Dict[str, Any]] = {}
trade_audit_store = v2_trade_audit.TradeAuditStore(max_entries=TRADE_REQUEST_STORE_MAX_ENTRIES)
session_diagnostic_store = v2_session_diagnostic.SessionDiagnosticStore(max_entries=TRADE_REQUEST_STORE_MAX_ENTRIES)
market_stream_runtime = v2_market_runtime.create_market_stream_runtime(ABNORMAL_SYMBOLS)
market_stream_runtime_control_lock = Lock()
market_stream_runtime_thread: Optional[Thread] = None
market_stream_runtime_stop_event = Event()
session_store = v2_session_store.FileSessionStore(session_root=Path(SESSION_DATA_DIR))
session_runtime_credentials: Dict[str, Any] = {
    "mode": "env_default",
    "login": 0,
    "password": "",
    "server": "",
}
light_snapshot_build_condition = Condition(snapshot_cache_lock)
light_snapshot_building = False
session_snapshot_epoch = 0

if hasattr(app, "on_event"):
    @app.on_event("startup")
    def _startup_v2_bus_runtime() -> None:
        _bootstrap_v2_bus_producer()
        _start_market_stream_runtime()


    @app.on_event("shutdown")
    def _shutdown_v2_bus_runtime() -> None:
        _stop_v2_bus_producer()
        _stop_market_stream_runtime()


def _load_bundle_runtime_info() -> Dict[str, str]:
    """读取部署包运行指纹；优先使用启动链显式传入的 manifest 路径。"""
    server_file = Path(__file__).resolve()
    explicit_manifest_path = os.getenv("MT5_BUNDLE_MANIFEST_PATH", "").strip()
    if explicit_manifest_path:
        manifest_path = Path(explicit_manifest_path).expanduser()
    else:
        manifest_path = server_file.parents[1] / "bundle_manifest.json"
    default_generated_at = datetime.fromtimestamp(server_file.stat().st_mtime, timezone.utc).isoformat()
    default_fingerprint = hashlib.sha256(server_file.read_bytes()).hexdigest()
    if not manifest_path.exists():
        return {
            "bundleFingerprint": default_fingerprint,
            "bundleGeneratedAt": default_generated_at,
            "bundleManifestPath": str(manifest_path),
            "bundleScriptPath": str(server_file),
        }
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "bundleFingerprint": default_fingerprint,
            "bundleGeneratedAt": default_generated_at,
            "bundleManifestPath": str(manifest_path),
            "bundleScriptPath": str(server_file),
        }
    return {
        "bundleFingerprint": str(payload.get("bundleFingerprint") or default_fingerprint),
        "bundleGeneratedAt": str(payload.get("generatedAt") or default_generated_at),
        "bundleManifestPath": str(manifest_path),
        "bundleScriptPath": str(server_file),
    }


BUNDLE_RUNTIME_INFO = _load_bundle_runtime_info()
BUNDLE_FINGERPRINT = str(BUNDLE_RUNTIME_INFO.get("bundleFingerprint") or "")
BUNDLE_GENERATED_AT = str(BUNDLE_RUNTIME_INFO.get("bundleGeneratedAt") or "")
BUNDLE_MANIFEST_PATH = str(BUNDLE_RUNTIME_INFO.get("bundleManifestPath") or "")
BUNDLE_SCRIPT_PATH = str(BUNDLE_RUNTIME_INFO.get("bundleScriptPath") or "")


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# 统一生成 v2 同步 token，供快照、增量和 WS 消息复用。
def _build_sync_token(server_time_ms: int, revision: str) -> str:
    payload = f"{int(server_time_ms)}:{revision}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def _clone_json_value(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False))


def _reset_account_publish_state_locked() -> None:
    """重置账户发布态，保证 bus 与运行态序号一起清零。"""
    account_publish_state.clear()
    account_publish_state.update({
        "runtimeSeq": 0,
        "runtimeToken": "",
        "runtimeDigest": "",
        "runtimeSnapshot": None,
        "previousRuntimeSeq": 0,
        "previousRuntimeToken": "",
        "previousRuntimeSnapshot": None,
        "busSeq": 0,
        "publishedAt": 0,
        "revisions": {
            "marketRevision": "",
            "accountRuntimeRevision": "",
            "accountHistoryRevision": "",
            "abnormalRevision": "",
        },
        "event": None,
        "abnormalSnapshot": None,
        "pendingEventType": "",
        "pendingReasons": [],
    })


def _read_account_publish_state() -> Dict[str, Any]:
    with snapshot_cache_lock:
        state = account_publish_state or {}
        event = state.get("event")
        return {
            "runtimeSeq": int(state.get("runtimeSeq", 0) or 0),
            "runtimeToken": str(state.get("runtimeToken", "")),
            "runtimeDigest": str(state.get("runtimeDigest", "")),
            "runtimeSnapshot": _clone_json_value(state.get("runtimeSnapshot")) if state.get("runtimeSnapshot") is not None else None,
            "previousRuntimeSeq": int(state.get("previousRuntimeSeq", 0) or 0),
            "previousRuntimeToken": str(state.get("previousRuntimeToken", "")),
            "previousRuntimeSnapshot": _clone_json_value(state.get("previousRuntimeSnapshot")) if state.get("previousRuntimeSnapshot") is not None else None,
            "busSeq": int(state.get("busSeq", 0) or 0),
            "publishedAt": int(state.get("publishedAt", 0) or 0),
            "revisions": _clone_json_value(state.get("revisions") or {}),
            "event": _clone_json_value(event) if event is not None else None,
            "abnormalSnapshot": _clone_json_value(state.get("abnormalSnapshot")) if state.get("abnormalSnapshot") is not None else None,
            "pendingEventType": str(state.get("pendingEventType", "") or ""),
            "pendingReasons": [str(item) for item in (state.get("pendingReasons") or [])],
        }


def _publish_v2_bus_event(event_type: str,
                          changes: Dict[str, Any],
                          revisions: Dict[str, str],
                          published_at: int,
                          runtime_snapshot: Optional[Dict[str, Any]] = None,
                          abnormal_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    with snapshot_cache_lock:
        bus_seq = int(account_publish_state.get("busSeq", 0) or 0) + 1
        event = {
            "type": str(event_type or "syncEvent"),
            "busSeq": bus_seq,
            "publishedAt": int(published_at or 0),
            "revisions": _clone_json_value(revisions or {}),
            "changes": _clone_json_value(changes or {}),
        }
        account_publish_state.update({
            "busSeq": bus_seq,
            "publishedAt": int(published_at or 0),
            "revisions": _clone_json_value(revisions or {}),
            "event": _clone_json_value(event),
            "runtimeSnapshot": _clone_json_value(runtime_snapshot) if runtime_snapshot is not None else account_publish_state.get("runtimeSnapshot"),
            "abnormalSnapshot": _clone_json_value(abnormal_snapshot) if abnormal_snapshot is not None else None,
            "pendingEventType": "",
            "pendingReasons": [],
        })
        return _clone_json_value(event)


def _request_v2_publish(reason: str, event_type: str = "syncEvent") -> None:
    """标记需要尽快发布，让登录/切账号/成交等事件不必被动等待下一拍。"""
    reason_text = str(reason or "").strip()
    event_type_text = str(event_type or "syncEvent").strip() or "syncEvent"
    with snapshot_cache_lock:
        pending_reasons = [str(item) for item in (account_publish_state.get("pendingReasons") or [])]
        if reason_text and reason_text not in pending_reasons:
            pending_reasons.append(reason_text)
        account_publish_state["pendingReasons"] = pending_reasons
        current_event_type = str(account_publish_state.get("pendingEventType", "") or "")
        if event_type_text == "syncBootstrap" or not current_event_type:
            account_publish_state["pendingEventType"] = event_type_text
    v2_bus_wake_event.set()


def _read_abnormal_sync_state_for_bus() -> Dict[str, Any]:
    with abnormal_state_lock:
        _ensure_abnormal_defaults_locked()
        if not abnormal_sync_state:
            _commit_abnormal_snapshot_locked()
        return {
            "seq": int(abnormal_sync_state.get("seq", 1) or 1),
            "previousSeq": int(abnormal_sync_state.get("previousSeq", 0) or 0),
            "snapshot": _clone_json_value(abnormal_sync_state.get("snapshot") or _build_abnormal_snapshot_locked()),
            "previousSnapshot": _clone_json_value(abnormal_sync_state.get("previousSnapshot")) if abnormal_sync_state.get("previousSnapshot") is not None else None,
            "lastError": str(abnormal_sync_state.get("lastError", "") or ""),
        }


def _build_v2_bus_changes(runtime_snapshot: Dict[str, Any],
                          abnormal_state: Dict[str, Any],
                          previous_revisions: Dict[str, str],
                          bootstrap: bool) -> Dict[str, Any]:
    current_revisions = {
        "marketRevision": str(runtime_snapshot.get("marketDigest", "")),
        "accountRuntimeRevision": str(runtime_snapshot.get("accountRevision", "")),
        "accountHistoryRevision": str(runtime_snapshot.get("historyRevision", "")),
        "abnormalRevision": str(abnormal_state.get("seq", "")),
    }
    if bootstrap:
        return {
            "market": {"snapshot": _clone_json_value(runtime_snapshot.get("market") or {})},
            "accountRuntime": {"snapshot": _clone_json_value(runtime_snapshot.get("account") or {})},
            "accountHistory": {
                "historyRevision": str(runtime_snapshot.get("historyRevision", "")),
                "tradeCount": int(runtime_snapshot.get("tradeCount", 0) or 0),
                "curvePointCount": int(runtime_snapshot.get("curvePointCount", 0) or 0),
            },
            "abnormal": {"snapshot": _clone_json_value(abnormal_state.get("snapshot") or {})},
        }

    changes: Dict[str, Any] = {}
    if current_revisions["marketRevision"] != str(previous_revisions.get("marketRevision", "")):
        changes["market"] = {"snapshot": _clone_json_value(runtime_snapshot.get("market") or {})}
    if current_revisions["accountRuntimeRevision"] != str(previous_revisions.get("accountRuntimeRevision", "")):
        changes["accountRuntime"] = {"snapshot": _clone_json_value(runtime_snapshot.get("account") or {})}
    if current_revisions["accountHistoryRevision"] != str(previous_revisions.get("accountHistoryRevision", "")):
        changes["accountHistory"] = {
            "historyRevision": str(runtime_snapshot.get("historyRevision", "")),
            "tradeCount": int(runtime_snapshot.get("tradeCount", 0) or 0),
            "curvePointCount": int(runtime_snapshot.get("curvePointCount", 0) or 0),
        }
    if current_revisions["abnormalRevision"] != str(previous_revisions.get("abnormalRevision", "")):
        previous_snapshot = abnormal_state.get("previousSnapshot") or {"records": [], "alerts": []}
        current_snapshot = abnormal_state.get("snapshot") or {"records": [], "alerts": []}
        changes["abnormal"] = {
            "meta": _clone_json_value(current_snapshot.get("abnormalMeta") or {}),
            "delta": {
                "records": _diff_abnormal_items(previous_snapshot.get("records") or [], current_snapshot.get("records") or []),
                "alerts": _diff_abnormal_items(previous_snapshot.get("alerts") or [], current_snapshot.get("alerts") or []),
            },
        }
        last_error = str(abnormal_state.get("lastError", "") or "")
        if last_error:
            changes["abnormal"]["meta"]["warning"] = last_error
    return changes


def _normalize_v2_stream_message(event_type: str,
                                 bus_seq: int,
                                 published_at: int,
                                 revisions: Dict[str, str],
                                 changes: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": str(event_type or "syncEvent"),
        "busSeq": int(bus_seq or 0),
        "publishedAt": int(published_at or 0),
        "revisions": _clone_json_value(revisions or {}),
        "changes": _clone_json_value(changes or {}),
    }


def _build_v2_stream_bootstrap_message(state: Dict[str, Any]) -> Dict[str, Any]:
    runtime_snapshot = state.get("runtimeSnapshot") or {}
    abnormal_snapshot = state.get("abnormalSnapshot") or {}
    changes = {
        "market": {"snapshot": _clone_json_value(runtime_snapshot.get("market") or {})},
        "accountRuntime": {"snapshot": _clone_json_value(runtime_snapshot.get("account") or {})},
        "accountHistory": {
            "historyRevision": str(runtime_snapshot.get("historyRevision", "")),
            "tradeCount": int(runtime_snapshot.get("tradeCount", 0) or 0),
            "curvePointCount": int(runtime_snapshot.get("curvePointCount", 0) or 0),
        },
        "abnormal": {"snapshot": _clone_json_value(abnormal_snapshot or {})},
    }
    return _normalize_v2_stream_message(
        event_type="syncBootstrap",
        bus_seq=int(state.get("busSeq", 0) or 0),
        published_at=int(state.get("publishedAt", 0) or 0),
        revisions=dict(state.get("revisions") or {}),
        changes=changes,
    )


def _build_v2_stream_event_for_client(last_bus_seq: int) -> Dict[str, Any]:
    state = _read_account_publish_state()
    current_bus_seq = int(state.get("busSeq", 0) or 0)
    published_at = int(state.get("publishedAt", 0) or 0)
    revisions = dict(state.get("revisions") or {})
    if current_bus_seq <= 0:
        return _normalize_v2_stream_message("heartbeat", 0, published_at, revisions, {})
    if int(last_bus_seq or 0) <= 0:
        return _build_v2_stream_bootstrap_message(state)
    if current_bus_seq <= int(last_bus_seq or 0):
        return _normalize_v2_stream_message("heartbeat", current_bus_seq, published_at, revisions, {})

    event = dict(state.get("event") or {})
    event_bus_seq = int(event.get("busSeq", current_bus_seq) or current_bus_seq)
    if event and int(last_bus_seq or 0) == max(0, event_bus_seq - 1):
        return _normalize_v2_stream_message(
            event_type=str(event.get("type", "") or "syncEvent"),
            bus_seq=event_bus_seq,
            published_at=int(event.get("publishedAt", published_at) or published_at),
            revisions=dict(event.get("revisions") or revisions),
            changes=dict(event.get("changes") or {}),
        )
    return _build_v2_stream_bootstrap_message(state)


# 生成交易命令摘要，用于 requestId 幂等的内容一致性校验。
def _trade_command_digest(command: Dict[str, Any]) -> str:
    payload = json.dumps(command or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


# 识别账户模式，避免 netting/hedging 下平仓语义混乱。
def _detect_account_mode() -> str:
    if mt5 is None:
        return "unknown"
    try:
        account = mt5.account_info()
    except Exception:
        account = None
    return v2_trade.detect_account_mode(mt5, account)


# 统一读取交易请求里对应的持仓，便于平仓和改单动作复用。
def _lookup_trade_position(params: Dict[str, Any], account_mode: str) -> Optional[Dict[str, Any]]:
    if account_mode not in {"netting", "hedging"}:
        return None
    if mt5 is None:
        return None
    try:
        positions = mt5.positions_get() or []
    except Exception:
        return None
    target_ticket = int(float(params.get("positionTicket") or params.get("positionId") or 0))
    target_symbol = str(params.get("symbol") or "").strip().upper()
    if target_ticket > 0:
        for position in positions:
            ticket = int(getattr(position, "ticket", 0) or 0)
            identifier = int(getattr(position, "identifier", 0) or 0)
            if ticket == target_ticket or identifier == target_ticket:
                side = "buy" if int(getattr(position, "type", 0) or 0) == 0 else "sell"
                return {"ticket": ticket, "positionTicket": ticket, "symbol": str(getattr(position, "symbol", "") or ""), "side": side}
        if account_mode == "hedging":
            # hedging 下显式 ticket 未命中时，禁止按 symbol 回退到其他仓位。
            return None
    if target_symbol:
        matches: List[Dict[str, Any]] = []
        for position in positions:
            symbol = str(getattr(position, "symbol", "") or "").upper()
            if symbol != target_symbol:
                continue
            side = "buy" if int(getattr(position, "type", 0) or 0) == 0 else "sell"
            ticket = int(getattr(position, "ticket", 0) or 0)
            matches.append({"ticket": ticket, "positionTicket": ticket, "symbol": symbol, "side": side})
        if account_mode == "netting" and len(matches) == 1:
            return matches[0]
        if account_mode == "hedging" and target_ticket > 0 and matches:
            return matches[0]
    return None


# 统一构建交易命令和 MT5 请求，校验失败时直接返回标准错误。
def _prepare_trade_command(payload: Dict[str, Any], account_mode: str) -> Dict[str, Any]:
    return v2_trade.prepare_trade_request(
        payload,
        account_mode=account_mode,
        mt5_module=mt5,
        position_lookup=_lookup_trade_position,
        symbol_info_lookup=(lambda symbol: mt5.symbol_info(symbol)) if mt5 is not None else None,
    )


# 调用 MT5 order_check，统一由服务端判定可执行性。
def _trade_check_request(mt5_request: Dict[str, Any]) -> Any:
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package is unavailable")
    _ensure_mt5()
    return mt5.order_check(mt5_request)


# 调用 MT5 order_send，统一由服务端发起真实交易动作。
def _trade_send_request(mt5_request: Dict[str, Any]) -> Any:
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package is unavailable")
    _ensure_mt5()
    return mt5.order_send(mt5_request)


# 保存提交结果并维护固定大小的幂等缓存。
def _store_trade_request_result(request_id: str, payload_digest: str, result_payload: Dict[str, Any]) -> None:
    request_id = str(request_id or "")
    if not request_id:
        return
    with trade_request_lock:
        trade_request_store[request_id] = {
            "payloadDigest": str(payload_digest or ""),
            "response": _clone_json_value(result_payload),
        }
        while len(trade_request_store) > TRADE_REQUEST_STORE_MAX_ENTRIES:
            oldest_key = next(iter(trade_request_store))
            trade_request_store.pop(oldest_key, None)


# 读取历史请求结果，供幂等和结果查询复用。
def _get_trade_request_entry(request_id: str) -> Optional[Dict[str, Any]]:
    key = str(request_id or "")
    if not key:
        return None
    with trade_request_lock:
        cached = trade_request_store.get(key)
        return None if cached is None else _clone_json_value(cached)


# 只返回对外响应体，供 result 接口复用。
def _get_trade_request_result(request_id: str) -> Optional[Dict[str, Any]]:
    entry = _get_trade_request_entry(request_id)
    if entry is None:
        return None
    response = entry.get("response")
    if not isinstance(response, dict):
        return None
    return dict(response)


# 保存批量提交结果，供 batch 查询接口复用。
def _store_batch_request_result(batch_id: str, result_payload: Dict[str, Any]) -> None:
    key = str(batch_id or "")
    if not key:
        return
    with trade_request_lock:
        batch_request_store[key] = _clone_json_value(result_payload)
        while len(batch_request_store) > TRADE_REQUEST_STORE_MAX_ENTRIES:
            oldest_key = next(iter(batch_request_store))
            batch_request_store.pop(oldest_key, None)


# 读取历史批量请求结果。
def _get_batch_request_result(batch_id: str) -> Optional[Dict[str, Any]]:
    key = str(batch_id or "")
    if not key:
        return None
    with trade_request_lock:
        payload = batch_request_store.get(key)
        return None if payload is None else _clone_json_value(payload)


def _resolve_trade_error_fields(error: Optional[Dict[str, Any]]) -> tuple[str, str]:
    if not isinstance(error, dict):
        return "", ""
    return str(error.get("code") or ""), str(error.get("message") or "")


def _format_trade_volume(value: Any) -> str:
    try:
        return f"{float(value):.2f} 手"
    except Exception:
        return "0.00 手"


def _build_single_trade_action_summary(command: Optional[Dict[str, Any]]) -> str:
    payload = dict(command or {})
    params = payload.get("params")
    params = dict(params) if isinstance(params, dict) else {}
    action = str(payload.get("action") or "").strip().upper()
    symbol = str(params.get("symbol") or payload.get("symbol") or "").strip().upper()
    volume = _format_trade_volume(params.get("volume"))
    if action == "OPEN_MARKET":
        side = str(params.get("side") or "").strip().lower()
        prefix = "买入" if side == "buy" else "卖出" if side == "sell" else "市价开仓"
        return f"{prefix} {symbol} {volume}".strip()
    if action == "CLOSE_POSITION":
        return f"平仓 {symbol} {volume}".strip()
    if action == "PENDING_ADD":
        return f"新增挂单 {symbol} {volume}".strip()
    if action == "PENDING_CANCEL":
        return f"撤销挂单 {symbol}".strip()
    if action == "PENDING_MODIFY":
        return f"修改挂单 {symbol}".strip()
    if action == "MODIFY_TPSL":
        return f"修改止盈止损 {symbol}".strip()
    if action == "CLOSE_BY":
        return f"对锁平仓 {symbol}".strip()
    if symbol:
        return f"{action} {symbol}".strip()
    return action or "交易命令"


def _build_batch_trade_action_summary(payload: Optional[Dict[str, Any]], result_payload: Optional[Dict[str, Any]]) -> str:
    source = payload if isinstance(payload, dict) else {}
    result = result_payload if isinstance(result_payload, dict) else {}
    summary = str(source.get("summary") or source.get("displayName") or "").strip()
    if summary:
        return summary
    batch_id = str(source.get("batchId") or result.get("batchId") or "").strip()
    items = source.get("items")
    if isinstance(items, list) and items:
        first_item = items[0] if isinstance(items[0], dict) else {}
        first_action = str(first_item.get("action") or "").strip().upper()
        if first_action:
            return f"批量{first_action} {batch_id}".strip()
    return f"批量交易 {batch_id}".strip() if batch_id else "批量交易"


def _append_trade_audit_entry(
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
    action_summary: str,
    server_time: int,
) -> None:
    if not str(trace_id or "").strip():
        return
    trade_audit_store.append(
        trace_id=str(trace_id or "").strip(),
        trace_type=str(trace_type or "").strip(),
        action=str(action or "").strip(),
        symbol=str(symbol or "").strip(),
        account_mode=str(account_mode or "").strip(),
        stage=str(stage or "").strip(),
        status=str(status or "").strip(),
        error_code=str(error_code or "").strip(),
        message=str(message or "").strip(),
        action_summary=str(action_summary or "").strip(),
        server_time=int(server_time or 0),
    )


def _record_single_trade_audit(
    *,
    command: Optional[Dict[str, Any]],
    account_mode: str,
    stage: str,
    status: str,
    error: Optional[Dict[str, Any]],
    message: str,
    server_time: int,
) -> None:
    normalized = v2_trade.normalize_trade_payload(command or {})
    params = normalized.get("params")
    params = dict(params) if isinstance(params, dict) else {}
    error_code, error_message = _resolve_trade_error_fields(error)
    _append_trade_audit_entry(
        trace_id=str(normalized.get("requestId") or ""),
        trace_type="single",
        action=str(normalized.get("action") or ""),
        symbol=str(params.get("symbol") or "").strip().upper(),
        account_mode=str(account_mode or ""),
        stage=stage,
        status=status,
        error_code=error_code,
        message=message or error_message,
        action_summary=_build_single_trade_action_summary(normalized),
        server_time=server_time,
    )


def _record_batch_trade_audit(
    *,
    payload: Optional[Dict[str, Any]],
    result_payload: Optional[Dict[str, Any]],
    stage: str,
    status: str,
    error: Optional[Dict[str, Any]],
    message: str,
    server_time: int,
) -> None:
    source = dict(payload or {})
    result = dict(result_payload or {})
    items = source.get("items")
    items = items if isinstance(items, list) else []
    first_item = items[0] if items and isinstance(items[0], dict) else {}
    first_params = first_item.get("params")
    first_params = first_params if isinstance(first_params, dict) else {}
    error_code, error_message = _resolve_trade_error_fields(error)
    _append_trade_audit_entry(
        trace_id=str(result.get("batchId") or source.get("batchId") or "").strip(),
        trace_type="batch",
        action=str(first_item.get("action") or "BATCH").strip().upper() or "BATCH",
        symbol=str(first_params.get("symbol") or source.get("symbol") or "").strip().upper(),
        account_mode=str(result.get("accountMode") or source.get("accountMode") or "").strip(),
        stage=stage,
        status=status,
        error_code=error_code,
        message=message or error_message,
        action_summary=_build_batch_trade_action_summary(source, result),
        server_time=server_time,
    )


def _append_session_diagnostic_entry(
    *,
    request_id: str,
    action: str,
    stage: str,
    status: str,
    message: str,
    server_time: int,
    error_code: str = "",
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    """向会话诊断缓存追加一条事实。"""
    if not str(request_id or "").strip():
        return
    session_diagnostic_store.append(
        request_id=str(request_id or "").strip(),
        action=str(action or "").strip(),
        stage=str(stage or "").strip(),
        status=str(status or "").strip(),
        message=str(message or "").strip(),
        error_code=str(error_code or "").strip(),
        server_time=int(server_time or 0),
        detail=dict(detail or {}),
    )


def _record_session_diagnostic_trace(
    *,
    request_id: str,
    action: str,
    trace_items: Optional[List[Dict[str, Any]]],
) -> None:
    """把探针子进程回传的阶段轨迹写回服务端诊断缓存。"""
    for item in trace_items or []:
        if not isinstance(item, dict):
            continue
        _append_session_diagnostic_entry(
            request_id=request_id,
            action=action,
            stage=str(item.get("stage") or "").strip(),
            status=str(item.get("status") or "").strip(),
            message=str(item.get("message") or "").strip(),
            server_time=int(item.get("serverTime") or _now_ms()),
            error_code=str(item.get("errorCode") or "").strip(),
            detail=item.get("detail") if isinstance(item.get("detail"), dict) else None,
        )


def _create_probe_trace_file() -> Optional[Path]:
    """为登录探针创建临时 trace 文件，供超时场景回收内部阶段。"""
    try:
        with tempfile.NamedTemporaryFile(prefix="mt5-login-probe-", suffix=".json", delete=False) as handle:
            return Path(handle.name)
    except Exception:
        return None


def _read_probe_trace_file(trace_file: Optional[Path]) -> List[Dict[str, Any]]:
    """读取登录探针落盘的阶段轨迹。"""
    if trace_file is None or not trace_file.exists():
        return []
    try:
        raw_text = trace_file.read_text(encoding="utf-8").strip()
    except Exception:
        return []
    if not raw_text:
        return []
    try:
        payload = json.loads(raw_text)
    except Exception:
        return []
    if isinstance(payload, dict):
        trace_items = payload.get("trace")
        return trace_items if isinstance(trace_items, list) else []
    return payload if isinstance(payload, list) else []


def _cleanup_probe_trace_file(trace_file: Optional[Path]) -> None:
    """删除登录探针 trace 临时文件。"""
    if trace_file is None:
        return
    try:
        trace_file.unlink(missing_ok=True)
    except Exception:
        pass


def _try_reuse_runtime_active_session(
    *,
    login_value: int,
    password_value: str,
    server_value: str,
    request_id: str = "",
    action: str = "login",
) -> Optional[Dict[str, str]]:
    """当服务器当前已激活同一远程账号时，直接复用现有会话。"""
    normalized_login = str(int(login_value or 0)).strip() if int(login_value or 0) > 0 else ""
    normalized_server = str(server_value or "").strip()
    normalized_server_identity = _normalize_session_server_identity(server_value)
    if not normalized_login or not normalized_server:
        return None
    runtime = _runtime_session_credentials_snapshot()
    if str(runtime.get("mode") or "") != "remote_active":
        return None
    runtime_login = str(_parse_login_value(runtime.get("login")) or "").strip()
    runtime_server = str(runtime.get("server") or "").strip()
    runtime_password = str(runtime.get("password") or "")
    if runtime_login != normalized_login:
        return None
    if not runtime_server or _normalize_session_server_identity(runtime_server) != normalized_server_identity:
        return None
    if not runtime_password:
        return None
    active_session = session_store.load_active_session() if hasattr(session_store, "load_active_session") else None
    if not isinstance(active_session, dict) or not active_session:
        return None
    active_login = str(active_session.get("login") or "").strip()
    active_server = str(active_session.get("server") or "").strip()
    if active_login != normalized_login:
        return None
    if not active_server or _normalize_session_server_identity(active_server) != normalized_server_identity:
        return None
    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage="runtime_session_reused",
        status="ok",
        message="服务器当前已是目标账号，直接复用现有远程会话",
        server_time=_now_ms(),
        detail={
            "login": normalized_login,
            "server": active_server,
        },
    )
    return {
        "login": normalized_login,
        "server": active_server,
        "password": runtime_password,
    }


def _try_reuse_saved_profile_session(
    *,
    login_value: int,
    password_value: str,
    server_value: str,
    request_id: str = "",
    action: str = "login",
) -> Optional[Dict[str, str]]:
    """当服务器已保存同一账号档案时，直接复用已保存账号。"""
    normalized_login = str(int(login_value or 0)).strip() if int(login_value or 0) > 0 else ""
    normalized_server = str(server_value or "").strip()
    normalized_server_identity = _normalize_session_server_identity(server_value)
    if not normalized_login or not normalized_server:
        return None
    if not hasattr(session_store, "list_profiles") or not hasattr(session_store, "load_profile"):
        return None
    for profile in session_store.list_profiles() or []:
        if not isinstance(profile, dict):
            continue
        profile_id = str(profile.get("profileId") or "").strip()
        profile_login = str(profile.get("login") or "").strip()
        profile_server = str(profile.get("server") or "").strip()
        if not profile_id or profile_login != normalized_login:
            continue
        if not profile_server or _normalize_session_server_identity(profile_server) != normalized_server_identity:
            continue
        record = session_store.load_profile(profile_id)
        if not isinstance(record, dict) or not record:
            continue
        encrypted_password = str(record.get("encryptedPassword") or "").strip()
        if not encrypted_password:
            continue
        try:
            cipher = base64.b64decode(encrypted_password, validate=True)
            saved_password = v2_session_crypto.unprotect_secret_for_machine(cipher).decode("utf-8")
        except Exception:
            continue
        if not saved_password:
            continue
        _append_session_diagnostic_entry(
            request_id=request_id,
            action=action,
            stage="saved_profile_reused",
            status="ok",
            message="已命中服务器保存的同账号档案，直接复用已保存会话",
            server_time=_now_ms(),
            detail={
                "profileId": profile_id,
                "login": normalized_login,
                "server": profile_server,
            },
        )
        return {
            "login": normalized_login,
            "server": profile_server,
            "password": saved_password,
        }
    return None


def _try_reuse_env_configured_session(
    *,
    login_value: int,
    password_value: str,
    server_value: str,
    request_id: str = "",
    action: str = "login",
) -> Optional[Dict[str, str]]:
    """当用户输入与服务器 env 配置一致时，直接复用 env 账号。"""
    normalized_login = str(int(login_value or 0)).strip() if int(login_value or 0) > 0 else ""
    normalized_server = str(server_value or "").strip()
    normalized_server_identity = _normalize_session_server_identity(server_value)
    env_login = str(_parse_login_value(LOGIN) or "").strip()
    env_server = str(SERVER or "").strip()
    env_password = str(PASSWORD or "")
    if not normalized_login or not normalized_server:
        return None
    if env_login != normalized_login:
        return None
    if not env_server or _normalize_session_server_identity(env_server) != normalized_server_identity:
        return None
    if not env_password:
        return None
    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage="env_session_reused",
        status="ok",
        message="已命中服务器 env 配置账号，直接复用服务器当前账号",
        server_time=_now_ms(),
        detail={
            "login": normalized_login,
            "server": env_server,
        },
    )
    return {
        "login": normalized_login,
        "server": env_server,
        "password": env_password,
    }


def _try_reuse_current_mt5_terminal_session(
    *,
    login_value: int,
    password_value: str,
    server_value: str,
    request_id: str = "",
    action: str = "login",
) -> Optional[Dict[str, str]]:
    """当主进程当前 MT5 终端已经是目标账号时，直接复用当前终端身份。"""
    normalized_login = str(int(login_value or 0)).strip() if int(login_value or 0) > 0 else ""
    normalized_server = str(server_value or "").strip()
    normalized_server_identity = _normalize_session_server_identity(server_value)
    if not normalized_login or not normalized_server:
        return None
    if mt5 is None:
        return None
    try:
        account = mt5.account_info()
    except Exception:
        account = None
    if account is None:
        return None
    current_login = str(getattr(account, "login", "") or "").strip()
    current_server = str(getattr(account, "server", "") or "").strip()
    if current_login != normalized_login:
        return None
    if not current_server or _normalize_session_server_identity(current_server) != normalized_server_identity:
        return None
    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage="current_terminal_session_reused",
        status="ok",
        message="主进程当前 MT5 终端已是目标账号，直接复用当前终端会话",
        server_time=_now_ms(),
        detail={
            "login": current_login,
            "server": current_server,
        },
    )
    return {
        "login": current_login,
        "server": current_server,
        "password": str(password_value or ""),
    }


def _build_session_diagnostic_payload(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """统一构造会话诊断接口返回结构。"""
    request_id = str(items[-1].get("requestId") or "").strip() if items else ""
    return {
        "ok": True,
        "requestId": request_id,
        "items": [dict(item) for item in items],
    }


def _normalize_session_server_identity(server: Any) -> str:
    """统一规范化会话服务器名，避免仅分隔符不同就丢失同一账号匹配。"""
    raw = str(server or "").strip().lower()
    if not raw:
        return ""
    return "".join(ch for ch in raw if ch.isalnum())


# 构建 v2 行情总快照返回体，统一保留市场区和账户区。
def _build_v2_market_snapshot_payload(market: Dict[str, Any],
                                      account: Dict[str, Any],
                                      server_time: int) -> Dict[str, Any]:
    return {
        "serverTime": int(server_time),
        "syncToken": _build_sync_token(server_time, "market-snapshot"),
        "market": dict(market or {}),
        "account": dict(account or {}),
    }


# 构建 v2 K 线返回体，强制把闭合历史和当前 patch 分层。
def _build_v2_market_candles_payload(symbol: str,
                                     interval: str,
                                     server_time: int,
                                     candles: List[Dict[str, Any]],
                                     latest_patch: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    safe_candles = [dict(item) for item in (candles or [])]
    safe_patch = None if latest_patch is None else dict(latest_patch)
    return {
        "symbol": str(symbol or ""),
        "interval": str(interval or ""),
        "serverTime": int(server_time),
        "candles": safe_candles,
        "latestPatch": safe_patch,
        "nextSyncToken": _build_sync_token(server_time, f"market:{symbol}:{interval}:{len(safe_candles)}"),
    }


# 构建 v2 账户快照返回体，统一输出当前账户、持仓和挂单。
def _build_v2_account_snapshot_payload(account: Dict[str, Any],
                                       positions: List[Dict[str, Any]],
                                       orders: List[Dict[str, Any]],
                                       server_time: int) -> Dict[str, Any]:
    return {
        "serverTime": int(server_time),
        "syncToken": _build_sync_token(server_time, "account-snapshot"),
        "account": dict(account or {}),
        "positions": [dict(item) for item in (positions or [])],
        "orders": [dict(item) for item in (orders or [])],
    }


# 统一生成稳定 JSON 摘要，供 v2 同步态判断是否变化。
def _stable_payload_digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


# 为 v2 同步层生成“状态不变则 token 不变”的稳定 token。
def _build_v2_state_token(sync_seq: int, state_digest: str) -> str:
    payload = f"v2-sync:{int(sync_seq)}:{state_digest}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


# 清理账户元数据里的易抖动字段，避免无效增量。
def _normalize_v2_account_meta(account_meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "login": str(account_meta.get("login", "")),
        "server": str(account_meta.get("server", "")),
        "source": str(account_meta.get("source", "")),
        "accountMode": str(account_meta.get("accountMode", "")),
        "currency": str(account_meta.get("currency", "")),
        "leverage": int(account_meta.get("leverage", 0) or 0),
        "name": str(account_meta.get("name", "")),
        "company": str(account_meta.get("company", "")),
        "tradeCount": int(account_meta.get("tradeCount", 0) or 0),
        "positionCount": int(account_meta.get("positionCount", 0) or 0),
        "pendingOrderCount": int(account_meta.get("pendingOrderCount", 0) or 0),
        "curvePointCount": int(account_meta.get("curvePointCount", 0) or 0),
        "historyRevision": str(account_meta.get("historyRevision", "")),
        "accountRevision": str(account_meta.get("accountRevision", "")),
    }


# 清理 diff 里的内部字段，避免把内部实现细节暴露给客户端。
def _sanitize_diff_payload(diff_payload: Dict[str, Any]) -> Dict[str, Any]:
    upsert_items: List[Dict[str, Any]] = []
    for item in (diff_payload.get("upsert") or []):
        safe_item = dict(item)
        safe_item.pop("_key", None)
        upsert_items.append(safe_item)
    return {
        "upsert": upsert_items,
        "remove": [str(value) for value in (diff_payload.get("remove") or [])],
    }


def _build_logged_out_account_snapshot() -> Dict[str, Any]:
    """构建未激活远程会话时的空账户快照，避免监控链误触发 MT5。"""
    history_revision = _build_history_revision_from_trades([])
    return {
        "accountMeta": {
            "login": "",
            "server": "",
            "source": "remote_logged_out",
            "accountMode": "",
            "updatedAt": _now_ms(),
            "currency": "",
            "leverage": 0,
            "name": "",
            "company": "",
            "tradeCount": 0,
            "positionCount": 0,
            "pendingOrderCount": 0,
            "curvePointCount": 0,
            "historyRevision": history_revision,
        },
        "positions": [],
        "pendingOrders": [],
    }


# 构建 v2 同步层运行态快照，统一 market/account 的摘要和计数。
def _build_v2_sync_runtime_snapshot(server_time: int) -> Dict[str, Any]:
    session_status = session_manager.build_status_payload()
    active_account = (session_status or {}).get("activeAccount") or None
    if active_account:
        snapshot = _build_account_light_snapshot_with_cache()
    else:
        snapshot = _build_logged_out_account_snapshot()
    market_section = _build_v2_market_section(server_time)
    account_section = _build_v2_account_section_from_snapshot(snapshot)
    history_revision = str((account_section.get("accountMeta") or {}).get("historyRevision", ""))
    normalized_account = {
        "accountMeta": _normalize_v2_account_meta(account_section.get("accountMeta") or {}),
        "overviewMetrics": [dict(item) for item in (account_section.get("overviewMetrics") or [])],
        "statsMetrics": [dict(item) for item in (account_section.get("statsMetrics") or [])],
        "positions": [dict(item) for item in (account_section.get("positions") or [])],
        "orders": [dict(item) for item in (account_section.get("orders") or [])],
    }
    market_digest = _stable_payload_digest(market_section)
    account_digest = _stable_payload_digest(normalized_account)
    # accountRevision 代表“对客户端可见的账户运行态是否发生变化”，必须排除 updatedAt 等抖动字段，
    # 否则会导致 stream 在真值未变时也持续发 accountRuntime 更新。
    account_revision = account_digest
    state_digest = _stable_payload_digest({
        "marketDigest": market_digest,
        "accountDigest": account_digest,
        "accountRevision": account_revision,
    })
    return {
        "serverTime": int(server_time),
        "market": dict(market_section),
        "account": normalized_account,
        "marketDigest": market_digest,
        "accountDigest": account_digest,
        "accountRevision": account_revision,
        "historyRevision": history_revision,
        "digest": state_digest,
        "tradeCount": int((account_section.get("accountMeta") or {}).get("tradeCount", 0) or 0),
        "curvePointCount": int((account_section.get("accountMeta") or {}).get("curvePointCount", 0) or 0),
    }


# 根据运行态快照构建稳定 summary，供 HTTP 和 WS 统一输出。
def _build_v2_sync_summary(runtime_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    account_meta = (runtime_snapshot.get("account") or {}).get("accountMeta") or {}
    market_symbols = (runtime_snapshot.get("market") or {}).get("symbols") or []
    return {
        "marketDigest": str(runtime_snapshot.get("marketDigest", "")),
        "accountDigest": str(runtime_snapshot.get("accountDigest", "")),
        "accountRevision": str(runtime_snapshot.get("accountRevision", "")),
        "historyRevision": str(runtime_snapshot.get("historyRevision", "")),
        "marketSymbolCount": len(market_symbols),
        "positionCount": len(((runtime_snapshot.get("account") or {}).get("positions") or [])),
        "orderCount": len(((runtime_snapshot.get("account") or {}).get("orders") or [])),
        "tradeCount": int(runtime_snapshot.get("tradeCount", 0) or 0),
        "curvePointCount": int(runtime_snapshot.get("curvePointCount", 0) or 0),
        "login": str(account_meta.get("login", "")),
        "server": str(account_meta.get("server", "")),
        "source": str(account_meta.get("source", "")),
    }


# 计算 v2 market/account 的最小增量事件，供 delta 与 stream 复用。
def _build_v2_sync_delta_events(previous_snapshot: Dict[str, Any], current_snapshot: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    market_delta: List[Dict[str, Any]] = []
    account_delta: List[Dict[str, Any]] = []

    previous_market = previous_snapshot.get("market") or {}
    current_market = current_snapshot.get("market") or {}
    market_changed_keys = sorted({
        key
        for key in set(previous_market.keys()) | set(current_market.keys())
        if previous_market.get(key) != current_market.get(key)
    })
    if market_changed_keys:
        market_delta.append({
            "type": "marketSnapshotChanged",
            "action": "market.snapshot",
            "digest": str(current_snapshot.get("marketDigest", "")),
            "revision": str(current_snapshot.get("marketDigest", "")),
            "changedKeys": market_changed_keys,
            "snapshot": dict(current_market),
        })

    previous_account = previous_snapshot.get("account") or {}
    current_account = current_snapshot.get("account") or {}
    previous_meta = previous_account.get("accountMeta") or {}
    current_meta = current_account.get("accountMeta") or {}
    meta_compare_exclude_keys = {"historyRevision", "accountRevision"}
    previous_meta_for_compare = {
        key: value for key, value in previous_meta.items() if key not in meta_compare_exclude_keys
    }
    current_meta_for_compare = {
        key: value for key, value in current_meta.items() if key not in meta_compare_exclude_keys
    }
    meta_changed = previous_meta_for_compare != current_meta_for_compare
    positions_diff = _sanitize_diff_payload(_diff_entities(
        previous_account.get("positions") or [],
        current_account.get("positions") or [],
        _position_key,
    ))
    orders_diff = _sanitize_diff_payload(_diff_entities(
        previous_account.get("orders") or [],
        current_account.get("orders") or [],
        _pending_key,
    ))
    has_positions_delta = bool(positions_diff["upsert"] or positions_diff["remove"])
    has_orders_delta = bool(orders_diff["upsert"] or orders_diff["remove"])
    account_revision_changed = str(previous_snapshot.get("accountRevision", "")) != str(current_snapshot.get("accountRevision", ""))
    history_revision_changed = str(previous_snapshot.get("historyRevision", "")) != str(current_snapshot.get("historyRevision", ""))
    if meta_changed or has_positions_delta or has_orders_delta or account_revision_changed or history_revision_changed:
        account_event: Dict[str, Any] = {
            "type": "accountSnapshotChanged",
            "action": "account.snapshot",
            "digest": str(current_snapshot.get("accountDigest", "")),
            "revision": str(current_snapshot.get("accountRevision", "")),
            "historyRevision": str(current_snapshot.get("historyRevision", "")),
            "accountMetaChanged": meta_changed,
            "positions": positions_diff,
            "orders": orders_diff,
            "snapshot": dict(current_account),
            "historyRevisionChanged": history_revision_changed,
        }
        if meta_changed:
            account_event["accountMeta"] = dict(current_meta)
        account_delta.append(account_event)

    return market_delta, account_delta


# 提交账户运行态发布序号，保留“当前 + 上一版本”用于单步增量计算。
def _commit_account_publish_runtime_locked(runtime_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    runtime_digest = str(runtime_snapshot.get("digest", ""))
    current_digest = str(account_publish_state.get("runtimeDigest", "") or "")
    if not current_digest:
        seq = 1
        token = _build_v2_state_token(seq, runtime_digest)
        account_publish_state.update({
            "runtimeSeq": seq,
            "runtimeToken": token,
            "runtimeDigest": runtime_digest,
            "runtimeSnapshot": runtime_snapshot,
            "previousRuntimeSeq": 0,
            "previousRuntimeToken": "",
            "previousRuntimeSnapshot": None,
        })
    elif current_digest == runtime_digest:
        account_publish_state["runtimeSnapshot"] = runtime_snapshot
    else:
        previous_seq = int(account_publish_state.get("runtimeSeq", 0) or 0)
        previous_token = str(account_publish_state.get("runtimeToken", "") or "")
        previous_snapshot = account_publish_state.get("runtimeSnapshot")
        seq = previous_seq + 1
        token = _build_v2_state_token(seq, runtime_digest)
        account_publish_state.update({
            "runtimeSeq": seq,
            "runtimeToken": token,
            "runtimeDigest": runtime_digest,
            "runtimeSnapshot": runtime_snapshot,
            "previousRuntimeSeq": previous_seq,
            "previousRuntimeToken": previous_token,
            "previousRuntimeSnapshot": previous_snapshot,
        })

    return {
        "seq": int(account_publish_state.get("runtimeSeq", 1) or 1),
        "token": str(account_publish_state.get("runtimeToken", "")),
        "snapshot": account_publish_state.get("runtimeSnapshot") or runtime_snapshot,
        "previousSeq": int(account_publish_state.get("previousRuntimeSeq", 0) or 0),
        "previousToken": str(account_publish_state.get("previousRuntimeToken", "")),
        "previousSnapshot": account_publish_state.get("previousRuntimeSnapshot"),
    }


def _v2_bus_publish_current_state() -> None:
    now_ms = _now_ms()
    runtime_snapshot = _build_v2_sync_runtime_snapshot(now_ms)
    _refresh_abnormal_state()
    abnormal_state = _read_abnormal_sync_state_for_bus()
    current_revisions = {
        "marketRevision": str(runtime_snapshot.get("marketDigest", "")),
        "accountRuntimeRevision": str(runtime_snapshot.get("accountRevision", "")),
        "accountHistoryRevision": str(runtime_snapshot.get("historyRevision", "")),
        "abnormalRevision": str(abnormal_state.get("seq", "")),
    }
    previous_bus_state = _read_account_publish_state()
    previous_revisions = dict(previous_bus_state.get("revisions") or {})
    bootstrap = int(previous_bus_state.get("busSeq", 0) or 0) <= 0
    pending_event_type = str(previous_bus_state.get("pendingEventType", "") or "")
    changes = _build_v2_bus_changes(
        runtime_snapshot=runtime_snapshot,
        abnormal_state=abnormal_state,
        previous_revisions=previous_revisions,
        bootstrap=bootstrap,
    )
    if not bootstrap and not changes and not pending_event_type:
        return
    event_type = "syncBootstrap" if bootstrap else (pending_event_type or "syncEvent")
    with snapshot_cache_lock:
        _commit_account_publish_runtime_locked(runtime_snapshot)
    _publish_v2_bus_event(
        event_type=event_type,
        changes=changes,
        revisions=current_revisions,
        published_at=now_ms,
        runtime_snapshot=runtime_snapshot,
        abnormal_snapshot=abnormal_state.get("snapshot") or {},
    )


def _v2_bus_producer_loop() -> None:
    interval_sec = max(0.2, float(V2_STREAM_PUSH_INTERVAL_MS) / 1000.0)
    while not v2_bus_stop_event.is_set():
        v2_bus_wake_event.wait(interval_sec)
        v2_bus_wake_event.clear()
        if v2_bus_stop_event.is_set():
            break
        try:
            _v2_bus_publish_current_state()
        except Exception:
            pass


def _start_v2_bus_producer_locked() -> None:
    global v2_bus_producer_started, v2_bus_producer_thread
    v2_bus_stop_event.clear()
    v2_bus_wake_event.clear()
    thread = Thread(target=_v2_bus_producer_loop, name="v2-bus-producer", daemon=True)
    thread.start()
    v2_bus_producer_thread = thread
    v2_bus_producer_started = True


def _bootstrap_v2_bus_producer() -> None:
    with v2_bus_producer_lock:
        alive = v2_bus_producer_thread is not None and v2_bus_producer_thread.is_alive()
        if not alive:
            _v2_bus_publish_current_state()
            _start_v2_bus_producer_locked()


def _stop_v2_bus_producer() -> None:
    global v2_bus_producer_started, v2_bus_producer_thread
    with v2_bus_producer_lock:
        thread = v2_bus_producer_thread
        if thread is None:
            v2_bus_producer_started = False
            return
        v2_bus_stop_event.set()
        v2_bus_wake_event.set()
        if thread.is_alive():
            thread.join(timeout=1.0)
        v2_bus_producer_thread = None
        v2_bus_producer_started = False


def _build_market_runtime_symbol_state(symbol: str) -> Dict[str, Any]:
    """兼容真实 runtime 字典和测试注入桩，统一读取产品级市场真值。"""
    runtime = market_stream_runtime
    build_method = getattr(runtime, "build_symbol_state", None)
    if callable(build_method):
        return dict(build_method(symbol) or {})
    return v2_market_runtime.build_symbol_state(runtime, symbol)


def _get_market_runtime_patch_row(symbol: str) -> Optional[Dict[str, Any]]:
    """兼容真实 runtime 字典和测试注入桩，统一读取当前 1m patch 原始行。"""
    runtime = market_stream_runtime
    getter = getattr(runtime, "get_latest_patch_row", None)
    if callable(getter):
        return getter(symbol)
    return v2_market_runtime.get_latest_patch_row(runtime, symbol)


def _build_market_runtime_interval_patch(symbol: str, interval: str) -> Optional[Dict[str, Any]]:
    """兼容真实 runtime 字典和测试注入桩，统一读取长周期聚合 patch。"""
    runtime = market_stream_runtime
    builder = getattr(runtime, "build_interval_patch", None)
    if callable(builder):
        return builder(symbol, interval)
    return v2_market_runtime.build_interval_patch(runtime, symbol, interval)


def _build_market_runtime_source_status() -> Dict[str, Any]:
    """构建市场运行时状态，供 `/v1/source` 和 `/v2/market` 统一复用。"""
    runtime = market_stream_runtime
    builder = getattr(runtime, "build_source_status", None)
    if callable(builder):
        return dict(builder() or {})
    return v2_market_runtime.build_source_status(runtime)


def _bootstrap_market_stream_runtime_from_rest() -> None:
    """在 WS 真值尚未连上前，用极小的 REST 窗口补一层冷启动底稿。"""
    runtime = market_stream_runtime
    if getattr(runtime, "bootstrap_from_rest", None):
        runtime.bootstrap_from_rest()
        return
    bootstrap_limit = _market_runtime_bootstrap_minute_limit()
    for symbol in ABNORMAL_SYMBOLS:
        try:
            snapshot = _build_market_runtime_symbol_state(symbol)
            if snapshot.get("latestPatch") is not None or snapshot.get("latestClosedCandle") is not None:
                continue
            rows = _fetch_market_candle_rows_with_cache(
                symbol,
                "1m",
                bootstrap_limit,
                start_time_ms=0,
                end_time_ms=0,
            )
            v2_market_runtime.bootstrap_symbol_from_rest_rows(runtime, symbol, rows, _now_ms())
        except Exception:
            continue


def _market_runtime_bootstrap_minute_limit() -> int:
    """返回市场运行态冷启动需要预热的 1m 数量，确保长周期 WS patch 从首轮就完整。"""
    supported_intervals = ("1m", "5m", "15m", "30m", "1h", "4h", "1d")
    max_minutes = 1
    for interval in supported_intervals:
        if interval == "1m":
            interval_minutes = 1
        elif interval == "5m":
            interval_minutes = 5
        elif interval == "15m":
            interval_minutes = 15
        elif interval == "30m":
            interval_minutes = 30
        elif interval == "1h":
            interval_minutes = 60
        elif interval == "4h":
            interval_minutes = 4 * 60
        elif interval == "1d":
            interval_minutes = 24 * 60
        else:
            interval_minutes = 1
        max_minutes = max(max_minutes, interval_minutes)
    return max_minutes


def _build_market_stream_upstream_url() -> str:
    """构建 Binance combined stream URL，只订阅当前服务需要的 1m K 线。"""
    streams = "/".join(symbol.lower() + "@kline_1m" for symbol in ABNORMAL_SYMBOLS)
    return _build_binance_ws_upstream_url("/stream", f"streams={streams}")


def _consume_market_stream_runtime_message(raw_message: Any) -> None:
    """解析 WS 文本并推进市场运行时。只处理 kline 事件，其它消息直接忽略。"""
    text = raw_message.decode("utf-8") if isinstance(raw_message, (bytes, bytearray)) else str(raw_message or "")
    if not text:
        return
    payload = json.loads(text)
    event = dict((payload or {}).get("data") or payload or {})
    if not isinstance(event.get("k"), dict):
        return
    v2_market_runtime.apply_ws_kline_event(market_stream_runtime, payload)


async def _run_market_stream_runtime_loop() -> None:
    """保持 Binance 市场 WS 长连接，并把 1m K 线持续写入内存真值。"""
    upstream_url = _build_market_stream_upstream_url()
    v2_market_runtime.mark_connection_state(
        market_stream_runtime,
        connecting=True,
        connected=False,
        updated_at_ms=_now_ms(),
        last_error="",
    )
    async with websockets.connect(
        upstream_url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
    ) as upstream:
        v2_market_runtime.mark_connection_state(
            market_stream_runtime,
            connecting=False,
            connected=True,
            updated_at_ms=_now_ms(),
            last_error="",
        )
        while not market_stream_runtime_stop_event.is_set():
            try:
                message = await asyncio.wait_for(upstream.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            _consume_market_stream_runtime_message(message)


def _market_stream_runtime_worker() -> None:
    """在线程里托管 asyncio WS 循环，断线后自动短暂退避重连。"""
    _bootstrap_market_stream_runtime_from_rest()
    while not market_stream_runtime_stop_event.is_set():
        try:
            asyncio.run(_run_market_stream_runtime_loop())
        except Exception as exc:
            v2_market_runtime.mark_connection_state(
                market_stream_runtime,
                connecting=False,
                connected=False,
                updated_at_ms=_now_ms(),
                last_error=str(exc),
            )
            if market_stream_runtime_stop_event.wait(1.0):
                break


def _start_market_stream_runtime() -> None:
    """启动 Binance 市场运行时后台线程。"""
    global market_stream_runtime_thread
    with market_stream_runtime_control_lock:
        thread = market_stream_runtime_thread
        if thread is not None and thread.is_alive():
            return
        market_stream_runtime_stop_event.clear()
        thread = Thread(target=_market_stream_runtime_worker, name="market-stream-runtime", daemon=True)
        thread.start()
        market_stream_runtime_thread = thread


def _stop_market_stream_runtime() -> None:
    """停止 Binance 市场运行时后台线程。"""
    global market_stream_runtime_thread
    with market_stream_runtime_control_lock:
        thread = market_stream_runtime_thread
        if thread is None:
            return
        market_stream_runtime_stop_event.set()
        if thread.is_alive():
            thread.join(timeout=1.0)
        market_stream_runtime_thread = None

def _apply_mt5_time_offset_ms(value: int) -> int:
    """把 EA 快照里的 MT5 服务器 wall-clock 毫秒时间归一化成 UTC 毫秒时间。"""
    if value <= 0:
        return 0
    zoneinfo = _resolve_mt5_server_zoneinfo()
    wall_clock = datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    localized = wall_clock.replace(tzinfo=zoneinfo)
    return int(localized.astimezone(timezone.utc).timestamp() * 1000)


def _strip_mt5_time_offset_ms(value: int) -> int:
    """把 UTC 毫秒时间还原成 MT5 服务器 wall-clock 毫秒时间。"""
    if value <= 0:
        return 0
    zoneinfo = _resolve_mt5_server_zoneinfo()
    utc_time = datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
    server_local = utc_time.astimezone(zoneinfo)
    wall_clock_as_utc = datetime(
        server_local.year,
        server_local.month,
        server_local.day,
        server_local.hour,
        server_local.minute,
        server_local.second,
        server_local.microsecond,
        tzinfo=timezone.utc,
    )
    return int(wall_clock_as_utc.timestamp() * 1000)


def _resolve_mt5_server_zoneinfo() -> ZoneInfo:
    """解析 MT5 服务器时区配置；缺失或非法时直接抛出明确错误。"""
    server_timezone = str(MT5_SERVER_TIMEZONE or "").strip()
    if not server_timezone:
        raise ValueError("MT5_SERVER_TIMEZONE 未配置，无法把 MT5 原始时间统一归一化为 UTC。")
    try:
        return ZoneInfo(server_timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(
            f"MT5_SERVER_TIMEZONE 无法解析：{server_timezone}。"
            "请确认服务器已安装 IANA 时区数据；Windows 部署包需随网关安装 tzdata 依赖。"
        ) from exc
    except Exception as exc:
        raise ValueError(f"无效的 MT5_SERVER_TIMEZONE 配置：{server_timezone}") from exc


def _resolve_gateway_readiness_error() -> str:
    """严格检查网关关键配置是否齐备，避免坏部署被健康检查误判成可用。"""
    if not _is_mt5_configured():
        return "MT5 登录凭据未配置，网关不会连接 MT5。"
    if not str(MT5_SERVER_TIMEZONE or "").strip():
        return "MT5_SERVER_TIMEZONE 未配置，账户接口不可用。"
    return ""


def _deal_time_ms(deal: Any) -> int:
    value = int(getattr(deal, "time_msc", 0) or 0)
    if value > 0:
        return _apply_mt5_time_offset_ms(value)
    return _apply_mt5_time_offset_ms(int(getattr(deal, "time", 0) or 0) * 1000)


def _deal_sort_timestamp_ms(deal: Any) -> int:
    """统一用毫秒级成交时间排序，避免同一秒内多笔成交的先后顺序丢失。"""
    value = int(getattr(deal, "time_msc", 0) or 0)
    if value > 0:
        return value
    return int(getattr(deal, "time", 0) or 0) * 1000


def _position_time_ms(position: Any) -> int:
    """统一把 MT5 持仓开仓时间转换为展示口径毫秒时间。"""
    value = int(getattr(position, "time_msc", 0) or 0)
    if value > 0:
        return _apply_mt5_time_offset_ms(value)
    return _apply_mt5_time_offset_ms(int(getattr(position, "time", 0) or 0) * 1000)


def _order_open_time_ms(order: Any) -> int:
    """统一把 MT5 挂单创建时间转换为展示口径毫秒时间。"""
    value = int(getattr(order, "time_setup_msc", 0) or 0)
    if value > 0:
        return _apply_mt5_time_offset_ms(value)
    return _apply_mt5_time_offset_ms(int(getattr(order, "time_setup", 0) or 0) * 1000)


def _trade_entry_phase_rank(entry_type: int) -> int:
    """统一成交生命周期顺序：开仓在前，反手居中，平仓在后。"""
    opens = _is_entry_open(entry_type)
    closes = _is_entry_close(entry_type)
    if opens and not closes:
        return 0
    if opens and closes:
        return 1
    if closes:
        return 2
    return 3


def _deal_sort_key(deal: Any) -> Tuple[int, int, int]:
    """统一 MT5 原始成交排序键，避免同一毫秒内开平仓顺序受输入顺序影响。"""
    entry_type = int(getattr(deal, "entry", 0) or 0)
    ticket = int(getattr(deal, "ticket", 0) or 0)
    return _deal_sort_timestamp_ms(deal), _trade_entry_phase_rank(entry_type), ticket


def _trade_record_sort_key(record: Dict[str, Any]) -> Tuple[int, int, int]:
    """统一 EA 原始成交记录排序键，保证同一时刻先处理开仓再处理平仓。"""
    timestamp = int(record.get("timestamp", record.get("time", 0)) or 0)
    entry_type = int(record.get("entryType", record.get("entry", 0)) or 0)
    ticket = int(record.get("dealTicket", record.get("ticket", 0)) or 0)
    return timestamp, _trade_entry_phase_rank(entry_type), ticket


def _deal_history_sort_key(item: Dict[str, Any]) -> Tuple[int, int, int, int]:
    """统一历史成交重放排序键，避免消费链再次受输入顺序影响。"""
    timestamp = int(item.get("timestamp", item.get("time", 0)) or 0)
    entry_type = int(item.get("entryType", item.get("entry", 0)) or 0)
    position_id = int(item.get("position_id", item.get("positionId", 0)) or 0)
    ticket = int(item.get("dealTicket", item.get("ticket", 0)) or 0)
    return timestamp, _trade_entry_phase_rank(entry_type), position_id, ticket


# 统一裁剪缓存条目，只保留最近访问的部分，避免快照缓存长期堆满内存。
def _trim_cache_entries_locked(cache: Dict[str, Any], limit: int) -> None:
    while len(cache) > max(1, limit):
        oldest_key = next(iter(cache))
        cache.pop(oldest_key, None)


# 统一写入有序缓存，命中时会刷新顺序，便于按最近使用裁剪。
def _remember_cache_entry_locked(cache: Dict[str, Any], key: str, value: Any, limit: int) -> None:
    cache.pop(key, None)
    cache[key] = value
    _trim_cache_entries_locked(cache, limit)


# 统一生成行情 K 线查询缓存键，避免同窗口短时间重复打上游。
def _build_market_candles_cache_key(symbol: str,
                                    interval: str,
                                    limit: int,
                                    start_time_ms: int,
                                    end_time_ms: int) -> str:
    return "|".join(
        [
            str(symbol or "").strip().upper(),
            str(interval or "").strip(),
            str(max(1, int(limit or 0))),
            str(max(0, int(start_time_ms or 0))),
            str(max(0, int(end_time_ms or 0))),
        ]
    )


# 行情 K 线短缓存：同一窗口短时间直接复用，减小手机反复切页时的上游压力。
def _get_cached_market_candle_rows(cache_key: str, now_ms: int) -> Optional[List[Any]]:
    with snapshot_cache_lock:
        cached = market_candles_cache.get(cache_key)
        if not cached:
            return None
        if now_ms - int(cached.get("builtAt", 0) or 0) > MARKET_CANDLES_CACHE_MS:
            market_candles_cache.pop(cache_key, None)
            return None
        cached["lastAccessAt"] = now_ms
        _remember_cache_entry_locked(
            market_candles_cache,
            cache_key,
            cached,
            MARKET_CANDLES_CACHE_MAX_ENTRIES,
        )
        return [list(item) if isinstance(item, (list, tuple)) else dict(item) for item in (cached.get("rows") or [])]


# 统一写入行情 K 线短缓存，命中时刷新最近使用顺序。
def _remember_market_candle_rows(cache_key: str, rows: List[Any], now_ms: int) -> None:
    normalized_rows: List[Any] = []
    for item in rows or []:
        if isinstance(item, (list, tuple)):
            normalized_rows.append(list(item))
        elif isinstance(item, dict):
            normalized_rows.append(dict(item))
        else:
            normalized_rows.append(item)
    with snapshot_cache_lock:
        _remember_cache_entry_locked(
            market_candles_cache,
            cache_key,
            {
                "builtAt": now_ms,
                "lastAccessAt": now_ms,
                "rows": normalized_rows,
            },
            MARKET_CANDLES_CACHE_MAX_ENTRIES,
        )


# 账户真值唯一化后，只允许复用来自 MT5 Python Pull 的标准账户快照。
def _is_mt5_pull_account_snapshot(snapshot: Optional[Dict[str, Any]]) -> bool:
    source = str(((snapshot or {}).get("accountMeta") or {}).get("source", ""))
    return source == "MT5 Python Pull"


# 同步增量链也只允许承接 canonical MT5 快照，避免旧 EA 状态继续参与 seq/delta 计算。
def _sanitize_mt5_pull_sync_state(state: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(state, dict):
        return None
    snapshot = state.get("snapshot")
    if not _is_mt5_pull_account_snapshot(snapshot):
        return None
    sanitized = dict(state)
    previous_snapshot = sanitized.get("previousSnapshot")
    if previous_snapshot is not None and not _is_mt5_pull_account_snapshot(previous_snapshot):
        sanitized["previousSnapshot"] = None
        sanitized["previousSeq"] = 0
    return sanitized


# 第 2 步收口后不再滑动延长任何旧账户缓存寿命，超过 TTL 就必须重建。
def _should_slide_snapshot_build_cache(cached: Optional[Dict[str, Any]], now_ms: int) -> bool:
    return False


def _fmt_money(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):,.2f}"


def _fmt_usd(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def _safe_div(a: float, b: float) -> float:
    if abs(b) < 1e-9:
        return 0.0
    return a / b


def _normalize_market_symbol(symbol: str) -> str:
    value = str(symbol or "").strip().upper()
    if value in {"BTC", "BTCUSD", MARKET_SYMBOL_BTC}:
        return MARKET_SYMBOL_BTC
    if value in {"XAU", "XAUUSD", MARKET_SYMBOL_XAU}:
        return MARKET_SYMBOL_XAU
    return value


def _resolve_symbol_descriptor(symbol: str) -> Dict[str, str]:
    normalized_market_symbol = _normalize_market_symbol(symbol)
    if normalized_market_symbol == MARKET_SYMBOL_BTC:
        return {
            "productId": "BTC",
            "marketSymbol": MARKET_SYMBOL_BTC,
            "tradeSymbol": "BTCUSD",
        }
    if normalized_market_symbol == MARKET_SYMBOL_XAU:
        return {
            "productId": "XAU",
            "marketSymbol": MARKET_SYMBOL_XAU,
            "tradeSymbol": "XAUUSD",
        }
    raw_symbol = str(symbol or "").strip().upper()
    return {
        "productId": raw_symbol,
        "marketSymbol": normalized_market_symbol,
        "tradeSymbol": raw_symbol,
    }


def _normalize_abnormal_symbol(symbol: str) -> str:
    return _normalize_market_symbol(symbol)


def _default_abnormal_symbol_config(symbol: str) -> Dict[str, Any]:
    normalized = _normalize_abnormal_symbol(symbol)
    if normalized == MARKET_SYMBOL_XAU:
        return {
            "symbol": normalized,
            "volumeThreshold": 3000.0,
            "amountThreshold": 15000000.0,
            "priceChangeThreshold": 10.0,
            "volumeEnabled": True,
            "amountEnabled": True,
            "priceChangeEnabled": True,
        }
    return {
        "symbol": MARKET_SYMBOL_BTC,
        "volumeThreshold": 1000.0,
        "amountThreshold": 70000000.0,
        "priceChangeThreshold": 200.0,
        "volumeEnabled": True,
        "amountEnabled": True,
        "priceChangeEnabled": True,
    }


def _abnormal_bool(value: Any, fallback: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return fallback


def _abnormal_float(value: Any, fallback: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(fallback)


def _sanitize_abnormal_symbol_config(symbol: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    defaults = _default_abnormal_symbol_config(symbol)
    return {
        "symbol": defaults["symbol"],
        "volumeThreshold": max(0.0, _abnormal_float(payload.get("volumeThreshold"), defaults["volumeThreshold"])),
        "amountThreshold": max(0.0, _abnormal_float(payload.get("amountThreshold"), defaults["amountThreshold"])),
        "priceChangeThreshold": max(0.0, _abnormal_float(payload.get("priceChangeThreshold"), defaults["priceChangeThreshold"])),
        "volumeEnabled": _abnormal_bool(payload.get("volumeEnabled"), defaults["volumeEnabled"]),
        "amountEnabled": _abnormal_bool(payload.get("amountEnabled"), defaults["amountEnabled"]),
        "priceChangeEnabled": _abnormal_bool(payload.get("priceChangeEnabled"), defaults["priceChangeEnabled"]),
    }


def _ensure_abnormal_defaults_locked() -> None:
    symbols = abnormal_config_state.setdefault("symbols", {})
    for symbol in ABNORMAL_SYMBOLS:
        normalized = _normalize_abnormal_symbol(symbol)
        if normalized not in symbols:
            symbols[normalized] = _default_abnormal_symbol_config(normalized)


def _copy_abnormal_config_locked() -> Dict[str, Any]:
    _ensure_abnormal_defaults_locked()
    return {
        "logicAnd": bool(abnormal_config_state.get("logicAnd", False)),
        "symbols": {
            symbol: dict(config)
            for symbol, config in (abnormal_config_state.get("symbols") or {}).items()
        },
    }


def _abnormal_snapshot_digest(snapshot: Dict[str, Any]) -> str:
    payload = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def _build_abnormal_snapshot_locked() -> Dict[str, Any]:
    config_snapshot = _copy_abnormal_config_locked()
    return {
        "abnormalMeta": {
            "updatedAt": _now_ms(),
            "logicAnd": bool(config_snapshot.get("logicAnd", False)),
            "recordCount": len(abnormal_record_store),
            "alertCount": len(abnormal_alert_store),
        },
        "configs": [
            dict(config_snapshot["symbols"][symbol])
            for symbol in ABNORMAL_SYMBOLS
            if symbol in config_snapshot["symbols"]
        ],
        "records": [dict(item) for item in abnormal_record_store],
        "alerts": [dict(item) for item in abnormal_alert_store],
    }


def _commit_abnormal_snapshot_locked() -> None:
    snapshot = _build_abnormal_snapshot_locked()
    digest = _abnormal_snapshot_digest(snapshot)
    state = abnormal_sync_state
    if not state:
        abnormal_sync_state.update({
            "seq": 1,
            "digest": digest,
            "snapshot": snapshot,
            "previousSeq": 0,
            "previousSnapshot": None,
        })
        return
    if state.get("digest") == digest:
        abnormal_sync_state["snapshot"] = snapshot
        return
    previous_seq = int(state.get("seq", 0))
    previous_snapshot = state.get("snapshot")
    abnormal_sync_state.update({
        "seq": previous_seq + 1,
        "digest": digest,
        "snapshot": snapshot,
        "previousSeq": previous_seq,
        "previousSnapshot": previous_snapshot,
    })


def _build_full_abnormal_response(snapshot: Dict[str, Any], sync_seq: int) -> Dict[str, Any]:
    meta = dict(snapshot.get("abnormalMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = ABNORMAL_DELTA_ENABLED
    last_error = str(abnormal_sync_state.get("lastError", "") or "")
    if last_error:
        meta["warning"] = last_error
    return {
        "abnormalMeta": meta,
        "configs": snapshot.get("configs") or [],
        "records": snapshot.get("records") or [],
        "alerts": snapshot.get("alerts") or [],
        "isDelta": False,
        "unchanged": False,
    }


def _diff_abnormal_items(previous: List[Dict[str, Any]], current: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    previous_ids = {str(item.get("id", "")) for item in (previous or [])}
    return [dict(item) for item in (current or []) if str(item.get("id", "")) not in previous_ids]


def _parse_recent_closed_kline(row: List[Any], symbol: str, now_ms: int) -> Optional[Dict[str, Any]]:
    if row is None or len(row) < 8:
        return None
    try:
        open_time = int(row[0])
        close_time = int(row[6])
        if close_time <= 0 or close_time >= now_ms:
            return None
        open_price = float(row[1])
        close_price = float(row[4])
        volume = float(row[5])
        amount = float(row[7])
    except Exception:
        return None
    price_change = abs(close_price - open_price)
    percent_change = abs(_safe_div(close_price - open_price, open_price) * 100.0)
    return {
        "symbol": _normalize_abnormal_symbol(symbol),
        "openTime": open_time,
        "closeTime": close_time,
        "openPrice": open_price,
        "closePrice": close_price,
        "volume": volume,
        "amount": amount,
        "priceChange": price_change,
        "percentChange": percent_change,
    }


def _fetch_recent_closed_binance_klines(symbol: str, limit: int = ABNORMAL_KLINE_LIMIT) -> List[Dict[str, Any]]:
    normalized_symbol = _normalize_abnormal_symbol(symbol)
    now_ms = _now_ms()
    with abnormal_state_lock:
        cached = abnormal_kline_cache.get(normalized_symbol)
        if cached and now_ms - int(cached.get("fetchedAt", 0) or 0) <= ABNORMAL_FETCH_CACHE_MS:
            return [dict(item) for item in (cached.get("items") or [])]

    upstream_url = _build_binance_rest_upstream_url(
        "/fapi/v1/klines",
        {"symbol": normalized_symbol, "interval": "1m", "limit": max(2, limit)},
    )
    request = urllib.request.Request(
        upstream_url,
        headers={"User-Agent": "mt5-gateway-abnormal-monitor/1.0"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=12) as response:
        body = response.read().decode("utf-8")
    rows = json.loads(body or "[]")
    items: List[Dict[str, Any]] = []
    for row in rows:
        item = _parse_recent_closed_kline(row, normalized_symbol, now_ms)
        if item is not None:
            items.append(item)
    items.sort(key=lambda current: int(current.get("closeTime", 0) or 0))
    with abnormal_state_lock:
        abnormal_kline_cache[normalized_symbol] = {
            "fetchedAt": now_ms,
            "items": [dict(item) for item in items],
        }
    return items


def _resolve_abnormal_fetch_limit(last_close_time: int, now_ms: int) -> int:
    """按断档分钟数动态放大异常补抓窗口，避免长时间断连后丢远期黄点。"""
    base_limit = max(2, ABNORMAL_KLINE_LIMIT)
    if last_close_time <= 0 or now_ms <= last_close_time:
        return base_limit
    gap_minutes = int(math.ceil(max(0, now_ms - last_close_time) / 60_000.0))
    # 多抓 2 根，给“上一根已处理 + 当前最新闭合根”留出缓冲。
    return max(base_limit, min(1500, gap_minutes + 2))


def _evaluate_abnormal_kline(kline: Dict[str, Any], config: Dict[str, Any], use_and_mode: bool) -> Dict[str, Any]:
    enabled_count = 0
    triggered: List[str] = []
    if config.get("volumeEnabled", False):
        enabled_count += 1
        if float(kline.get("volume", 0.0) or 0.0) >= float(config.get("volumeThreshold", 0.0) or 0.0):
            triggered.append("成交量")
    if config.get("amountEnabled", False):
        enabled_count += 1
        if float(kline.get("amount", 0.0) or 0.0) >= float(config.get("amountThreshold", 0.0) or 0.0):
            triggered.append("成交额")
    if config.get("priceChangeEnabled", False):
        enabled_count += 1
        if float(kline.get("priceChange", 0.0) or 0.0) >= float(config.get("priceChangeThreshold", 0.0) or 0.0):
            triggered.append("价格变化")
    if enabled_count == 0:
        return {"participating": False, "abnormal": False, "summary": ""}
    abnormal = len(triggered) == enabled_count if use_and_mode else bool(triggered)
    return {"participating": True, "abnormal": abnormal, "summary": " / ".join(triggered)}


def _build_abnormal_record(symbol: str, kline: Dict[str, Any], summary: str) -> Dict[str, Any]:
    normalized_symbol = _normalize_abnormal_symbol(symbol)
    close_time = int(kline.get("closeTime", 0) or 0)
    record_id = hashlib.sha1(f"{normalized_symbol}:{close_time}:{summary}".encode("utf-8")).hexdigest()
    return {
        "id": record_id,
        "symbol": normalized_symbol,
        "timestamp": _now_ms(),
        "closeTime": close_time,
        "openPrice": float(kline.get("openPrice", 0.0) or 0.0),
        "closePrice": float(kline.get("closePrice", 0.0) or 0.0),
        "volume": float(kline.get("volume", 0.0) or 0.0),
        "amount": float(kline.get("amount", 0.0) or 0.0),
        "priceChange": float(kline.get("priceChange", 0.0) or 0.0),
        "percentChange": float(kline.get("percentChange", 0.0) or 0.0),
        "triggerSummary": summary or "",
    }


def _compose_abnormal_alert_line(asset: str, trigger_summary: str) -> str:
    return f"{asset} 的 {trigger_summary} 出现异常！"


def _is_abnormal_alert_eligible_locked(record: Optional[Dict[str, Any]], now_ms: int) -> bool:
    if not record:
        return False
    symbol = _normalize_abnormal_symbol(record.get("symbol"))
    last_notify = int(abnormal_last_notify_at.get(symbol, 0) or 0)
    return now_ms - last_notify >= 5 * 60 * 1000


def _build_abnormal_alert(symbols: List[str], close_time: int, content: str) -> Dict[str, Any]:
    normalized_symbols = sorted({_normalize_abnormal_symbol(symbol) for symbol in (symbols or []) if symbol})
    alert_key = ",".join(normalized_symbols)
    alert_id = hashlib.sha1(f"{alert_key}:{close_time}:{content}".encode("utf-8")).hexdigest()
    return {
        "id": alert_id,
        "symbols": normalized_symbols,
        "title": "异常提醒",
        "content": content,
        "closeTime": int(close_time or 0),
        "createdAt": _now_ms(),
    }


def _build_abnormal_alerts_locked(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[int, Dict[str, Dict[str, Any]]] = {}
    for record in records or []:
        close_time = int(record.get("closeTime", 0) or 0)
        if close_time <= 0:
            continue
        grouped.setdefault(close_time, {})[_normalize_abnormal_symbol(record.get("symbol"))] = record

    created: List[Dict[str, Any]] = []
    for close_time in sorted(grouped.keys()):
        current_group = grouped.get(close_time) or {}
        now_ms = _now_ms()
        btc_record = current_group.get(MARKET_SYMBOL_BTC)
        xau_record = current_group.get(MARKET_SYMBOL_XAU)
        btc_eligible = _is_abnormal_alert_eligible_locked(btc_record, now_ms)
        xau_eligible = _is_abnormal_alert_eligible_locked(xau_record, now_ms)
        if btc_eligible and xau_eligible:
            content = _compose_abnormal_alert_line("BTC", str(btc_record.get("triggerSummary", "")))
            content += "\n" + _compose_abnormal_alert_line("XAU", str(xau_record.get("triggerSummary", "")))
            created.append(_build_abnormal_alert([MARKET_SYMBOL_BTC, MARKET_SYMBOL_XAU], close_time, content))
            abnormal_last_notify_at[MARKET_SYMBOL_BTC] = now_ms
            abnormal_last_notify_at[MARKET_SYMBOL_XAU] = now_ms
            continue
        if btc_eligible and btc_record:
            created.append(_build_abnormal_alert(
                [MARKET_SYMBOL_BTC],
                close_time,
                _compose_abnormal_alert_line("BTC", str(btc_record.get("triggerSummary", ""))),
            ))
            abnormal_last_notify_at[MARKET_SYMBOL_BTC] = now_ms
        if xau_eligible and xau_record:
            created.append(_build_abnormal_alert(
                [MARKET_SYMBOL_XAU],
                close_time,
                _compose_abnormal_alert_line("XAU", str(xau_record.get("triggerSummary", ""))),
            ))
            abnormal_last_notify_at[MARKET_SYMBOL_XAU] = now_ms
    return created


def _append_abnormal_updates_locked(records: List[Dict[str, Any]]) -> None:
    existing_record_ids = {str(item.get("id", "")) for item in abnormal_record_store}
    appended_records: List[Dict[str, Any]] = []
    for record in sorted(records or [], key=lambda item: (int(item.get("closeTime", 0) or 0), str(item.get("symbol", "")))):
        record_id = str(record.get("id", ""))
        if not record_id or record_id in existing_record_ids:
            continue
        abnormal_record_store.insert(0, dict(record))
        existing_record_ids.add(record_id)
        appended_records.append(dict(record))
    if appended_records:
        del abnormal_record_store[ABNORMAL_RECORD_LIMIT:]

    existing_alert_ids = {str(item.get("id", "")) for item in abnormal_alert_store}
    for alert in _build_abnormal_alerts_locked(appended_records):
        alert_id = str(alert.get("id", ""))
        if not alert_id or alert_id in existing_alert_ids:
            continue
        abnormal_alert_store.insert(0, dict(alert))
        existing_alert_ids.add(alert_id)
    if abnormal_alert_store:
        del abnormal_alert_store[ABNORMAL_ALERT_LIMIT:]

    _commit_abnormal_snapshot_locked()


def _set_abnormal_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    with abnormal_state_lock:
        _ensure_abnormal_defaults_locked()
        current = _copy_abnormal_config_locked()
        normalized = {
            "logicAnd": _abnormal_bool((payload or {}).get("logicAnd"), current.get("logicAnd", False)),
            "symbols": {symbol: _default_abnormal_symbol_config(symbol) for symbol in ABNORMAL_SYMBOLS},
        }
        raw_configs = (payload or {}).get("configs")
        if isinstance(raw_configs, list):
            for item in raw_configs:
                if not isinstance(item, dict):
                    continue
                symbol = _normalize_abnormal_symbol(item.get("symbol"))
                if symbol not in normalized["symbols"]:
                    continue
                normalized["symbols"][symbol] = _sanitize_abnormal_symbol_config(symbol, item)
        changed = normalized != current
        if changed:
            abnormal_config_state["logicAnd"] = normalized["logicAnd"]
            abnormal_config_state["symbols"] = normalized["symbols"]
            _commit_abnormal_snapshot_locked()
        return {
            "ok": True,
            "changed": changed,
            "config": {
                "logicAnd": normalized["logicAnd"],
                "configs": [dict(normalized["symbols"][symbol]) for symbol in ABNORMAL_SYMBOLS],
            },
        }


def _refresh_abnormal_state() -> None:
    try:
        with abnormal_state_lock:
            _ensure_abnormal_defaults_locked()
            config_snapshot = _copy_abnormal_config_locked()
            last_close_by_symbol = dict(abnormal_last_close_time_by_symbol)
            now_ms = _now_ms()

        new_records: List[Dict[str, Any]] = []
        for symbol in ABNORMAL_SYMBOLS:
            fetch_limit = _resolve_abnormal_fetch_limit(int(last_close_by_symbol.get(symbol, 0) or 0), now_ms)
            recent_klines = _fetch_recent_closed_binance_klines(symbol, fetch_limit)
            symbol_config = (config_snapshot.get("symbols") or {}).get(symbol) or _default_abnormal_symbol_config(symbol)
            last_close_time = int(last_close_by_symbol.get(symbol, 0) or 0)
            for kline in recent_klines:
                close_time = int(kline.get("closeTime", 0) or 0)
                if close_time <= last_close_time:
                    continue
                evaluation = _evaluate_abnormal_kline(kline, symbol_config, bool(config_snapshot.get("logicAnd", False)))
                if evaluation.get("abnormal"):
                    new_records.append(_build_abnormal_record(symbol, kline, str(evaluation.get("summary", ""))))
                last_close_time = max(last_close_time, close_time)
            if last_close_time > 0:
                last_close_by_symbol[symbol] = last_close_time

        with abnormal_state_lock:
            for symbol, close_time in last_close_by_symbol.items():
                abnormal_last_close_time_by_symbol[symbol] = max(
                    int(abnormal_last_close_time_by_symbol.get(symbol, 0) or 0),
                    int(close_time or 0),
                )
            abnormal_sync_state.pop("lastError", None)
            if new_records or not abnormal_sync_state:
                _append_abnormal_updates_locked(new_records)
    except Exception as exc:
        with abnormal_state_lock:
            _ensure_abnormal_defaults_locked()
            if not abnormal_sync_state:
                _commit_abnormal_snapshot_locked()
            abnormal_sync_state["lastError"] = str(exc)


def _build_abnormal_response(since_seq: int, delta: bool) -> Dict[str, Any]:
    with abnormal_state_lock:
        _ensure_abnormal_defaults_locked()
        if not abnormal_sync_state:
            _commit_abnormal_snapshot_locked()
        snapshot = abnormal_sync_state.get("snapshot") or _build_abnormal_snapshot_locked()
        sync_seq = int(abnormal_sync_state.get("seq", 1) or 1)
        previous_seq = int(abnormal_sync_state.get("previousSeq", 0) or 0)
        previous_snapshot = abnormal_sync_state.get("previousSnapshot")

    if not ABNORMAL_DELTA_ENABLED or not delta or since_seq <= 0:
        return _build_full_abnormal_response(snapshot, sync_seq)

    meta = dict(snapshot.get("abnormalMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = ABNORMAL_DELTA_ENABLED
    last_error = str(abnormal_sync_state.get("lastError", "") or "")
    if last_error:
        meta["warning"] = last_error

    if since_seq == sync_seq:
        return {"abnormalMeta": meta, "isDelta": True, "unchanged": True}

    if previous_snapshot is not None and since_seq == previous_seq:
        return {
            "abnormalMeta": meta,
            "isDelta": True,
            "unchanged": False,
            "delta": {
                "records": _diff_abnormal_items(previous_snapshot.get("records") or [], snapshot.get("records") or []),
                "alerts": _diff_abnormal_items(previous_snapshot.get("alerts") or [], snapshot.get("alerts") or []),
            },
        }

    return _build_full_abnormal_response(snapshot, sync_seq)


def _build_v2_abnormal_snapshot_response() -> Dict[str, Any]:
    with abnormal_state_lock:
        _ensure_abnormal_defaults_locked()
        if not abnormal_sync_state:
            _commit_abnormal_snapshot_locked()
        snapshot = abnormal_sync_state.get("snapshot") or _build_abnormal_snapshot_locked()
        sync_seq = int(abnormal_sync_state.get("seq", 1) or 1)

    payload = _clone_json_value(snapshot)
    meta = dict(payload.get("abnormalMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = ABNORMAL_DELTA_ENABLED
    payload["abnormalMeta"] = meta
    return payload


def _build_v2_abnormal_history_response(symbol: str, start_time: int, end_time: int, limit: int) -> Dict[str, Any]:
    normalized_symbol = _normalize_abnormal_symbol(symbol) if str(symbol or "").strip() else ""
    start_value = int(start_time or 0)
    end_value = int(end_time or 0)
    safe_limit = max(1, min(5000, int(limit or ABNORMAL_RECORD_LIMIT)))
    with abnormal_state_lock:
        _ensure_abnormal_defaults_locked()
        if not abnormal_sync_state:
            _commit_abnormal_snapshot_locked()
        sync_seq = int(abnormal_sync_state.get("seq", 1) or 1)
        records = [_clone_json_value(item) for item in abnormal_record_store]

    filtered: List[Dict[str, Any]] = []
    for record in records:
        current_symbol = _normalize_abnormal_symbol(record.get("symbol"))
        close_time = int(record.get("closeTime", 0) or 0)
        if normalized_symbol and current_symbol != normalized_symbol:
            continue
        if start_value > 0 and close_time < start_value:
            continue
        if end_value > 0 and close_time > end_value:
            continue
        filtered.append(record)
        if len(filtered) >= safe_limit:
            break
    return {
        "abnormalMeta": {
            "syncSeq": sync_seq,
            "deltaEnabled": ABNORMAL_DELTA_ENABLED,
        },
        "symbol": normalized_symbol,
        "startTime": start_value,
        "endTime": end_value,
        "limit": safe_limit,
        "records": filtered,
    }


def _is_buy_trade_type(trade_type: int) -> bool:
    return trade_type in (0, 2, 4, 6)


def _order_side(order_type: int) -> str:
    return "Buy" if _is_buy_trade_type(order_type) else "Sell"


def _is_trade_deal_type(deal_type: int) -> bool:
    # MT5 deal type: 0=BUY, 1=SELL
    return deal_type in (0, 1)


def _is_buy_deal_type(deal_type: int) -> bool:
    return deal_type == 0


def _is_entry_open(entry_type: int) -> bool:
    # MT5 entry: 0=IN, 2=INOUT
    return entry_type in (0, 2)


def _is_entry_close(entry_type: int) -> bool:
    # MT5 entry: 1=OUT, 2=INOUT, 3=OUT_BY
    return entry_type in (1, 2, 3)


def _curve_exposure_side_for_close(deal_type: int) -> str:
    # 平仓成交方向与原持仓方向相反，关闭曝光时需要回到原持仓侧。
    return "Sell" if _is_buy_deal_type(deal_type) else "Buy"


def _range_hours(range_key: str) -> int:
    mapping = {
        "1d": 24,
        "7d": 24 * 7,
        "1m": 24 * 30,
        "3m": 24 * 90,
        "1y": 24 * 365,
        "all": 24 * SNAPSHOT_RANGE_ALL_DAYS,
    }
    return mapping.get(range_key.lower(), 24 * 7)


# MT5 Python 历史接口对“本地无时区 datetime”更稳定，避免 UTC aware 时间把当天后半段成交截掉。
def _mt5_history_window(range_key: str) -> Tuple[datetime, datetime]:
    now_local = datetime.now()
    to_time = now_local + timedelta(hours=MT5_HISTORY_LOOKAHEAD_HOURS)
    from_time = now_local - timedelta(hours=_range_hours(range_key))
    return from_time, to_time


def _light_snapshot_trade_count() -> int:
    """按全量历史同口径计算轻快照成交数，保证和 history 接口真值一致。"""
    trade_state = _light_snapshot_trade_state()
    return int(trade_state.get("tradeCount", 0) or 0)


def _light_snapshot_trade_state() -> Dict[str, Any]:
    """按全量历史同口径生成轻快照成交状态，避免轻/全量口径分叉。"""
    raw_deals = _progressive_trade_history_deals("all")
    trades = _map_trade_deals(raw_deals)
    return {
        "tradeCount": len(trades),
        "historyRevision": _build_history_revision_from_trades(trades),
    }


def _parse_login_value(raw_login: Any) -> int:
    """把账号值转换成 int，失败时返回 0。"""
    try:
        return int(str(raw_login or "").strip())
    except Exception:
        return 0


def _set_runtime_session_credentials(login: Any, password: Any, server: Any) -> None:
    """更新当前会话的运行时凭据（仅内存）。"""
    with state_lock:
        session_runtime_credentials.clear()
        session_runtime_credentials.update(
            {
                "mode": "remote_active",
                "login": _parse_login_value(login),
                "password": str(password or ""),
                "server": str(server or "").strip(),
            }
        )


def _clear_runtime_session_credentials() -> None:
    """清理当前会话的运行时凭据（仅内存）。"""
    with state_lock:
        session_runtime_credentials.clear()
        session_runtime_credentials.update(
            {
                "mode": "remote_logged_out",
                "login": 0,
                "password": "",
                "server": "",
            }
        )


def _runtime_session_credentials_snapshot() -> Dict[str, Any]:
    """读取运行时凭据快照。"""
    with state_lock:
        return dict(session_runtime_credentials)


def _current_mt5_credentials() -> Dict[str, Any]:
    """读取当前 MT5 凭据，统一处理 env 与远程会话模式。"""
    env_login = _parse_login_value(LOGIN)
    env_password = str(PASSWORD or "")
    env_server = str(SERVER or "").strip()
    runtime = _runtime_session_credentials_snapshot()
    mode = str(runtime.get("mode") or "env_default")
    runtime_login = _parse_login_value(runtime.get("login"))
    runtime_password = str(runtime.get("password") or "")
    runtime_server = str(runtime.get("server") or "").strip()

    if mode == "remote_active":
        return {
            "login": runtime_login,
            "password": runtime_password,
            "server": runtime_server,
            "mode": "remote_active",
            "source": "remote_active",
        }

    if mode == "remote_logged_out":
        return {
            "login": 0,
            "password": "",
            "server": "",
            "mode": "remote_logged_out",
            "source": "remote_logged_out",
        }

    return {
        "login": env_login,
        "password": env_password,
        "server": env_server,
        "mode": "env_default",
        "source": "env_default",
    }


def _parse_bool_flag(value: Any, default: bool = False) -> bool:
    """严格解析布尔开关，避免字符串 'false' 被当成真值。"""
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


def _is_mt5_configured() -> bool:
    credentials = _current_mt5_credentials()
    return int(credentials.get("login") or 0) > 0 and bool(credentials.get("password")) and bool(credentials.get("server"))


def _normalize_path(raw: str) -> str:
    value = (raw or "").strip().strip('"').strip("'")
    if not value:
        return ""
    expanded = Path(os.path.expandvars(os.path.expanduser(value)))
    try:
        return str(expanded.resolve())
    except Exception:
        return str(expanded)


def _ps_quote(value: str) -> str:
    """PowerShell 单引号转义，保证路径在远端命令里不被截断。"""
    return "'" + str(value or "").replace("'", "''") + "'"


def _normalize_windows_executable_identity(path_value: str) -> str:
    """统一成 PowerShell 进程查询使用的路径格式。"""
    normalized = _normalize_path(path_value)
    if not normalized:
        return ""
    return normalized.replace("\\", "/").lower()


def _build_mt5_terminal_stop_command(path_value: str, wait_timeout_ms: int = 15000) -> str:
    """按精确 exe 路径生成 MT5 终端停止命令，避免误伤其他同名终端。"""
    normalized_executable_path = _normalize_windows_executable_identity(path_value)
    if not normalized_executable_path:
        return "throw 'MT5 terminal path is not configured.'"
    wait_seconds = max(1, int(math.ceil(max(1, int(wait_timeout_ms or 0)) / 1000.0)))
    return (
        "$targetPath = "
        + _ps_quote(normalized_executable_path)
        + "; "
        + "$matchProcess = { "
        + "Get-CimInstance Win32_Process | Where-Object { "
        + "$normalizedExecutablePath = (([string]$_.ExecutablePath) -replace '\\\\', '/').ToLowerInvariant(); "
        + "$normalizedExecutablePath -eq $targetPath "
        + "} "
        + "}; "
        + "$matched = @(& $matchProcess); "
        + "$matched | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }; "
        + "$remaining = @(); "
        + "$deadline = [DateTime]::UtcNow.AddSeconds("
        + str(wait_seconds)
        + "); "
        + "do { "
        + "$remaining = @(& $matchProcess); "
        + "if ($remaining.Count -eq 0) { break }; "
        + "Start-Sleep -Milliseconds 200; "
        + "} while ([DateTime]::UtcNow -lt $deadline); "
        + "if ($remaining.Count -gt 0) { "
        + "$remainingIds = [string]::Join(',', @($remaining | ForEach-Object { [string]$_.ProcessId })); "
        + "throw ('MT5 terminal process still running after wait: ' + $remainingIds) "
        + "}; "
        + "@{ matchedCount = $matched.Count; remainingCount = $remaining.Count; terminalPath = $targetPath } | ConvertTo-Json -Compress"
    )


def _build_mt5_terminal_start_command(path_value: str, wait_timeout_ms: int = 15000) -> str:
    """按精确 exe 路径生成 MT5 终端启动命令，并等待目标进程真正出现。"""
    normalized_executable_path = _normalize_windows_executable_identity(path_value)
    resolved_path = _normalize_path(path_value)
    if not normalized_executable_path or not resolved_path:
        return "throw 'MT5 terminal path is not configured.'"
    working_directory = str(Path(resolved_path).parent)
    wait_seconds = max(1, int(math.ceil(max(1, int(wait_timeout_ms or 0)) / 1000.0)))
    return (
        "$targetPath = "
        + _ps_quote(normalized_executable_path)
        + "; "
        + "$targetExecutable = "
        + _ps_quote(resolved_path)
        + "; "
        + "$workingDirectory = "
        + _ps_quote(working_directory)
        + "; "
        + "$matchProcess = { "
        + "Get-CimInstance Win32_Process | Where-Object { "
        + "$normalizedExecutablePath = (([string]$_.ExecutablePath) -replace '\\\\', '/').ToLowerInvariant(); "
        + "$normalizedExecutablePath -eq $targetPath "
        + "} "
        + "}; "
        + "Start-Process -FilePath $targetExecutable -WorkingDirectory $workingDirectory; "
        + "$matched = @(); "
        + "$deadline = [DateTime]::UtcNow.AddSeconds("
        + str(wait_seconds)
        + "); "
        + "do { "
        + "$matched = @(& $matchProcess); "
        + "if ($matched.Count -gt 0) { break }; "
        + "Start-Sleep -Milliseconds 200; "
        + "} while ([DateTime]::UtcNow -lt $deadline); "
        + "if ($matched.Count -eq 0) { "
        + "throw ('MT5 terminal process did not appear after start: ' + $targetPath) "
        + "}; "
        + "@{ matchedCount = $matched.Count; terminalPath = $targetPath } | ConvertTo-Json -Compress"
    )


def _discover_terminal_candidates() -> List[str]:
    candidates: List[str] = []
    seen = set()

    def add(path_value: str) -> None:
        normalized = _normalize_path(path_value)
        if not normalized:
            return
        if normalized in seen:
            return
        if not Path(normalized).exists():
            return
        seen.add(normalized)
        candidates.append(normalized)

    if PATH:
        add(PATH)

    static_candidates = [
        r"C:\Program Files\MetaTrader 5\terminal64.exe",
        r"C:\Program Files (x86)\MetaTrader 5\terminal64.exe",
        r"C:\Program Files\IC Markets - MetaTrader 5 - 01\terminal64.exe",
        r"C:\Program Files\IC Markets - MetaTrader 5\terminal64.exe",
    ]
    for path_value in static_candidates:
        add(path_value)

    roots = []
    local_app_data = os.getenv("LOCALAPPDATA", "")
    app_data = os.getenv("APPDATA", "")
    if local_app_data:
        roots.append(Path(local_app_data) / "Programs")
        roots.append(Path(local_app_data) / "MetaQuotes" / "Terminal")
    if app_data:
        roots.append(Path(app_data) / "MetaQuotes" / "Terminal")

    for root in roots:
        if not root.exists():
            continue
        try:
            for child in root.iterdir():
                if not child.is_dir():
                    continue
                add(str(child / "terminal64.exe"))
                add(str(child / "terminal.exe"))
        except Exception:
            continue

    return candidates


MT5_TERMINAL_CANDIDATES = _discover_terminal_candidates()


def _resolve_direct_login_terminal_path() -> Tuple[Optional[str], str]:
    """解析直登链应操作的终端路径：优先显式配置，其次使用已发现候选。"""
    explicit_path = _normalize_path(str(PATH or ""))
    if explicit_path:
        return explicit_path, "configured_path"
    for candidate in MT5_TERMINAL_CANDIDATES:
        normalized_candidate = _normalize_path(candidate)
        if normalized_candidate:
            return normalized_candidate, "discovered_candidate"
    return None, "missing"


def _now_monotonic_ms() -> int:
    """返回单调时钟毫秒值，供切号耗时统计使用。"""
    return int(time.monotonic() * 1000)


def _build_mt5_gui_window_detection_command(path_value: Optional[str] = None) -> str:
    """检测当前机器上是否存在可附着的 MT5 终端进程。"""
    normalized_target_path = _normalize_windows_executable_identity(str(path_value or ""))
    return (
        "$targetPath = "
        + _ps_quote(normalized_target_path)
        + "; "
        + "$currentSessionId = [int](Get-Process -Id $PID -ErrorAction SilentlyContinue).SessionId; "
        + "$normalizePath = { "
        + "param([string]$value) "
        + "if ([string]::IsNullOrWhiteSpace($value)) { return '' } "
        + "return (($value -replace '\\\\', '/').ToLowerInvariant()) "
        + "}; "
        + "$items = @(); "
        + "Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { "
        + "$_.Name -eq 'terminal64.exe' -or $_.Name -eq 'terminal.exe' -or $_.Name -eq 'terminal64' -or $_.Name -eq 'terminal' "
        + "} | ForEach-Object { "
        + "$rawExecutablePath = [string]$_.ExecutablePath; "
        + "$normalizedExecutablePath = & $normalizePath ([string]$_.ExecutablePath); "
        + "$items += @{ "
        + "processId = [int]$_.ProcessId; "
        + "sessionId = [int]$_.SessionId; "
        + "sameSession = ([int]$_.SessionId -eq $currentSessionId); "
        + "executablePath = $rawExecutablePath; "
        + "normalizedExecutablePath = $normalizedExecutablePath; "
        + "exactPath = (-not [string]::IsNullOrWhiteSpace($targetPath)) -and ($normalizedExecutablePath -eq $targetPath) "
        + "} "
        + "}; "
        + "@{ "
        + "processCount = $items.Count; "
        + "sameSessionCount = @($items | Where-Object { $_.sameSession }).Count; "
        + "exactPathCount = @($items | Where-Object { $_.sameSession -and $_.exactPath }).Count; "
        + "items = $items "
        + "} | ConvertTo-Json -Compress"
    )


def _parse_mt5_gui_detection_payload(payload_text: str) -> Dict[str, Any]:
    """解析 MT5 终端检测结果。"""
    try:
        payload = json.loads(str(payload_text or "").strip() or "{}")
    except Exception:
        return {
            "processCount": 0,
            "sameSessionCount": 0,
            "exactPathCount": 0,
            "items": [],
        }
    if not isinstance(payload, dict):
        return {
            "processCount": 0,
            "sameSessionCount": 0,
            "exactPathCount": 0,
            "items": [],
        }
    items = payload.get("items")
    if not isinstance(items, list):
        items = []
    return {
        "processCount": int(payload.get("processCount") or 0),
        "sameSessionCount": int(payload.get("sameSessionCount") or 0),
        "exactPathCount": int(payload.get("exactPathCount") or 0),
        "items": items,
    }


def _inspect_mt5_gui_terminals(path_value: Optional[str] = None) -> Dict[str, Any]:
    """读取当前机器上 MT5 终端进程明细。"""
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            _build_mt5_gui_window_detection_command(path_value),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=15,
        check=False,
    )
    if completed.returncode != 0:
        return {
            "processCount": 0,
            "sameSessionCount": 0,
            "exactPathCount": 0,
            "items": [],
        }
    stdout_text = (completed.stdout or b"").decode("utf-8", errors="replace").strip()
    return _parse_mt5_gui_detection_payload(stdout_text)


def _resolve_attachable_mt5_gui_terminal_path(payload: Optional[Dict[str, Any]], preferred_path: Optional[str] = None) -> Optional[str]:
    """只把当前网关同会话、且可明确定位的 MT5 终端认作可附着目标。"""
    normalized_preferred_path = _normalize_path(str(preferred_path or ""))
    source = dict(payload or {})
    items = source.get("items")
    if not isinstance(items, list):
        items = []
    same_session_items: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if not bool(item.get("sameSession")):
            continue
        normalized_path = _normalize_path(str(item.get("executablePath") or ""))
        if not normalized_path:
            continue
        normalized_item = dict(item)
        normalized_item["normalizedExecutablePath"] = normalized_path
        same_session_items.append(normalized_item)
    if normalized_preferred_path:
        for item in same_session_items:
            if bool(item.get("exactPath")) or str(item.get("normalizedExecutablePath") or "") == normalized_preferred_path:
                return normalized_preferred_path
    if len(same_session_items) == 1:
        return str(same_session_items[0].get("normalizedExecutablePath") or "").strip() or None
    return None


def _detect_mt5_gui_window(path_value: Optional[str] = None) -> bool:
    """判断当前是否存在可附着的 MT5 终端进程。"""
    payload = _inspect_mt5_gui_terminals(path_value)
    return _resolve_attachable_mt5_gui_terminal_path(payload, preferred_path=path_value) is not None


def _launch_mt5_terminal_from_mt5_path() -> None:
    """按 MT5_PATH 拉起 MT5，不在这里等待窗口出现。"""
    terminal_path = _normalize_path(str(PATH or ""))
    if not terminal_path:
        raise RuntimeError("MT5_PATH is not configured")
    working_directory = str(Path(terminal_path).parent)
    start_command = (
        "$targetExecutable = "
        + _ps_quote(terminal_path)
        + "; "
        + "$workingDirectory = "
        + _ps_quote(working_directory)
        + "; "
        + "Start-Process -FilePath $targetExecutable -WorkingDirectory $workingDirectory"
    )
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            start_command,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=15,
        check=False,
    )
    if completed.returncode != 0:
        stderr_text = (completed.stderr or b"").decode("utf-8", errors="replace").strip()
        stdout_text = (completed.stdout or b"").decode("utf-8", errors="replace").strip()
        raise RuntimeError(stderr_text or stdout_text or "MT5 terminal launch failed")


def _wait_for_mt5_gui_window_ready(timeout_ms: int) -> bool:
    """轮询直到可附着的 MT5 终端进程出现。"""
    deadline = time.monotonic() + max(0.5, float(timeout_ms or 0) / 1000.0)
    terminal_path = _normalize_path(str(PATH or ""))
    while time.monotonic() <= deadline:
        if _detect_mt5_gui_window(terminal_path):
            return True
        time.sleep(0.25)
    return False


def _attach_current_mt5_gui_terminal(timeout_ms: int) -> Dict[str, Any]:
    """附着当前已存在的 MT5 GUI 终端；必要时接管目标终端后重试一次。"""
    preferred_path = _normalize_path(str(PATH or ""))
    payload = _inspect_mt5_gui_terminals(preferred_path)
    attach_path = _resolve_attachable_mt5_gui_terminal_path(payload, preferred_path=preferred_path)
    if not attach_path:
        return {
            "ok": False,
            "message": "已检测到 MT5 终端进程，但当前会话没有可附着的目标终端",
            "detail": {
                "preferredPath": preferred_path or "",
                "processCount": int(payload.get("processCount") or 0),
                "sameSessionCount": int(payload.get("sameSessionCount") or 0),
                "exactPathCount": int(payload.get("exactPathCount") or 0),
            },
        }
    attempt_budget_ms = max(1000, int(timeout_ms or MT5_INIT_TIMEOUT_MS))
    initialize_budget_ms = _split_attach_phase_budget(attempt_budget_ms, phase_count=3, phase_index=0)
    # 附着现有 GUI 终端前，先断开 Python 侧可能残留的旧 MT5 句柄，
    # 避免上一轮失败态继续污染这次按路径 initialize。
    _shutdown_mt5()
    initialized, init_message = _mt5_initialize(attach_path, timeout_ms=initialize_budget_ms)
    if initialized:
        return {
            "ok": True,
            "message": "已按目标终端路径附着当前 MT5 实例",
            "detail": {
                "attachPath": attach_path,
                "attachMode": "direct_initialize",
                "timeoutMs": initialize_budget_ms,
            },
        }
    recycle_budget_ms = max(0, attempt_budget_ms - initialize_budget_ms)
    if recycle_budget_ms < 3000:
        return {
            "ok": False,
            "message": f"按目标终端路径附着失败: {init_message}",
            "detail": {
                "attachPath": attach_path,
                "attachMode": "direct_initialize",
                "initializeError": str(init_message or "").strip(),
                "timeoutMs": initialize_budget_ms,
            },
        }
    try:
        recycled = _recycle_mt5_terminal_for_attach(attach_path, wait_timeout_ms=recycle_budget_ms)
    except Exception as exc:
        return {
            "ok": False,
            "message": f"按目标终端路径附着失败，且重启终端失败: {exc}",
            "detail": {
                "attachPath": attach_path,
                "attachMode": "recycle_after_initialize_failed",
                "initializeError": str(init_message or "").strip(),
                "recycleError": str(exc),
                "timeoutMs": recycle_budget_ms,
            },
        }
    retry_budget_ms = max(1000, int(recycled.get("remainingBudgetMs") or 0))
    retried, retry_message, retry_attempt_count = _retry_attach_initialize_after_recycle(
        attach_path,
        retry_budget_ms,
    )
    if retried:
        return {
            "ok": True,
            "message": "首次附着失败后，已重启目标终端并附着成功",
            "detail": {
                "attachPath": attach_path,
                "attachMode": "recycled_terminal_then_initialize",
                "initializeError": str(init_message or "").strip(),
                "retryInitializeAttemptCount": int(retry_attempt_count),
                "timeoutMs": retry_budget_ms,
            },
        }
    return {
        "ok": False,
        "message": f"按目标终端路径附着失败；重启终端后再次附着仍失败: {retry_message}",
        "detail": {
            "attachPath": attach_path,
            "attachMode": "recycled_terminal_then_initialize_failed",
            "initializeError": str(init_message or "").strip(),
            "retryInitializeError": str(retry_message or "").strip(),
            "retryInitializeAttemptCount": int(retry_attempt_count),
            "timeoutMs": retry_budget_ms,
        },
    }


def _read_current_mt5_gui_account() -> Optional[Dict[str, str]]:
    """读取当前 MT5 GUI 终端的真实账号。"""
    if mt5 is None:
        return None
    try:
        account = mt5.account_info()
    except Exception:
        return None
    if account is None:
        return None
    login_value = str(getattr(account, "login", "") or "").strip()
    server_value = str(getattr(account, "server", "") or "").strip()
    if not login_value:
        return None
    return {
        "login": login_value,
        "server": server_value,
    }


def _call_mt5_account_switch_api(login: str, password: str, server: str, timeout_ms: int) -> Dict[str, Any]:
    """调用一次 mt5.login(...)，返回原始错误摘要供后续轮询使用。"""
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package is unavailable")
    login_value = int(str(login or "").strip())
    server_value = str(server or "").strip()
    effective_timeout_ms = max(1000, int(timeout_ms or MT5_INIT_TIMEOUT_MS))
    try:
        ok = bool(mt5.login(login_value, password=str(password or ""), server=server_value, timeout=effective_timeout_ms))
    except TypeError:
        ok = bool(mt5.login(login_value, password=str(password or ""), server=server_value))
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc
    return {
        "ok": ok,
        "error": "" if ok else str(mt5.last_error()),
    }


def _build_mt5_account_switch_controller(
    stage_reporter: Optional[Callable[[str, str, str, Optional[Dict[str, Any]]], None]] = None,
) -> v2_mt5_account_switch.Mt5GuiController:
    """构造服务端正式 MT5 GUI 切号控制器。"""
    return v2_mt5_account_switch.Mt5GuiController(
        detect_window=_detect_mt5_gui_window,
        launch_terminal=_launch_mt5_terminal_from_mt5_path,
        wait_window_ready=_wait_for_mt5_gui_window_ready,
        attach_terminal=_attach_current_mt5_gui_terminal,
        read_account=_read_current_mt5_gui_account,
        perform_switch=_call_mt5_account_switch_api,
        monotonic_ms=_now_monotonic_ms,
        total_timeout_ms=MT5_GUI_SWITCH_TOTAL_TIMEOUT_MS,
        report_stage=stage_reporter,
    )


def _build_switch_flow_detail(result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """裁剪切号结果 detail，供诊断和错误回执复用。"""
    payload = dict(result or {})
    return {
        "elapsedMs": int(payload.get("elapsedMs") or 0),
        "baselineAccount": payload.get("baselineAccount"),
        "finalAccount": payload.get("finalAccount"),
        "loginError": str(payload.get("loginError") or "").strip(),
        "lastObservedAccount": payload.get("lastObservedAccount"),
    }


def _switch_mt5_account_via_gui_flow(*, login: str, password: str, server: str, request_id: str, action: str) -> Dict[str, Any]:
    """执行新的 MT5 GUI 切号主链，并把真实阶段写入诊断时间线。"""
    failure_code = "SESSION_SWITCH_FAILED" if str(action or "").strip() == "switch" else "SESSION_LOGIN_FAILED"

    def _report_stage(stage: str, status: str, message: str, detail: Optional[Dict[str, Any]] = None) -> None:
        _append_session_diagnostic_entry(
            request_id=request_id,
            action=action,
            stage=stage,
            status=status,
            message=message,
            server_time=_now_ms(),
            error_code=failure_code if str(status or "").strip() == "failed" else "",
            detail=detail,
        )

    result = _build_mt5_account_switch_controller(stage_reporter=_report_stage).switch_account(
        login=str(login or "").strip(),
        password=str(password or ""),
        server=str(server or "").strip(),
    )
    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage=str(result.get("stage") or ""),
        status="ok" if bool(result.get("ok")) else "failed",
        message=str(result.get("message") or ""),
        server_time=_now_ms(),
        error_code="" if bool(result.get("ok")) else failure_code,
        detail=_build_switch_flow_detail(result),
    )
    return result


def _server_candidates() -> List[str]:
    credentials = _current_mt5_credentials()
    mode = str(credentials.get("mode") or "env_default")
    values: List[str] = []
    seen = set()
    primary_server = str(credentials.get("server") or "").strip()
    base_values = [primary_server] + SERVER_ALIASES_RAW.split(",")
    if mode == "env_default":
        base_values = [primary_server, SERVER] + SERVER_ALIASES_RAW.split(",")
    for raw in base_values:
        server = (raw or "").strip()
        if not server:
            continue
        key = server.lower()
        if key in seen:
            continue
        seen.add(key)
        values.append(server)
    return values


def _mt5_initialize(
    path_value: Optional[str],
    login: Optional[int] = None,
    password: Optional[str] = None,
    server_name: Optional[str] = None,
    timeout_ms: Optional[int] = None,
) -> Tuple[bool, str]:
    effective_timeout_ms = max(1000, int(timeout_ms or MT5_INIT_TIMEOUT_MS))
    kwargs = {"timeout": effective_timeout_ms}
    auth_requested = bool(login and password and server_name)
    if path_value:
        kwargs["path"] = path_value
    if login and password and server_name:
        kwargs["login"] = int(login)
        kwargs["password"] = password
        kwargs["server"] = server_name
    try:
        ok = bool(mt5.initialize(**kwargs))
    except TypeError:
        # Backward compatibility for MT5 package variants
        # that do not accept timeout and/or auth fields together.
        legacy_kwargs = dict(kwargs)
        legacy_kwargs.pop("timeout", None)
        try:
            ok = bool(mt5.initialize(**legacy_kwargs))
        except TypeError as auth_signature_error:
            if auth_requested:
                return False, f"MT5 initialize does not support authenticated init: {auth_signature_error}"
            legacy_kwargs.pop("login", None)
            legacy_kwargs.pop("password", None)
            legacy_kwargs.pop("server", None)
            ok = bool(mt5.initialize(**legacy_kwargs))
    except Exception as exc:
        return False, str(exc)
    return ok, str(mt5.last_error())


def _split_attach_phase_budget(total_timeout_ms: int, phase_count: int, phase_index: int) -> int:
    """把单次附着预算分配给初始化/重启/重试三个阶段，避免第一步独占全部等待时间。"""
    phases = max(1, int(phase_count))
    index = max(0, min(int(phase_index), phases - 1))
    timeout_ms = max(1000, int(total_timeout_ms or 0))
    fair_share_ms = max(1000, int(timeout_ms / phases))
    if index + 1 >= phases:
        spent_ms = fair_share_ms * index
        return max(1000, timeout_ms - spent_ms)
    return fair_share_ms


def _retry_attach_initialize_after_recycle(path_value: str, wait_timeout_ms: int) -> Tuple[bool, str, int]:
    """重启终端后在剩余预算内短轮询附着，避免进程刚出现就立刻判死。"""
    total_budget_ms = max(1000, int(wait_timeout_ms or 0))
    deadline = time.monotonic() + (float(total_budget_ms) / 1000.0)
    last_message = ""
    attempt_count = 0
    while True:
        remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
        if remaining_ms <= 0:
            break
        attempt_count += 1
        attempt_timeout_ms = min(4000, max(1000, remaining_ms))
        _shutdown_mt5()
        initialized, init_message = _mt5_initialize(path_value, timeout_ms=attempt_timeout_ms)
        if initialized:
            return True, "", attempt_count
        last_message = str(init_message or "").strip()
        remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
        if remaining_ms <= 1500:
            break
        time.sleep(min(1.5, float(remaining_ms) / 1000.0))
    return False, last_message, attempt_count


def _mt5_login() -> Tuple[bool, str]:
    credentials = _current_mt5_credentials()
    login_value = int(credentials.get("login") or 0)
    password_value = str(credentials.get("password") or "")
    account = mt5.account_info()
    server_candidates = _server_candidates()
    if login_value <= 0 or not password_value:
        return False, "resolved credentials missing login/password"
    if account is not None and int(getattr(account, "login", 0)) == login_value:
        account_server = str(getattr(account, "server", "")).strip().lower()
        if not account_server or account_server in [candidate.lower() for candidate in server_candidates]:
            return True, "already-logged-in"

    errors = []
    for server_name in server_candidates:
        try:
            ok = bool(mt5.login(login_value, password=password_value, server=server_name, timeout=MT5_INIT_TIMEOUT_MS))
        except TypeError:
            ok = bool(mt5.login(login_value, password=password_value, server=server_name))
        except Exception as exc:
            errors.append(f"{server_name}: {exc}")
            continue
        if ok:
            return True, server_name
        errors.append(f"{server_name}: {mt5.last_error()}")
    return False, " ; ".join(errors)


def _ensure_mt5() -> None:
    global mt5_last_connected_path
    credentials = _current_mt5_credentials()
    resolved_login = int(credentials.get("login") or 0)
    resolved_password = str(credentials.get("password") or "")
    if mt5 is None:
        raise RuntimeError("MetaTrader5 python package is not installed in gateway environment.")
    if not _is_mt5_configured():
        raise RuntimeError("MT5 credentials are not configured.")

    attempts: List[Optional[str]] = [None] + MT5_TERMINAL_CANDIDATES
    errors = []
    server_candidates = _server_candidates()
    for path_value in attempts:
        label = path_value if path_value else "<auto>"
        for server_name in server_candidates:
            initialized, init_message = _mt5_initialize(
                path_value,
                login=resolved_login,
                password=resolved_password,
                server_name=server_name,
            )
            if initialized:
                mt5_last_connected_path = label
                return
            errors.append(f"init+login({label},{server_name})={init_message}")

    discovered_hint = ", ".join(MT5_TERMINAL_CANDIDATES[:3]) if MT5_TERMINAL_CANDIDATES else "none"
    raise RuntimeError(
        "MT5 login failed. "
        + " | ".join(errors[-6:])
        + f" | configured_server={credentials.get('server')} | credential_source={credentials.get('source')} | discovered_terminal_candidates={discovered_hint}"
    )


def _shutdown_mt5() -> None:
    if mt5 is not None:
        mt5.shutdown()


def _restart_mt5_terminal_for_direct_login(path_value: Optional[str], wait_timeout_ms: Optional[int] = None) -> Dict[str, Any]:
    """真实清理目标 MT5 终端进程，确保下一次直登面对的是干净终端。"""
    normalized_path = _normalize_path(str(path_value or ""))
    if not normalized_path:
        return {
            "skipped": True,
            "message": "未找到可用的 MT5 终端路径，无法按路径清理服务器 MT5 终端进程",
        }
    effective_timeout_ms = max(1000, int(wait_timeout_ms or MT5_INIT_TIMEOUT_MS))
    stop_command = _build_mt5_terminal_stop_command(normalized_path, wait_timeout_ms=effective_timeout_ms)
    timeout_seconds = max(10, int(math.ceil(effective_timeout_ms / 1000.0)) + 10)
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            stop_command,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_seconds,
        check=False,
    )
    stdout_text = (completed.stdout or b"").decode("utf-8", errors="replace").strip()
    stderr_text = (completed.stderr or b"").decode("utf-8", errors="replace").strip()
    if completed.returncode != 0:
        raise RuntimeError(stderr_text or stdout_text or "MT5 terminal process cleanup failed")
    try:
        payload = json.loads(stdout_text or "{}")
    except Exception as exc:
        raise RuntimeError(f"MT5 terminal cleanup returned invalid JSON: {stdout_text or stderr_text}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"MT5 terminal cleanup returned invalid payload: {stdout_text or stderr_text}")
    payload["skipped"] = False
    return payload


def _start_mt5_terminal_for_direct_login(path_value: Optional[str], wait_timeout_ms: Optional[int] = None) -> Dict[str, Any]:
    """按目标路径显式拉起 MT5 终端，并等待进程真正出现。"""
    normalized_path = _normalize_path(str(path_value or ""))
    if not normalized_path:
        raise RuntimeError("MT5 terminal path is not configured.")
    effective_timeout_ms = max(1000, int(wait_timeout_ms or MT5_INIT_TIMEOUT_MS))
    start_command = _build_mt5_terminal_start_command(normalized_path, wait_timeout_ms=effective_timeout_ms)
    timeout_seconds = max(10, int(math.ceil(effective_timeout_ms / 1000.0)) + 10)
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            start_command,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_seconds,
        check=False,
    )
    stdout_text = (completed.stdout or b"").decode("utf-8", errors="replace").strip()
    stderr_text = (completed.stderr or b"").decode("utf-8", errors="replace").strip()
    if completed.returncode != 0:
        raise RuntimeError(stderr_text or stdout_text or "MT5 terminal process start failed")
    try:
        payload = json.loads(stdout_text or "{}")
    except Exception as exc:
        raise RuntimeError(f"MT5 terminal start returned invalid JSON: {stdout_text or stderr_text}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"MT5 terminal start returned invalid payload: {stdout_text or stderr_text}")
    payload["skipped"] = False
    return payload


def _recycle_mt5_terminal_for_attach(path_value: str, wait_timeout_ms: int) -> Dict[str, Any]:
    """附着前接管目标 MT5 终端：先清理再拉起，并返回剩余预算。"""
    effective_timeout_ms = max(1000, int(wait_timeout_ms or 0))
    stop_budget_ms = _split_attach_phase_budget(effective_timeout_ms, phase_count=2, phase_index=0)
    start_budget_ms = max(1000, effective_timeout_ms - stop_budget_ms)
    stop_result = _restart_mt5_terminal_for_direct_login(path_value, wait_timeout_ms=stop_budget_ms)
    start_result = _start_mt5_terminal_for_direct_login(path_value, wait_timeout_ms=start_budget_ms)
    return {
        "terminalPath": str(start_result.get("terminalPath") or stop_result.get("terminalPath") or path_value or "").strip(),
        "stopMatchedCount": int(stop_result.get("matchedCount") or 0),
        "stopRemainingCount": int(stop_result.get("remainingCount") or 0),
        "startMatchedCount": int(start_result.get("matchedCount") or 0),
        "remainingBudgetMs": max(1000, int(start_budget_ms)),
    }


def _clear_session_related_runtime_state() -> None:
    """清理会话切换后最相关的运行时缓存。"""
    global session_snapshot_epoch
    with snapshot_cache_lock:
        session_snapshot_epoch += 1
        snapshot_build_cache.clear()
        snapshot_sync_cache.clear()
        v2_sync_state.clear()
        health_status_cache.clear()
        light_snapshot_build_condition.notify_all()
    with trade_request_lock:
        trade_request_store.clear()


def _invalidate_account_runtime_cache_after_trade_commit() -> None:
    """成交成功后立刻作废账户相关快照缓存，保证下一次 revision 重新按真值构建。"""
    with snapshot_cache_lock:
        snapshot_build_cache.clear()
        snapshot_sync_cache.clear()
        v2_sync_state.clear()
        health_status_cache.clear()
        light_snapshot_build_condition.notify_all()


def _publish_account_trade_commit_sync_state() -> None:
    """成交成功后立即发布最新 bus 状态，推动客户端尽快看到新的 historyRevision。"""
    try:
        _request_v2_publish("trade_commit")
        _v2_bus_publish_current_state()
    except Exception as exc:
        sys.stderr.write(f"[v2_trade_submit] publish current state failed: {exc}\n")
        sys.stderr.flush()


def _build_session_summary() -> Dict[str, Any]:
    """抽取会话摘要，供健康和管理面板统一展示。"""
    status = session_manager.build_status_payload()
    return {
        "activeAccount": status.get("activeAccount"),
        "savedAccountCount": int(status.get("savedAccountCount") or 0),
    }


def _on_session_changed(action: str, profile: Optional[Dict[str, Any]]) -> None:
    """会话变化回调，统一清理缓存并维护运行态。"""
    _clear_session_related_runtime_state()
    _request_v2_publish(str(action or "session_changed"))
    if str(action or "") == "logout":
        _clear_runtime_session_credentials()


def _read_lightweight_current_mt5_account_identity() -> Optional[Dict[str, str]]:
    """轻量读取当前 MT5 终端实际账号，只用于 APP 重启后的恢复确认。"""
    account = _read_current_mt5_gui_account()
    if account is not None:
        return account
    if mt5 is None or not _detect_mt5_gui_window(_normalize_path(str(PATH or ""))):
        return None
    initialized, _ = _mt5_initialize(None)
    if not initialized:
        return None
    try:
        return _read_current_mt5_gui_account()
    finally:
        _shutdown_mt5()


def _build_verified_session_status_payload() -> Dict[str, Any]:
    """在返回 session status 前做一次轻量账号确认。"""
    status = session_manager.build_status_payload()
    active_account = (status or {}).get("activeAccount") or None
    if not isinstance(active_account, dict) or not active_account:
        return status
    current_account = _read_lightweight_current_mt5_account_identity()
    if not isinstance(current_account, dict) or not current_account:
        return status
    active_login = str(active_account.get("login") or "").strip()
    current_login = str(current_account.get("login") or "").strip()
    if active_login and current_login and active_login == current_login:
        return status
    session_store.clear_active_session()
    _on_session_changed("logout", active_account)
    return session_manager.build_status_payload()


def _restore_runtime_session_from_active_session(active_session: Optional[Dict[str, Any]]) -> bool:
    """服务端重启后尝试从已保存账号恢复远程会话运行时凭据。"""
    if not isinstance(active_session, dict) or not active_session:
        return False
    profile_id = str(active_session.get("profileId") or "").strip()
    active_login = str(active_session.get("login") or "").strip()
    active_server = str(active_session.get("server") or "").strip()
    if not profile_id or not active_login or not active_server:
        return False
    if not hasattr(session_store, "load_profile"):
        return False
    record = session_store.load_profile(profile_id)
    if not isinstance(record, dict) or not record:
        return False
    raw_profile = record.get("profile")
    if not isinstance(raw_profile, dict) or not raw_profile:
        return False
    saved_login = str(raw_profile.get("login") or "").strip()
    saved_server = str(raw_profile.get("server") or "").strip()
    if (
        saved_login != active_login
        or _normalize_session_server_identity(saved_server) != _normalize_session_server_identity(active_server)
    ):
        return False
    encrypted_password = str(record.get("encryptedPassword") or "").strip()
    if not encrypted_password:
        return False
    try:
        cipher = base64.b64decode(encrypted_password, validate=True)
        password = v2_session_crypto.unprotect_secret_for_machine(cipher).decode("utf-8")
    except Exception:
        return False
    if not password:
        return False
    _set_runtime_session_credentials(saved_login, password, saved_server)
    return True


def _probe_mt5_authenticated_session(login_value: int,
                                     password_value: str,
                                     server_value: str,
                                     path_value: Optional[str],
                                     request_id: str = "",
                                     action: str = "login") -> Dict[str, str]:
    """在独立进程里执行 MT5 登录探针，避免主进程被阻塞调用拖死。"""
    probe_script = Path(__file__).resolve().parent / "mt5_login_probe.py"
    if not probe_script.exists():
        raise RuntimeError(f"mt5 login probe script missing: {probe_script}")
    trace_file = _create_probe_trace_file()
    payload = {
        "login": int(login_value),
        "password": str(password_value or ""),
        "server": str(server_value or ""),
        "path": str(path_value or ""),
        "timeoutMs": int(MT5_INIT_TIMEOUT_MS),
        "traceFile": str(trace_file or ""),
    }
    # 额外预留 5 秒给独立进程启动、JSON 编解码和退出，不放宽 MT5 自身登录预算。
    process_timeout_seconds = max(5, int(math.ceil(MT5_INIT_TIMEOUT_MS / 1000.0)) + 5)
    try:
        try:
            completed = subprocess.run(
                [sys.executable, str(probe_script)],
                input=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=process_timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            _record_session_diagnostic_trace(
                request_id=request_id,
                action=action,
                trace_items=_read_probe_trace_file(trace_file),
            )
            _append_session_diagnostic_entry(
                request_id=request_id,
                action=action,
                stage="probe_timeout",
                status="failed",
                message=f"MT5 登录探针在 {process_timeout_seconds}s 后超时",
                server_time=_now_ms(),
                error_code="SESSION_PROBE_TIMEOUT",
            )
            raise RuntimeError(f"MT5 login probe timed out after {process_timeout_seconds}s") from exc
        stderr_text = (completed.stderr or b"").decode("utf-8", errors="replace").strip()
        stdout_text = (completed.stdout or b"").decode("utf-8", errors="replace").strip()
        try:
            result = json.loads(stdout_text or "{}")
        except Exception as exc:
            _append_session_diagnostic_entry(
                request_id=request_id,
                action=action,
                stage="probe_invalid_json",
                status="failed",
                message=stdout_text or stderr_text or "MT5 登录探针返回了无效 JSON",
                server_time=_now_ms(),
                error_code="SESSION_PROBE_INVALID_JSON",
            )
            raise RuntimeError(f"MT5 login probe returned invalid JSON: {stdout_text or stderr_text}") from exc
        if not isinstance(result, dict):
            raise RuntimeError(f"MT5 login probe returned invalid payload: {stdout_text or stderr_text}")
        _record_session_diagnostic_trace(
            request_id=request_id,
            action=action,
            trace_items=result.get("trace") if isinstance(result.get("trace"), list) else [],
        )
        if not bool(result.get("ok", False)):
            error_message = str(result.get("error") or stderr_text or "MT5 login probe failed")
            raise RuntimeError(error_message)
        canonical_login = str(result.get("login") or "").strip()
        canonical_server = str(result.get("server") or "").strip()
        if not canonical_login or not canonical_server:
            raise RuntimeError("MT5 login probe returned incomplete canonical identity")
        return {
            "login": canonical_login,
            "server": canonical_server,
        }
    finally:
        _cleanup_probe_trace_file(trace_file)


def _login_mt5_in_isolated_process(login_value: int,
                                   password_value: str,
                                   server_value: str,
                                   path_value: Optional[str],
                                   request_id: str = "",
                                   action: str = "login") -> Dict[str, str]:
    """在隔离进程里执行单路径直登，避免主进程 MT5 IPC 黏连导致初始化卡死。"""
    helper_script = Path(__file__).resolve().parent / "mt5_direct_login.py"
    if not helper_script.exists():
        raise RuntimeError(f"mt5 direct login script missing: {helper_script}")
    trace_file = _create_probe_trace_file()
    payload = {
        "login": int(login_value),
        "password": str(password_value or ""),
        "server": str(server_value or ""),
        "path": str(path_value or ""),
        "timeoutMs": int(MT5_INIT_TIMEOUT_MS),
        "traceFile": str(trace_file or ""),
    }
    # 直登 helper 按官方顺序执行 initialize + login，两段都要共享同一超时预算。
    process_timeout_seconds = max(10, int(math.ceil((MT5_INIT_TIMEOUT_MS * 2) / 1000.0)) + 10)
    try:
        try:
            completed = subprocess.run(
                [sys.executable, str(helper_script)],
                input=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=process_timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            _record_session_diagnostic_trace(
                request_id=request_id,
                action=action,
                trace_items=_read_probe_trace_file(trace_file),
            )
            raise RuntimeError(f"MT5 direct login timed out after {process_timeout_seconds}s") from exc
        stderr_text = (completed.stderr or b"").decode("utf-8", errors="replace").strip()
        stdout_text = (completed.stdout or b"").decode("utf-8", errors="replace").strip()
        try:
            result = json.loads(stdout_text or "{}")
        except Exception as exc:
            raise RuntimeError(f"MT5 direct login returned invalid JSON: {stdout_text or stderr_text}") from exc
        if not isinstance(result, dict):
            raise RuntimeError(f"MT5 direct login returned invalid payload: {stdout_text or stderr_text}")
        _record_session_diagnostic_trace(
            request_id=request_id,
            action=action,
            trace_items=result.get("trace") if isinstance(result.get("trace"), list) else [],
        )
        if not bool(result.get("ok", False)):
            raise RuntimeError(str(result.get("error") or stderr_text or "MT5 direct login failed"))
        canonical_login = str(result.get("login") or "").strip()
        canonical_server = str(result.get("server") or "").strip()
        if not canonical_login or not canonical_server:
            raise RuntimeError("MT5 direct login returned incomplete canonical identity")
        return {
            "login": canonical_login,
            "server": canonical_server,
        }
    finally:
        _cleanup_probe_trace_file(trace_file)


def _login_mt5_direct_with_input_credentials(*,
                                             login_value: int,
                                             password_value: str,
                                             server_value: str,
                                             request_id: str = "",
                                             action: str = "login") -> Dict[str, str]:
    """先退出当前 MT5，再只按本次输入的账号密码执行一次正式直登。"""
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package is unavailable")
    normalized_login = str(int(login_value or 0)).strip() if int(login_value or 0) > 0 else ""
    normalized_server = str(server_value or "").strip()
    if not normalized_login or not normalized_server or not str(password_value or ""):
        raise RuntimeError("MT5 direct login missing required credentials")

    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage="direct_logout_start",
        status="pending",
        message="开始清理网关当前 MT5 API 连接",
        server_time=_now_ms(),
    )
    try:
        _shutdown_mt5()
    except Exception as exc:
        _append_session_diagnostic_entry(
            request_id=request_id,
            action=action,
            stage="direct_logout_failed",
            status="failed",
            message=f"退出服务器当前 MT5 账号失败: {exc}",
            server_time=_now_ms(),
            error_code="SESSION_DIRECT_LOGOUT_FAILED",
        )
        raise RuntimeError(f"MT5 logout failed: {exc}") from exc
    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage="direct_logout_ok",
        status="ok",
        message="已完成网关当前 MT5 API 连接清理",
        server_time=_now_ms(),
        detail={
            "note": "该步骤仅清理网关进程中的 MT5 API 连接，不等于终端账号已退出",
        },
    )
    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage="direct_terminal_reset_start",
        status="pending",
        message="开始按目标路径清理服务器 MT5 终端进程",
        server_time=_now_ms(),
        detail={},
    )
    terminal_path, terminal_path_source = _resolve_direct_login_terminal_path()
    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage="direct_terminal_path_resolved",
        status="ok",
        message="已解析本次直登将操作的 MT5 终端路径",
        server_time=_now_ms(),
        detail={
            "terminalPath": str(terminal_path or "").strip(),
            "pathSource": terminal_path_source,
        },
    )
    try:
        terminal_reset_result = _restart_mt5_terminal_for_direct_login(terminal_path)
    except Exception as exc:
        _append_session_diagnostic_entry(
            request_id=request_id,
            action=action,
            stage="direct_terminal_reset_failed",
            status="failed",
            message=f"清理服务器 MT5 终端进程失败: {exc}",
            server_time=_now_ms(),
            error_code="SESSION_DIRECT_TERMINAL_RESET_FAILED",
        )
        raise RuntimeError(f"MT5 terminal reset failed: {exc}") from exc
    reset_detail = {
        "terminalPath": str(terminal_reset_result.get("terminalPath") or terminal_path or "").strip(),
        "pathSource": terminal_path_source,
        "matchedCount": int(terminal_reset_result.get("matchedCount") or 0),
        "remainingCount": int(terminal_reset_result.get("remainingCount") or 0),
    }
    if bool(terminal_reset_result.get("skipped")):
        _append_session_diagnostic_entry(
            request_id=request_id,
            action=action,
            stage="direct_terminal_reset_skipped",
            status="ok",
            message=str(terminal_reset_result.get("message") or "未执行服务器 MT5 终端进程清理"),
            server_time=_now_ms(),
            detail=reset_detail,
        )
    else:
        _append_session_diagnostic_entry(
            request_id=request_id,
            action=action,
            stage="direct_terminal_reset_ok",
            status="ok",
            message="已完成服务器 MT5 终端进程清理",
            server_time=_now_ms(),
            detail=reset_detail,
        )

    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage="direct_terminal_start_start",
        status="pending",
        message="开始按目标路径启动服务器 MT5 终端",
        server_time=_now_ms(),
        detail={
            "terminalPath": str(terminal_path or "").strip(),
            "pathSource": terminal_path_source,
        },
    )
    try:
        terminal_start_result = _start_mt5_terminal_for_direct_login(terminal_path)
    except Exception as exc:
        _append_session_diagnostic_entry(
            request_id=request_id,
            action=action,
            stage="direct_terminal_start_failed",
            status="failed",
            message=f"启动服务器 MT5 终端失败: {exc}",
            server_time=_now_ms(),
            error_code="SESSION_DIRECT_TERMINAL_START_FAILED",
            detail={
                "terminalPath": str(terminal_path or "").strip(),
                "pathSource": terminal_path_source,
            },
        )
        raise RuntimeError(f"MT5 terminal start failed: {exc}") from exc
    _append_session_diagnostic_entry(
        request_id=request_id,
        action=action,
        stage="direct_terminal_start_ok",
        status="ok",
        message="已显式启动服务器 MT5 终端",
        server_time=_now_ms(),
        detail={
            "terminalPath": str(terminal_start_result.get("terminalPath") or terminal_path or "").strip(),
            "pathSource": terminal_path_source,
            "matchedCount": int(terminal_start_result.get("matchedCount") or 0),
        },
    )

    return _login_mt5_in_isolated_process(
        login_value=login_value,
        password_value=password_value,
        server_value=normalized_server,
        path_value=terminal_path,
        request_id=request_id,
        action=action,
    )


def _build_direct_session_result(*,
                                 action: str,
                                 login: str,
                                 server: str,
                                 request_id: str = "") -> Dict[str, Any]:
    """把独立 MT5 实例直登结果映射成统一会话回执结构。"""
    safe_login = str(login or "").strip()
    safe_server = str(server or "").strip()
    message_action = "登录" if str(action or "").strip() == "login" else "切换"
    account_summary = _build_direct_session_account_summary(login=safe_login, server=safe_server) or {
        "login": safe_login,
        "server": safe_server,
    }
    return {
        "ok": True,
        "state": "activated",
        "requestId": str(request_id or "").strip(),
        "stage": "direct_identity_confirmed",
        "message": f"已通过独立 MT5 实例{message_action}目标账号",
        "elapsedMs": 0,
        "baselineAccount": None,
        "finalAccount": account_summary,
        "loginError": "",
        "lastObservedAccount": dict(account_summary),
        "login": safe_login,
        "server": safe_server,
    }


def _mask_direct_session_login(login: str) -> str:
    """把登录账号裁剪成统一掩码格式。"""
    safe_login = str(login or "").strip()
    return f"****{safe_login[-4:]}" if safe_login else "****"


def _build_direct_session_profile_id(login: str, server: str) -> str:
    """按账号和服务器构造稳定 profileId，保证直登链回执字段齐全。"""
    safe_login = str(login or "").strip()
    safe_server = str(server or "").strip().lower().replace(" ", "_")
    normalized_server = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in safe_server)
    if not safe_login or not normalized_server:
        return ""
    return f"acct_{safe_login}_{normalized_server}"


def _build_direct_session_account_summary(*, login: str, server: str) -> Optional[Dict[str, Any]]:
    """把账号身份补全成会话摘要结构，避免上层只拿到半截字段。"""
    safe_login = str(login or "").strip()
    safe_server = str(server or "").strip()
    if not safe_login or not safe_server:
        return None
    masked_login = _mask_direct_session_login(safe_login)
    return {
        "profileId": _build_direct_session_profile_id(safe_login, safe_server),
        "login": safe_login,
        "loginMasked": masked_login,
        "server": safe_server,
        "displayName": f"{safe_server} {masked_login}",
        "active": False,
        "state": "",
    }


def _build_direct_session_account_summary_from_detail(*,
                                                      detail: Dict[str, Any],
                                                      login_keys: Tuple[str, ...],
                                                      server_keys: Tuple[str, ...]) -> Optional[Dict[str, Any]]:
    """从诊断 detail 里恢复账号摘要，兼容 expected/actual 等不同键名。"""
    login_value = ""
    server_value = ""
    for key in login_keys:
        text = str(detail.get(key) or "").strip()
        if text:
            login_value = text
            break
    for key in server_keys:
        text = str(detail.get(key) or "").strip()
        if text:
            server_value = text
            break
    return _build_direct_session_account_summary(login=login_value, server=server_value)


def _build_failed_direct_session_result(*,
                                        action: str,
                                        request_id: str,
                                        error: Exception) -> Dict[str, Any]:
    """从最新诊断时间线提取失败阶段，统一包装直登主链异常。"""
    safe_request_id = str(request_id or "").strip()
    timeline = session_diagnostic_store.lookup(safe_request_id) if safe_request_id else []
    latest_item = timeline[-1] if timeline else {}
    latest_detail = dict(latest_item.get("detail") or {}) if isinstance(latest_item, dict) else {}
    baseline_account = latest_detail.get("baselineAccount")
    if not isinstance(baseline_account, dict) or not baseline_account:
        baseline_account = _build_direct_session_account_summary_from_detail(
            detail=latest_detail,
            login_keys=("expectedLogin", "login"),
            server_keys=("expectedServer", "server"),
        )
    final_account = latest_detail.get("finalAccount")
    if not isinstance(final_account, dict) or not final_account:
        final_account = _build_direct_session_account_summary_from_detail(
            detail=latest_detail,
            login_keys=("actualLogin", "login"),
            server_keys=("actualServer", "server"),
        )
    last_observed_account = latest_detail.get("lastObservedAccount")
    if not isinstance(last_observed_account, dict) or not last_observed_account:
        last_observed_account = final_account
    return {
        "ok": False,
        "stage": str(latest_item.get("stage") or "direct_login_failed").strip(),
        "message": str(error or "").strip() or str(latest_item.get("message") or "").strip() or "独立 MT5 实例直登失败",
        "elapsedMs": 0,
        "baselineAccount": baseline_account,
        "finalAccount": final_account,
        "loginError": str(latest_detail.get("loginError") or error or "").strip(),
        "lastObservedAccount": last_observed_account,
        "requestId": safe_request_id,
        "action": str(action or "").strip(),
    }


class _SessionGatewayAdapter:
    """会话管理器使用的最小 MT5 网关适配器。"""

    def login_mt5(self, login: str, password: str, server: str, request_id: str = "") -> Dict[str, Any]:
        """按给定账号参数执行独立 MT5 实例直登。"""
        password_value = str(password or "")
        try:
            direct_result = _login_mt5_direct_with_input_credentials(
                login_value=int(str(login or "").strip() or "0"),
                password_value=password_value,
                server_value=str(server or "").strip(),
                request_id=request_id,
                action="login",
            )
        except Exception as exc:
            raise Mt5AccountSwitchFlowError(
                _build_failed_direct_session_result(
                    action="login",
                    request_id=request_id,
                    error=exc,
                )
            ) from exc
        canonical_login = str(direct_result.get("login") or "").strip()
        canonical_server = str(direct_result.get("server") or "").strip()
        _set_runtime_session_credentials(canonical_login, password_value, canonical_server)
        return _build_direct_session_result(
            action="login",
            login=canonical_login,
            server=canonical_server,
            request_id=request_id,
        )

    def switch_mt5_account(self, login: str, password: str, server: str, request_id: str = "") -> Dict[str, Any]:
        """按已保存账号执行独立 MT5 实例直登切换。"""
        password_value = str(password or "")
        try:
            direct_result = _login_mt5_direct_with_input_credentials(
                login_value=int(str(login or "").strip() or "0"),
                password_value=password_value,
                server_value=str(server or "").strip(),
                request_id=request_id,
                action="switch",
            )
        except Exception as exc:
            raise Mt5AccountSwitchFlowError(
                _build_failed_direct_session_result(
                    action="switch",
                    request_id=request_id,
                    error=exc,
                )
            ) from exc
        canonical_login = str(direct_result.get("login") or "").strip()
        canonical_server = str(direct_result.get("server") or "").strip()
        _set_runtime_session_credentials(canonical_login, password_value, canonical_server)
        return _build_direct_session_result(
            action="switch",
            login=canonical_login,
            server=canonical_server,
            request_id=request_id,
        )

    def clear_account_caches(self) -> None:
        """清理会话切换后的运行时缓存。"""
        _clear_session_related_runtime_state()

    def force_account_resync(self) -> None:
        """触发强一致刷新（通过重置同步状态强制下游全量拉取）。"""
        _clear_session_related_runtime_state()

    def logout_mt5(self) -> None:
        """执行 MT5 退出。"""
        _shutdown_mt5()
        _clear_runtime_session_credentials()


def _build_session_manager() -> v2_session_manager.AccountSessionManager:
    """构建全局会话管理器实例。"""
    gateway = _SessionGatewayAdapter()
    manager = v2_session_manager.AccountSessionManager(
        store=session_store,
        gateway=gateway,
        on_session_changed=_on_session_changed,
    )
    active_session = session_store.load_active_session()
    if isinstance(active_session, dict) and active_session:
        credentials = _current_mt5_credentials()
        active_login = str(active_session.get("login") or "").strip()
        active_server = str(active_session.get("server") or "").strip()
        runtime_login = str(credentials.get("login") or "").strip()
        runtime_server = str(credentials.get("server") or "").strip()
        runtime_mode = str(credentials.get("mode") or "")
        if (
            runtime_mode != "remote_active"
            or not active_login
            or not active_server
            or active_login != runtime_login
            or _normalize_session_server_identity(active_server) != _normalize_session_server_identity(runtime_server)
        ):
            restored = _restore_runtime_session_from_active_session(active_session)
            if not restored:
                session_store.clear_active_session()
    return manager


session_manager = _build_session_manager()
session_envelope_crypto = v2_session_crypto.LoginEnvelopeCrypto(now_ms_provider=_now_ms)


class Mt5AccountSwitchFlowError(RuntimeError):
    """承载结构化 MT5 切号失败结果，供接口层直接透传。"""

    def __init__(self, result: Dict[str, Any]):
        self.result = dict(result or {})
        super().__init__(str(self.result.get("message") or "MT5 账户切换失败"))


def _deal_profit(deal) -> float:
    return (
        float(getattr(deal, "profit", 0.0))
        + float(getattr(deal, "commission", 0.0))
        + float(getattr(deal, "swap", 0.0))
    )


def _build_curve_from_deals(
    deals: List[Any],
    current_positions: Optional[List[Dict]] = None,
    account: Optional[Any] = None,
) -> List[Dict]:
    # 完整历史链复用同一份成交历史，避免曲线和交易列表重复扫 MT5。
    deal_rows = deals or []
    account = account or mt5.account_info()
    if account is None:
        return []

    current_balance = float(getattr(account, "balance", 0.0))
    current_equity = float(getattr(account, "equity", 0.0))
    realized = 0.0
    deal_history: List[Dict[str, Any]] = []
    for deal in deal_rows:
        profit = float(getattr(deal, "profit", 0.0))
        commission = float(getattr(deal, "commission", 0.0))
        swap = float(getattr(deal, "swap", 0.0))
        realized += profit + commission + swap
        deal_history.append({
            "timestamp": _deal_time_ms(deal),
            "price": float(getattr(deal, "price", 0.0)),
            "profit": profit,
            "commission": commission,
            "swap": swap,
            "entry": int(getattr(deal, "entry", 0)),
            "deal_type": int(getattr(deal, "type", -1)),
            "volume": abs(float(getattr(deal, "volume", 0.0))),
            "symbol": str(getattr(deal, "symbol", "") or ""),
            "position_id": int(getattr(deal, "position_id", 0)),
        })

    start_balance = current_balance - realized
    positions = current_positions or _map_positions()
    contract_cache: Dict[str, float] = {}
    contract_size_fn = lambda symbol: _contract_size_for_symbol(symbol, contract_cache)
    leverage = float(getattr(mt5.account_info(), "leverage", 0.0) or 0.0) if mt5 is not None else 0.0
    return _replay_curve_from_history(
        deal_history=deal_history,
        start_balance=start_balance,
        open_positions=positions,
        current_balance=current_balance,
        current_equity=current_equity,
        leverage=leverage,
        contract_size_fn=contract_size_fn,
        now_ms=_now_ms(),
    )


def _build_curve(range_key: str, current_positions: Optional[List[Dict]] = None) -> List[Dict]:
    raw_deals = _progressive_trade_history_deals(range_key)
    return _build_curve_from_deals(raw_deals, current_positions=current_positions)


def _contract_size_for_symbol(symbol: str, cache: Dict[str, float]) -> float:
    if not symbol:
        return 1.0
    cached = cache.get(symbol)
    if cached is not None:
        return cached
    if mt5 is None:
        cache[symbol] = 1.0
        return 1.0
    size = 1.0
    sinfo = mt5.symbol_info(symbol)
    if sinfo is not None and float(getattr(sinfo, "trade_contract_size", 0.0) or 0.0) > 0:
        size = float(getattr(sinfo, "trade_contract_size", 1.0))
    cache[symbol] = size
    return size


def _curve_point(timestamp: int, equity: float, balance: float, position_ratio: float = 0.0) -> Dict[str, float]:
    safe_ratio = float(position_ratio or 0.0)
    if not math.isfinite(safe_ratio) or safe_ratio < 0.0:
        safe_ratio = 0.0
    return {
        "timestamp": int(timestamp),
        "equity": float(equity),
        "balance": float(balance),
        "positionRatio": safe_ratio,
    }


def _add_curve_exposure(
    exposures: Dict[Tuple[str, str], Dict[str, float]],
    symbol: str,
    side: str,
    volume: float,
    price: float,
    contract_size: float,
) -> None:
    if not symbol or volume <= 0.0 or price <= 0.0 or contract_size <= 0.0:
        return
    key = (symbol, side)
    state = exposures.setdefault(key, {"volume": 0.0, "open_notional": 0.0, "contract_size": contract_size})
    state["volume"] += volume
    state["open_notional"] += volume * price * contract_size
    state["contract_size"] = contract_size


def _remove_curve_exposure(
    exposures: Dict[Tuple[str, str], Dict[str, float]],
    symbol: str,
    side: str,
    volume: float,
) -> None:
    if not symbol or volume <= 0.0:
        return
    key = (symbol, side)
    state = exposures.get(key)
    if not state:
        return
    contract_size = state.get("contract_size", 1.0)
    current_volume = state.get("volume", 0.0)
    if current_volume <= 0.0:
        exposures.pop(key, None)
        return
    remove_volume = min(volume, current_volume)
    avg_open_price = state.get("open_notional", 0.0) / max(current_volume * contract_size, 1e-9)
    reduction = avg_open_price * remove_volume * contract_size
    state["volume"] = max(0.0, current_volume - remove_volume)
    state["open_notional"] = max(0.0, state.get("open_notional", 0.0) - reduction)
    if state["volume"] <= 1e-9:
        exposures.pop(key, None)


def _calculate_curve_floating(
    exposures: Dict[Tuple[str, str], Dict[str, float]],
    last_price_by_symbol: Dict[str, float],
) -> float:
    # 计算每个持仓按照最新价格的浮动盈亏，缺少价格时就跳过
    total = 0.0
    for (symbol, side), state in exposures.items():
        volume = state.get("volume", 0.0)
        contract_size = state.get("contract_size", 1.0)
        if volume <= 0.0 or contract_size <= 0.0:
            continue
        price = float(last_price_by_symbol.get(symbol, 0.0) or 0.0)
        if price <= 0.0:
            price = float(last_price_by_symbol.get(_normalize_curve_market_symbol(symbol), 0.0) or 0.0)
        if price <= 0.0:
            continue
        avg_price = state.get("open_notional", 0.0) / max(volume * contract_size, 1e-9)
        direction = 1.0 if side == "Buy" else -1.0
        total += (price - avg_price) * direction * volume * contract_size
    return total


def _calculate_curve_market_value(
    exposures: Dict[Tuple[str, str], Dict[str, float]],
    last_price_by_symbol: Dict[str, float],
) -> float:
    total = 0.0
    for (symbol, _side), state in exposures.items():
        volume = float(state.get("volume", 0.0) or 0.0)
        contract_size = float(state.get("contract_size", 1.0) or 1.0)
        if volume <= 0.0 or contract_size <= 0.0:
            continue
        price = float(last_price_by_symbol.get(symbol, 0.0) or 0.0)
        if price <= 0.0:
            price = float(last_price_by_symbol.get(_normalize_curve_market_symbol(symbol), 0.0) or 0.0)
        if price <= 0.0:
            continue
        total += abs(volume * price * contract_size)
    return total


def _resolve_effective_leverage(leverage: float) -> float:
    return max(1.0, float(leverage or 0.0))


def _curve_position_ratio_from_margin(margin: float, equity: float) -> float:
    ratio = _safe_div(max(0.0, float(margin or 0.0)), max(1.0, float(equity or 0.0)))
    if not math.isfinite(ratio) or ratio < 0.0:
        return 0.0
    return ratio


def _calculate_curve_position_ratio(
    exposures: Dict[Tuple[str, str], Dict[str, float]],
    last_price_by_symbol: Dict[str, float],
    equity: float,
    leverage: float,
) -> float:
    market_value = _calculate_curve_market_value(exposures, last_price_by_symbol)
    margin = _safe_div(market_value, _resolve_effective_leverage(leverage))
    return _curve_position_ratio_from_margin(margin, equity)


def _resolve_positions_market_value(positions: List[Dict[str, Any]]) -> float:
    total = 0.0
    for position in positions or []:
        total += max(0.0, float(position.get("marketValue", 0.0) or 0.0))
    return total


def _has_curve_exposure(exposures: Dict[Tuple[str, str], Dict[str, float]]) -> bool:
    for state in exposures.values():
        if float(state.get("volume", 0.0) or 0.0) > 0.0:
            return True
    return False


def _normalize_curve_market_symbol(symbol: str) -> str:
    return _normalize_market_symbol(symbol)


def _curve_sampling_interval(duration_ms: int) -> Tuple[str, int]:
    safe_duration = max(0, int(duration_ms or 0))
    minute_ms = 60 * 1000
    hour_ms = 60 * minute_ms
    day_ms = 24 * hour_ms
    if safe_duration <= 6 * hour_ms:
        return "1m", minute_ms
    if safe_duration <= 3 * day_ms:
        return "5m", 5 * minute_ms
    if safe_duration <= 14 * day_ms:
        return "15m", 15 * minute_ms
    if safe_duration <= 60 * day_ms:
        return "1h", hour_ms
    return "4h", 4 * hour_ms


def _curve_sampling_mt5_timeframe(interval: str) -> Any:
    safe_interval = str(interval or "").strip().lower()
    if mt5 is None:
        return None
    mapping = {
        "1m": "TIMEFRAME_M1",
        "5m": "TIMEFRAME_M5",
        "15m": "TIMEFRAME_M15",
        "1h": "TIMEFRAME_H1",
        "4h": "TIMEFRAME_H4",
    }
    timeframe_name = mapping.get(safe_interval, "TIMEFRAME_M1")
    return getattr(mt5, timeframe_name, None)


def _extract_mt5_rate_field(rate: Any, key: str) -> Any:
    if rate is None:
        return None
    if isinstance(rate, dict):
        return rate.get(key)
    if hasattr(rate, key):
        return getattr(rate, key)
    try:
        return rate[key]
    except Exception:
        return None


def _fetch_curve_price_samples_from_mt5(symbol: str,
                                        start_ms: int,
                                        end_ms: int) -> List[Dict[str, float]]:
    safe_symbol = str(symbol or "").strip()
    safe_start = max(0, int(start_ms or 0))
    safe_end = max(safe_start, int(end_ms or 0))
    if mt5 is None or not safe_symbol or safe_end <= safe_start:
        return []

    interval, interval_ms = _curve_sampling_interval(safe_end - safe_start)
    timeframe = _curve_sampling_mt5_timeframe(interval)
    if timeframe is None:
        return []

    selector = getattr(mt5, "symbol_select", None)
    if callable(selector):
        try:
            selector(safe_symbol, True)
        except Exception:
            pass

    query_start_ms = _strip_mt5_time_offset_ms(safe_start)
    query_end_ms = _strip_mt5_time_offset_ms(safe_end)
    date_from = datetime.fromtimestamp(query_start_ms / 1000.0, tz=timezone.utc)
    date_to = datetime.fromtimestamp(query_end_ms / 1000.0, tz=timezone.utc)

    rates = mt5.copy_rates_range(safe_symbol, timeframe, date_from, date_to)
    if rates is None:
        return []

    collected: List[Dict[str, float]] = []
    for rate in rates:
        open_seconds = _extract_mt5_rate_field(rate, "time")
        close_price = _extract_mt5_rate_field(rate, "close")
        try:
            open_ms = int(open_seconds or 0) * 1000
            close_price_value = float(close_price or 0.0)
        except Exception:
            continue
        if open_ms <= 0 or close_price_value <= 0.0:
            continue
        close_ms = _apply_mt5_time_offset_ms(open_ms + interval_ms - 1)
        if close_ms <= safe_start or close_ms >= safe_end:
            continue
        collected.append(
            {
                "timestamp": close_ms,
                "symbol": safe_symbol,
                "price": close_price_value,
            }
        )
    return collected


def _fetch_curve_price_samples(symbol: str,
                               start_ms: int,
                               end_ms: int,
                               fetch_rows_fn=None) -> List[Dict[str, float]]:
    safe_start = max(0, int(start_ms or 0))
    safe_end = max(safe_start, int(end_ms or 0))
    if safe_end <= safe_start:
        return []

    if fetch_rows_fn is None:
        return _fetch_curve_price_samples_from_mt5(symbol, safe_start, safe_end)

    normalized_symbol = _normalize_curve_market_symbol(symbol)
    if not normalized_symbol:
        return []

    interval, interval_ms = _curve_sampling_interval(safe_end - safe_start)
    fetcher = fetch_rows_fn
    cursor = safe_start
    collected: Dict[int, Dict[str, float]] = {}

    while cursor < safe_end:
        remaining = safe_end - cursor
        limit = max(1, min(1500, int(math.ceil(remaining / max(interval_ms, 1))) + 2))
        rows = fetcher(
            normalized_symbol,
            interval,
            limit,
            start_time_ms=cursor,
            end_time_ms=safe_end,
        ) or []
        if not rows:
            break
        last_open_time = cursor
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 5:
                continue
            open_time = int(row[0] or 0)
            close_time = int(row[6] or 0) if len(row) > 6 else (open_time + interval_ms - 1)
            close_price = float(row[4] or 0.0)
            last_open_time = max(last_open_time, open_time)
            if close_price <= 0.0 or close_time <= safe_start or close_time >= safe_end:
                continue
            collected[close_time] = {
                "timestamp": close_time,
                "symbol": normalized_symbol,
                "price": close_price,
            }
        next_cursor = last_open_time + interval_ms
        if next_cursor <= cursor:
            break
        cursor = next_cursor

    return [collected[key] for key in sorted(collected.keys())]


def _append_curve_history_samples(points: List[Dict[str, float]],
                                  exposures: Dict[Tuple[str, str], Dict[str, float]],
                                  last_price_by_symbol: Dict[str, float],
                                  running_balance: float,
                                  leverage: float,
                                  start_ms: int,
                                  end_ms: int,
                                  fetch_rows_fn=None) -> None:
    safe_start = int(start_ms or 0)
    safe_end = int(end_ms or 0)
    if safe_end <= safe_start or not _has_curve_exposure(exposures):
        return

    sample_map: Dict[int, Dict[str, float]] = {}
    active_symbols = sorted({
        _normalize_curve_market_symbol(symbol)
        for symbol, _side in exposures.keys()
        if symbol and float((exposures.get((symbol, _side)) or {}).get("volume", 0.0) or 0.0) > 0.0
    })
    for symbol in active_symbols:
        for sample in _fetch_curve_price_samples(symbol, safe_start, safe_end, fetch_rows_fn):
            timestamp = int(sample.get("timestamp", 0) or 0)
            if timestamp <= safe_start or timestamp >= safe_end:
                continue
            price = float(sample.get("price", 0.0) or 0.0)
            if price <= 0.0:
                continue
            sample_map.setdefault(timestamp, {})[symbol] = price

    for timestamp in sorted(sample_map.keys()):
        for symbol, price in sample_map[timestamp].items():
            last_price_by_symbol[symbol] = price
        floating = _calculate_curve_floating(exposures, last_price_by_symbol)
        equity = running_balance + floating
        points.append(_curve_point(
            timestamp,
            equity,
            running_balance,
            _calculate_curve_position_ratio(exposures, last_price_by_symbol, equity, leverage),
        ))


def _inject_positions_into_exposures(
    exposures: Dict[Tuple[str, str], Dict[str, float]],
    positions: List[Dict[str, Any]],
    open_position_ids: set,
    contract_size_fn,
    last_price_by_symbol: Dict[str, float],
) -> None:
    # 把当前未平仓持仓注入到曝光映射里，避免与本窗口内开仓的重复叠加
    for position in positions or []:
        position_id = int(position.get("positionId", 0) or 0)
        ticket = int(position.get("positionTicket", 0))
        if (position_id > 0 and position_id in open_position_ids) or (ticket > 0 and ticket in open_position_ids):
            continue
        symbol = str(position.get("code") or position.get("productName") or "").strip()
        if not symbol:
            continue
        side = str(position.get("side") or "Buy")
        volume = float(position.get("quantity", 0.0))
        if volume <= 0.0:
            continue
        open_price = float(position.get("costPrice", 0.0))
        contract_size = contract_size_fn(symbol)
        latest_price = float(position.get("latestPrice", 0.0))
        if open_price <= 0.0 and latest_price > 0.0:
            open_price = latest_price
        _add_curve_exposure(exposures, symbol, side, volume, open_price, contract_size)
        if latest_price > 0.0:
            last_price_by_symbol.setdefault(symbol, latest_price)


def _resolve_open_position_ids_from_history(deal_history: List[Dict[str, Any]]) -> set:
    # 只保留在历史窗口末尾仍然未平的 position id，避免把已平历史仓位误当成当前持仓。
    states: Dict[Tuple[int, str], float] = {}
    sorted_deals = sorted(deal_history or [], key=_deal_history_sort_key)
    for deal in sorted_deals:
        position_id = int(deal.get("position_id", 0) or 0)
        if position_id <= 0:
            continue
        entry = int(deal.get("entry", 0) or 0)
        # deal_type=0 是合法 Buy 取值，不能再用 `or -1` 吞掉。
        deal_type = int(deal.get("deal_type", -1))
        volume = abs(float(deal.get("volume", 0.0) or 0.0))
        if volume <= 0.0:
            continue
        if _is_entry_close(entry):
            close_side = _curve_exposure_side_for_close(deal_type)
            close_key = (position_id, close_side)
            states[close_key] = max(0.0, float(states.get(close_key, 0.0) or 0.0) - volume)
        if _is_entry_open(entry):
            open_side = "Buy" if _is_buy_deal_type(deal_type) else "Sell"
            open_key = (position_id, open_side)
            states[open_key] = float(states.get(open_key, 0.0) or 0.0) + volume
    return {
        position_id
        for (position_id, _side), volume in states.items()
        if float(volume or 0.0) > 1e-9
    }


def _replay_curve_from_history(
    deal_history: List[Dict[str, Any]],
    start_balance: float,
    open_positions: List[Dict[str, Any]],
    current_balance: float,
    current_equity: float,
    leverage: float,
    contract_size_fn,
    now_ms: int,
    fetch_rows_fn=None,
) -> List[Dict[str, float]]:
    # 以时间序列方式重放成交和持仓，生成 equity/balance 分离的曲线点
    if not deal_history:
        market_value = _resolve_positions_market_value(open_positions)
        return [_curve_point(
            now_ms,
            current_equity,
            current_balance,
            _curve_position_ratio_from_margin(
                _safe_div(market_value, _resolve_effective_leverage(leverage)),
                current_equity,
            ),
        )]

    exposures: Dict[Tuple[str, str], Dict[str, float]] = {}
    last_price_by_symbol: Dict[str, float] = {}
    open_position_ids = _resolve_open_position_ids_from_history(deal_history)
    _inject_positions_into_exposures(exposures, open_positions, open_position_ids, contract_size_fn, last_price_by_symbol)

    sorted_deals = sorted(deal_history, key=_deal_history_sort_key)
    running_balance = float(start_balance)
    points: List[Dict[str, float]] = []
    first_ts = max(int(sorted_deals[0].get("timestamp", 0)), 0)
    floating = _calculate_curve_floating(exposures, last_price_by_symbol)
    first_equity = running_balance + floating
    points.append(_curve_point(
        first_ts,
        first_equity,
        running_balance,
        _calculate_curve_position_ratio(exposures, last_price_by_symbol, first_equity, leverage),
    ))

    last_event_ts = first_ts
    for index, deal in enumerate(sorted_deals):
        timestamp = int(deal.get("timestamp", 0))
        if index > 0 and timestamp > last_event_ts:
            _append_curve_history_samples(
                points,
                exposures,
                last_price_by_symbol,
                running_balance,
                leverage,
                last_event_ts,
                timestamp,
                fetch_rows_fn,
            )
        symbol = str(deal.get("symbol", "") or "")
        price = float(deal.get("price", 0.0))
        if symbol and price > 0.0:
            last_price_by_symbol[_normalize_curve_market_symbol(symbol)] = price

        entry = int(deal.get("entry", 0))
        deal_type = int(deal.get("deal_type", -1))
        volume = abs(float(deal.get("volume", 0.0)))
        direction = "Buy" if _is_buy_deal_type(deal_type) else "Sell"

        if _is_entry_close(entry):
            _remove_curve_exposure(exposures, symbol, _curve_exposure_side_for_close(deal_type), volume)
        if _is_entry_open(entry):
            contract_size = contract_size_fn(symbol)
            _add_curve_exposure(exposures, symbol, direction, volume, price, contract_size)

        running_balance += (
            float(deal.get("profit", 0.0))
            + float(deal.get("commission", 0.0))
            + float(deal.get("swap", 0.0))
        )
        floating = _calculate_curve_floating(exposures, last_price_by_symbol)
        equity = running_balance + floating
        points.append(_curve_point(
            timestamp,
            equity,
            running_balance,
            _calculate_curve_position_ratio(exposures, last_price_by_symbol, equity, leverage),
        ))
        last_event_ts = timestamp

    if now_ms > last_event_ts:
        _append_curve_history_samples(
            points,
            exposures,
            last_price_by_symbol,
            running_balance,
            leverage,
            last_event_ts,
            now_ms,
            fetch_rows_fn,
        )

    current_market_value = _resolve_positions_market_value(open_positions)
    if current_market_value <= 0.0:
        current_market_value = _calculate_curve_market_value(exposures, last_price_by_symbol)
    final_timestamp = max(now_ms, last_event_ts)
    final_point = _curve_point(
        final_timestamp,
        current_equity,
        current_balance,
        _curve_position_ratio_from_margin(
            _safe_div(current_market_value, _resolve_effective_leverage(leverage)),
            current_equity,
        ),
    )
    if points and int(points[-1].get("timestamp", 0) or 0) >= final_timestamp:
        points[-1] = final_point
    else:
        points.append(final_point)
    return points


def _curve_indicators(points: List[Dict]) -> List[Dict]:
    if not points:
        return []

    values = [p["equity"] for p in points]
    returns = []
    for i in range(1, len(values)):
        returns.append(_safe_div(values[i] - values[i - 1], values[i - 1]))

    peak = values[0]
    max_dd = 0.0
    for value in values:
        peak = max(peak, value)
        max_dd = max(max_dd, _safe_div(peak - value, peak))

    def n_return(n: int) -> float:
        if len(values) < 2:
            return 0.0
        idx = max(0, len(values) - 1 - n)
        return _safe_div(values[-1] - values[idx], values[idx])

    volatility = (pstdev(returns) if len(returns) > 1 else 0.0) * math.sqrt(365.0)
    sharpe = 0.0
    if volatility > 1e-9 and returns:
        sharpe = (mean(returns) * 365.0) / volatility

    return [
        {"name": "1D Return", "value": _fmt_pct(n_return(24))},
        {"name": "7D Return", "value": _fmt_pct(n_return(24 * 7))},
        {"name": "30D Return", "value": _fmt_pct(n_return(24 * 30))},
        {"name": "Max Drawdown", "value": _fmt_pct(max_dd)},
        {"name": "Volatility", "value": _fmt_pct(volatility)},
        {"name": "Sharpe", "value": f"{sharpe:.2f}"},
    ]


def _map_positions() -> List[Dict]:
    positions = mt5.positions_get() or []
    orders = mt5.orders_get() or []
    pending_by_symbol: Dict[str, Dict[str, float]] = {}
    for order in orders:
        symbol = getattr(order, "symbol", "")
        if not symbol:
            continue
        state = pending_by_symbol.setdefault(symbol, {"count": 0, "lots": 0.0, "notional": 0.0})
        volume = abs(float(getattr(order, "volume_current", 0.0)))
        order_price = float(getattr(order, "price_open", 0.0))
        if order_price <= 0.0:
            order_price = float(getattr(order, "price_current", 0.0))
        state["count"] += 1
        state["lots"] += volume
        state["notional"] += abs(volume * order_price)

    total_mv = 0.0
    mapped = []
    for position in positions:
        symbol = getattr(position, "symbol", "--")
        symbol_descriptor = _resolve_symbol_descriptor(symbol)
        position_type = int(getattr(position, "type", 0))
        side = "Buy" if position_type == 0 else "Sell"
        volume = float(getattr(position, "volume", 0.0))
        price_open = float(getattr(position, "price_open", 0.0))
        price_current = float(getattr(position, "price_current", 0.0))
        total_pnl = float(getattr(position, "profit", 0.0))

        contract_size = 1.0
        sinfo = mt5.symbol_info(symbol)
        if sinfo is not None and getattr(sinfo, "trade_contract_size", 0.0) > 0:
            contract_size = float(getattr(sinfo, "trade_contract_size", 1.0))

        market_value = abs(volume * price_current * contract_size)
        total_mv += market_value
        ret = _safe_div(price_current - price_open, price_open)

        mapped.append(
            {
                "productId": symbol_descriptor["productId"],
                "marketSymbol": symbol_descriptor["marketSymbol"],
                "tradeSymbol": symbol_descriptor["tradeSymbol"],
                "productName": symbol_descriptor["tradeSymbol"],
                "code": symbol_descriptor["tradeSymbol"],
                "side": side,
                "positionId": int(getattr(position, "identifier", 0) or getattr(position, "ticket", 0)),
                "positionTicket": int(getattr(position, "ticket", 0)),
                "openTime": _position_time_ms(position),
                "quantity": volume,
                "sellableQuantity": volume,
                "costPrice": price_open,
                "latestPrice": price_current,
                "marketValue": market_value,
                "positionRatio": 0.0,
                "dayPnL": total_pnl * 0.2,
                "totalPnL": total_pnl,
                "returnRate": ret,
                "pendingLots": pending_by_symbol.get(symbol, {}).get("lots", 0.0),
                "pendingCount": int(pending_by_symbol.get(symbol, {}).get("count", 0)),
                "pendingPrice": _safe_div(
                    pending_by_symbol.get(symbol, {}).get("notional", 0.0),
                    pending_by_symbol.get(symbol, {}).get("lots", 0.0),
                ),
                "takeProfit": float(getattr(position, "tp", 0.0)),
                "stopLoss": float(getattr(position, "sl", 0.0)),
                "storageFee": float(getattr(position, "swap", 0.0)),
            }
        )

    for item in mapped:
        item["positionRatio"] = _safe_div(item["marketValue"], total_mv)
    return mapped


def _map_pending_orders() -> List[Dict]:
    orders = mt5.orders_get() or []
    mapped = []
    for order in orders:
        symbol = getattr(order, "symbol", "")
        if not symbol:
            continue
        symbol_descriptor = _resolve_symbol_descriptor(symbol)

        order_type = int(getattr(order, "type", 0))
        side = _order_side(order_type)
        volume = abs(float(getattr(order, "volume_current", 0.0)))
        if volume <= 0.0:
            volume = abs(float(getattr(order, "volume_initial", 0.0)))
        if volume <= 0.0:
            continue

        price_open = float(getattr(order, "price_open", 0.0))
        price_current = float(getattr(order, "price_current", 0.0))
        pending_price = price_open if price_open > 0.0 else price_current
        latest_price = price_current if price_current > 0.0 else pending_price

        mapped.append(
            {
                "productId": symbol_descriptor["productId"],
                "marketSymbol": symbol_descriptor["marketSymbol"],
                "tradeSymbol": symbol_descriptor["tradeSymbol"],
                "productName": symbol_descriptor["tradeSymbol"],
                "code": symbol_descriptor["tradeSymbol"],
                "side": side,
                "orderId": int(getattr(order, "ticket", 0)),
                "openTime": _order_open_time_ms(order),
                "quantity": 0.0,
                "sellableQuantity": 0.0,
                "costPrice": 0.0,
                "latestPrice": latest_price,
                "marketValue": 0.0,
                "positionRatio": 0.0,
                "dayPnL": 0.0,
                "totalPnL": 0.0,
                "returnRate": 0.0,
                "pendingLots": volume,
                "pendingCount": 1,
                "pendingPrice": pending_price,
                "takeProfit": float(getattr(order, "tp", 0.0)),
                "stopLoss": float(getattr(order, "sl", 0.0)),
            }
        )

    mapped.sort(key=lambda item: item["pendingLots"], reverse=True)
    return mapped


def _progressive_trade_history_deals(range_key: str) -> List[Any]:
    from_time, to_time = _mt5_history_window(range_key)
    if range_key.lower() != "all":
        return mt5.history_deals_get(from_time, to_time) or []

    max_days = max(30, SNAPSHOT_RANGE_ALL_DAYS)
    progressive_days = [30, 90, 180, 365, 730, 1095, 1825, 3650, max_days]
    unique_days: List[int] = []
    for days in progressive_days:
        safe_days = max(30, min(days, max_days))
        if safe_days not in unique_days:
            unique_days.append(safe_days)

    now_local = datetime.now()
    deals: List[Any] = []
    previous_count = -1
    for days in unique_days:
        window_from = now_local - timedelta(days=days)
        deals = mt5.history_deals_get(window_from, to_time) or []
        if len(deals) <= previous_count:
            break
        previous_count = len(deals)
    return deals


def _map_trade_deals(deals: List[Any]) -> List[Dict]:
    mapped = []
    open_batches: Dict[str, List[Dict[str, Any]]] = {}
    contract_size_cache: Dict[str, float] = {}
    volume_epsilon = 1e-9

    def contract_size_of(symbol: str) -> float:
        cached = contract_size_cache.get(symbol)
        if cached is not None:
            return cached
        size = 1.0
        sinfo = mt5.symbol_info(symbol)
        if sinfo is not None and getattr(sinfo, "trade_contract_size", 0.0) > 0:
            size = float(getattr(sinfo, "trade_contract_size", 1.0))
        contract_size_cache[symbol] = size
        return size

    def lifecycle_key(position_id: int, order_id: int, ticket: int) -> str:
        if position_id > 0:
            return f"position:{position_id}"
        if order_id > 0:
            return f"order:{order_id}"
        return f"ticket:{ticket}"

    def infer_original_side(deal_type: int) -> str:
        return "Buy" if deal_type == 1 else "Sell"

    def append_open_batch(key: str,
                          symbol: str,
                          side: str,
                          open_time_ms: int,
                          open_price: float,
                          volume: float) -> None:
        if volume <= volume_epsilon:
            return
        batch = {
            "symbol": symbol,
            "side": side,
            "open_time": int(open_time_ms),
            "open_price": float(open_price),
            "remaining_volume": float(volume),
        }
        primary_queue = open_batches.setdefault(key, [])
        primary_queue.append(batch)

    def consume_queue(queue: List[Dict[str, Any]], close_volume: float) -> Tuple[List[Tuple[Dict[str, Any], float]], float]:
        matches: List[Tuple[Dict[str, Any], float]] = []
        remaining = float(close_volume)
        while remaining > volume_epsilon and queue:
            batch = queue[0]
            batch_remaining = float(batch.get("remaining_volume", 0.0))
            if batch_remaining <= volume_epsilon:
                queue.pop(0)
                continue
            matched = min(remaining, batch_remaining)
            if matched <= volume_epsilon:
                queue.pop(0)
                continue
            matches.append((dict(batch), matched))
            batch["remaining_volume"] = max(0.0, batch_remaining - matched)
            remaining -= matched
            if float(batch.get("remaining_volume", 0.0)) <= volume_epsilon:
                queue.pop(0)
        return matches, max(0.0, remaining)

    def consume_open_batches(key: str, close_volume: float) -> Tuple[List[Tuple[Dict[str, Any], float]], float]:
        queue = open_batches.setdefault(key, [])
        return consume_queue(queue, close_volume)

    def resolve_split_ticket(ticket: int, split_count: int, split_index: int) -> int:
        if split_count <= 1 or ticket <= 0:
            return ticket
        return ticket * 1000 + split_index

    def append_close_record(symbol: str,
                            ticket: int,
                            order_id: int,
                            position_id: int,
                            entry_type: int,
                            close_time: int,
                            close_price: float,
                            close_volume: float,
                            total_volume: float,
                            total_profit: float,
                            total_commission: float,
                            total_swap: float,
                            open_time: int,
                            open_price: float,
                            side: str,
                            split_count: int,
                            split_index: int,
                            remark: str) -> None:
        if close_volume <= volume_epsilon or total_volume <= volume_epsilon:
            return
        symbol_descriptor = _resolve_symbol_descriptor(symbol)
        ratio = close_volume / total_volume
        storage_fee = (total_commission + total_swap) * ratio
        amount = abs(close_volume * close_price * contract_size_of(symbol))
        mapped.append(
            {
                "timestamp": close_time,
                "productId": symbol_descriptor["productId"],
                "marketSymbol": symbol_descriptor["marketSymbol"],
                "tradeSymbol": symbol_descriptor["tradeSymbol"],
                "productName": symbol_descriptor["tradeSymbol"],
                "code": symbol_descriptor["tradeSymbol"],
                "side": side,
                "price": close_price,
                "quantity": close_volume,
                "amount": amount,
                "fee": storage_fee,
                "profit": total_profit * ratio,
                "openTime": open_time,
                "closeTime": close_time,
                "openPrice": open_price if open_price > 0.0 else close_price,
                "closePrice": close_price if close_price > 0.0 else open_price,
                "storageFee": storage_fee,
                "dealTicket": resolve_split_ticket(ticket, split_count, split_index),
                "orderId": order_id,
                "positionId": position_id,
                "entryType": entry_type,
                "remark": remark or "",
            }
        )

    sorted_deals = sorted(deals, key=_deal_sort_key)

    for deal in sorted_deals:
        symbol = getattr(deal, "symbol", "")
        if not symbol:
            continue
        deal_type = int(getattr(deal, "type", -1))
        if not _is_trade_deal_type(deal_type):
            continue

        volume = abs(float(getattr(deal, "volume", 0.0)))
        if volume <= 0.0:
            continue

        ticket = int(getattr(deal, "ticket", 0))
        order_id = int(getattr(deal, "order", 0))
        position_id = int(getattr(deal, "position_id", 0))
        key = lifecycle_key(position_id, order_id, ticket)
        entry_type = int(getattr(deal, "entry", 0))
        ts = _deal_time_ms(deal)
        price = float(getattr(deal, "price", 0.0))
        profit = float(getattr(deal, "profit", 0.0))
        commission = float(getattr(deal, "commission", 0.0))
        swap = float(getattr(deal, "swap", 0.0))
        current_side = "Buy" if _is_buy_deal_type(deal_type) else "Sell"
        comment = getattr(deal, "comment", "") or ""

        if _is_entry_close(entry_type):
            original_side = infer_original_side(deal_type)
            matches, remaining_after_close = consume_open_batches(key, volume)
            if entry_type == 2 and matches:
                synthetic_close_volume = 0.0
                reverse_open_volume = remaining_after_close
            elif entry_type == 2:
                synthetic_close_volume = volume
                reverse_open_volume = 0.0
            else:
                synthetic_close_volume = remaining_after_close
                reverse_open_volume = 0.0
            split_count = len(matches) + (1 if synthetic_close_volume > volume_epsilon else 0)
            split_index = 0
            for batch, matched_volume in matches:
                split_index += 1
                append_close_record(
                    symbol=symbol,
                    ticket=ticket,
                    order_id=order_id,
                    position_id=position_id,
                    entry_type=entry_type,
                    close_time=ts,
                    close_price=price,
                    close_volume=matched_volume,
                    total_volume=volume,
                    total_profit=profit,
                    total_commission=commission,
                    total_swap=swap,
                    open_time=int(batch.get("open_time", ts)),
                    open_price=float(batch.get("open_price", price)),
                    side=str(batch.get("side", infer_original_side(deal_type))),
                    split_count=split_count,
                    split_index=split_index,
                    remark=comment,
                )
            if synthetic_close_volume > volume_epsilon:
                split_index += 1
                append_close_record(
                    symbol=symbol,
                    ticket=ticket,
                    order_id=order_id,
                    position_id=position_id,
                    entry_type=entry_type,
                    close_time=ts,
                    close_price=price,
                    close_volume=synthetic_close_volume,
                    total_volume=volume,
                    total_profit=profit,
                    total_commission=commission,
                    total_swap=swap,
                    open_time=ts,
                    open_price=price,
                    side=original_side,
                    split_count=split_count,
                    split_index=split_index,
                    remark=comment,
                )
        else:
            reverse_open_volume = 0.0

        if _is_entry_open(entry_type):
            open_volume = volume if entry_type != 2 else reverse_open_volume
            append_open_batch(
                key=key,
                symbol=symbol,
                side=current_side,
                open_time_ms=ts,
                open_price=price,
                volume=open_volume,
            )

    mapped.sort(key=lambda item: item["timestamp"], reverse=True)
    return mapped


def _map_trades(range_key: str = "all") -> List[Dict]:
    deals = _progressive_trade_history_deals(range_key)
    return _map_trade_deals(deals)


def _build_overview(positions: List[Dict], trades: List[Dict]) -> List[Dict]:
    account = mt5.account_info()
    if account is None:
        return []

    equity = float(account.equity)
    balance = float(account.balance)
    margin = float(account.margin)
    free_margin = float(account.margin_free)
    market_value = sum(p["marketValue"] for p in positions)
    total_pnl = sum(p["totalPnL"] for p in positions)
    realized_pnl = 0.0
    for trade in trades:
        profit = float(trade.get("profit", 0.0) or 0.0)
        commission = float(trade.get("commission", trade.get("fee", 0.0)) or 0.0)
        storage_fee = float(trade.get("swap", trade.get("storageFee", 0.0)) or 0.0)
        realized_pnl += profit + commission + storage_fee
    cumulative_pnl = realized_pnl + total_pnl
    day_pnl = sum(p["dayPnL"] for p in positions)

    today_ts = int(
        datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
    )
    day_fee = sum(t["fee"] for t in trades if t["timestamp"] >= today_ts)
    day_pnl -= day_fee

    total_asset = balance
    day_return = _safe_div(day_pnl, max(1.0, equity - day_pnl))
    total_return = _safe_div(cumulative_pnl, max(1.0, balance - realized_pnl))
    position_ratio = _safe_div(market_value, max(1.0, equity))
    position_return = _safe_div(total_pnl, max(1.0, total_asset))
    leverage = int(getattr(account, "leverage", 0) or 0)
    margin_level = float(getattr(account, "margin_level", 0.0) or 0.0)
    margin_level_text = "--" if not math.isfinite(margin_level) else f"{margin_level:.2f}%"

    return [
        {"name": "Total Asset", "value": _fmt_usd(total_asset)},
        {"name": "Margin", "value": _fmt_usd(margin)},
        {"name": "Free Fund", "value": _fmt_usd(free_margin)},
        {"name": "Position Market Value", "value": _fmt_usd(market_value)},
        {"name": "Position PnL", "value": _fmt_money(total_pnl)},
        {"name": "Position Return", "value": _fmt_pct(position_return)},
        {"name": "Daily PnL", "value": _fmt_money(day_pnl)},
        {"name": "Cumulative PnL", "value": _fmt_money(cumulative_pnl)},
        {"name": "Current Equity", "value": _fmt_usd(equity)},
        {"name": "Daily Return", "value": _fmt_pct(day_return)},
        {"name": "Total Return", "value": _fmt_pct(total_return)},
        {"name": "Position Ratio", "value": _fmt_pct(position_ratio)},
        {"name": "Leverage", "value": f"{leverage}x" if leverage > 0 else "--"},
        {"name": "Margin Level", "value": margin_level_text},
    ]


def _build_stats(positions: List[Dict], trades: List[Dict], points: List[Dict]) -> List[Dict]:
    open_position_pnl = sum(p["totalPnL"] for p in positions)
    realized_pnl = sum(float(t.get("profit", 0.0)) for t in trades)
    total_pnl = realized_pnl + open_position_pnl

    total_count = len(trades)
    buy_count = len([t for t in trades if t.get("side") == "Buy"])
    sell_count = len([t for t in trades if t.get("side") == "Sell"])

    wins = [float(t.get("profit", 0.0)) for t in trades if float(t.get("profit", 0.0)) > 0.0]
    losses = [float(t.get("profit", 0.0)) for t in trades if float(t.get("profit", 0.0)) < 0.0]
    win_count = len(wins)
    loss_count = len(losses)
    avg_win = mean(wins) if wins else 0.0
    avg_loss = mean(losses) if losses else 0.0
    ratio = abs(avg_win / avg_loss) if abs(avg_loss) > 1e-9 else 0.0
    win_rate = _safe_div(win_count, max(1, win_count + loss_count))

    now = datetime.now(timezone.utc)
    month_start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    year_start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
    month_start_ms = int(month_start.timestamp() * 1000)
    year_start_ms = int(year_start.timestamp() * 1000)
    month_profit = sum(float(t.get("profit", 0.0)) for t in trades if int(t.get("closeTime", t.get("timestamp", 0))) >= month_start_ms)
    ytd_profit = sum(float(t.get("profit", 0.0)) for t in trades if int(t.get("closeTime", t.get("timestamp", 0))) >= year_start_ms)

    close_times = [int(t.get("closeTime", t.get("timestamp", 0))) for t in trades if int(t.get("closeTime", t.get("timestamp", 0))) > 0]
    if close_times:
        span_days = max(1.0, (max(close_times) - min(close_times)) / (24.0 * 60.0 * 60.0 * 1000.0))
    else:
        span_days = 1.0
    daily_avg_profit = _safe_div(realized_pnl, span_days)

    max_dd = 0.0
    if points:
        peak = points[0]["equity"]
        for point in points:
            peak = max(peak, point["equity"])
            max_dd = max(max_dd, _safe_div(peak - point["equity"], peak))

    volatility = 0.0
    if len(points) > 2:
        returns = []
        for i in range(1, len(points)):
            returns.append(_safe_div(points[i]["equity"] - points[i - 1]["equity"], points[i - 1]["equity"]))
        volatility = (pstdev(returns) if len(returns) > 1 else 0.0) * math.sqrt(365.0)

    market_value = sum(p["marketValue"] for p in positions)
    top5 = sum(sorted([p["positionRatio"] for p in positions], reverse=True)[:5])
    latest_equity = points[-1]["equity"] if points else max(1.0, market_value)
    cumulative_return = _safe_div(realized_pnl, max(1.0, latest_equity - realized_pnl))

    longest_win = 0
    longest_loss = 0
    current_win = 0
    current_loss = 0
    ordered_trades = sorted(trades, key=lambda t: int(t.get("closeTime", t.get("timestamp", 0))))
    for trade in ordered_trades:
        pnl = float(trade.get("profit", 0.0))
        if pnl > 0.0:
            current_win += 1
            current_loss = 0
            longest_win = max(longest_win, current_win)
        elif pnl < 0.0:
            current_loss += 1
            current_win = 0
            longest_loss = max(longest_loss, current_loss)

    symbol_exposure: Dict[str, float] = {}
    for position in positions:
        code = str(position.get("code", ""))
        if not code:
            continue
        symbol_exposure[code] = symbol_exposure.get(code, 0.0) + float(position.get("marketValue", 0.0))
    symbols = sorted(symbol_exposure.items(), key=lambda item: item[1], reverse=True)
    asset_distribution = " / ".join([symbol for symbol, _ in symbols[:3]]) if symbols else "--"

    return [
        {"name": "Cumulative Profit", "value": _fmt_money(total_pnl)},
        {"name": "Cumulative Return", "value": _fmt_pct(cumulative_return)},
        {"name": "Month Profit", "value": _fmt_money(month_profit)},
        {"name": "YTD Profit", "value": _fmt_money(ytd_profit)},
        {"name": "Daily Avg Profit", "value": _fmt_money(daily_avg_profit)},
        {"name": "Total Trades", "value": str(total_count)},
        {"name": "Buy Count", "value": str(buy_count)},
        {"name": "Sell Count", "value": str(sell_count)},
        {"name": "Win Rate", "value": _fmt_pct(win_rate)},
        {"name": "Win/Loss Trades", "value": f"{win_count} / {loss_count}"},
        {"name": "Avg Profit/Trade", "value": _fmt_usd(avg_win)},
        {"name": "Avg Loss/Trade", "value": _fmt_money(avg_loss)},
        {"name": "PnL Ratio", "value": f"{ratio:.2f}"},
        {"name": "Max Drawdown", "value": _fmt_pct(max_dd)},
        {"name": "Volatility", "value": _fmt_pct(volatility)},
        {"name": "Position Utilization", "value": _fmt_pct(_safe_div(market_value, max(1.0, latest_equity)))},
        {"name": "Single Position Max", "value": _fmt_pct(max([p["positionRatio"] for p in positions], default=0.0))},
        {"name": "Concentration", "value": _fmt_pct(top5)},
        {"name": "Consecutive Win/Loss", "value": f"{longest_win} / {longest_loss}"},
        {"name": "Current Position Amount", "value": _fmt_usd(market_value)},
        {"name": "Asset Distribution", "value": asset_distribution},
        {"name": "Top-5 Position Ratio", "value": _fmt_pct(top5)},
    ]


def _snapshot_from_mt5(range_key: str) -> Dict:
    with state_lock:
        _ensure_mt5()
        try:
            account = mt5.account_info()
            if account is None:
                raise RuntimeError("account_info is None")
            positions = _map_positions()
            pending_orders = _map_pending_orders()
            raw_deals = _progressive_trade_history_deals(range_key)
            points = _build_curve_from_deals(raw_deals, current_positions=positions, account=account)
            trades = _map_trade_deals(raw_deals)
            overview = _build_overview(positions, trades)
            indicators = _curve_indicators(points)
            stats = _build_stats(positions, trades, points)
            return {
                "accountMeta": {
                    "login": str(getattr(account, "login", LOGIN)),
                    "server": str(getattr(account, "server", SERVER)),
                    "source": "MT5 Python Pull",
                    "accountMode": _detect_account_mode(),
                    "updatedAt": _now_ms(),
                    "currency": str(getattr(account, "currency", "")),
                    "leverage": int(getattr(account, "leverage", 0) or 0),
                    "balance": float(getattr(account, "balance", 0.0) or 0.0),
                    "equity": float(getattr(account, "equity", 0.0) or 0.0),
                    "margin": float(getattr(account, "margin", 0.0) or 0.0),
                    "freeMargin": float(getattr(account, "margin_free", 0.0) or 0.0),
                    "marginLevel": float(getattr(account, "margin_level", 0.0) or 0.0),
                    "profit": float(getattr(account, "profit", 0.0) or 0.0),
                    "name": str(getattr(account, "name", "")),
                    "company": str(getattr(account, "company", "")),
                    "range": range_key,
                    "tradeCount": len(trades),
                    "positionCount": len(positions),
                    "pendingOrderCount": len(pending_orders),
                    "curvePointCount": len(points),
                },
                "overviewMetrics": overview,
                "curvePoints": points,
                "curveIndicators": indicators,
                "positions": positions,
                "pendingOrders": pending_orders,
                "trades": trades,
                "statsMetrics": stats,
            }
        finally:
            _shutdown_mt5()


# 构建只包含账户摘要、当前持仓和挂单的轻快照，供高频 v2 接口复用。
def _snapshot_from_mt5_light() -> Dict:
    with state_lock:
        _ensure_mt5()
        try:
            account = mt5.account_info()
            if account is None:
                raise RuntimeError("account_info is None")
            positions = _map_positions()
            pending_orders = _map_pending_orders()
            trade_state = _light_snapshot_trade_state()
            trade_count = int(trade_state.get("tradeCount", 0) or 0)
            history_revision = str(trade_state.get("historyRevision", "") or "")
            if not history_revision:
                raise RuntimeError("light snapshot missing historyRevision")
            overview = _build_overview(positions, [])
            return {
                "accountMeta": {
                    "login": str(getattr(account, "login", LOGIN)),
                    "server": str(getattr(account, "server", SERVER)),
                    "source": "MT5 Python Pull",
                    "accountMode": _detect_account_mode(),
                    "updatedAt": _now_ms(),
                    "currency": str(getattr(account, "currency", "")),
                    "leverage": int(getattr(account, "leverage", 0) or 0),
                    "balance": float(getattr(account, "balance", 0.0) or 0.0),
                    "equity": float(getattr(account, "equity", 0.0) or 0.0),
                    "margin": float(getattr(account, "margin", 0.0) or 0.0),
                    "freeMargin": float(getattr(account, "margin_free", 0.0) or 0.0),
                    "marginLevel": float(getattr(account, "margin_level", 0.0) or 0.0),
                    "profit": float(getattr(account, "profit", 0.0) or 0.0),
                    "name": str(getattr(account, "name", "")),
                    "company": str(getattr(account, "company", "")),
                    "range": "light",
                    # 轻快照只读取历史成交总数，供客户端判断是否需要补拉全量历史。
                    "tradeCount": trade_count,
                    "historyRevision": history_revision,
                    "positionCount": len(positions),
                    "pendingOrderCount": len(pending_orders),
                    "curvePointCount": 0,
                },
                "overviewMetrics": overview,
                "curveIndicators": [],
                "statsMetrics": [],
                "positions": positions,
                "pendingOrders": pending_orders,
            }
        finally:
            _shutdown_mt5()


def _require_trade_contract_size(item: Dict[str, Any], context: str) -> float:
    value = item.get("contractSize", item.get("contract_size"))
    try:
        contract_size = float(value)
    except Exception as exc:
        raise RuntimeError(f"{context} missing contractSize") from exc
    if contract_size <= 0.0:
        raise RuntimeError(f"{context} missing contractSize")
    return contract_size


def _should_rebuild_ea_trade_records(records: List[Dict[str, Any]]) -> bool:
    if not records:
        return False
    identified_count = 0
    raw_like_count = 0
    for item in records:
        if not isinstance(item, dict):
            continue
        if any(key in item for key in ("entryType", "dealType", "dealTicket", "positionId", "orderId")):
            identified_count += 1
        timestamp = int(item.get("timestamp", item.get("time", 0)) or 0)
        price = float(item.get("price", 0.0) or 0.0)
        open_time = int(item.get("openTime", timestamp) or timestamp)
        close_time = int(item.get("closeTime", timestamp) or timestamp)
        open_price = float(item.get("openPrice", price) or price)
        close_price = float(item.get("closePrice", price) or price)
        if open_time == timestamp and close_time == timestamp and abs(open_price - price) < 1e-9 and abs(close_price - price) < 1e-9:
            raw_like_count += 1
    return identified_count > 0 and raw_like_count >= max(1, len(records) - 1)


def _rebuild_ea_trade_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rebuilt: List[Dict[str, Any]] = []
    open_batches: Dict[str, List[Dict[str, Any]]] = {}
    open_batches_by_symbol_side: Dict[str, List[Dict[str, Any]]] = {}
    volume_epsilon = 1e-9

    def contract_size_of(item: Dict[str, Any]) -> float:
        return _require_trade_contract_size(item, "ea trade")

    def lifecycle_key(position_id: int, order_id: int, ticket: int) -> str:
        if position_id > 0:
            return f"position:{position_id}"
        if order_id > 0:
            return f"order:{order_id}"
        return f"ticket:{ticket}"

    def infer_original_side(deal_type: int) -> str:
        return "Buy" if deal_type == 1 else "Sell"

    def symbol_side_key(symbol: str, side: str) -> str:
        return f"{symbol}:{side}"

    def append_open_batch(key: str,
                          symbol: str,
                          side: str,
                          open_time_ms: int,
                          open_price: float,
                          volume: float) -> None:
        if volume <= volume_epsilon:
            return
        batch = {
            "symbol": symbol,
            "side": side,
            "open_time": int(open_time_ms),
            "open_price": float(open_price),
            "remaining_volume": float(volume),
        }
        open_batches.setdefault(key, []).append(batch)
        open_batches_by_symbol_side.setdefault(symbol_side_key(symbol, side), []).append(batch)

    def consume_queue(queue: List[Dict[str, Any]], close_volume: float) -> Tuple[List[Tuple[Dict[str, Any], float]], float]:
        matches: List[Tuple[Dict[str, Any], float]] = []
        remaining = float(close_volume)
        while remaining > volume_epsilon and queue:
            batch = queue[0]
            batch_remaining = float(batch.get("remaining_volume", 0.0))
            if batch_remaining <= volume_epsilon:
                queue.pop(0)
                continue
            matched = min(remaining, batch_remaining)
            if matched <= volume_epsilon:
                queue.pop(0)
                continue
            matches.append((dict(batch), matched))
            batch["remaining_volume"] = max(0.0, batch_remaining - matched)
            remaining -= matched
            if float(batch.get("remaining_volume", 0.0)) <= volume_epsilon:
                queue.pop(0)
        return matches, max(0.0, remaining)

    def consume_open_batches(key: str, close_volume: float) -> Tuple[List[Tuple[Dict[str, Any], float]], float]:
        return consume_queue(open_batches.setdefault(key, []), close_volume)

    def consume_open_batches_by_symbol_side(symbol: str,
                                            side: str,
                                            close_volume: float) -> Tuple[List[Tuple[Dict[str, Any], float]], float]:
        return consume_queue(open_batches_by_symbol_side.setdefault(symbol_side_key(symbol, side), []), close_volume)

    def resolve_split_ticket(ticket: int, split_count: int, split_index: int) -> int:
        if split_count <= 1 or ticket <= 0:
            return ticket
        return ticket * 1000 + split_index

    def append_close_record(symbol: str,
                            ticket: int,
                            order_id: int,
                            position_id: int,
                            entry_type: int,
                            close_time: int,
                            close_price: float,
                            close_volume: float,
                            total_volume: float,
                            total_profit: float,
                            total_commission: float,
                            total_swap: float,
                            open_time: int,
                            open_price: float,
                            side: str,
                            split_count: int,
                            split_index: int,
                            remark: str,
                            contract_size: float) -> None:
        if close_volume <= volume_epsilon or total_volume <= volume_epsilon:
            return
        ratio = close_volume / total_volume
        commission_fee = abs(total_commission) * ratio
        storage_fee = total_swap * ratio
        amount = abs(close_volume * close_price * contract_size)
        rebuilt.append(
            {
                "timestamp": close_time,
                "productName": symbol,
                "code": symbol,
                "side": side,
                "price": close_price,
                "quantity": close_volume,
                "amount": amount,
                "fee": commission_fee,
                "profit": total_profit * ratio,
                "openTime": open_time,
                "closeTime": close_time,
                "openPrice": open_price if open_price > 0.0 else close_price,
                "closePrice": close_price if close_price > 0.0 else open_price,
                "storageFee": storage_fee,
                "dealTicket": resolve_split_ticket(ticket, split_count, split_index),
                "orderId": order_id,
                "positionId": position_id,
                "entryType": entry_type,
                "remark": remark or "",
            }
        )

    sorted_records = sorted(records, key=_trade_record_sort_key)
    for record in sorted_records:
        symbol = str(record.get("code") or record.get("productName") or "").strip()
        if not symbol:
            continue
        deal_type = int(record.get("dealType", 0 if str(record.get("side", "")).strip().lower() == "buy" else 1) or 0)
        if not _is_trade_deal_type(deal_type):
            continue
        volume = abs(float(record.get("quantity", record.get("volume", 0.0)) or 0.0))
        if volume <= 0.0:
            continue
        ticket = int(record.get("dealTicket", record.get("ticket", 0)) or 0)
        order_id = int(record.get("orderId", record.get("order", 0)) or 0)
        position_id = int(record.get("positionId", record.get("position_id", 0)) or 0)
        key = lifecycle_key(position_id, order_id, ticket)
        entry_type = int(record.get("entryType", record.get("entry", 0)) or 0)
        timestamp = int(record.get("timestamp", record.get("time", 0)) or 0)
        price = float(record.get("price", 0.0) or 0.0)
        profit = float(record.get("profit", 0.0) or 0.0)
        commission = float(record.get("commission", record.get("fee", 0.0)) or 0.0)
        swap = float(record.get("swap", record.get("storageFee", 0.0)) or 0.0)
        current_side = "Buy" if _is_buy_deal_type(deal_type) else "Sell"
        comment = str(record.get("remark", "") or "")
        contract_size = contract_size_of(record)

        if _is_entry_close(entry_type):
            original_side = infer_original_side(deal_type)
            matches, remaining_after_close = consume_open_batches(key, volume)

            split_count = len(matches) if matches else 1
            if matches:
                for split_index, (batch, matched_volume) in enumerate(matches):
                    append_close_record(
                        symbol=symbol,
                        ticket=ticket,
                        order_id=order_id,
                        position_id=position_id,
                        entry_type=entry_type,
                        close_time=timestamp,
                        close_price=price,
                        close_volume=matched_volume,
                        total_volume=volume,
                        total_profit=profit,
                        total_commission=commission,
                        total_swap=swap,
                        open_time=int(batch.get("open_time", timestamp) or timestamp),
                        open_price=float(batch.get("open_price", price) or price),
                        side=original_side,
                        split_count=split_count,
                        split_index=split_index,
                        remark=comment,
                        contract_size=contract_size,
                    )
            elif remaining_after_close > volume_epsilon:
                append_close_record(
                    symbol=symbol,
                    ticket=ticket,
                    order_id=order_id,
                    position_id=position_id,
                    entry_type=entry_type,
                    close_time=timestamp,
                    close_price=price,
                    close_volume=remaining_after_close,
                    total_volume=volume,
                    total_profit=profit,
                    total_commission=commission,
                    total_swap=swap,
                    open_time=timestamp,
                    open_price=price,
                    side=original_side,
                    split_count=1,
                    split_index=0,
                    remark=comment,
                    contract_size=contract_size,
                )

        if _is_entry_open(entry_type):
            append_open_batch(key, symbol, current_side, timestamp, price, volume)

    rebuilt.sort(key=lambda item: int(item.get("closeTime", item.get("timestamp", 0)) or 0), reverse=True)
    return rebuilt


def _normalize_ea_snapshot_trades(account_meta: Dict[str, Any], trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    source = str((account_meta or {}).get("source", "") or "")
    if "MT5 EA Push" not in source:
        return trades
    if not _should_rebuild_ea_trade_records(trades):
        return trades
    rebuilt = _rebuild_ea_trade_records(trades)
    return rebuilt if rebuilt else trades


def _is_ea_push_snapshot_source(account_meta: Dict[str, Any], source_fallback: str) -> bool:
    """统一判断当前快照是否来自 EA 推送链。"""
    source = str((account_meta or {}).get("source", "") or "").strip()
    fallback = str(source_fallback or "").strip()
    return "MT5 EA Push" in source or "MT5 EA Push" in fallback


def _normalize_ea_snapshot_time_field(item: Dict[str, Any], key: str) -> None:
    """把 EA 快照里的 MT5 原始毫秒时间统一归一化成 UTC 毫秒时间。"""
    if not isinstance(item, dict):
        return
    value = int(item.get(key, 0) or 0)
    if value <= 0:
        return
    item[key] = _apply_mt5_time_offset_ms(value)


def _normalize_ea_snapshot_time_fields(account_meta: Dict[str, Any],
                                       source_fallback: str,
                                       payload: Dict[str, Any]) -> None:
    """统一修正 EA 推送快照里的时间字段，避免成交历史和图表继续直接消费券商墙上时间。"""
    if not _is_ea_push_snapshot_source(account_meta, source_fallback):
        return

    for point in payload.get("curvePoints") or []:
        _normalize_ea_snapshot_time_field(point, "timestamp")

    for position in payload.get("positions") or []:
        _normalize_ea_snapshot_time_field(position, "openTime")

    for order in payload.get("pendingOrders") or []:
        _normalize_ea_snapshot_time_field(order, "openTime")

    for trade in payload.get("trades") or []:
        _normalize_ea_snapshot_time_field(trade, "timestamp")
        _normalize_ea_snapshot_time_field(trade, "openTime")
        _normalize_ea_snapshot_time_field(trade, "closeTime")


def _overview_metric_value(metrics: List[Dict[str, Any]], *names: str) -> float:
    if not metrics or not names:
        return 0.0
    normalized_names = [str(name or "").strip().lower() for name in names if str(name or "").strip()]
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        metric_name = str(metric.get("name", "") or "").strip().lower()
        if not metric_name:
            continue
        if any(candidate in metric_name for candidate in normalized_names):
            raw_value = str(metric.get("value", "") or "")
            number = "".join(ch for ch in raw_value if ch.isdigit() or ch in ".-+")
            try:
                return float(number)
            except Exception:
                return 0.0
    return 0.0


def _rebuild_sparse_ea_curve_points(account_meta: Dict[str, Any],
                                    overview_metrics: List[Dict[str, Any]],
                                    curve_points: List[Dict[str, Any]],
                                    raw_trades: List[Dict[str, Any]],
                                    positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    source = str((account_meta or {}).get("source", "") or "")
    if "MT5 EA Push" not in source:
        return curve_points
    if not raw_trades:
        return curve_points

    deal_history: List[Dict[str, Any]] = []
    realized = 0.0
    contract_size_cache: Dict[str, float] = {}

    def contract_size_fn(symbol: str) -> float:
        cached = contract_size_cache.get(symbol)
        if cached is not None:
            return cached
        for item in raw_trades:
            item_symbol = str(item.get("code") or item.get("productName") or item.get("symbol") or "").strip()
            if item_symbol != symbol:
                continue
            cached = _require_trade_contract_size(item, f"ea curve trade {symbol}")
            contract_size_cache[symbol] = cached
            return cached
        raise RuntimeError(f"ea curve trade {symbol} missing contractSize")

    for item in sorted(raw_trades, key=lambda current: int(current.get("timestamp", current.get("time", 0)) or 0)):
        entry = int(item.get("entryType", item.get("entry", 0)) or 0)
        deal_type = int(item.get("dealType", item.get("deal_type", 0)) or 0)
        volume = abs(float(item.get("quantity", item.get("volume", 0.0)) or 0.0))
        symbol = str(item.get("code") or item.get("productName") or item.get("symbol") or "").strip()
        if not symbol or volume <= 0.0 or not _is_trade_deal_type(deal_type):
            continue
        profit = float(item.get("profit", 0.0) or 0.0)
        commission = float(item.get("commission", item.get("fee", 0.0)) or 0.0)
        swap = float(item.get("swap", item.get("storageFee", 0.0)) or 0.0)
        realized += profit + commission + swap
        deal_history.append({
            "timestamp": int(item.get("timestamp", item.get("time", 0)) or 0),
            "price": float(item.get("price", 0.0) or 0.0),
            "profit": profit,
            "commission": commission,
            "swap": swap,
            "entry": entry,
            "deal_type": deal_type,
            "volume": volume,
            "symbol": symbol,
            "position_id": int(item.get("positionId", item.get("position_id", 0)) or 0),
        })

    if not deal_history:
        return curve_points

    current_balance = float((account_meta or {}).get("balance", 0.0) or 0.0)
    current_equity = float((account_meta or {}).get("equity", 0.0) or 0.0)
    leverage = float((account_meta or {}).get("leverage", 0.0) or 0.0)
    if current_balance <= 0.0:
        current_balance = _overview_metric_value(overview_metrics, "balance", "结余")
    if current_equity <= 0.0:
        current_equity = _overview_metric_value(overview_metrics, "current equity", "equity", "净资产", "净值")
    if leverage <= 0.0:
        leverage = _overview_metric_value(overview_metrics, "leverage", "杠杆")
    if current_balance <= 0.0:
        current_balance = current_equity
    if current_equity <= 0.0:
        current_equity = current_balance
    start_balance = current_balance - realized

    rebuilt = _replay_curve_from_history(
        deal_history=deal_history,
        start_balance=start_balance,
        open_positions=positions or [],
        current_balance=current_balance,
        current_equity=current_equity,
        leverage=leverage,
        contract_size_fn=contract_size_fn,
        now_ms=max(int((account_meta or {}).get("updatedAt", 0) or 0), _now_ms()),
        fetch_rows_fn=lambda symbol, interval, limit, **kwargs: [],
    )
    return rebuilt if rebuilt else curve_points


def _enrich_position_symbol_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    symbol_descriptor = _resolve_symbol_descriptor(
        str(item.get("tradeSymbol") or item.get("code") or item.get("symbol") or item.get("productName") or "")
    )
    enriched = dict(item)
    enriched["productId"] = str(item.get("productId") or symbol_descriptor["productId"])
    enriched["marketSymbol"] = str(item.get("marketSymbol") or symbol_descriptor["marketSymbol"])
    enriched["tradeSymbol"] = str(item.get("tradeSymbol") or symbol_descriptor["tradeSymbol"])
    enriched["code"] = str(item.get("code") or enriched["tradeSymbol"])
    enriched["productName"] = str(item.get("productName") or enriched["tradeSymbol"])
    return enriched


def _enrich_trade_symbol_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    symbol_descriptor = _resolve_symbol_descriptor(
        str(item.get("tradeSymbol") or item.get("code") or item.get("symbol") or item.get("productName") or "")
    )
    enriched = dict(item)
    enriched["productId"] = str(item.get("productId") or symbol_descriptor["productId"])
    enriched["marketSymbol"] = str(item.get("marketSymbol") or symbol_descriptor["marketSymbol"])
    enriched["tradeSymbol"] = str(item.get("tradeSymbol") or symbol_descriptor["tradeSymbol"])
    enriched["code"] = str(item.get("code") or enriched["tradeSymbol"])
    enriched["productName"] = str(item.get("productName") or enriched["tradeSymbol"])
    return enriched


def _build_history_revision_from_snapshot(snapshot: Dict[str, Any]) -> str:
    return _build_history_revision_from_trades(snapshot.get("trades") or [])


def _build_history_revision_from_trades(trades: List[Dict[str, Any]]) -> str:
    normalized_trades: List[Dict[str, Any]] = []
    for item in trades:
        if not isinstance(item, dict):
            continue
        normalized_trades.append(dict(item))
    normalized_trades.sort(
        key=lambda item: (
            int(item.get("closeTime", 0) or 0),
            int(item.get("openTime", 0) or 0),
            int(item.get("dealTicket", 0) or 0),
            int(item.get("positionId", 0) or 0),
            int(item.get("orderId", 0) or 0),
        )
    )
    payload = json.dumps(
        {"trades": normalized_trades},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def _normalize_snapshot(payload: Optional[Dict], source_fallback: str) -> Dict:
    data = payload or {}
    account_meta = data.get("accountMeta") or {}
    account_meta.setdefault("login", str(LOGIN))
    account_meta.setdefault("server", SERVER)
    account_meta.setdefault("source", source_fallback)
    account_meta.setdefault("updatedAt", _now_ms())
    data["accountMeta"] = account_meta
    data["overviewMetrics"] = data.get("overviewMetrics") or []
    data["curvePoints"] = data.get("curvePoints") or []
    data["curveIndicators"] = data.get("curveIndicators") or []
    data["positions"] = [_enrich_position_symbol_fields(dict(item)) for item in (data.get("positions") or [])]
    data["pendingOrders"] = [_enrich_position_symbol_fields(dict(item)) for item in (data.get("pendingOrders") or [])]
    data["trades"] = [_enrich_trade_symbol_fields(dict(item)) for item in (data.get("trades") or [])]
    data["statsMetrics"] = data.get("statsMetrics") or []
    _normalize_ea_snapshot_time_fields(account_meta, source_fallback, data)
    account_meta["historyRevision"] = _build_history_revision_from_snapshot(data)
    return data


def _clone_snapshot_payload(snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    safe_snapshot = snapshot or {}
    cloned: Dict[str, Any] = {}
    for key, value in safe_snapshot.items():
        if isinstance(value, dict):
            cloned[key] = dict(value)
        elif isinstance(value, list):
            cloned[key] = [dict(item) if isinstance(item, dict) else item for item in value]
        else:
            cloned[key] = value
    return cloned


def _stable_json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def _normalize_digest_curve_points(points: List[Dict]) -> List[Dict]:
    if not points:
        return []
    keep_timestamp = len(points) > 1
    normalized: List[Dict] = []
    for item in points:
        point = {
            "equity": round(float(item.get("equity", 0.0) or 0.0), 2),
            "balance": round(float(item.get("balance", 0.0) or 0.0), 2),
            "positionRatio": round(float(item.get("positionRatio", 0.0) or 0.0), 6),
        }
        if keep_timestamp:
            point["timestamp"] = int(item.get("timestamp", 0) or 0)
        if normalized and normalized[-1] == point:
            continue
        normalized.append(point)
    return normalized


def _normalize_digest_stats_metrics(metrics: List[Dict]) -> List[Dict]:
    normalized: List[Dict] = []
    for item in metrics or []:
        name = str(item.get("name", ""))
        if name.strip().lower() == "pushed at":
            continue
        normalized.append({
            "name": name,
            "value": str(item.get("value", "")),
        })
    return normalized


def _snapshot_digest(snapshot: Dict) -> str:
    account_meta = snapshot.get("accountMeta") or {}
    core = {
        "accountMeta": {
            "login": str(account_meta.get("login", "")),
            "server": str(account_meta.get("server", "")),
            "source": str(account_meta.get("source", "")),
        },
        "overviewMetrics": snapshot.get("overviewMetrics") or [],
        "curvePoints": _normalize_digest_curve_points(snapshot.get("curvePoints") or []),
        "curveIndicators": snapshot.get("curveIndicators") or [],
        "positions": snapshot.get("positions") or [],
        "pendingOrders": snapshot.get("pendingOrders") or [],
        "trades": snapshot.get("trades") or [],
        "statsMetrics": _normalize_digest_stats_metrics(snapshot.get("statsMetrics") or []),
    }
    payload = json.dumps(core, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def _position_key(item: Dict) -> str:
    ticket = int(item.get("positionTicket", 0) or 0)
    if ticket > 0:
        return f"position:{ticket}"
    code = str(item.get("code", "")).upper()
    side = str(item.get("side", "")).upper()
    qty = round(float(item.get("quantity", 0.0) or 0.0), 4)
    cost = round(float(item.get("costPrice", 0.0) or 0.0), 2)
    return f"position:{code}|{side}|{qty}|{cost}"


def _pending_key(item: Dict) -> str:
    order_id = int(item.get("orderId", 0) or 0)
    if order_id > 0:
        return f"pending:{order_id}"
    code = str(item.get("code", "")).upper()
    side = str(item.get("side", "")).upper()
    lots = round(float(item.get("pendingLots", 0.0) or 0.0), 4)
    price = round(float(item.get("pendingPrice", 0.0) or 0.0), 4)
    return f"pending:{code}|{side}|{lots}|{price}"


def _trade_key(item: Dict) -> str:
    ticket = int(item.get("dealTicket", 0) or 0)
    if ticket > 0:
        return f"trade:{ticket}"
    order_id = int(item.get("orderId", 0) or 0)
    position_id = int(item.get("positionId", 0) or 0)
    close_time = int(item.get("closeTime", item.get("timestamp", 0)) or 0)
    qty = round(float(item.get("quantity", 0.0) or 0.0), 4)
    return f"trade:{order_id}|{position_id}|{close_time}|{qty}"


def _index_entities(items: List[Dict], key_fn) -> Dict[str, Dict]:
    indexed: Dict[str, Dict] = {}
    for item in items:
        key = key_fn(item)
        payload = dict(item)
        payload["_key"] = key
        indexed[key] = payload
    return indexed


def _diff_entities(previous: List[Dict], current: List[Dict], key_fn) -> Dict[str, List]:
    previous_map = _index_entities(previous, key_fn)
    current_map = _index_entities(current, key_fn)

    upsert: List[Dict] = []
    for key, item in current_map.items():
        if key not in previous_map or previous_map.get(key) != item:
            upsert.append(item)

    remove = [key for key in previous_map.keys() if key not in current_map]
    return {"upsert": upsert, "remove": remove}


def _diff_curve_points(previous: List[Dict], current: List[Dict]) -> Dict[str, Any]:
    if not previous:
        return {"reset": False, "append": current}
    if not current:
        return {"reset": True, "append": []}

    prev_by_ts = {int(item.get("timestamp", 0)): item for item in previous if int(item.get("timestamp", 0)) > 0}
    cur_by_ts = {int(item.get("timestamp", 0)): item for item in current if int(item.get("timestamp", 0)) > 0}
    if not prev_by_ts or not cur_by_ts:
        return {"reset": True, "append": current}

    for ts, prev_item in prev_by_ts.items():
        cur_item = cur_by_ts.get(ts)
        if cur_item is None:
            return {"reset": True, "append": current}
        if cur_item != prev_item:
            return {"reset": True, "append": current}

    prev_last_ts = max(prev_by_ts.keys())
    append = [item for item in current if int(item.get("timestamp", 0)) > prev_last_ts]
    return {"reset": False, "append": append}


def _build_delta_snapshot(previous: Dict, current: Dict) -> Dict:
    return {
        "positions": _diff_entities(previous.get("positions") or [], current.get("positions") or [], _position_key),
        "pendingOrders": _diff_entities(previous.get("pendingOrders") or [], current.get("pendingOrders") or [], _pending_key),
        "trades": _diff_entities(previous.get("trades") or [], current.get("trades") or [], _trade_key),
        "curvePoints": _diff_curve_points(previous.get("curvePoints") or [], current.get("curvePoints") or []),
    }


def _build_full_response(snapshot: Dict, sync_seq: int) -> Dict:
    meta = dict(snapshot.get("accountMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = SNAPSHOT_DELTA_ENABLED
    return {
        "accountMeta": meta,
        "isDelta": False,
        "unchanged": False,
        "overviewMetrics": snapshot.get("overviewMetrics") or [],
        "curvePoints": snapshot.get("curvePoints") or [],
        "curveIndicators": snapshot.get("curveIndicators") or [],
        "positions": snapshot.get("positions") or [],
        "pendingOrders": snapshot.get("pendingOrders") or [],
        "trades": snapshot.get("trades") or [],
        "statsMetrics": snapshot.get("statsMetrics") or [],
    }


def _build_summary_response(snapshot: Dict, sync_seq: int) -> Dict:
    meta = dict(snapshot.get("accountMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = SNAPSHOT_DELTA_ENABLED
    return {
        "accountMeta": meta,
        "isDelta": False,
        "unchanged": False,
        "overviewMetrics": snapshot.get("overviewMetrics") or [],
        "statsMetrics": snapshot.get("statsMetrics") or [],
    }


def _build_live_response(snapshot: Dict, sync_seq: int) -> Dict:
    meta = dict(snapshot.get("accountMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = SNAPSHOT_DELTA_ENABLED
    return {
        "accountMeta": meta,
        "isDelta": False,
        "unchanged": False,
        "overviewMetrics": snapshot.get("overviewMetrics") or [],
        "positions": snapshot.get("positions") or [],
        "statsMetrics": snapshot.get("statsMetrics") or [],
    }


def _build_pending_response(snapshot: Dict, sync_seq: int) -> Dict:
    meta = dict(snapshot.get("accountMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = SNAPSHOT_DELTA_ENABLED
    return {
        "accountMeta": meta,
        "isDelta": False,
        "unchanged": False,
        "pendingOrders": snapshot.get("pendingOrders") or [],
    }


def _build_trades_response(snapshot: Dict, sync_seq: int) -> Dict:
    meta = dict(snapshot.get("accountMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = SNAPSHOT_DELTA_ENABLED
    return {
        "accountMeta": meta,
        "isDelta": False,
        "unchanged": False,
        "trades": snapshot.get("trades") or [],
    }


def _build_curve_response(snapshot: Dict, sync_seq: int) -> Dict:
    meta = dict(snapshot.get("accountMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = SNAPSHOT_DELTA_ENABLED
    return {
        "accountMeta": meta,
        "isDelta": False,
        "unchanged": False,
        "curvePoints": snapshot.get("curvePoints") or [],
        "curveIndicators": snapshot.get("curveIndicators") or [],
    }


def _build_snapshot_with_cache(range_key: str) -> Dict:
    while True:
        now_ms = _now_ms()
        with snapshot_cache_lock:
            cached = snapshot_build_cache.get(range_key)
            if cached and (now_ms - int(cached.get("builtAt", 0))) <= SNAPSHOT_BUILD_CACHE_MS:
                cached_snapshot = cached.get("snapshot")
                if _is_mt5_pull_account_snapshot(cached_snapshot):
                    cached["lastAccessAt"] = now_ms
                    _remember_cache_entry_locked(snapshot_build_cache, range_key, cached, SNAPSHOT_BUILD_CACHE_MAX_ENTRIES)
                    return cached_snapshot
                snapshot_build_cache.pop(range_key, None)
            if _should_slide_snapshot_build_cache(cached, now_ms):
                cached["builtAt"] = now_ms
                cached["lastAccessAt"] = now_ms
                _remember_cache_entry_locked(snapshot_build_cache, range_key, cached, SNAPSHOT_BUILD_CACHE_MAX_ENTRIES)
                return cached.get("snapshot")
            build_epoch = int(session_snapshot_epoch)

        snapshot = _normalize_snapshot(_select_snapshot(range_key), "MT5 Gateway")
        with snapshot_cache_lock:
            if build_epoch != int(session_snapshot_epoch):
                continue
            built_at = _now_ms()
            _remember_cache_entry_locked(snapshot_build_cache, range_key, {
                "builtAt": built_at,
                "lastAccessAt": built_at,
                "snapshot": snapshot,
            }, SNAPSHOT_BUILD_CACHE_MAX_ENTRIES)
            return snapshot


# 构建账户轻快照缓存，避免高频 v2 snapshot/sync 重复生成整份历史对象。
def _build_account_light_snapshot_with_cache() -> Dict:
    global light_snapshot_building
    cache_key = "account-light"
    while True:
        now_ms = _now_ms()
        with light_snapshot_build_condition:
            cached = snapshot_build_cache.get(cache_key)
            if cached and (now_ms - int(cached.get("builtAt", 0))) <= SNAPSHOT_BUILD_CACHE_MS:
                cached_snapshot = cached.get("snapshot")
                if _is_mt5_pull_account_snapshot(cached_snapshot):
                    cached["lastAccessAt"] = now_ms
                    _remember_cache_entry_locked(snapshot_build_cache, cache_key, cached, SNAPSHOT_BUILD_CACHE_MAX_ENTRIES)
                    return cached_snapshot
                snapshot_build_cache.pop(cache_key, None)
            if _should_slide_snapshot_build_cache(cached, now_ms):
                cached["builtAt"] = now_ms
                cached["lastAccessAt"] = now_ms
                _remember_cache_entry_locked(snapshot_build_cache, cache_key, cached, SNAPSHOT_BUILD_CACHE_MAX_ENTRIES)
                return cached.get("snapshot")
            while light_snapshot_building:
                light_snapshot_build_condition.wait()
                now_ms = _now_ms()
                cached = snapshot_build_cache.get(cache_key)
                if cached and (now_ms - int(cached.get("builtAt", 0))) <= SNAPSHOT_BUILD_CACHE_MS:
                    cached_snapshot = cached.get("snapshot")
                    if _is_mt5_pull_account_snapshot(cached_snapshot):
                        cached["lastAccessAt"] = now_ms
                        _remember_cache_entry_locked(snapshot_build_cache, cache_key, cached, SNAPSHOT_BUILD_CACHE_MAX_ENTRIES)
                        return cached_snapshot
                    snapshot_build_cache.pop(cache_key, None)
            build_epoch = int(session_snapshot_epoch)
            light_snapshot_building = True

        try:
            snapshot = _build_account_light_snapshot()
        except Exception:
            with light_snapshot_build_condition:
                light_snapshot_building = False
                light_snapshot_build_condition.notify_all()
            raise

        should_retry = False
        with light_snapshot_build_condition:
            if build_epoch != int(session_snapshot_epoch):
                light_snapshot_building = False
                light_snapshot_build_condition.notify_all()
                should_retry = True
            else:
                built_at = _now_ms()
                _remember_cache_entry_locked(snapshot_build_cache, cache_key, {
                    "builtAt": built_at,
                    "lastAccessAt": built_at,
                    "snapshot": snapshot,
                }, SNAPSHOT_BUILD_CACHE_MAX_ENTRIES)
                light_snapshot_building = False
                light_snapshot_build_condition.notify_all()
                return snapshot
        if should_retry:
            continue


def _snapshot_trades_from_mt5(range_key: str) -> List[Dict[str, Any]]:
    with state_lock:
        _ensure_mt5()
        try:
            return _map_trades(range_key)
        finally:
            _shutdown_mt5()


def _build_trade_history_with_cache(range_key: str, fallback_snapshot: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    cache_key = f"{range_key}:trade-history"
    while True:
        now_ms = _now_ms()
        with snapshot_cache_lock:
            cached = snapshot_build_cache.get(cache_key)
            if cached and (now_ms - int(cached.get("builtAt", 0))) <= SNAPSHOT_BUILD_CACHE_MS:
                if str(cached.get("source") or "") == "MT5 Python Pull":
                    cached["lastAccessAt"] = now_ms
                    _remember_cache_entry_locked(snapshot_build_cache, cache_key, cached, SNAPSHOT_BUILD_CACHE_MAX_ENTRIES)
                    return [dict(item) for item in (cached.get("trades") or [])]
                snapshot_build_cache.pop(cache_key, None)
            build_epoch = int(session_snapshot_epoch)

        if _is_mt5_pull_account_snapshot(fallback_snapshot):
            trades = [dict(item) for item in ((fallback_snapshot or {}).get("trades") or [])]
        else:
            if mt5 is None or not _is_mt5_configured():
                raise RuntimeError("MT5 Python Pull is not configured.")
            trades = [dict(item) for item in (_snapshot_trades_from_mt5(range_key) or [])]

        with snapshot_cache_lock:
            if build_epoch != int(session_snapshot_epoch):
                continue
            built_at = _now_ms()
            _remember_cache_entry_locked(snapshot_build_cache, cache_key, {
                "builtAt": built_at,
                "lastAccessAt": built_at,
                "source": "MT5 Python Pull",
                "trades": trades,
            }, SNAPSHOT_BUILD_CACHE_MAX_ENTRIES)
            return [dict(item) for item in trades]


def _build_summary_snapshot(snapshot: Dict) -> Dict:
    summary = {
        "accountMeta": dict(snapshot.get("accountMeta") or {}),
        "overviewMetrics": snapshot.get("overviewMetrics") or [],
        "statsMetrics": snapshot.get("statsMetrics") or [],
    }
    return _normalize_snapshot(summary, str((snapshot.get("accountMeta") or {}).get("source", "MT5 Gateway")))


def _build_live_snapshot(snapshot: Dict) -> Dict:
    live = {
        "accountMeta": dict(snapshot.get("accountMeta") or {}),
        "overviewMetrics": snapshot.get("overviewMetrics") or [],
        "positions": snapshot.get("positions") or [],
        "statsMetrics": snapshot.get("statsMetrics") or [],
    }
    return _normalize_snapshot(live, str((snapshot.get("accountMeta") or {}).get("source", "MT5 Gateway")))


def _build_pending_snapshot(snapshot: Dict) -> Dict:
    return {
        "accountMeta": dict(snapshot.get("accountMeta") or {}),
        "pendingOrders": snapshot.get("pendingOrders") or [],
    }


def _build_trades_snapshot(snapshot: Dict) -> Dict:
    return {
        "accountMeta": dict(snapshot.get("accountMeta") or {}),
        "trades": snapshot.get("trades") or [],
    }


def _build_curve_snapshot(snapshot: Dict) -> Dict:
    return {
        "accountMeta": dict(snapshot.get("accountMeta") or {}),
        "curvePoints": snapshot.get("curvePoints") or [],
        "curveIndicators": snapshot.get("curveIndicators") or [],
    }


def _projection_profile(name: str) -> Dict[str, Any]:
    profiles = {
        "snapshot": {
            "cacheSuffix": "snapshot",
            "project": lambda snapshot: snapshot,
            "buildResponse": _build_full_response,
            "deltaKeys": ["positions", "pendingOrders", "trades", "curvePoints"],
            "carryKeys": ["overviewMetrics", "curveIndicators", "statsMetrics"],
        },
        "summary": {
            "cacheSuffix": "summary",
            "project": _build_summary_snapshot,
            "buildResponse": _build_summary_response,
            "deltaKeys": [],
            "carryKeys": [],
        },
        "live": {
            "cacheSuffix": "live",
            "project": _build_live_snapshot,
            "buildResponse": _build_live_response,
            "deltaKeys": ["positions"],
            "carryKeys": ["overviewMetrics", "statsMetrics"],
        },
        "pending": {
            "cacheSuffix": "pending",
            "project": _build_pending_snapshot,
            "buildResponse": _build_pending_response,
            "deltaKeys": ["pendingOrders"],
            "carryKeys": [],
        },
        "trades": {
            "cacheSuffix": "trades",
            "project": _build_trades_snapshot,
            "buildResponse": _build_trades_response,
            "deltaKeys": ["trades"],
            "carryKeys": [],
        },
        "curve": {
            "cacheSuffix": "curve",
            "project": _build_curve_snapshot,
            "buildResponse": _build_curve_response,
            "deltaKeys": ["curvePoints"],
            "carryKeys": ["curveIndicators"],
        },
    }
    return profiles[name]


def _build_scoped_delta_snapshot(previous: Dict, current: Dict, keys: List[str]) -> Dict:
    payload: Dict[str, Any] = {}
    if "positions" in keys:
        payload["positions"] = _diff_entities(previous.get("positions") or [], current.get("positions") or [], _position_key)
    if "pendingOrders" in keys:
        payload["pendingOrders"] = _diff_entities(previous.get("pendingOrders") or [], current.get("pendingOrders") or [], _pending_key)
    if "trades" in keys:
        payload["trades"] = _diff_entities(previous.get("trades") or [], current.get("trades") or [], _trade_key)
    if "curvePoints" in keys:
        payload["curvePoints"] = _diff_curve_points(previous.get("curvePoints") or [], current.get("curvePoints") or [])
    return payload


def _build_projected_snapshot_response(range_key: str, since_seq: int, delta: bool, projection_name: str) -> Dict:
    profile = _projection_profile(projection_name)
    snapshot = _build_snapshot_with_cache(range_key)
    projected_snapshot = profile["project"](snapshot)
    digest = _snapshot_digest(projected_snapshot)
    cache_key = f"{range_key}:{profile['cacheSuffix']}"

    with snapshot_cache_lock:
        state = _sanitize_mt5_pull_sync_state(snapshot_sync_cache.get(cache_key))
        if state is None:
            snapshot_sync_cache.pop(cache_key, None)
        previous_snapshot: Optional[Dict] = None
        previous_seq = 0

        if state is None:
            sync_seq = 1
            _remember_cache_entry_locked(snapshot_sync_cache, cache_key, {
                "seq": sync_seq,
                "digest": digest,
                "snapshot": projected_snapshot,
                "previousSeq": 0,
                "previousSnapshot": None,
            }, SNAPSHOT_SYNC_CACHE_MAX_ENTRIES)
            changed = True
        else:
            if state.get("digest") == digest:
                sync_seq = int(state.get("seq", 1))
                changed = False
                projected_snapshot = state.get("snapshot") or projected_snapshot
                previous_seq = int(state.get("previousSeq", 0))
                previous_snapshot = state.get("previousSnapshot")
                _remember_cache_entry_locked(snapshot_sync_cache, cache_key, state, SNAPSHOT_SYNC_CACHE_MAX_ENTRIES)
            else:
                previous_seq = int(state.get("seq", 0))
                previous_snapshot = state.get("snapshot")
                sync_seq = previous_seq + 1
                _remember_cache_entry_locked(snapshot_sync_cache, cache_key, {
                    "seq": sync_seq,
                    "digest": digest,
                    "snapshot": projected_snapshot,
                    "previousSeq": previous_seq,
                    "previousSnapshot": previous_snapshot,
                }, SNAPSHOT_SYNC_CACHE_MAX_ENTRIES)
                changed = True

    if not SNAPSHOT_DELTA_ENABLED or not delta or since_seq <= 0:
        return profile["buildResponse"](projected_snapshot, sync_seq)

    meta = dict(projected_snapshot.get("accountMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = SNAPSHOT_DELTA_ENABLED

    if since_seq == sync_seq:
        return {"accountMeta": meta, "isDelta": True, "unchanged": True}

    if not profile["deltaKeys"]:
        return profile["buildResponse"](projected_snapshot, sync_seq)

    if changed and previous_snapshot is not None and since_seq == previous_seq:
        delta_payload = _build_scoped_delta_snapshot(previous_snapshot, projected_snapshot, profile["deltaKeys"])
        delta_response = {
            "accountMeta": meta,
            "isDelta": True,
            "unchanged": False,
            "delta": delta_payload,
        }
        for key in profile["carryKeys"]:
            delta_response[key] = projected_snapshot.get(key) or []
        full_response = profile["buildResponse"](projected_snapshot, sync_seq)
        if _stable_json_size(delta_response) <= _stable_json_size(full_response) * SNAPSHOT_DELTA_FALLBACK_RATIO:
            return delta_response

    return profile["buildResponse"](projected_snapshot, sync_seq)


def _build_snapshot_response(range_key: str, since_seq: int, delta: bool) -> Dict:
    return _build_projected_snapshot_response(range_key, since_seq, delta, projection_name="snapshot")


def _build_summary_snapshot_response(range_key: str, since_seq: int, delta: bool) -> Dict:
    return _build_projected_snapshot_response(range_key, since_seq, delta, projection_name="summary")


def _build_live_snapshot_response(range_key: str, since_seq: int, delta: bool) -> Dict:
    return _build_projected_snapshot_response(range_key, since_seq, delta, projection_name="live")


def _build_pending_snapshot_response(range_key: str, since_seq: int, delta: bool) -> Dict:
    return _build_projected_snapshot_response(range_key, since_seq, delta, projection_name="pending")


def _build_trades_snapshot_response(range_key: str, since_seq: int, delta: bool) -> Dict:
    snapshot = _build_snapshot_with_cache(range_key)
    trades_snapshot = {
        "accountMeta": dict(snapshot.get("accountMeta") or {}),
        "trades": _build_trade_history_with_cache(range_key, snapshot),
    }
    digest = _snapshot_digest(trades_snapshot)
    cache_key = f"{range_key}:trades"

    with snapshot_cache_lock:
        state = _sanitize_mt5_pull_sync_state(snapshot_sync_cache.get(cache_key))
        if state is None:
            snapshot_sync_cache.pop(cache_key, None)
        previous_snapshot: Optional[Dict] = None
        previous_seq = 0

        if state is None:
            sync_seq = 1
            _remember_cache_entry_locked(snapshot_sync_cache, cache_key, {
                "seq": sync_seq,
                "digest": digest,
                "snapshot": trades_snapshot,
                "previousSeq": 0,
                "previousSnapshot": None,
            }, SNAPSHOT_SYNC_CACHE_MAX_ENTRIES)
        else:
            if state.get("digest") == digest:
                sync_seq = int(state.get("seq", 1))
                trades_snapshot = state.get("snapshot") or trades_snapshot
                previous_seq = int(state.get("previousSeq", 0))
                previous_snapshot = state.get("previousSnapshot")
                _remember_cache_entry_locked(snapshot_sync_cache, cache_key, state, SNAPSHOT_SYNC_CACHE_MAX_ENTRIES)
            else:
                previous_seq = int(state.get("seq", 0))
                previous_snapshot = state.get("snapshot")
                sync_seq = previous_seq + 1
                _remember_cache_entry_locked(snapshot_sync_cache, cache_key, {
                    "seq": sync_seq,
                    "digest": digest,
                    "snapshot": trades_snapshot,
                    "previousSeq": previous_seq,
                    "previousSnapshot": previous_snapshot,
                }, SNAPSHOT_SYNC_CACHE_MAX_ENTRIES)

    if not SNAPSHOT_DELTA_ENABLED or not delta or since_seq <= 0:
        return _build_trades_response(trades_snapshot, sync_seq)

    meta = dict(trades_snapshot.get("accountMeta") or {})
    meta["syncSeq"] = sync_seq
    meta["deltaEnabled"] = SNAPSHOT_DELTA_ENABLED

    if since_seq == sync_seq:
        return {"accountMeta": meta, "isDelta": True, "unchanged": True}

    if previous_snapshot is None or since_seq < previous_seq:
        return _build_trades_response(trades_snapshot, sync_seq)

    delta_payload = _build_scoped_delta_snapshot(previous_snapshot, trades_snapshot, ["trades"])
    return {
        "accountMeta": meta,
        "isDelta": True,
        "unchanged": False,
        "trades": delta_payload.get("trades", {"upsert": [], "remove": []}),
    }


def _build_curve_snapshot_response(range_key: str, since_seq: int, delta: bool) -> Dict:
    return _build_projected_snapshot_response(range_key, since_seq, delta, projection_name="curve")


def _is_ea_snapshot_fresh() -> bool:
    if ea_snapshot_cache is None:
        return False
    updated_at = int((ea_snapshot_cache.get("accountMeta") or {}).get("updatedAt", 0))
    reference = max(updated_at, ea_snapshot_received_at_ms)
    return (_now_ms() - reference) <= EA_SNAPSHOT_TTL_SEC * 1000


def _select_snapshot(range_key: str) -> Dict:
    # 账户真值唯一化：主链只认 MT5 Python Pull，不再在 EA / stale cache 之间切源。
    if mt5 is None or not _is_mt5_configured():
        raise RuntimeError("MT5 Python Pull is not configured.")
    return _snapshot_from_mt5(range_key)


# 统一把完整快照裁成账户轻快照，供高频账户同步接口复用。
def _strip_account_light_snapshot(snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    safe_snapshot = snapshot or {}
    return {
        "accountMeta": dict(safe_snapshot.get("accountMeta") or {}),
        "overviewMetrics": [dict(item) for item in (safe_snapshot.get("overviewMetrics") or [])],
        "curveIndicators": [dict(item) for item in (safe_snapshot.get("curveIndicators") or [])],
        "statsMetrics": [dict(item) for item in (safe_snapshot.get("statsMetrics") or [])],
        "positions": [dict(item) for item in (safe_snapshot.get("positions") or [])],
        "pendingOrders": [dict(item) for item in (safe_snapshot.get("pendingOrders") or [])],
    }


# 轻快照是 canonical 全快照的纯投影，避免和历史链出现不同字段口径。
def _build_account_light_snapshot() -> Dict[str, Any]:
    return _strip_account_light_snapshot(_snapshot_from_mt5_light())


def _join_query(params: Dict[str, Any]) -> str:
    clean_params: List[Tuple[str, Any]] = []
    for key, value in params.items():
        if value is None:
            continue
        clean_params.append((key, value))
    return urllib.parse.urlencode(clean_params, doseq=True)


def _build_binance_rest_upstream_url(path_value: str, query_params: Dict[str, Any]) -> str:
    safe_path = "/" + (path_value or "").lstrip("/")
    upstream = BINANCE_REST_UPSTREAM + safe_path
    query_string = _join_query(query_params)
    if query_string:
        upstream += "?" + query_string
    return upstream


def _build_binance_ws_upstream_url(path_value: str, query_string: str) -> str:
    safe_path = "/" + (path_value or "").lstrip("/")
    upstream = BINANCE_WS_UPSTREAM + safe_path
    if query_string:
        upstream += "?" + query_string
    return upstream


def _proxy_binance_rest(path_value: str, request: Request) -> Response:
    upstream_url = _build_binance_rest_upstream_url(path_value, dict(request.query_params))
    upstream_request = urllib.request.Request(
        upstream_url,
        headers={
            "User-Agent": "mt5-gateway-binance-proxy/1.0",
            "Accept-Encoding": "identity",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(upstream_request, timeout=25) as upstream_response:
            body = upstream_response.read()
            content_type = upstream_response.headers.get("Content-Type", "application/json")
            return Response(
                content=body,
                status_code=getattr(upstream_response, "status", 200),
                media_type=content_type.split(";")[0],
                headers={"Content-Type": content_type},
            )
    except urllib.error.HTTPError as exc:
        body = exc.read()
        content_type = exc.headers.get("Content-Type", "application/json")
        return Response(
            content=body,
            status_code=exc.code,
            media_type=content_type.split(";")[0],
            headers={"Content-Type": content_type},
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Binance REST proxy failed: {exc}")


# 基于现有快照构建 v2 账户区返回体，统一附带账号与历史修订号。
def _build_v2_account_section_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    meta = dict(snapshot.get("accountMeta") or {})
    positions = [_enrich_position_symbol_fields(dict(item)) for item in (snapshot.get("positions") or [])]
    orders = [_enrich_position_symbol_fields(dict(item)) for item in (snapshot.get("pendingOrders") or [])]
    history_revision = str(meta.get("historyRevision") or "").strip()
    if not history_revision:
        raise RuntimeError("account snapshot missing historyRevision")
    account_meta_for_revision = {
        "login": str(meta.get("login", "")),
        "server": str(meta.get("server", "")),
        "source": str(meta.get("source", "")),
        "currency": str(meta.get("currency", "")),
        "leverage": int(meta.get("leverage", 0) or 0),
        "name": str(meta.get("name", "")),
        "company": str(meta.get("company", "")),
        "tradeCount": int(meta.get("tradeCount", 0) or 0),
        "positionCount": len(positions),
        "pendingOrderCount": len(orders),
        "curvePointCount": int(meta.get("curvePointCount", 0) or 0),
        "historyRevision": history_revision,
    }
    account_revision_payload = {
        "accountMeta": account_meta_for_revision,
        "positions": positions,
        "orders": orders,
    }
    account_revision = _stable_payload_digest(account_revision_payload)
    meta["historyRevision"] = history_revision
    meta["accountRevision"] = account_revision
    return {
        "accountMeta": meta,
        "overviewMetrics": [dict(item) for item in (snapshot.get("overviewMetrics") or [])],
        "statsMetrics": [dict(item) for item in (snapshot.get("statsMetrics") or [])],
        "positions": positions,
        "orders": orders,
    }


# 构建单个产品的行情同步摘要，供 v2 stream 纯消费链直接落地。
def _build_market_stream_symbol_state(symbol: str, server_time: int) -> Dict[str, Any]:
    runtime_state = _build_market_runtime_symbol_state(symbol)
    if runtime_state.get("latestPatch") is not None or runtime_state.get("latestClosedCandle") is not None:
        return runtime_state
    symbol_descriptor = _resolve_symbol_descriptor(symbol)
    rows = _fetch_market_candle_rows_with_cache(
        symbol_descriptor["marketSymbol"],
        "1m",
        2,
        start_time_ms=0,
        end_time_ms=0,
    )
    closed_rows, patch_row = v2_market.separate_closed_rest_rows(rows, server_time)
    latest_closed_payload: Optional[Dict[str, Any]] = None
    latest_patch_payload: Optional[Dict[str, Any]] = None
    if closed_rows:
        latest_closed_payload = v2_market.build_market_candle_payload(
            symbol_descriptor["marketSymbol"],
            "1m",
            row=closed_rows[-1],
            is_closed=True,
            source="binance-rest",
        )
    if patch_row is not None:
        latest_patch_payload = v2_market.build_market_candle_payload(
            symbol_descriptor["marketSymbol"],
            "1m",
            row=patch_row,
            is_closed=False,
            source="binance-rest",
        )
    latest_price = 0.0
    latest_open_time = 0
    latest_close_time = 0
    if latest_patch_payload is not None:
        latest_price = float(latest_patch_payload.get("close", 0.0) or 0.0)
        latest_open_time = int(latest_patch_payload.get("openTime", 0) or 0)
        latest_close_time = int(latest_patch_payload.get("closeTime", 0) or 0)
    elif latest_closed_payload is not None:
        latest_price = float(latest_closed_payload.get("close", 0.0) or 0.0)
        latest_open_time = int(latest_closed_payload.get("openTime", 0) or 0)
        latest_close_time = int(latest_closed_payload.get("closeTime", 0) or 0)
    return {
        "productId": symbol_descriptor["productId"],
        "marketSymbol": symbol_descriptor["marketSymbol"],
        "tradeSymbol": symbol_descriptor["tradeSymbol"],
        "interval": "1m",
        "latestPrice": latest_price,
        "latestOpenTime": latest_open_time,
        "latestCloseTime": latest_close_time,
        "latestClosedCandle": latest_closed_payload,
        "latestPatch": latest_patch_payload,
    }


# 基于 Binance 数据构建 v2 行情区统一返回体。
def _build_v2_market_section(server_time: int) -> Dict[str, Any]:
    symbol_states: List[Dict[str, Any]] = []
    for symbol in ABNORMAL_SYMBOLS:
        symbol_states.append(_build_market_stream_symbol_state(symbol, server_time))
    runtime_status = _build_market_runtime_source_status()
    return {
        "source": "binance",
        "symbols": [MARKET_SYMBOL_BTC, MARKET_SYMBOL_XAU],
        "symbolStates": symbol_states,
        "restUpstream": BINANCE_REST_UPSTREAM,
        "wsUpstream": BINANCE_WS_UPSTREAM,
        **runtime_status,
    }


# 从 Binance REST 拉指定周期 K 线原始行，供 v2 行情真值接口使用。
def _fetch_binance_kline_rows(symbol: str,
                             interval: str,
                             limit: int,
                             *,
                             start_time_ms: int = 0,
                             end_time_ms: int = 0) -> List[Any]:
    safe_symbol = str(symbol or "").strip().upper()
    safe_interval = str(interval or "").strip()
    safe_limit = max(1, min(int(limit), 1500))
    query_payload = {
        "symbol": safe_symbol,
        "interval": safe_interval,
        "limit": safe_limit,
    }
    if int(start_time_ms or 0) > 0:
        query_payload["startTime"] = int(start_time_ms)
    if int(end_time_ms or 0) > 0:
        query_payload["endTime"] = int(end_time_ms)
    query = urllib.parse.urlencode(query_payload)
    upstream_url = f"{BINANCE_REST_UPSTREAM}/fapi/v1/klines?{query}"
    request = urllib.request.Request(
        upstream_url,
        headers={
            "User-Agent": "mt5-gateway-v2-market/1.0",
            "Accept-Encoding": "identity",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=25) as response:
        body = response.read().decode("utf-8")
    payload = json.loads(body or "[]")
    if not isinstance(payload, list):
        raise ValueError("Binance kline payload is not a list")
    return payload


def _fetch_binance_kline_rows_resilient(symbol: str,
                                        interval: str,
                                        limit: int,
                                        *,
                                        start_time_ms: int = 0,
                                        end_time_ms: int = 0) -> List[Any]:
    last_error: Optional[Exception] = None
    for _ in range(max(0, MARKET_CANDLES_UPSTREAM_RETRY) + 1):
        try:
            return _fetch_binance_kline_rows(
                symbol,
                interval,
                limit,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return []


# 行情 K 线大窗口查询按块拉取，避免单次 limit 过大把 Binance REST 拖到超时。
def _fetch_market_candle_rows_with_cache(symbol: str,
                                         interval: str,
                                         limit: int,
                                         *,
                                         start_time_ms: int = 0,
                                         end_time_ms: int = 0) -> List[Any]:
    safe_symbol = str(symbol or "").strip().upper()
    safe_interval = str(interval or "").strip()
    safe_limit = max(1, min(int(limit or 0), 1500))
    safe_start = max(0, int(start_time_ms or 0))
    safe_end = max(0, int(end_time_ms or 0))
    cache_key = _build_market_candles_cache_key(
        safe_symbol,
        safe_interval,
        safe_limit,
        safe_start,
        safe_end,
    )
    now_ms = _now_ms()
    cached_rows = _get_cached_market_candle_rows(cache_key, now_ms)
    if cached_rows is not None:
        return cached_rows

    rows = _fetch_market_candle_rows_paged(
        safe_symbol,
        safe_interval,
        safe_limit,
        start_time_ms=safe_start,
        end_time_ms=safe_end,
    )
    _remember_market_candle_rows(cache_key, rows, _now_ms())
    return rows


# 按开始/结束时间自动选择向前或向后分页，保证返回仍是同一份升序 K 线列表。
def _fetch_market_candle_rows_paged(symbol: str,
                                    interval: str,
                                    limit: int,
                                    *,
                                    start_time_ms: int = 0,
                                    end_time_ms: int = 0) -> List[Any]:
    safe_limit = max(1, min(int(limit or 0), 1500))
    if safe_limit <= MARKET_CANDLES_UPSTREAM_CHUNK_LIMIT:
        return _fetch_binance_kline_rows_resilient(
            symbol,
            interval,
            safe_limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
    if int(start_time_ms or 0) > 0:
        return _fetch_market_candle_rows_forward(
            symbol,
            interval,
            safe_limit,
            start_time_ms=int(start_time_ms or 0),
            end_time_ms=int(end_time_ms or 0),
        )
    return _fetch_market_candle_rows_backward(
        symbol,
        interval,
        safe_limit,
        end_time_ms=int(end_time_ms or 0),
    )


# 已知 startTime 时从旧到新分页，避免精确窗口查询重复回卷。
def _fetch_market_candle_rows_forward(symbol: str,
                                      interval: str,
                                      limit: int,
                                      *,
                                      start_time_ms: int,
                                      end_time_ms: int = 0) -> List[Any]:
    remaining = max(1, min(int(limit or 0), 1500))
    cursor = max(0, int(start_time_ms or 0))
    safe_end = max(0, int(end_time_ms or 0))
    collected: Dict[int, Any] = {}

    while remaining > 0:
        chunk_limit = min(remaining, MARKET_CANDLES_UPSTREAM_CHUNK_LIMIT)
        rows = _fetch_binance_kline_rows_resilient(
            symbol,
            interval,
            chunk_limit,
            start_time_ms=cursor,
            end_time_ms=safe_end,
        ) or []
        if not rows:
            break
        last_open_time = cursor
        new_count = 0
        for row in rows:
            open_time = _extract_market_row_open_time(row)
            if open_time <= 0:
                continue
            if safe_end > 0 and open_time > safe_end:
                continue
            last_open_time = max(last_open_time, open_time)
            if open_time in collected:
                continue
            collected[open_time] = row
            new_count += 1
        if new_count <= 0:
            break
        remaining -= new_count
        if len(rows) < chunk_limit:
            break
        next_cursor = last_open_time + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor

    return [collected[key] for key in sorted(collected.keys())]


# 未指定 startTime 时从新到旧分页，再统一升序返回，适合图表整窗历史拉取。
def _fetch_market_candle_rows_backward(symbol: str,
                                       interval: str,
                                       limit: int,
                                       *,
                                       end_time_ms: int = 0) -> List[Any]:
    remaining = max(1, min(int(limit or 0), 1500))
    cursor_end = max(0, int(end_time_ms or 0))
    collected: Dict[int, Any] = {}

    while remaining > 0:
        chunk_limit = min(remaining, MARKET_CANDLES_UPSTREAM_CHUNK_LIMIT)
        rows = _fetch_binance_kline_rows_resilient(
            symbol,
            interval,
            chunk_limit,
            start_time_ms=0,
            end_time_ms=cursor_end,
        ) or []
        if not rows:
            break
        first_open_time: Optional[int] = None
        new_count = 0
        for row in rows:
            open_time = _extract_market_row_open_time(row)
            if open_time <= 0:
                continue
            if first_open_time is None:
                first_open_time = open_time
            else:
                first_open_time = min(first_open_time, open_time)
            if open_time in collected:
                continue
            collected[open_time] = row
            new_count += 1
        if new_count <= 0 or first_open_time is None:
            break
        remaining -= new_count
        if len(rows) < chunk_limit or first_open_time <= 0:
            break
        cursor_end = first_open_time - 1

    return [collected[key] for key in sorted(collected.keys())]


# 统一提取 K 线开盘时间，供分页去重和翻页游标使用。
def _extract_market_row_open_time(row: Any) -> int:
    if isinstance(row, (list, tuple)) and row:
        try:
            return int(row[0] or 0)
        except Exception:
            return 0
    if isinstance(row, dict):
        try:
            return int((row.get("k") or row).get("t") or (row.get("k") or row).get("openTime") or 0)
        except Exception:
            return 0
    return 0


async def _pipe_websocket_client_to_upstream(client: WebSocket, upstream) -> None:
    while True:
        message = await client.receive()
        message_type = message.get("type", "")
        if message_type == "websocket.disconnect":
            return
        if message.get("text") is not None:
            await upstream.send(message["text"])
        elif message.get("bytes") is not None:
            await upstream.send(message["bytes"])


async def _pipe_websocket_upstream_to_client(client: WebSocket, upstream) -> None:
    async for message in upstream:
        if isinstance(message, bytes):
            await client.send_bytes(message)
        else:
            await client.send_text(message)


async def _proxy_binance_websocket(client: WebSocket, path_value: str) -> None:
    upstream_url = _build_binance_ws_upstream_url(path_value, str(client.url.query or ""))
    await client.accept()
    try:
        async with websockets.connect(
            upstream_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=None,
        ) as upstream:
            client_to_upstream = asyncio.create_task(_pipe_websocket_client_to_upstream(client, upstream))
            upstream_to_client = asyncio.create_task(_pipe_websocket_upstream_to_client(client, upstream))
            done, pending = await asyncio.wait(
                {client_to_upstream, upstream_to_client},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            for task in done:
                task.result()
    except WebSocketDisconnect:
        return
    except Exception as exc:
        await client.close(code=1011, reason=f"Binance WS proxy failed: {exc}")


@app.get("/health")
def health():
    try:
        now_ms = _now_ms()
        with snapshot_cache_lock:
            cached_payload = _clone_json_value(health_status_cache.get("payload")) if health_status_cache.get("payload") else None
            cached_built_at = int(health_status_cache.get("builtAt", 0) or 0)
        if cached_payload and (now_ms - cached_built_at) <= HEALTH_CACHE_MS:
            cached_payload["healthCached"] = True
            cached_payload["healthCacheAgeMs"] = max(0, now_ms - cached_built_at)
            return cached_payload

        info = {
            "ok": True,
            "gatewayMode": GATEWAY_MODE,
            "bundleFingerprint": BUNDLE_FINGERPRINT,
            "bundleGeneratedAt": BUNDLE_GENERATED_AT,
            "bundleManifestPath": BUNDLE_MANIFEST_PATH,
            "bundleScriptPath": BUNDLE_SCRIPT_PATH,
            "mt5PackageAvailable": mt5 is not None,
            "mt5Configured": _is_mt5_configured(),
            "mt5ConfiguredLogin": str(LOGIN),
            "mt5ConfiguredServer": SERVER,
            "mt5PathEnv": PATH or "",
            "mt5TimeOffsetMinutes": MT5_TIME_OFFSET_MINUTES,
            "mt5ServerTimezone": MT5_SERVER_TIMEZONE,
            "mt5LastConnectedPath": mt5_last_connected_path,
            "mt5DiscoveredTerminalCandidates": MT5_TERMINAL_CANDIDATES[:6],
            "eaSnapshotFresh": _is_ea_snapshot_fresh(),
            "eaSnapshotReceivedAt": ea_snapshot_received_at_ms,
            "snapshotDeltaEnabled": SNAPSHOT_DELTA_ENABLED,
            "snapshotBuildCacheMs": SNAPSHOT_BUILD_CACHE_MS,
            "v2StreamPushIntervalMs": V2_STREAM_PUSH_INTERVAL_MS,
            "healthCached": False,
            "healthCacheAgeMs": 0,
            "session": _build_session_summary(),
            "login": str(LOGIN),
            "server": SERVER,
            "mt5Connected": bool(mt5_last_connected_path) and _is_mt5_configured(),
            "mt5ProbeDeferred": True,
            "lastError": "",
        }
        readiness_error = _resolve_gateway_readiness_error()
        if readiness_error:
            info["ok"] = False
            info["lastError"] = readiness_error
        with snapshot_cache_lock:
            health_status_cache["payload"] = _clone_json_value(info)
            health_status_cache["builtAt"] = now_ms
        return info
    except Exception as exc:
        now_ms = _now_ms()
        with snapshot_cache_lock:
            cached_payload = _clone_json_value(health_status_cache.get("payload")) if health_status_cache.get("payload") else None
            cached_built_at = int(health_status_cache.get("builtAt", 0) or 0)
        if cached_payload:
            cached_payload["healthCached"] = True
            cached_payload["healthCacheAgeMs"] = max(0, now_ms - cached_built_at)
            cached_payload["warning"] = f"实时健康检查失败，暂时返回最近一次结果：{exc}"
            return cached_payload
        return {"ok": False, "error": str(exc)}


@app.get("/binance-rest/{path_value:path}")
def binance_rest_proxy(path_value: str, request: Request):
    return _proxy_binance_rest(path_value, request)


@app.get("/v1/source")
def source_status():
    return {
        "gatewayMode": GATEWAY_MODE,
        "bundleFingerprint": BUNDLE_FINGERPRINT,
        "bundleGeneratedAt": BUNDLE_GENERATED_AT,
        "bundleManifestPath": BUNDLE_MANIFEST_PATH,
        "bundleScriptPath": BUNDLE_SCRIPT_PATH,
        "mt5PackageAvailable": mt5 is not None,
        "mt5Configured": _is_mt5_configured(),
        "mt5ConfiguredLogin": str(LOGIN),
        "mt5ConfiguredServer": SERVER,
        "mt5PathEnv": PATH or "",
        "mt5TimeOffsetMinutes": MT5_TIME_OFFSET_MINUTES,
        "mt5ServerTimezone": MT5_SERVER_TIMEZONE,
        "mt5LastConnectedPath": mt5_last_connected_path,
        "mt5DiscoveredTerminalCandidates": MT5_TERMINAL_CANDIDATES[:6],
        "eaSnapshotFresh": _is_ea_snapshot_fresh(),
        "eaSnapshotReceivedAt": ea_snapshot_received_at_ms,
        "snapshotDeltaEnabled": SNAPSHOT_DELTA_ENABLED,
        "snapshotBuildCacheMs": SNAPSHOT_BUILD_CACHE_MS,
        "v2StreamPushIntervalMs": V2_STREAM_PUSH_INTERVAL_MS,
        "healthCacheMs": HEALTH_CACHE_MS,
        "session": _build_session_summary(),
        **_build_market_runtime_source_status(),
    }


@app.get("/v2/session/status")
def v2_session_status():
    """返回当前会话状态。"""
    return _build_verified_session_status_payload()


@app.get("/v2/session/public-key")
def v2_session_public_key():
    """返回登录信封公钥和当前会话摘要。"""
    status = session_manager.build_status_payload()
    return session_envelope_crypto.build_public_key_payload(
        active_account=status.get("activeAccount"),
        saved_accounts=status.get("savedAccounts"),
    )


def _build_session_error_detail(
    code: str,
    message: str,
    request_id: str = "",
    result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """统一构造会话接口错误明细，避免客户端只拿到模糊的 HTTP 500。"""
    detail = {
        "code": str(code or "").strip(),
        "message": str(message or "").strip(),
    }
    safe_request_id = str(request_id or "").strip()
    if safe_request_id:
        detail["requestId"] = safe_request_id
    result_payload = dict(result or {})
    if result_payload:
        detail["stage"] = str(result_payload.get("stage") or "").strip()
        detail["elapsedMs"] = int(result_payload.get("elapsedMs") or 0)
        detail["baselineAccount"] = result_payload.get("baselineAccount")
        detail["finalAccount"] = result_payload.get("finalAccount")
        detail["loginError"] = str(result_payload.get("loginError") or "").strip()
        detail["lastObservedAccount"] = result_payload.get("lastObservedAccount")
    return detail


@app.post("/v2/session/login")
def v2_session_login(payload: Dict[str, Any]):
    """接收加密登录信封并登录新账号。"""
    safe_payload = dict(payload or {})
    request_id = str(safe_payload.get("requestId") or "")
    _append_session_diagnostic_entry(
        request_id=request_id,
        action="login",
        stage="request_received",
        status="accepted",
        message="已收到会话登录请求",
        server_time=_now_ms(),
    )
    try:
        plain = session_envelope_crypto.decrypt_login_envelope(safe_payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    request_id = str(plain.get("requestId") or safe_payload.get("requestId") or "")
    _append_session_diagnostic_entry(
        request_id=request_id,
        action="login",
        stage="envelope_decrypted",
        status="ok",
        message="登录信封解密完成",
        server_time=_now_ms(),
        detail={
            "login": str(plain.get("login") or ""),
            "server": str(plain.get("server") or ""),
            "remember": bool(plain.get("remember")),
        },
    )
    remember = _parse_bool_flag(safe_payload.get("saveAccount"), default=_parse_bool_flag(plain.get("remember")))
    try:
        payload = session_manager.login_new_account(
            login=str(plain.get("login") or ""),
            password=str(plain.get("password") or ""),
            server=str(plain.get("server") or ""),
            remember=remember,
            request_id=request_id,
        )
        _append_session_diagnostic_entry(
            request_id=request_id,
            action="login",
            stage="login_accepted",
            status="ok",
            message=str(payload.get("message") or "登录成功"),
            server_time=_now_ms(),
        )
        return payload
    except ValueError as exc:
        _append_session_diagnostic_entry(
            request_id=request_id,
            action="login",
            stage="login_invalid",
            status="failed",
            message=str(exc),
            server_time=_now_ms(),
            error_code="SESSION_LOGIN_INVALID",
        )
        raise HTTPException(status_code=400, detail=_build_session_error_detail("SESSION_LOGIN_INVALID", str(exc), request_id=request_id))
    except Mt5AccountSwitchFlowError as exc:
        raise HTTPException(
            status_code=502,
            detail=_build_session_error_detail(
                "SESSION_LOGIN_FAILED",
                str(exc),
                request_id=request_id,
                result=exc.result,
            ),
        )
    except Exception as exc:
        _append_session_diagnostic_entry(
            request_id=request_id,
            action="login",
            stage="login_failed",
            status="failed",
            message=str(exc),
            server_time=_now_ms(),
            error_code="SESSION_LOGIN_FAILED",
        )
        raise HTTPException(status_code=502, detail=_build_session_error_detail("SESSION_LOGIN_FAILED", str(exc), request_id=request_id))


@app.post("/v2/session/switch")
def v2_session_switch(payload: Dict[str, Any]):
    """切换到已保存账号。"""
    safe_payload = dict(payload or {})
    request_id = str(safe_payload.get("requestId") or "")
    profile_id = str(safe_payload.get("accountProfileId") or safe_payload.get("profileId") or "").strip()
    if not profile_id:
        raise HTTPException(status_code=400, detail="accountProfileId is required")
    _append_session_diagnostic_entry(
        request_id=request_id,
        action="switch",
        stage="request_received",
        status="accepted",
        message="已收到会话切换请求",
        server_time=_now_ms(),
        detail={"profileId": profile_id},
    )
    try:
        switch_payload = session_manager.switch_saved_account(profile_id, request_id=request_id)
        _append_session_diagnostic_entry(
            request_id=request_id,
            action="switch",
            stage="switch_accepted",
            status="ok",
            message=str(switch_payload.get("message") or "切换成功"),
            server_time=_now_ms(),
        )
        return switch_payload
    except ValueError as exc:
        _append_session_diagnostic_entry(
            request_id=request_id,
            action="switch",
            stage="switch_invalid",
            status="failed",
            message=str(exc),
            server_time=_now_ms(),
            error_code="SESSION_SWITCH_INVALID",
        )
        raise HTTPException(status_code=400, detail=_build_session_error_detail("SESSION_SWITCH_INVALID", str(exc), request_id=request_id))
    except Mt5AccountSwitchFlowError as exc:
        raise HTTPException(
            status_code=502,
            detail=_build_session_error_detail(
                "SESSION_SWITCH_FAILED",
                str(exc),
                request_id=request_id,
                result=exc.result,
            ),
        )
    except Exception as exc:
        _append_session_diagnostic_entry(
            request_id=request_id,
            action="switch",
            stage="switch_failed",
            status="failed",
            message=str(exc),
            server_time=_now_ms(),
            error_code="SESSION_SWITCH_FAILED",
        )
        raise HTTPException(status_code=502, detail=_build_session_error_detail("SESSION_SWITCH_FAILED", str(exc), request_id=request_id))


@app.post("/v2/session/logout")
def v2_session_logout(payload: Dict[str, Any]):
    """退出当前激活账号。"""
    request_id = str((payload or {}).get("requestId") or "")
    return session_manager.logout_current_session(request_id=request_id)


@app.get("/v2/session/diagnostic/latest")
def v2_session_diagnostic_latest(requestId: str = Query(default="")):
    """返回最近一次或指定 requestId 的会话诊断时间线。"""
    items = session_diagnostic_store.latest_timeline(requestId)
    return _build_session_diagnostic_payload(items)


@app.get("/v2/session/diagnostic/lookup")
def v2_session_diagnostic_lookup(requestId: str = Query(default="")):
    """按 requestId 返回完整会话诊断时间线。"""
    items = session_diagnostic_store.lookup(requestId)
    return _build_session_diagnostic_payload(items)


@app.get("/v2/market/snapshot")
def v2_market_snapshot():
    try:
        now_ms = _now_ms()
        session_status = session_manager.build_status_payload()
        active_account = (session_status or {}).get("activeAccount") or None
        if active_account:
            snapshot = _build_account_light_snapshot_with_cache()
        else:
            snapshot = _build_logged_out_account_snapshot()
        return _build_v2_market_snapshot_payload(
            market=_build_v2_market_section(now_ms),
            account=_build_v2_account_section_from_snapshot(snapshot),
            server_time=now_ms,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v2/market/candles")
def v2_market_candles(symbol: str,
                      interval: str,
                      limit: int = Query(default=300, ge=1, le=1500),
                      startTime: int = Query(default=0, ge=0),
                      endTime: int = Query(default=0, ge=0)):
    try:
        now_ms = _now_ms()
        rest_rows = _fetch_market_candle_rows_with_cache(
            symbol,
            interval,
            limit,
            start_time_ms=startTime,
            end_time_ms=endTime,
        )
        closed_rest_rows, latest_rest_patch = v2_market.separate_closed_rest_rows(rest_rows, now_ms)
        runtime_patch_row = None
        patch_source = "binance-rest"
        if str(interval or "").strip() == "1m":
            runtime_patch_row = _get_market_runtime_patch_row(symbol)
            if runtime_patch_row is not None:
                patch_source = "binance-ws"
        else:
            runtime_patch_row = _build_market_runtime_interval_patch(symbol, str(interval or "").strip())
            if runtime_patch_row is not None:
                patch_source = "binance-ws"
        return v2_market.build_market_candles_response(
            symbol=symbol,
            interval=interval,
            server_time=now_ms,
            rest_rows=closed_rest_rows,
            latest_patch=runtime_patch_row if runtime_patch_row is not None else latest_rest_patch,
            patch_source=patch_source,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v2/account/snapshot")
def v2_account_snapshot():
    try:
        now_ms = _now_ms()
        session_status = session_manager.build_status_payload()
        active_account = (session_status or {}).get("activeAccount") or None
        if active_account:
            snapshot = _build_account_light_snapshot_with_cache()
        else:
            snapshot = _build_logged_out_account_snapshot()
        account_section = _build_v2_account_section_from_snapshot(snapshot)
        snapshot_model = v2_account.build_account_snapshot_model(
            {
                "metrics": {
                    "balance": (snapshot.get("accountMeta") or {}).get("balance"),
                    "equity": (snapshot.get("accountMeta") or {}).get("equity"),
                    "margin": (snapshot.get("accountMeta") or {}).get("margin"),
                    "freeMargin": (snapshot.get("accountMeta") or {}).get("freeMargin"),
                    "marginLevel": (snapshot.get("accountMeta") or {}).get("marginLevel"),
                    "profit": (snapshot.get("accountMeta") or {}).get("profit"),
                },
                "positions": account_section.get("positions") or [],
                "orders": account_section.get("orders") or [],
            }
        )
        snapshot_model["overviewMetrics"] = [dict(item) for item in (snapshot.get("overviewMetrics") or [])]
        snapshot_model["curveIndicators"] = [dict(item) for item in (snapshot.get("curveIndicators") or [])]
        snapshot_model["statsMetrics"] = [dict(item) for item in (snapshot.get("statsMetrics") or [])]
        response = v2_account.build_account_snapshot_response(
            snapshot_model,
            account_meta={
                **(account_section.get("accountMeta") or {}),
                "serverTime": now_ms,
                "syncToken": _build_sync_token(now_ms, "account-snapshot"),
            },
        )
        response["account"] = {
            "balance": snapshot_model.get("balance"),
            "equity": snapshot_model.get("equity"),
            "margin": snapshot_model.get("margin"),
            "freeMargin": snapshot_model.get("freeMargin"),
            "marginLevel": snapshot_model.get("marginLevel"),
            "profit": snapshot_model.get("profit"),
            "leverage": (snapshot.get("accountMeta") or {}).get("leverage"),
        }
        return response
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v2/account/history")
def v2_account_history(
    range: str = Query(default="all", pattern="^(1d|7d|1m|3m|1y|all)$"),
    cursor: str = Query(default=""),
):
    try:
        now_ms = _now_ms()
        range_key = range.lower()
        session_status = session_manager.build_status_payload()
        active_account = (session_status or {}).get("activeAccount") or None
        if active_account:
            snapshot = _build_snapshot_with_cache(range_key)
            trades = _build_trade_history_with_cache(range_key, snapshot)
        else:
            snapshot = _build_logged_out_account_snapshot()
            trades = []
        history_model = v2_account.build_account_history_model(
            {
                "trades": trades,
                "orders": snapshot.get("pendingOrders") or [],
                "curvePoints": snapshot.get("curvePoints") or [],
            }
        )
        history_model["overviewMetrics"] = [dict(item) for item in (snapshot.get("overviewMetrics") or [])]
        history_model["curveIndicators"] = [dict(item) for item in (snapshot.get("curveIndicators") or [])]
        history_model["statsMetrics"] = [dict(item) for item in (snapshot.get("statsMetrics") or [])]
        payload = v2_account.build_account_history_response(
            history_model,
            account_meta={
                **(snapshot.get("accountMeta") or {}),
                "serverTime": now_ms,
                "syncToken": _build_sync_token(now_ms, f"account-history:{range_key}:{cursor}"),
            },
        )
        payload["nextCursor"] = ""
        return payload
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v2/account/full")
def v2_account_full():
    try:
        now_ms = _now_ms()
        session_status = session_manager.build_status_payload()
        active_account = (session_status or {}).get("activeAccount") or None
        if active_account:
            snapshot = _build_snapshot_with_cache("all")
        else:
            snapshot = _build_logged_out_account_snapshot()
        account_section = _build_v2_account_section_from_snapshot(snapshot)
        full_model = v2_account.build_account_full_model(
            {
                "accountMeta": {
                    **(snapshot.get("accountMeta") or {}),
                    "historyRevision": (account_section.get("accountMeta") or {}).get("historyRevision", ""),
                },
                "overviewMetrics": [dict(item) for item in (snapshot.get("overviewMetrics") or [])],
                "curveIndicators": [dict(item) for item in (snapshot.get("curveIndicators") or [])],
                "statsMetrics": [dict(item) for item in (snapshot.get("statsMetrics") or [])],
                "positions": account_section.get("positions") or [],
                "orders": account_section.get("orders") or [],
                "trades": snapshot.get("trades") or [],
                "curvePoints": snapshot.get("curvePoints") or [],
            }
        )
        return v2_account.build_account_full_response(
            full_model,
            account_meta={
                **(account_section.get("accountMeta") or {}),
                "serverTime": now_ms,
                "syncToken": _build_sync_token(now_ms, "account-full"),
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/v2/trade/check")
def v2_trade_check(payload: Dict[str, Any]):
    try:
        now_ms = _now_ms()
        account_mode = _detect_account_mode()
        prepared = _prepare_trade_command(payload or {}, account_mode)
        command = prepared.get("command") or v2_trade.normalize_trade_payload(payload or {})
        request_id = str(command.get("requestId") or "")
        action = str(command.get("action") or "")
        prepare_error = prepared.get("error")
        if prepare_error is not None:
            response = v2_trade_models.build_trade_check_response(
                request_id=request_id,
                action=action,
                account_mode=account_mode,
                status=v2_trade_models.STATUS_NOT_EXECUTABLE,
                error=prepare_error,
                check=None,
                server_time=now_ms,
            )
            _record_single_trade_audit(
                command=command,
                account_mode=account_mode,
                stage="check",
                status=response.get("status", ""),
                error=response.get("error"),
                message=str((response.get("error") or {}).get("message") or "检查未通过"),
                server_time=now_ms,
            )
            return response

        check_result = v2_trade_models.mt5_result_to_dict(_trade_check_request(prepared.get("request") or {}))
        check_error = v2_trade_models.error_from_retcode(
            check_result.get("retcode", -1),
            check_result.get("comment", ""),
        )
        response = v2_trade_models.build_trade_check_response(
            request_id=request_id,
            action=action,
            account_mode=account_mode,
            status=v2_trade_models.STATUS_EXECUTABLE if check_error is None else v2_trade_models.STATUS_NOT_EXECUTABLE,
            error=check_error,
            check=check_result,
            server_time=now_ms,
        )
        _record_single_trade_audit(
            command=command,
            account_mode=account_mode,
            stage="check",
            status=response.get("status", ""),
            error=response.get("error"),
            message="检查通过" if check_error is None else str((check_error or {}).get("message") or "检查未通过"),
            server_time=now_ms,
        )
        return response
    except Exception as exc:
        now_ms = _now_ms()
        command = v2_trade.normalize_trade_payload(payload or {})
        response = v2_trade_models.build_trade_check_response(
            request_id=str(command.get("requestId") or ""),
            action=str(command.get("action") or ""),
            account_mode=_detect_account_mode(),
            status=v2_trade_models.STATUS_NOT_EXECUTABLE,
            error=v2_trade_models.build_error(v2_trade_models.ERROR_TIMEOUT, str(exc)),
            check=None,
            server_time=now_ms,
        )
        _record_single_trade_audit(
            command=command,
            account_mode=str(response.get("accountMode") or ""),
            stage="check",
            status=response.get("status", ""),
            error=response.get("error"),
            message=str(exc),
            server_time=now_ms,
        )
        return response


@app.post("/v2/trade/submit")
def v2_trade_submit(payload: Dict[str, Any]):
    now_ms = _now_ms()
    account_mode = _detect_account_mode()
    prepared = _prepare_trade_command(payload or {}, account_mode)
    command = prepared.get("command") or v2_trade.normalize_trade_payload(payload or {})
    request_id = str(command.get("requestId") or "")
    action = str(command.get("action") or "")
    command_digest = _trade_command_digest(command)

    existing_entry = _get_trade_request_entry(request_id)
    if existing_entry is not None:
        existing_digest = str(existing_entry.get("payloadDigest") or "")
        existing_response = existing_entry.get("response") if isinstance(existing_entry.get("response"), dict) else {}
        if existing_digest and existing_digest != command_digest:
            response = v2_trade_models.build_trade_submit_response(
                request_id=request_id,
                action=action,
                account_mode=str(existing_response.get("accountMode") or account_mode),
                status=v2_trade_models.STATUS_FAILED,
                error=v2_trade_models.build_error(
                    v2_trade_models.ERROR_DUPLICATE_PAYLOAD_MISMATCH,
                    "相同 requestId 的 payload 不一致",
                ),
                check=existing_response.get("check"),
                result=existing_response.get("result"),
                server_time=now_ms,
                idempotent=True,
            )
            _record_single_trade_audit(
                command=command,
                account_mode=str(response.get("accountMode") or ""),
                stage="submit",
                status=str(response.get("status") or ""),
                error=response.get("error"),
                message=str((response.get("error") or {}).get("message") or "交易失败"),
                server_time=now_ms,
            )
            return response
        response = v2_trade_models.build_trade_submit_response(
            request_id=request_id,
            action=action,
            account_mode=str(existing_response.get("accountMode") or account_mode),
            status=v2_trade_models.STATUS_DUPLICATE,
            error=v2_trade_models.build_error(
                v2_trade_models.ERROR_DUPLICATE,
                "重复 requestId，已按幂等返回",
            ),
            check=existing_response.get("check"),
            result=existing_response.get("result"),
            server_time=now_ms,
            idempotent=True,
        )
        _record_single_trade_audit(
            command=command,
            account_mode=str(response.get("accountMode") or ""),
            stage="submit",
            status=str(response.get("status") or ""),
            error=response.get("error"),
            message=str((response.get("error") or {}).get("message") or "重复 requestId，已按幂等返回"),
            server_time=now_ms,
        )
        return response

    prepare_error = prepared.get("error")
    if prepare_error is not None:
        response = v2_trade_models.build_trade_submit_response(
            request_id=request_id,
            action=action,
            account_mode=account_mode,
            status=v2_trade_models.STATUS_FAILED,
            error=prepare_error,
            check=None,
            result=None,
            server_time=now_ms,
            idempotent=False,
        )
        _store_trade_request_result(request_id, command_digest, response)
        _record_single_trade_audit(
            command=command,
            account_mode=account_mode,
            stage="submit",
            status=str(response.get("status") or ""),
            error=response.get("error"),
            message=str((response.get("error") or {}).get("message") or "交易失败"),
            server_time=now_ms,
        )
        return response

    try:
        check_result = v2_trade_models.mt5_result_to_dict(_trade_check_request(prepared.get("request") or {}))
    except Exception as exc:
        response = v2_trade_models.build_trade_submit_response(
            request_id=request_id,
            action=action,
            account_mode=account_mode,
            status=v2_trade_models.STATUS_FAILED,
            error=v2_trade_models.build_error(v2_trade_models.ERROR_TIMEOUT, str(exc)),
            check=None,
            result=None,
            server_time=now_ms,
            idempotent=False,
        )
        _store_trade_request_result(request_id, command_digest, response)
        _record_single_trade_audit(
            command=command,
            account_mode=account_mode,
            stage="submit",
            status=str(response.get("status") or ""),
            error=response.get("error"),
            message=str(exc),
            server_time=now_ms,
        )
        return response

    check_error = v2_trade_models.error_from_retcode(
        check_result.get("retcode", -1),
        check_result.get("comment", ""),
    )
    if check_error is not None:
        response = v2_trade_models.build_trade_submit_response(
            request_id=request_id,
            action=action,
            account_mode=account_mode,
            status=v2_trade_models.STATUS_FAILED,
            error=check_error,
            check=check_result,
            result=None,
            server_time=now_ms,
            idempotent=False,
        )
        _store_trade_request_result(request_id, command_digest, response)
        _record_single_trade_audit(
            command=command,
            account_mode=account_mode,
            stage="submit",
            status=str(response.get("status") or ""),
            error=response.get("error"),
            message=str((response.get("error") or {}).get("message") or "交易失败"),
            server_time=now_ms,
        )
        return response

    try:
        send_result = v2_trade_models.mt5_result_to_dict(_trade_send_request(prepared.get("request") or {}))
    except Exception as exc:
        response = v2_trade_models.build_trade_submit_response(
            request_id=request_id,
            action=action,
            account_mode=account_mode,
            status=v2_trade_models.STATUS_ACCEPTED,
            error=v2_trade_models.build_error(v2_trade_models.ERROR_RESULT_UNKNOWN, str(exc)),
            check=check_result,
            result=None,
            server_time=now_ms,
            idempotent=False,
        )
        _store_trade_request_result(request_id, command_digest, response)
        _record_single_trade_audit(
            command=command,
            account_mode=account_mode,
            stage="submit",
            status=str(response.get("status") or ""),
            error=response.get("error"),
            message=str((response.get("error") or {}).get("message") or str(exc)),
            server_time=now_ms,
        )
        return response

    send_error = v2_trade_models.error_from_retcode(
        send_result.get("retcode", -1),
        send_result.get("comment", ""),
    )
    response = v2_trade_models.build_trade_submit_response(
        request_id=request_id,
        action=action,
        account_mode=account_mode,
        status=v2_trade_models.STATUS_ACCEPTED if send_error is None else v2_trade_models.STATUS_FAILED,
        error=send_error,
        check=check_result,
        result=send_result,
        server_time=now_ms,
        idempotent=False,
    )
    _store_trade_request_result(request_id, command_digest, response)
    _record_single_trade_audit(
        command=command,
        account_mode=account_mode,
        stage="submit",
        status=str(response.get("status") or ""),
        error=response.get("error"),
        message="交易已受理" if send_error is None else str((response.get("error") or {}).get("message") or "交易失败"),
        server_time=now_ms,
    )
    if send_error is None:
        _invalidate_account_runtime_cache_after_trade_commit()
        _publish_account_trade_commit_sync_state()
    return response


@app.get("/v2/trade/result")
def v2_trade_result(requestId: str = Query(default="")):
    request_id = str(requestId or "").strip()
    if not request_id:
        _record_single_trade_audit(
            command={"requestId": request_id, "action": "", "params": {}},
            account_mode="unknown",
            stage="result",
            status="FAILED",
            error=v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_PARAMS, "requestId 不能为空"),
            message="requestId 不能为空",
            server_time=_now_ms(),
        )
        raise HTTPException(
            status_code=400,
            detail=v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_PARAMS, "requestId 不能为空"),
        )
    payload = _get_trade_request_result(request_id)
    if payload is None:
        _record_single_trade_audit(
            command={"requestId": request_id, "action": "", "params": {}},
            account_mode="unknown",
            stage="result",
            status="FAILED",
            error=v2_trade_models.build_error(v2_trade_models.ERROR_REQUEST_NOT_FOUND, "未找到对应 requestId"),
            message="未找到对应 requestId",
            server_time=_now_ms(),
        )
        raise HTTPException(
            status_code=404,
            detail=v2_trade_models.build_error(v2_trade_models.ERROR_REQUEST_NOT_FOUND, "未找到对应 requestId"),
        )
    _record_single_trade_audit(
        command={"requestId": request_id, "action": payload.get("action"), "params": {"symbol": payload.get("symbol", "")}},
        account_mode=str(payload.get("accountMode") or "unknown"),
        stage="result",
        status=str(payload.get("status") or ""),
        error=payload.get("error"),
        message=str((payload.get("error") or {}).get("message") or payload.get("status") or "结果已返回"),
        server_time=int(payload.get("serverTime") or _now_ms()),
    )
    return payload


@app.post("/v2/trade/batch/submit")
def v2_trade_batch_submit(payload: Dict[str, Any]):
    now_ms = _now_ms()
    account_mode = _detect_account_mode()
    result = v2_trade_batch.submit_trade_batch(
        payload or {},
        account_mode=account_mode,
        mt5_module=mt5,
        position_lookup=_lookup_trade_position,
        symbol_info_lookup=(lambda symbol: mt5.symbol_info(symbol)) if mt5 is not None else None,
        check_request=_trade_check_request,
        send_request=_trade_send_request,
        server_time=now_ms,
    )
    _store_batch_request_result(str(result.get("batchId") or ""), result)
    _record_batch_trade_audit(
        payload=payload,
        result_payload=result,
        stage="batch_submit",
        status=str(result.get("status") or ""),
        error=result.get("error"),
        message=str((result.get("error") or {}).get("message") or result.get("status") or "批量提交完成"),
        server_time=int(result.get("serverTime") or now_ms),
    )
    if str(result.get("status") or "") in {v2_trade_models.STATUS_ACCEPTED, v2_trade_models.STATUS_PARTIAL}:
        _invalidate_account_runtime_cache_after_trade_commit()
        _publish_account_trade_commit_sync_state()
    return result


@app.get("/v2/trade/batch/result")
def v2_trade_batch_result(batchId: str = Query(default="")):
    batch_id = str(batchId or "").strip()
    if not batch_id:
        _record_batch_trade_audit(
            payload={"batchId": batch_id},
            result_payload={"batchId": batch_id, "accountMode": "unknown"},
            stage="batch_result",
            status="FAILED",
            error=v2_trade_models.build_error(v2_trade_models.ERROR_BATCH_INVALID_ID, "batchId 不能为空"),
            message="batchId 不能为空",
            server_time=_now_ms(),
        )
        raise HTTPException(
            status_code=400,
            detail=v2_trade_models.build_error(v2_trade_models.ERROR_BATCH_INVALID_ID, "batchId 不能为空"),
        )
    payload = _get_batch_request_result(batch_id)
    if payload is None:
        _record_batch_trade_audit(
            payload={"batchId": batch_id},
            result_payload={"batchId": batch_id, "accountMode": "unknown"},
            stage="batch_result",
            status="FAILED",
            error=v2_trade_models.build_error(v2_trade_models.ERROR_BATCH_NOT_FOUND, "未找到对应 batchId"),
            message="未找到对应 batchId",
            server_time=_now_ms(),
        )
        raise HTTPException(
            status_code=404,
            detail=v2_trade_models.build_error(v2_trade_models.ERROR_BATCH_NOT_FOUND, "未找到对应 batchId"),
        )
    _record_batch_trade_audit(
        payload={"batchId": batch_id},
        result_payload=payload,
        stage="batch_result",
        status=str(payload.get("status") or ""),
        error=payload.get("error"),
        message=str((payload.get("error") or {}).get("message") or payload.get("status") or "批量结果已返回"),
        server_time=int(payload.get("serverTime") or _now_ms()),
    )
    return payload


@app.get("/v2/trade/audit/recent")
def v2_trade_audit_recent(limit: int = Query(default=50, ge=1, le=200)):
    now_ms = _now_ms()
    return {
        "items": trade_audit_store.recent(limit),
        "serverTime": now_ms,
    }


@app.get("/v2/trade/audit/lookup")
def v2_trade_audit_lookup(id: str = Query(default="")):
    now_ms = _now_ms()
    trace_id = str(id or "").strip()
    if not trace_id:
        raise HTTPException(
            status_code=400,
            detail=v2_trade_models.build_error(v2_trade_models.ERROR_INVALID_PARAMS, "id 不能为空"),
        )
    return {
        "id": trace_id,
        "items": trade_audit_store.lookup(trace_id),
        "serverTime": now_ms,
    }


@app.post("/v1/ea/snapshot")
def ingest_ea_snapshot(payload: Dict, x_bridge_token: Optional[str] = Header(default=None)):
    global ea_snapshot_cache, ea_snapshot_received_at_ms, ea_snapshot_change_digest

    if EA_INGEST_TOKEN:
        if not x_bridge_token or x_bridge_token != EA_INGEST_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid bridge token.")

    normalized = _normalize_snapshot(payload, "MT5 EA Push")
    normalized["accountMeta"]["source"] = "MT5 EA Push"
    normalized["accountMeta"]["updatedAt"] = _now_ms()
    change_digest = _snapshot_digest(normalized)
    changed = False
    received_at = _now_ms()

    with state_lock:
        changed = ea_snapshot_cache is None or change_digest != ea_snapshot_change_digest
        ea_snapshot_received_at_ms = received_at
        if changed:
            ea_snapshot_cache = normalized
            ea_snapshot_change_digest = change_digest
    return {"ok": True, "receivedAt": ea_snapshot_received_at_ms, "changed": changed}


@app.get("/v1/snapshot")
def snapshot(
    range: str = Query(default="7d", pattern="^(1d|7d|1m|3m|1y|all)$"),
    since: int = Query(default=0, ge=0),
    delta: int = Query(default=1, ge=0, le=1),
):
    try:
        return _build_snapshot_response(range.lower(), since_seq=since, delta=(delta == 1))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v1/summary")
def summary(
    range: str = Query(default="7d", pattern="^(1d|7d|1m|3m|1y|all)$"),
    since: int = Query(default=0, ge=0),
    delta: int = Query(default=1, ge=0, le=1),
):
    try:
        return _build_summary_snapshot_response(range.lower(), since_seq=since, delta=(delta == 1))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v1/live")
def live(
    range: str = Query(default="7d", pattern="^(1d|7d|1m|3m|1y|all)$"),
    since: int = Query(default=0, ge=0),
    delta: int = Query(default=1, ge=0, le=1),
):
    try:
        return _build_live_snapshot_response(range.lower(), since_seq=since, delta=(delta == 1))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v1/pending")
def pending(
    range: str = Query(default="7d", pattern="^(1d|7d|1m|3m|1y|all)$"),
    since: int = Query(default=0, ge=0),
    delta: int = Query(default=1, ge=0, le=1),
):
    try:
        return _build_pending_snapshot_response(range.lower(), since_seq=since, delta=(delta == 1))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v1/trades")
def trades(
    range: str = Query(default="7d", pattern="^(1d|7d|1m|3m|1y|all)$"),
    since: int = Query(default=0, ge=0),
    delta: int = Query(default=1, ge=0, le=1),
):
    try:
        return _build_trades_snapshot_response(range.lower(), since_seq=since, delta=(delta == 1))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v1/curve")
def curve(
    range: str = Query(default="7d", pattern="^(1d|7d|1m|3m|1y|all)$"),
    since: int = Query(default=0, ge=0),
    delta: int = Query(default=1, ge=0, le=1),
):
    try:
        return _build_curve_snapshot_response(range.lower(), since_seq=since, delta=(delta == 1))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v1/abnormal")
def abnormal(
    since: int = Query(default=0, ge=0),
    delta: int = Query(default=1, ge=0, le=1),
):
    try:
        return _build_abnormal_response(since_seq=since, delta=(delta == 1))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v2/abnormal/snapshot")
def v2_abnormal_snapshot():
    try:
        return _build_v2_abnormal_snapshot_response()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/v2/abnormal/history")
def v2_abnormal_history(
    symbol: str = Query(default=""),
    startTime: int = Query(default=0, ge=0),
    endTime: int = Query(default=0, ge=0),
    limit: int = Query(default=200, ge=1, le=5000),
):
    try:
        return _build_v2_abnormal_history_response(
            symbol=symbol,
            start_time=startTime,
            end_time=endTime,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/v1/abnormal/config")
def abnormal_config(payload: Dict[str, Any]):
    try:
        return _set_abnormal_config(payload or {})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# 清空运行时缓存，让管理面板可以强制下一轮重新构建快照。
@app.post("/internal/admin/cache/clear")
def admin_cache_clear():
    with snapshot_cache_lock:
        cleared = {
            "snapshotBuildCache": len(snapshot_build_cache),
            "snapshotSyncCache": len(snapshot_sync_cache),
            "marketCandlesCache": len(market_candles_cache),
            "accountPublishState": 1 if account_publish_state.get("runtimeSeq", 0) or account_publish_state.get("busSeq", 0) else 0,
            "v2SyncState": 1 if account_publish_state.get("runtimeSeq", 0) else 0,
            "v2BusState": 1 if account_publish_state.get("busSeq", 0) else 0,
            "abnormalSyncState": 1 if abnormal_sync_state else 0,
            "tradeRequestStore": len(trade_request_store),
        }
        snapshot_build_cache.clear()
        snapshot_sync_cache.clear()
        market_candles_cache.clear()
        v2_sync_state.clear()
        _reset_account_publish_state_locked()
        abnormal_sync_state.clear()
        trade_request_store.clear()
        batch_request_store.clear()
        trade_audit_store.clear()
        session_diagnostic_store.clear()
    return {"ok": True, "cleared": cleared}


@app.websocket("/binance-ws/{path_value:path}")
async def binance_ws_proxy(client: WebSocket, path_value: str):
    await _proxy_binance_websocket(client, path_value)


@app.websocket("/v2/stream")
async def v2_stream(client: WebSocket):
    await client.accept()
    current_bus_seq = 0
    push_interval_sec = float(V2_STREAM_PUSH_INTERVAL_MS) / 1000.0
    loop = asyncio.get_running_loop()
    next_tick = loop.time()
    try:
        while True:
            message = _build_v2_stream_event_for_client(last_bus_seq=current_bus_seq)
            current_bus_seq = int(message.get("busSeq", current_bus_seq) or current_bus_seq)
            message["serverTime"] = _now_ms()
            await client.send_json(message)
            next_tick += push_interval_sec
            sleep_sec = next_tick - loop.time()
            if sleep_sec > 0:
                await asyncio.sleep(sleep_sec)
            else:
                next_tick = loop.time()
    except WebSocketDisconnect:
        return
    except Exception as exc:
        try:
            await client.close(code=1011, reason=f"v2 stream failed: {exc}")
        except Exception:
            return


if __name__ == "__main__":
    _configure_windows_event_loop_policy()
    uvicorn.run("server_v2:app", host=HOST, port=PORT, reload=False)
