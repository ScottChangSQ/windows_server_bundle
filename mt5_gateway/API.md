# MT5 Gateway API Contract

## GET `/v1/snapshot?range=1d|7d|1m|3m|1y|all`

完整账户快照。首次进入账户页或需要兜底重建本地缓存时使用。

Response JSON:

```json
{
  "accountMeta": {
    "login": "7400048",
    "server": "ICMarketsSC-MT5-6",
    "source": "MT5 Python Pull",
    "updatedAt": 1760000000000,
    "syncSeq": 1
  },
  "overviewMetrics": [{"name": "Total Asset", "value": "$100,000.00"}],
  "curvePoints": [{"timestamp": 1760000000000, "equity": 100000.0, "balance": 100000.0}],
  "curveIndicators": [{"name": "1D Return", "value": "+0.20%"}],
  "positions": [{
    "productName": "XAUUSD",
    "code": "XAUUSD",
    "quantity": 1.0,
    "costPrice": 2300.0,
    "latestPrice": 2310.0,
    "totalPnL": 200.0
  }],
  "pendingOrders": [{
    "productName": "XAUUSD",
    "code": "XAUUSD",
    "pendingLots": 0.5,
    "pendingPrice": 2290.0
  }],
  "trades": [{
    "timestamp": 1760000000000,
    "productName": "XAUUSD",
    "code": "XAUUSD",
    "side": "Buy",
    "price": 2300.0,
    "quantity": 1.0,
    "profit": 200.0
  }],
  "statsMetrics": [{"name": "Win Rate", "value": "+58.00%"}]
}
```

## GET `/v1/summary?range=1d|7d|1m|3m|1y|all`

轻量摘要接口，只返回账户元信息和汇总指标。

## GET `/v1/live?range=1d|7d|1m|3m|1y|all`

后台轻实时接口。只返回：

- `accountMeta`
- `overviewMetrics`
- `positions`
- `statsMetrics`

用于后台常驻、悬浮窗和低流量保活。

## GET `/v1/pending?range=1d|7d|1m|3m|1y|all`

挂单接口。只返回：

- `accountMeta`
- `pendingOrders`

支持 `since` 与 `delta=1`，用于只同步挂单变化。

## GET `/v1/trades?range=1d|7d|1m|3m|1y|all`

历史成交接口。只返回：

- `accountMeta`
- `trades`

支持 `since` 与 `delta=1`，用于只同步新增或变化的成交。

## GET `/v1/curve?range=1d|7d|1m|3m|1y|all`

权益曲线接口。只返回：

- `accountMeta`
- `curvePoints`
- `curveIndicators`

曲线点会以历史成交、持仓、开平仓时间、价格与手数重新重算，使 `equity` 与 `balance` 能真实区分；如果缺少价格数据，会自动降级而不是让接口失败。

支持 `since` 与 `delta=1`，用于只追加新曲线点。

## 增量参数

以上接口都支持：

- `since=<syncSeq>`
- `delta=1`

当数据未变化时，会返回：

```json
{
  "accountMeta": {
    "syncSeq": 12
  },
  "isDelta": true,
  "unchanged": true
}
```

## POST `/v1/ea/snapshot`

- Content-Type: `application/json`
- Optional header: `X-Bridge-Token: <EA_INGEST_TOKEN>`
- Body: same structure as `/v1/snapshot` response

Response:

```json
{"ok": true, "receivedAt": 1760000000000}
```

## GET `/binance-rest/fapi/v1/klines`

Binance Futures REST 透传接口。App 现在会通过这个入口拉 BTC / XAU 的 K 线和行情刷新，不再由手机直接访问 Binance 官方域名。

示例：

```text
/binance-rest/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=300
```

## WebSocket `/binance-ws/ws/{stream}` / `/binance-ws/stream?streams=...`

Binance WebSocket 透传接口。主监控页会通过这里订阅 BTC / XAU 的实时 K 线更新。

示例：

```text
ws://43.155.214.62:8787/binance-ws/stream?streams=btcusdt@kline_1m/xauusdt@kline_1m
```
