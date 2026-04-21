#property strict

input string GatewayUrl = "http://127.0.0.1:8787/v1/ea/snapshot";
input string BridgeToken = "";
input int PushIntervalSeconds = 10;
input int HeartbeatIntervalSeconds = 20;
input int RequestTimeoutMs = 15000;
input int TradeHistoryDays = 30;
input int MaxTradeItems = 200;

string g_lastSuccessfulState = "";
datetime g_lastSuccessfulPushTime = 0;
string g_lastBuildError = "";

string JsonEscape(string value)
{
   string escaped = value;
   StringReplace(escaped, "\\", "\\\\");
   StringReplace(escaped, "\"", "\\\"");
   StringReplace(escaped, "\r", " ");
   StringReplace(escaped, "\n", " ");
   return escaped;
}

string BuildMetric(string name, string value)
{
   return StringFormat("{\"name\":\"%s\",\"value\":\"%s\"}", JsonEscape(name), JsonEscape(value));
}

void SetBuildError(string message)
{
   g_lastBuildError = message;
   Print("MT5BridgePushEA: ", message);
}

bool TryGetContractSize(string symbol, double &contractSize)
{
   contractSize = 0.0;
   if(!SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE, contractSize) || contractSize <= 0.0)
   {
      SetBuildError(StringFormat("missing contractSize for symbol=%s", symbol));
      return false;
   }
   return true;
}

string BuildOverviewMetrics()
{
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double margin = AccountInfoDouble(ACCOUNT_MARGIN);
   double freeMargin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   double balance = AccountInfoDouble(ACCOUNT_BALANCE);

   string json = "[";
   json += BuildMetric("Total Asset", DoubleToString(equity, 2));
   json += "," + BuildMetric("Margin", DoubleToString(margin, 2));
   json += "," + BuildMetric("Free Fund", DoubleToString(freeMargin, 2));
   json += "," + BuildMetric("Current Equity", DoubleToString(equity, 2));
   json += "," + BuildMetric("Balance", DoubleToString(balance, 2));
   json += "]";
   return json;
}

string BuildCurvePoints()
{
   long nowMs = (long)TimeCurrent() * 1000;
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double balance = AccountInfoDouble(ACCOUNT_BALANCE);
   return StringFormat("[{\"timestamp\":%I64d,\"equity\":%.2f,\"balance\":%.2f}]", nowMs, equity, balance);
}

string BuildCurveStateSignature()
{
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double balance = AccountInfoDouble(ACCOUNT_BALANCE);
   return StringFormat("%.2f|%.2f", equity, balance);
}

string BuildCurveIndicators()
{
   string json = "[";
   json += BuildMetric("1D Return", "0.00%");
   json += "," + BuildMetric("7D Return", "0.00%");
   json += "," + BuildMetric("30D Return", "0.00%");
   json += "," + BuildMetric("Max Drawdown", "0.00%");
   json += "," + BuildMetric("Volatility", "0.00%");
   json += "]";
   return json;
}

bool BuildPositions(string &json)
{
   json = "[]";
   int total = PositionsTotal();
   if(total <= 0)
      return true;

   double totalMarketValue = 0.0;
   int i;
   for(i = 0; i < total; i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket))
         continue;

      string symbol = PositionGetString(POSITION_SYMBOL);
      double volume = PositionGetDouble(POSITION_VOLUME);
      double priceCurrent = PositionGetDouble(POSITION_PRICE_CURRENT);
      double contractSize = 0.0;
      if(!TryGetContractSize(symbol, contractSize))
         return false;
      totalMarketValue += MathAbs(volume * priceCurrent * contractSize);
   }

   json = "[";
   bool first = true;
   for(i = 0; i < total; i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket))
         continue;

      string symbol = PositionGetString(POSITION_SYMBOL);
      long positionType = PositionGetInteger(POSITION_TYPE);
      string side = (positionType == POSITION_TYPE_BUY ? "Buy" : "Sell");
      double volume = PositionGetDouble(POSITION_VOLUME);
      double priceOpen = PositionGetDouble(POSITION_PRICE_OPEN);
      double priceCurrent = PositionGetDouble(POSITION_PRICE_CURRENT);
      double profit = PositionGetDouble(POSITION_PROFIT);
      double takeProfit = PositionGetDouble(POSITION_TP);
      double stopLoss = PositionGetDouble(POSITION_SL);
      double storageFee = PositionGetDouble(POSITION_SWAP);
      double contractSize = 0.0;
      if(!TryGetContractSize(symbol, contractSize))
         return false;
      double marketValue = MathAbs(volume * priceCurrent * contractSize);
      double ratio = 0.0;
      if(totalMarketValue > 0.0)
         ratio = marketValue / totalMarketValue;

      string item = StringFormat(
         "{\"productName\":\"%s\",\"code\":\"%s\",\"side\":\"%s\",\"quantity\":%.4f,\"sellableQuantity\":%.4f,\"costPrice\":%.5f,\"latestPrice\":%.5f,\"marketValue\":%.2f,\"positionRatio\":%.6f,\"dayPnL\":%.2f,\"totalPnL\":%.2f,\"returnRate\":%.6f,\"takeProfit\":%.5f,\"stopLoss\":%.5f,\"storageFee\":%.2f}",
         JsonEscape(symbol),
         JsonEscape(symbol),
         side,
         volume,
         volume,
         priceOpen,
         priceCurrent,
         marketValue,
         ratio,
         profit * 0.2,
         profit,
         (priceOpen == 0.0 ? 0.0 : (priceCurrent - priceOpen) / priceOpen),
         takeProfit,
         stopLoss,
         storageFee
      );

      if(!first)
         json += ",";
      json += item;
      first = false;
   }

   json += "]";
   return true;
}

bool BuildTrades(string &json)
{
   json = "[]";
   int historyDays = TradeHistoryDays;
   if(historyDays <= 0)
      historyDays = 3650;
   datetime fromTime = TimeCurrent() - historyDays * 24 * 60 * 60;
   datetime toTime = TimeCurrent();
   if(!HistorySelect(fromTime, toTime))
      return true;

   int total = HistoryDealsTotal();
   if(total <= 0)
      return true;

   int maxItems = MaxTradeItems;
   if(maxItems <= 0 || maxItems > total)
      maxItems = total;
   int start = total - 1;
   int end = MathMax(0, total - maxItems);

   json = "[";
   bool first = true;
   int i;
   for(i = start; i >= end; i--)
   {
      ulong dealTicket = HistoryDealGetTicket(i);
      if(dealTicket == 0)
         continue;

      string symbol = HistoryDealGetString(dealTicket, DEAL_SYMBOL);
      long dealType = HistoryDealGetInteger(dealTicket, DEAL_TYPE);
      string side = (dealType == DEAL_TYPE_BUY ? "Buy" : "Sell");
      long entryType = HistoryDealGetInteger(dealTicket, DEAL_ENTRY);
      long orderId = HistoryDealGetInteger(dealTicket, DEAL_ORDER);
      long positionId = HistoryDealGetInteger(dealTicket, DEAL_POSITION_ID);
      double volume = HistoryDealGetDouble(dealTicket, DEAL_VOLUME);
      double price = HistoryDealGetDouble(dealTicket, DEAL_PRICE);
      double contractSize = 0.0;
      if(!TryGetContractSize(symbol, contractSize))
         return false;
      double commission = HistoryDealGetDouble(dealTicket, DEAL_COMMISSION);
      double swapFee = HistoryDealGetDouble(dealTicket, DEAL_SWAP);
      double fee = MathAbs(commission);
      double profit = HistoryDealGetDouble(dealTicket, DEAL_PROFIT);
      long timeMs = (long)HistoryDealGetInteger(dealTicket, DEAL_TIME_MSC);
      if(timeMs <= 0)
         timeMs = (long)HistoryDealGetInteger(dealTicket, DEAL_TIME) * 1000;
      double amount = MathAbs(volume * price * contractSize);
      string comment = HistoryDealGetString(dealTicket, DEAL_COMMENT);

      string item = StringFormat(
         "{\"timestamp\":%I64d,\"productName\":\"%s\",\"code\":\"%s\",\"side\":\"%s\",\"price\":%.5f,\"openPrice\":%.5f,\"closePrice\":%.5f,\"quantity\":%.4f,\"amount\":%.2f,\"contractSize\":%.8f,\"fee\":%.2f,\"commission\":%.2f,\"profit\":%.2f,\"openTime\":%I64d,\"closeTime\":%I64d,\"storageFee\":%.2f,\"swap\":%.2f,\"dealTicket\":%I64u,\"orderId\":%I64d,\"positionId\":%I64d,\"entryType\":%I64d,\"dealType\":%I64d,\"remark\":\"%s\"}",
         timeMs,
         JsonEscape(symbol),
         JsonEscape(symbol),
         side,
         price,
         price,
         price,
         volume,
         amount,
         contractSize,
         fee,
         commission,
         profit,
         timeMs,
         timeMs,
         swapFee,
         swapFee,
         dealTicket,
         orderId,
         positionId,
         entryType,
         dealType,
         JsonEscape(comment)
      );

      if(!first)
         json += ",";
      json += item;
      first = false;
   }

   json += "]";
   return true;
}

string BuildStats()
{
   string json = "[";
   json += BuildMetric("Data Source", "MT5 EA Push");
   json += "," + BuildMetric("Pushed At", TimeToString(TimeCurrent(), TIME_DATE | TIME_SECONDS));
   json += "]";
   return json;
}

string BuildSnapshotJsonFromParts(string overviewMetrics,
                                  string curvePoints,
                                  string curveIndicators,
                                  string positions,
                                  string trades,
                                  string stats)
{
   long nowMs = (long)TimeCurrent() * 1000;
   long login = AccountInfoInteger(ACCOUNT_LOGIN);
   string server = AccountInfoString(ACCOUNT_SERVER);

   string accountMeta = StringFormat(
      "{\"login\":\"%I64d\",\"server\":\"%s\",\"source\":\"MT5 EA Push\",\"updatedAt\":%I64d}",
      login,
      JsonEscape(server),
      nowMs
   );

   string json = "{";
   json += "\"accountMeta\":" + accountMeta;
   json += ",\"overviewMetrics\":" + overviewMetrics;
   json += ",\"curvePoints\":" + curvePoints;
   json += ",\"curveIndicators\":" + curveIndicators;
   json += ",\"positions\":" + positions;
   json += ",\"trades\":" + trades;
   json += ",\"statsMetrics\":" + stats;
   json += "}";

   return json;
}

string BuildSnapshotStateSignature(string overviewMetrics,
                                   string curveIndicators,
                                   string positions,
                                   string trades)
{
   long login = AccountInfoInteger(ACCOUNT_LOGIN);
   string server = AccountInfoString(ACCOUNT_SERVER);
   return StringFormat("%I64d|%s|%s|%s|%s|%s",
      login,
      JsonEscape(server),
      BuildCurveStateSignature(),
      overviewMetrics,
      curveIndicators,
      positions + "|" + trades);
}

int BuildRequestPayload(string body, char &payload[])
{
   // WebRequest 只接受纯 JSON 字节，不能把结尾的空字符一起发出去。
   int copied = StringToCharArray(body, payload, 0, -1, CP_UTF8);
   if(copied <= 0)
   {
      ArrayResize(payload, 0);
      return 0;
   }

   if(payload[copied - 1] == 0)
      copied--;

   ArrayResize(payload, copied);
   return copied;
}

bool PushSnapshotBody(string body)
{
   char payload[];
   if(BuildRequestPayload(body, payload) <= 0)
   {
      Print("MT5BridgePushEA: payload build failed.");
      return false;
   }

   string headers = "Content-Type: application/json\r\n";
   if(StringLen(BridgeToken) > 0)
      headers += "X-Bridge-Token: " + BridgeToken + "\r\n";

   char response[];
   string responseHeaders = "";
   int code = WebRequest("POST", GatewayUrl, headers, RequestTimeoutMs, payload, response, responseHeaders);
   if(code == -1)
   {
      Print("MT5BridgePushEA: WebRequest failed, error=", GetLastError());
      return false;
   }

   string responseText = CharArrayToString(response, 0, -1, CP_UTF8);
   if(code < 200 || code >= 300)
   {
      Print("MT5BridgePushEA: push status=", code, " response=", responseText, " payloadBytes=", ArraySize(payload));
      return false;
   }
   return true;
}

bool BuildSnapshotParts(string &overviewMetrics,
                        string &curvePoints,
                        string &curveIndicators,
                        string &positions,
                        string &trades,
                        string &stats)
{
   g_lastBuildError = "";
   overviewMetrics = BuildOverviewMetrics();
   curvePoints = BuildCurvePoints();
   curveIndicators = BuildCurveIndicators();
   if(!BuildPositions(positions))
      return false;
   if(!BuildTrades(trades))
      return false;
   stats = BuildStats();
   return true;
}

bool PushSnapshot()
{
   string overviewMetrics = "";
   string curvePoints = "";
   string curveIndicators = "";
   string positions = "";
   string trades = "";
   string stats = "";
   if(!BuildSnapshotParts(overviewMetrics, curvePoints, curveIndicators, positions, trades, stats))
      return false;
   string body = BuildSnapshotJsonFromParts(
      overviewMetrics,
      curvePoints,
      curveIndicators,
      positions,
      trades,
      stats
   );
   return PushSnapshotBody(body);
}

int OnInit()
{
   int intervalSeconds = PushIntervalSeconds;
   if(intervalSeconds < 1)
      intervalSeconds = 10;

   int heartbeatSeconds = HeartbeatIntervalSeconds;
   if(heartbeatSeconds < intervalSeconds)
      heartbeatSeconds = intervalSeconds * 2;

   EventSetTimer(intervalSeconds);
   Print("MT5BridgePushEA started. Gateway=", GatewayUrl, " interval=", intervalSeconds, "s heartbeat=", heartbeatSeconds, "s");

   string overviewMetrics = "";
   string curvePoints = "";
   string curveIndicators = "";
   string positions = "";
   string trades = "";
   string stats = "";
   if(!BuildSnapshotParts(overviewMetrics, curvePoints, curveIndicators, positions, trades, stats))
      return(INIT_SUCCEEDED);
   string body = BuildSnapshotJsonFromParts(
      overviewMetrics,
      curvePoints,
      curveIndicators,
      positions,
      trades,
      stats
   );
   string stateSignature = BuildSnapshotStateSignature(
      overviewMetrics,
      curveIndicators,
      positions,
      trades
   );
   if(PushSnapshotBody(body))
   {
      g_lastSuccessfulState = stateSignature;
      g_lastSuccessfulPushTime = TimeCurrent();
   }
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   Print("MT5BridgePushEA stopped.");
}

void OnTimer()
{
   int heartbeatSeconds = HeartbeatIntervalSeconds;
   if(heartbeatSeconds < PushIntervalSeconds)
      heartbeatSeconds = PushIntervalSeconds * 2;

   string overviewMetrics = "";
   string curvePoints = "";
   string curveIndicators = "";
   string positions = "";
   string trades = "";
   string stats = "";
   if(!BuildSnapshotParts(overviewMetrics, curvePoints, curveIndicators, positions, trades, stats))
      return;
   string stateSignature = BuildSnapshotStateSignature(
      overviewMetrics,
      curveIndicators,
      positions,
      trades
   );

   datetime now = TimeCurrent();
   bool stateChanged = (StringLen(g_lastSuccessfulState) == 0 || g_lastSuccessfulState != stateSignature);
   bool heartbeatDue = (g_lastSuccessfulPushTime <= 0 || (now - g_lastSuccessfulPushTime) >= heartbeatSeconds);
   if(!stateChanged && !heartbeatDue)
      return;

   string body = BuildSnapshotJsonFromParts(
      overviewMetrics,
      curvePoints,
      curveIndicators,
      positions,
      trades,
      stats
   );
   if(PushSnapshotBody(body))
   {
      g_lastSuccessfulState = stateSignature;
      g_lastSuccessfulPushTime = now;
   }
}
