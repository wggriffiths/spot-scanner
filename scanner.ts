import { parseArgs } from "jsr:@std/cli/parse-args";
import { isRunningAsService, runAsWindowsService, runServiceCommand } from "./service.ts";

const BINANCE_BASE = "https://api.binance.com";
const TOP_PAIRS_LIMIT = 200;
const CONCURRENCY_LIMIT = 12;
const MIN_24H_QUOTE_VOLUME_USDT = 5_000_000;
const SERVER_PORT = 8000;
const DASHBOARD_PATH = "/dashboard";

const ABNORMAL_VOLUME_MULTIPLIER = 3;
const ABNORMAL_PRICE_CHANGE_PERCENT = 1.5;
const MIN_AVG_VOLUME_1H = 50_000;
const ALERT_COOLDOWN_MS = 15 * 60_000;
const REQUIRE_BREAKOUT_FILTER = false;
const MAX_PERSISTED_ALERTS = 500;
const EARLY_CANDIDATE_TTL_MS = 5 * 60_000;
const HISTORY_FILE = "./alerts_history.json";
const HISTORY_SAVE_DEBOUNCE_MS = 5_000;
const PERF_LOG_FILE = "./performance_log.json";
const MAX_PERF_ENTRIES = 1_000;

const KLINE_INTERVAL = "5m";
const KLINES_REQUIRED = 13;
const STREAM_CHUNK_SIZE = 80;
const STREAM_RECONNECT_DELAY_MS = 3_000;

const SYMBOL_REGEX = /^[A-Z0-9]{1,15}USDT$/;

const RECENT_WINDOW_SEC = 10;
const BASELINE_WINDOW_SEC = 60;
const SCORE_INTERVAL_MS = 5_000;
const MICRO_MIN_NOTIONAL_10S = 30_000;
const MICRO_CANDIDATE_SCORE = 50;
const MICRO_TRIGGER_SCORE = 72;
const MICRO_TRIGGER_COOLDOWN_MS = 45_000;
const MAX_CONFIRM_LOOKBACK_PUMP_PERCENT = 8;

const ENABLE_DEPTH_IMBALANCE = false;
const DEPTH_LEVELS_TO_SUM = 5;

const RUN_ID = crypto.randomUUID();
const RUN_STARTED_AT = new Date().toISOString();

// ---------------------------------------------------------------------------
// CLI args
// Check --help / --version directly against Deno.args before parseArgs —
// this is required for compiled binaries where the JSR module may not
// resolve boolean flags reliably.
// ---------------------------------------------------------------------------
if (Deno.args.includes("--help") || Deno.args.includes("-h")) {
  console.log(`
Spot Scanner v0.1.1

Usage:
  scanner [options]
  scanner <command>

Commands:
  install        Install as a system service (requires admin/root)
  uninstall      Remove the system service (requires admin/root)
  start          Start the installed service (requires admin/root)
  stop           Stop the running service (requires admin/root)

Options:
  --port <number>      HTTP port (default: ${SERVER_PORT})
  --pairs-limit <n>    Max USDT pairs to scan (default: ${TOP_PAIRS_LIMIT})
  --dashboard-only     Serve dashboard without live scanning
  --version            Show version
  --help               Show this help
`);
  Deno.exit(0);
}

if (Deno.args.includes("--version") || Deno.args.includes("-v")) {
  console.log("v0.1.1");
  Deno.exit(0);
}

// Service sub-commands (install / uninstall / start / stop)
const _serviceCommands = ["install", "uninstall", "start", "stop"];
if (Deno.args[0] && _serviceCommands.includes(Deno.args[0])) {
  await runServiceCommand(Deno.args[0]);
  Deno.exit(0);
}

const _args = parseArgs(Deno.args, {
  boolean: ["dashboard-only", "service"],
  string: ["port", "pairs-limit"],
  default: { port: String(SERVER_PORT), "pairs-limit": String(TOP_PAIRS_LIMIT) },
});

const _port = Math.trunc(Number(_args.port));
if (!Number.isFinite(_port) || _port < 1 || _port > 65535) {
  console.error(`Invalid --port: ${_args.port}`);
  Deno.exit(1);
}

const _pairsLimit = Math.trunc(Number(_args["pairs-limit"]));
if (!Number.isFinite(_pairsLimit) || _pairsLimit < 1) {
  console.error(`Invalid --pairs-limit: ${_args["pairs-limit"]}`);
  Deno.exit(1);
}

const _dashboardOnly = _args["dashboard-only"] as boolean;
const _runAsService  = _args["service"] as boolean;

type Ticker24h = {
  symbol: string;
  quoteVolume: string;
};

type ScannablePair = {
  symbol: string;
  quoteVolume24h: number;
};

type ExchangeInfo = {
  symbols: Array<{
    symbol: string;
    status: string;
    quoteAsset: string;
    isSpotTradingAllowed: boolean;
  }>;
};

type Alert = {
  symbol: string;
  alert_type: "candle" | "micro";
  run_id: string;
  entry_time: string;
  volume_5m: number;
  avg_volume_1h: number;
  volume_ratio: number;
  price_change_5m: number;
  score: number;
  timestamp: string;
};

type EarlyCandidate = {
  symbol: string;
  run_id: string;
  entry_time: string;
  momentum_score: number;
  tps_now: number;
  tps_baseline: number;
  buy_ratio: number;
  cvd_rate: number;
  spread_bps: number;
  spread_compression: number;
  depth_imbalance: number;
  recent_notional: number;
  timestamp: string;
};

type Candle = {
  open: number;
  high: number;
  close: number;
  volume: number;
  closeTime: number;
};

type PendingPerformance = {
  symbol: string;
  alert_type: "candle" | "micro";
  alert_timestamp: string;
  run_id: string;
  entry_time: string;
  run_started_at: string;
  entry_price: number;
  started_at: number;
  forward_1m: number | null;
  forward_3m: number | null;
  forward_5m: number | null;
  forward_15m: number | null;
  max_price: number;
  min_price: number;
};

type PerformanceEntry = {
  symbol: string;
  alert_type: "candle" | "micro";
  alert_timestamp: string;
  run_id: string;
  entry_time: string;
  run_started_at: string;
  entry_price: number;
  forward_1m: number | null;
  forward_3m: number | null;
  forward_5m: number | null;
  forward_15m: number | null;
  max_up_15m: number | null;
  max_down_15m: number | null;
  is_false: boolean;
};

type TradeSecondStat = {
  trades: number;
  buyNotional: number;
  sellNotional: number;
};

type MomentumFeatures = {
  symbol: string;
  score: number;
  tpsNow: number;
  tpsBaseline: number;
  tpsZ: number;
  buyRatio: number;
  cvdRate: number;
  cvdZ: number;
  spreadBps: number;
  spreadCompression: number;
  depthImbalance: number;
  recentNotional: number;
};

let latestAlerts: Alert[] = [];
let latestEarlyCandidates: EarlyCandidate[] = [];
let lastScanAt: string | null = null;
let lastAggTradeAt: string | null = null;
let lastBookTickerAt: string | null = null;
let lastKlineAt: string | null = null;
let lastScannedPairs = 0;
let watchedSymbols: string[] = [];
let momentumTickRunning = false;
const startedAt = new Date().toISOString();

const cooldownUntilBySymbol = new Map<string, number>();
const lastAlertCloseTimeBySymbol = new Map<string, number>();
const triggerCooldownUntilBySymbol = new Map<string, number>();

const candleHistoryBySymbol = new Map<string, Candle[]>();
const tradeStatsBySymbol = new Map<string, Map<number, TradeSecondStat>>();
const spreadSamplesBySymbol = new Map<string, Map<number, number[]>>();
const latestSpreadBpsBySymbol = new Map<string, number>();
const depthImbalanceBySymbol = new Map<string, number>();

const earlyCandidateBySymbol = new Map<string, EarlyCandidate>();
const streamReconnectCountByType = new Map<string, number>();
const streamConnectedByType = new Map<string, number>();

const latestPriceBySymbol = new Map<string, number>();
const pendingPerformanceBySymbol = new Map<string, PendingPerformance>();
let performanceLog: PerformanceEntry[] = [];
let _savePerfTimer: number | undefined;
let invalidSymbolCount = 0;

const VALID_SYMBOLS = new Set<string>();

type RunSummary = {
  run_id: string;
  run_started_at: string | null;
  alerts: number;
  performance: number;
};

const DASHBOARD_HTML_URL = new URL("./dashboard.html", import.meta.url);
let dashboardTemplate: string | null = null;

async function serveDashboard(initialPayload: unknown): Promise<Response> {
  if (!dashboardTemplate) {
    dashboardTemplate = await Deno.readTextFile(DASHBOARD_HTML_URL);
  }
  // Escape `</` to prevent XSS if any data value contains `</script>`.
  const safeJson = JSON.stringify(initialPayload).replace(/</g, "\\u003c");
  const html = dashboardTemplate.replace("__INITIAL_DATA__", safeJson);
  return new Response(html, { headers: { "content-type": "text/html; charset=utf-8" } });
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) for ${url}`);
  }
  return (await response.json()) as T;
}

function toNumber(value: string): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function isValidSymbol(symbol: string): boolean {
  if (!SYMBOL_REGEX.test(symbol)) return false;
  return VALID_SYMBOLS.has(symbol);
}

function markInvalidSymbol(symbol: string): void {
  invalidSymbolCount += 1;
  if (invalidSymbolCount % 100 === 0) {
    console.warn(`Ignored ${invalidSymbolCount} invalid symbols (latest: ${symbol})`);
  }
}

function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, v) => sum + v, 0) / values.length;
}

function stddev(values: number[], avg?: number): number {
  if (values.length < 2) return 0;
  const m = avg ?? mean(values);
  const variance = values.reduce((sum, v) => sum + (v - m) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

async function getTopUsdtPairs(limit = TOP_PAIRS_LIMIT): Promise<ScannablePair[]> {
  const [exchangeInfo, tickers] = await Promise.all([
    fetchJson<ExchangeInfo>(`${BINANCE_BASE}/api/v3/exchangeInfo`),
    fetchJson<Ticker24h[]>(`${BINANCE_BASE}/api/v3/ticker/24hr`),
  ]);

  const validSpotSymbols = new Set(
    exchangeInfo.symbols
      .filter((s) => s.quoteAsset === "USDT" && s.status === "TRADING" && s.isSpotTradingAllowed)
      .map((s) => s.symbol),
  );
  VALID_SYMBOLS.clear();
  for (const sym of validSpotSymbols) VALID_SYMBOLS.add(sym);

  return tickers
    .filter((t) => validSpotSymbols.has(t.symbol))
    .map((t) => ({ symbol: t.symbol, quoteVolume24h: toNumber(t.quoteVolume) }))
    .filter((t) => t.quoteVolume24h > MIN_24H_QUOTE_VOLUME_USDT)
    .sort((a, b) => b.quoteVolume24h - a.quoteVolume24h)
    .slice(0, limit)
    .map((t) => ({ symbol: t.symbol, quoteVolume24h: t.quoteVolume24h }));
}

let _saveHistoryTimer: number | undefined;

function scheduleHistorySave(): void {
  clearTimeout(_saveHistoryTimer);
  _saveHistoryTimer = setTimeout(async () => {
    try {
      await Deno.writeTextFile(HISTORY_FILE, JSON.stringify(latestAlerts));
    } catch (err) {
      console.warn("Failed to save alert history:", err instanceof Error ? err.message : String(err));
    }
  }, HISTORY_SAVE_DEBOUNCE_MS);
}

async function loadAlertHistory(): Promise<void> {
  try {
    const raw = await Deno.readTextFile(HISTORY_FILE);
    const parsed: unknown = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      latestAlerts = (parsed as Alert[]).slice(0, MAX_PERSISTED_ALERTS);
      console.log(`Loaded ${latestAlerts.length} alerts from history.`);
    }
  } catch {
    // No history file yet or unreadable — start fresh.
  }
}

function schedulePerformanceSave(): void {
  clearTimeout(_savePerfTimer);
  _savePerfTimer = setTimeout(async () => {
    try {
      await Deno.writeTextFile(PERF_LOG_FILE, JSON.stringify(performanceLog));
    } catch (err) {
      console.warn("Failed to save performance log:", err instanceof Error ? err.message : String(err));
    }
  }, 5_000);
}

async function loadPerformanceLog(): Promise<void> {
  try {
    const raw = await Deno.readTextFile(PERF_LOG_FILE);
    const parsed: unknown = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      performanceLog = (parsed as PerformanceEntry[]).slice(0, MAX_PERF_ENTRIES);
      console.log(`Loaded ${performanceLog.length} performance entries.`);
    }
  } catch { /* no file yet */ }
}

function initPerformanceTracking(alert: Alert): void {
  const entryPrice = latestPriceBySymbol.get(alert.symbol);
  if (!entryPrice || entryPrice <= 0) return;
  pendingPerformanceBySymbol.set(alert.symbol, {
    symbol: alert.symbol,
    alert_type: alert.alert_type,
    alert_timestamp: alert.timestamp,
    run_id: alert.run_id,
    entry_time: alert.entry_time,
    run_started_at: RUN_STARTED_AT,
    entry_price: entryPrice,
    started_at: Date.now(),
    forward_1m: null,
    forward_3m: null,
    forward_5m: null,
    forward_15m: null,
    max_price: entryPrice,
    min_price: entryPrice,
  });
}

function updatePendingPerformance(symbol: string, price: number, now: number): void {
  const p = pendingPerformanceBySymbol.get(symbol);
  if (!p) return;

  if (price > p.max_price) p.max_price = price;
  if (price < p.min_price) p.min_price = price;

  const elapsed = now - p.started_at;
  const fwd = (price - p.entry_price) / p.entry_price * 100;

  if (p.forward_1m  === null && elapsed >=     60_000) p.forward_1m  = fwd;
  if (p.forward_3m  === null && elapsed >=  3 * 60_000) p.forward_3m  = fwd;
  if (p.forward_5m  === null && elapsed >=  5 * 60_000) p.forward_5m  = fwd;
  if (p.forward_15m === null && elapsed >= 15 * 60_000) {
    p.forward_15m = fwd;
    performanceLog = [{
      symbol: p.symbol,
      alert_type: p.alert_type,
      alert_timestamp: p.alert_timestamp,
      run_id: p.run_id,
      entry_time: p.entry_time,
      run_started_at: p.run_started_at,
      entry_price: p.entry_price,
      forward_1m:   p.forward_1m,
      forward_3m:   p.forward_3m,
      forward_5m:   p.forward_5m,
      forward_15m:  p.forward_15m,
      max_up_15m:   (p.max_price - p.entry_price) / p.entry_price * 100,
      max_down_15m: (p.min_price - p.entry_price) / p.entry_price * 100,
      is_false:     ((p.max_price - p.entry_price) / p.entry_price * 100) < 0.3,
    }, ...performanceLog].slice(0, MAX_PERF_ENTRIES);
    pendingPerformanceBySymbol.delete(symbol);
    schedulePerformanceSave();
  }
}

function pushAlert(alert: Alert): void {
  latestAlerts = [alert, ...latestAlerts]
    .sort((a, b) => Date.parse(b.timestamp) - Date.parse(a.timestamp))
    .slice(0, MAX_PERSISTED_ALERTS);
  scheduleHistorySave();
  initPerformanceTracking(alert);
}

function pushEarlyCandidate(candidate: EarlyCandidate): void {
  earlyCandidateBySymbol.set(candidate.symbol, candidate);
  latestEarlyCandidates = Array.from(earlyCandidateBySymbol.values())
    .sort((a, b) => b.momentum_score - a.momentum_score)
    .slice(0, MAX_PERSISTED_ALERTS);
}

function upsertClosedCandle(symbol: string, candle: Candle): void {
  const history = candleHistoryBySymbol.get(symbol) ?? [];
  const existingIndex = history.findIndex((c) => c.closeTime === candle.closeTime);
  if (existingIndex >= 0) {
    history[existingIndex] = candle;
  } else {
    history.push(candle);
  }

  history.sort((a, b) => a.closeTime - b.closeTime);
  candleHistoryBySymbol.set(symbol, history.slice(-KLINES_REQUIRED));
}

function scanSymbolFromHistory(symbol: string): Alert | null {
  const candles = candleHistoryBySymbol.get(symbol);
  if (!candles || candles.length < KLINES_REQUIRED) return null;

  const now = Date.now();
  const cooldownUntil = cooldownUntilBySymbol.get(symbol);
  if (cooldownUntil && now < cooldownUntil) return null;

  const latest = candles[candles.length - 1];
  const previous12 = candles.slice(0, KLINES_REQUIRED - 1);

  const open = latest.open;
  const close = latest.close;
  const volume5m = latest.volume;

  if (open <= 0 || volume5m <= 0) return null;

  const avgVolume1h = previous12.reduce((sum, c) => sum + c.volume, 0) / previous12.length;
  if (avgVolume1h < MIN_AVG_VOLUME_1H) return null;

  const volumeRatio = volume5m / avgVolume1h;
  const priceChange5m = ((close - open) / open) * 100;
  const high1h = Math.max(...previous12.map((c) => c.high));
  const isBreakout = close > high1h;

  const isAbnormal =
    volume5m > ABNORMAL_VOLUME_MULTIPLIER * avgVolume1h &&
    priceChange5m > ABNORMAL_PRICE_CHANGE_PERCENT;

  if (REQUIRE_BREAKOUT_FILTER && !isBreakout) return null;
  if (!isAbnormal) return null;

  const lastAlertCloseTime = lastAlertCloseTimeBySymbol.get(symbol);
  if (lastAlertCloseTime === latest.closeTime) return null;

  // Normalized 0-100 score: volume (log2 scale, up to 65) + price (linear, up to 35).
  // At minimum trigger (3× vol + 1.5% chg) ≈ 41. At 10× vol + 5% chg ≈ 95.
  const volScore = clamp(Math.log2(Math.max(1, volumeRatio)) * 20, 0, 65);
  const priceScore = clamp(priceChange5m * 6, 0, 35);
  const score = volScore + priceScore;

  cooldownUntilBySymbol.set(symbol, now + ALERT_COOLDOWN_MS);
  lastAlertCloseTimeBySymbol.set(symbol, latest.closeTime);

  return {
    symbol,
    alert_type: "candle",
    run_id: RUN_ID,
    entry_time: new Date(latest.closeTime).toISOString(),
    volume_5m: volume5m,
    avg_volume_1h: avgVolume1h,
    volume_ratio: volumeRatio,
    price_change_5m: priceChange5m,
    score,
    timestamp: new Date(latest.closeTime).toISOString(),
  };
}

async function runWithConcurrency<T>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<void>,
): Promise<void> {
  let nextIndex = 0;

  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const currentIndex = nextIndex++;
      if (currentIndex >= items.length) break;
      try {
        await fn(items[currentIndex]);
      } catch (err) {
        console.warn(`[bootstrap] ${String((items[currentIndex] as { symbol?: unknown }).symbol ?? "?")} failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  });

  await Promise.allSettled(workers);
}

async function bootstrapCandleHistory(pairs: ScannablePair[]): Promise<void> {
  await runWithConcurrency(pairs, CONCURRENCY_LIMIT, async (pair) => {
    const klines = await fetchJson<string[][]>(
      `${BINANCE_BASE}/api/v3/klines?symbol=${pair.symbol}&interval=${KLINE_INTERVAL}&limit=${KLINES_REQUIRED}`,
    );

    if (klines.length < KLINES_REQUIRED) return;

    for (const kline of klines) {
      upsertClosedCandle(pair.symbol, {
        open: toNumber(kline[1]),
        high: toNumber(kline[2]),
        close: toNumber(kline[4]),
        volume: toNumber(kline[5]),
        closeTime: toNumber(kline[6]),
      });
    }
  });
}

function streamChunks(symbols: string[], chunkSize: number): string[][] {
  const chunks: string[][] = [];
  for (let i = 0; i < symbols.length; i += chunkSize) {
    chunks.push(symbols.slice(i, i + chunkSize));
  }
  return chunks;
}

function parseCombinedData(rawData: string): Record<string, unknown> | null {
  let payload: unknown;
  try {
    payload = JSON.parse(rawData);
  } catch {
    return null;
  }

  if (!payload || typeof payload !== "object") return null;
  const data = (payload as { data?: unknown }).data;
  if (!data || typeof data !== "object") return null;
  return data as Record<string, unknown>;
}

function ensureTradeSecondStat(symbol: string, second: number): TradeSecondStat {
  const secondMap = tradeStatsBySymbol.get(symbol) ?? new Map<number, TradeSecondStat>();
  tradeStatsBySymbol.set(symbol, secondMap);

  const existing = secondMap.get(second);
  if (existing) return existing;

  const created: TradeSecondStat = { trades: 0, buyNotional: 0, sellNotional: 0 };
  secondMap.set(second, created);
  return created;
}

function pruneTradeStats(symbol: string, minSecond: number): void {
  const secondMap = tradeStatsBySymbol.get(symbol);
  if (!secondMap) return;
  for (const second of secondMap.keys()) {
    if (second < minSecond) {
      secondMap.delete(second);
    }
  }
}

function pruneSpreadSamples(symbol: string, minSecond: number): void {
  const secondMap = spreadSamplesBySymbol.get(symbol);
  if (!secondMap) return;
  for (const second of secondMap.keys()) {
    if (second < minSecond) {
      secondMap.delete(second);
    }
  }
}

function handleKlineMessage(rawData: string): void {
  const data = parseCombinedData(rawData);
  if (!data) return;

  const symbol = data.s;
  const kline = data.k;
  if (typeof symbol !== "string" || !kline || typeof kline !== "object") return;
  if (!isValidSymbol(symbol)) { markInvalidSymbol(symbol); return; }

  const isClosed = (kline as { x?: unknown }).x;
  if (isClosed !== true) return;

  const candle: Candle = {
    open: toNumber(String((kline as { o?: unknown }).o ?? "0")),
    high: toNumber(String((kline as { h?: unknown }).h ?? "0")),
    close: toNumber(String((kline as { c?: unknown }).c ?? "0")),
    volume: toNumber(String((kline as { v?: unknown }).v ?? "0")),
    closeTime: Number((kline as { T?: unknown }).T ?? 0),
  };

  if (!Number.isFinite(candle.closeTime) || candle.closeTime <= 0) return;

  upsertClosedCandle(symbol, candle);
  const candleAlert = scanSymbolFromHistory(symbol);
  if (candleAlert) {
    pushAlert(candleAlert);
    console.log(`[${candleAlert.timestamp}] Candle alert: ${candleAlert.symbol}`);
  }

  lastKlineAt = new Date().toISOString();
  lastScanAt = new Date().toISOString();
}

function handleAggTradeMessage(rawData: string): void {
  const data = parseCombinedData(rawData);
  if (!data) return;

  const symbol = data.s;
  const price = data.p;
  const qty = data.q;
  const isBuyerMarketMaker = data.m;
  const tradeTime = data.T;

  if (
    typeof symbol !== "string" ||
    typeof price !== "string" ||
    typeof qty !== "string" ||
    typeof isBuyerMarketMaker !== "boolean"
  ) {
    return;
  }
  if (!isValidSymbol(symbol)) { markInvalidSymbol(symbol); return; }

  const priceNum = toNumber(price);
  const notional = priceNum * toNumber(qty);
  if (!Number.isFinite(notional) || notional <= 0) return;

  if (priceNum > 0) {
    latestPriceBySymbol.set(symbol, priceNum);
    updatePendingPerformance(symbol, priceNum, Date.now());
  }

  const second = Math.floor((typeof tradeTime === "number" ? tradeTime : Date.now()) / 1000);
  const stat = ensureTradeSecondStat(symbol, second);
  stat.trades += 1;

  if (isBuyerMarketMaker) {
    stat.sellNotional += notional;
  } else {
    stat.buyNotional += notional;
  }

  pruneTradeStats(symbol, second - (BASELINE_WINDOW_SEC + 10));
  lastAggTradeAt = new Date().toISOString();
  lastScanAt = new Date().toISOString();
}

function handleBookTickerMessage(rawData: string): void {
  const data = parseCombinedData(rawData);
  if (!data) return;

  const symbol = data.s;
  const bid = data.b;
  const ask = data.a;

  if (typeof symbol !== "string" || typeof bid !== "string" || typeof ask !== "string") return;
  if (!isValidSymbol(symbol)) { markInvalidSymbol(symbol); return; }

  const bidNum = toNumber(bid);
  const askNum = toNumber(ask);
  if (bidNum <= 0 || askNum <= bidNum) return;

  const mid = (bidNum + askNum) / 2;
  const spreadBps = ((askNum - bidNum) / mid) * 10_000;
  if (!Number.isFinite(spreadBps) || spreadBps <= 0) return;

  latestSpreadBpsBySymbol.set(symbol, spreadBps);

  const second = Math.floor(Date.now() / 1000);
  const secondMap = spreadSamplesBySymbol.get(symbol) ?? new Map<number, number[]>();
  spreadSamplesBySymbol.set(symbol, secondMap);
  const samples = secondMap.get(second) ?? [];
  samples.push(spreadBps);
  secondMap.set(second, samples);

  pruneSpreadSamples(symbol, second - (BASELINE_WINDOW_SEC + 10));
  lastBookTickerAt = new Date().toISOString();
}

function handleDepthMessage(rawData: string): void {
  if (!ENABLE_DEPTH_IMBALANCE) return;

  const data = parseCombinedData(rawData);
  if (!data) return;

  const symbol = data.s;
  const bids = data.b;
  const asks = data.a;

  if (!Array.isArray(bids) || !Array.isArray(asks) || typeof symbol !== "string") return;
  if (!isValidSymbol(symbol)) { markInvalidSymbol(symbol); return; }

  const topBids = bids.slice(0, DEPTH_LEVELS_TO_SUM) as string[][];
  const topAsks = asks.slice(0, DEPTH_LEVELS_TO_SUM) as string[][];

  const bidNotional = topBids.reduce((sum, level) => {
    const p = toNumber(String(level?.[0] ?? "0"));
    const q = toNumber(String(level?.[1] ?? "0"));
    return sum + p * q;
  }, 0);

  const askNotional = topAsks.reduce((sum, level) => {
    const p = toNumber(String(level?.[0] ?? "0"));
    const q = toNumber(String(level?.[1] ?? "0"));
    return sum + p * q;
  }, 0);

  const total = bidNotional + askNotional;
  if (total <= 0) return;

  const imbalance = (bidNotional - askNotional) / total;
  depthImbalanceBySymbol.set(symbol, imbalance);
}

function secRangeValues<T>(
  secondMap: Map<number, T>,
  startSec: number,
  endSec: number,
  mapFn: (value: T | undefined) => number,
): number[] {
  const out: number[] = [];
  for (let second = startSec; second <= endSec; second++) {
    out.push(mapFn(secondMap.get(second)));
  }
  return out;
}

function getMomentumFeatures(symbol: string, nowSec: number): MomentumFeatures | null {
  const tradeMap = tradeStatsBySymbol.get(symbol);
  if (!tradeMap) return null;

  const recentStart = nowSec - RECENT_WINDOW_SEC + 1;
  const recentEnd = nowSec;
  const baselineStart = nowSec - BASELINE_WINDOW_SEC;
  const baselineEnd = nowSec - RECENT_WINDOW_SEC;
  if (baselineEnd <= baselineStart) return null;

  let recentTrades = 0;
  let recentBuy = 0;
  let recentSell = 0;

  for (let second = recentStart; second <= recentEnd; second++) {
    const stat = tradeMap.get(second);
    if (!stat) continue;
    recentTrades += stat.trades;
    recentBuy += stat.buyNotional;
    recentSell += stat.sellNotional;
  }

  const recentNotional = recentBuy + recentSell;
  if (recentNotional < MICRO_MIN_NOTIONAL_10S) return null;

  const baselineTradePerSec = secRangeValues(tradeMap, baselineStart, baselineEnd, (stat) =>
    stat ? stat.trades : 0,
  );

  const baselineCvdPerSec = secRangeValues(tradeMap, baselineStart, baselineEnd, (stat) =>
    stat ? stat.buyNotional - stat.sellNotional : 0,
  );

  const tpsNow = recentTrades / RECENT_WINDOW_SEC;
  const tpsBaseline = mean(baselineTradePerSec);
  const tpsStd = stddev(baselineTradePerSec, tpsBaseline);
  const tpsZ = tpsStd > 0 ? (tpsNow - tpsBaseline) / tpsStd : tpsNow - tpsBaseline;

  const cvdRate = (recentBuy - recentSell) / RECENT_WINDOW_SEC;
  const cvdBaseline = mean(baselineCvdPerSec);
  const cvdStd = stddev(baselineCvdPerSec, cvdBaseline);
  const cvdZ = cvdStd > 0 ? (cvdRate - cvdBaseline) / cvdStd : cvdRate - cvdBaseline;

  const buyRatio = recentNotional > 0 ? recentBuy / recentNotional : 0;

  const spreadNow = latestSpreadBpsBySymbol.get(symbol) ?? 0;
  const spreadMap = spreadSamplesBySymbol.get(symbol) ?? new Map<number, number[]>();
  const baselineSpreadValues = secRangeValues(spreadMap, baselineStart, baselineEnd, (samples) => {
    if (!samples || samples.length === 0) return 0;
    return mean(samples);
  }).filter((v) => v > 0);

  const spreadBaseline = mean(baselineSpreadValues);
  const spreadCompression =
    spreadNow > 0 && spreadBaseline > 0 ? (spreadBaseline - spreadNow) / spreadBaseline : 0;

  const depthImbalance = ENABLE_DEPTH_IMBALANCE ? (depthImbalanceBySymbol.get(symbol) ?? 0) : 0;

  let score = 0;
  score += clamp(tpsZ, 0, 6) * 18;
  score += clamp(cvdZ, 0, 6) * 16;
  score += clamp((buyRatio - 0.5) * 120, 0, 24);
  score += clamp(spreadCompression * 30, 0, 20);
  score += clamp(Math.log10(recentNotional / MICRO_MIN_NOTIONAL_10S + 1) * 10, 0, 16);
  if (ENABLE_DEPTH_IMBALANCE) {
    score += clamp(depthImbalance * 30, 0, 20);
  }

  return {
    symbol,
    score,
    tpsNow,
    tpsBaseline,
    tpsZ,
    buyRatio,
    cvdRate,
    cvdZ,
    spreadBps: spreadNow,
    spreadCompression,
    depthImbalance,
    recentNotional,
  };
}

async function confirmNotBlownTop(symbol: string): Promise<boolean> {
  const klines = await fetchJson<string[][]>(
    `${BINANCE_BASE}/api/v3/klines?symbol=${symbol}&interval=1m&limit=15`,
  );

  if (klines.length < 5) return false;

  const firstOpen = toNumber(klines[0][1]);
  const latestClose = toNumber(klines[klines.length - 1][4]);
  if (firstOpen <= 0 || latestClose <= 0) return false;

  const lookbackChange = ((latestClose - firstOpen) / firstOpen) * 100;
  return lookbackChange < MAX_CONFIRM_LOOKBACK_PUMP_PERCENT;
}

async function maybeTriggerConfirmedAlert(features: MomentumFeatures): Promise<void> {
  const now = Date.now();
  const triggerCooldownUntil = triggerCooldownUntilBySymbol.get(features.symbol) ?? 0;
  if (now < triggerCooldownUntil) return;

  if (features.score < MICRO_TRIGGER_SCORE) return;

  triggerCooldownUntilBySymbol.set(features.symbol, now + MICRO_TRIGGER_COOLDOWN_MS);

  let confirmed = false;
  try {
    confirmed = await confirmNotBlownTop(features.symbol);
  } catch {
    confirmed = false;
  }

  if (!confirmed) return;

  const candleBased = scanSymbolFromHistory(features.symbol);
  if (candleBased) {
    pushAlert(candleBased);
    console.log(`[${candleBased.timestamp}] Triggered + candle confirm: ${features.symbol}`);
    return;
  }

  const candles = candleHistoryBySymbol.get(features.symbol) ?? [];
  const latest = candles[candles.length - 1];
  const previous = candles.slice(0, Math.max(0, candles.length - 1));
  const fallbackVolume5m = latest?.volume ?? 0;
  const fallbackAvg1h =
    previous.length > 0 ? previous.reduce((sum, c) => sum + c.volume, 0) / previous.length : 0;
  const fallbackPriceChange =
    latest && latest.open > 0 ? ((latest.close - latest.open) / latest.open) * 100 : 0;
  const fallbackVolumeRatio =
    fallbackAvg1h > 0 ? fallbackVolume5m / fallbackAvg1h : 0;

  const fallbackAlert: Alert = {
    symbol: features.symbol,
    alert_type: "micro",
    run_id: RUN_ID,
    entry_time: new Date().toISOString(),
    volume_5m: fallbackVolume5m,
    avg_volume_1h: fallbackAvg1h,
    volume_ratio: fallbackVolumeRatio > 0
      ? fallbackVolumeRatio
      : Math.max(0, features.tpsBaseline > 0 ? features.tpsNow / features.tpsBaseline : 0),
    price_change_5m: fallbackPriceChange,
    score: features.score,
    timestamp: new Date().toISOString(),
  };

  pushAlert(fallbackAlert);
  cooldownUntilBySymbol.set(features.symbol, now + ALERT_COOLDOWN_MS);
  console.log(`[${fallbackAlert.timestamp}] Triggered micro alert: ${features.symbol}`);
}

async function runMomentumTick(): Promise<void> {
  if (momentumTickRunning) return;
  momentumTickRunning = true;

  try {
    const nowSec = Math.floor(Date.now() / 1000);

    // Prune stale early candidates to prevent unbounded map growth.
    const nowMs = Date.now();
    for (const [sym, candidate] of earlyCandidateBySymbol) {
      if (nowMs - Date.parse(candidate.timestamp) > EARLY_CANDIDATE_TTL_MS) {
        earlyCandidateBySymbol.delete(sym);
      }
    }

    for (const symbol of watchedSymbols) {
      const features = getMomentumFeatures(symbol, nowSec);
      if (!features) continue;

      if (features.score >= MICRO_CANDIDATE_SCORE) {
        pushEarlyCandidate({
          symbol: features.symbol,
          run_id: RUN_ID,
          entry_time: new Date().toISOString(),
          momentum_score: features.score,
          tps_now: features.tpsNow,
          tps_baseline: features.tpsBaseline,
          buy_ratio: features.buyRatio,
          cvd_rate: features.cvdRate,
          spread_bps: features.spreadBps,
          spread_compression: features.spreadCompression,
          depth_imbalance: features.depthImbalance,
          recent_notional: features.recentNotional,
          timestamp: new Date().toISOString(),
        });
      }

      await maybeTriggerConfirmedAlert(features);
    }

    lastScanAt = new Date().toISOString();
  } finally {
    momentumTickRunning = false;
  }
}

function startChunkedStream(
  symbols: string[],
  streamSuffix: string,
  streamType: string,
  chunkLabel: string,
  onData: (rawData: string) => void,
): void {
  const chunks = streamChunks(symbols, STREAM_CHUNK_SIZE);

  chunks.forEach((chunk, chunkIndex) => {
    const streamPath = chunk.map((s) => `${s.toLowerCase()}@${streamSuffix}`).join("/");
    const wsUrl = `wss://stream.binance.com:9443/stream?streams=${streamPath}`;

    const connect = () => {
      const ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        streamConnectedByType.set(streamType, (streamConnectedByType.get(streamType) ?? 0) + 1);
        console.log(`${chunkLabel} chunk ${chunkIndex + 1} connected (${chunk.length} symbols)`);
      };

      ws.onmessage = (event) => {
        if (typeof event.data === "string") {
          onData(event.data);
        }
      };

      ws.onerror = () => {
        ws.close();
      };

      ws.onclose = () => {
        streamConnectedByType.set(streamType, Math.max(0, (streamConnectedByType.get(streamType) ?? 0) - 1));
        streamReconnectCountByType.set(streamType, (streamReconnectCountByType.get(streamType) ?? 0) + 1);
        console.log(`${chunkLabel} chunk ${chunkIndex + 1} closed. Reconnecting...`);
        setTimeout(connect, STREAM_RECONNECT_DELAY_MS);
      };
    };

    connect();
  });
}

function startDepthStreams(symbols: string[]): void {
  if (!ENABLE_DEPTH_IMBALANCE) return;
  startChunkedStream(symbols, `depth${DEPTH_LEVELS_TO_SUM}@100ms`, "depth", "Depth", handleDepthMessage);
}

async function startStreamingScanner(pairsLimit: number): Promise<void> {
  await loadAlertHistory();
  await loadPerformanceLog();
  const pairs = await getTopUsdtPairs(pairsLimit);
  watchedSymbols = pairs.map((p) => p.symbol);
  lastScannedPairs = watchedSymbols.length;

  console.log(`Bootstrapping ${pairs.length} symbols with ${KLINES_REQUIRED} x 5m candles...`);
  await bootstrapCandleHistory(pairs);

  startChunkedStream(watchedSymbols, `kline_${KLINE_INTERVAL}`, "kline", "Kline", handleKlineMessage);
  startChunkedStream(watchedSymbols, "aggTrade", "aggTrade", "AggTrade", handleAggTradeMessage);
  startChunkedStream(watchedSymbols, "bookTicker", "bookTicker", "BookTicker", handleBookTickerMessage);
  startDepthStreams(watchedSymbols);

  setInterval(() => {
    void runMomentumTick();
  }, SCORE_INTERVAL_MS);

  lastScanAt = new Date().toISOString();
  console.log(`Micro-sieve started for ${watchedSymbols.length} symbols.`);
}

function buildPerformancePayload() {
  const summary = (["candle", "micro"] as const).map((type) => {
    const entries = performanceLog.filter((e) => e.alert_type === type && e.forward_15m !== null);
    const count = entries.length;
    const avg = (key: keyof PerformanceEntry) =>
      count > 0 ? entries.reduce((s, e) => s + (Number(e[key]) || 0), 0) / count : null;
    const falseRate = count > 0 ? entries.filter((e) => e.is_false).length / count : null;
    return {
      alert_type: type,
      count,
      false_rate: falseRate,
      hit_rate_15m: count > 0 ? entries.filter((e) => (e.forward_15m ?? 0) > 0).length / count : null,
      avg_forward_1m:   avg("forward_1m"),
      avg_forward_3m:   avg("forward_3m"),
      avg_forward_5m:   avg("forward_5m"),
      avg_forward_15m:  avg("forward_15m"),
      avg_max_up_15m:   avg("max_up_15m"),
      avg_max_down_15m: avg("max_down_15m"),
    };
  });
  return {
    run_id: RUN_ID,
    run_started_at: RUN_STARTED_AT,
    summary,
    pending_count: pendingPerformanceBySymbol.size,
    pending: [...pendingPerformanceBySymbol.values()].map((p) => ({
      symbol: p.symbol,
      alert_type: p.alert_type,
      run_id: p.run_id,
      entry_time: p.entry_time,
      run_started_at: p.run_started_at,
      entry_price: p.entry_price,
      elapsed_s: Math.round((Date.now() - p.started_at) / 1000),
      forward_1m: p.forward_1m,
      forward_3m: p.forward_3m,
      forward_5m: p.forward_5m,
    })),
    recent: performanceLog.slice(0, 100),
  };
}

function buildRunsPayload(): RunSummary[] {
  const runs = new Map<string, RunSummary>();

  const upsert = (run_id: string, started_at: string | null, incAlerts: boolean, incPerf: boolean) => {
    const existing = runs.get(run_id) ?? { run_id, run_started_at: started_at, alerts: 0, performance: 0 };
    if (!existing.run_started_at && started_at) existing.run_started_at = started_at;
    if (incAlerts) existing.alerts += 1;
    if (incPerf) existing.performance += 1;
    runs.set(run_id, existing);
  };

  for (const a of latestAlerts) {
    if (a.run_id) upsert(a.run_id, a.entry_time ?? null, true, false);
  }
  for (const p of performanceLog) {
    if (p.run_id) upsert(p.run_id, p.run_started_at ?? p.entry_time ?? null, false, true);
  }

  // Include current run even if empty
  upsert(RUN_ID, RUN_STARTED_AT, false, false);

  return [...runs.values()].sort((a, b) => {
    const ta = a.run_started_at ? Date.parse(a.run_started_at) : 0;
    const tb = b.run_started_at ? Date.parse(b.run_started_at) : 0;
    return tb - ta;
  });
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

async function openDashboardInBrowser(url: string): Promise<void> {
  const candidates: Array<{ cmd: string; args: string[] }> = [
    { cmd: "cmd.exe", args: ["/c", "start", "", url] },
    { cmd: "powershell.exe", args: ["-NoProfile", "-Command", `Start-Process '${url}'`] },
    { cmd: "xdg-open", args: [url] },
    { cmd: "open", args: [url] },
  ];

  for (const candidate of candidates) {
    try {
      const process = new Deno.Command(candidate.cmd, {
        args: candidate.args,
        stdin: "null",
        stdout: "null",
        stderr: "null",
      }).spawn();
      const status = await process.status;
      if (status.success) {
        console.log(`Opened dashboard in browser: ${url}`);
        return;
      }
    } catch {
      // Try next launcher.
    }
  }

  console.log(`Could not auto-open browser. Open manually: ${url}`);
}

Deno.serve({ port: _port }, (req) => {
  const url = new URL(req.url);

  if (req.method === "GET" && url.pathname === DASHBOARD_PATH) {
    const initialPayload = {
      scanned_pairs: lastScannedPairs,
      last_scan_at: lastScanAt,
      run_id: RUN_ID,
      run_started_at: RUN_STARTED_AT,
      runs: buildRunsPayload(),
      alerts_count: latestAlerts.length,
      candidates_count: latestEarlyCandidates.length,
      alerts: latestAlerts,
      early_candidates: latestEarlyCandidates,
    };
    return serveDashboard(initialPayload);
  }

  if (req.method === "GET" && url.pathname === "/health") {
    const nowMs = Date.now();
    const startMs = new Date(startedAt).getTime();
    const streamTypes = ["kline", "aggTrade", "bookTicker", "depth"];
    return jsonResponse({
      status: "ok",
      started_at: startedAt,
      uptime_s: Math.floor((nowMs - startMs) / 1000),
      watched_symbols: watchedSymbols.length,
      last_events: {
        kline: lastKlineAt,
        aggtrade: lastAggTradeAt,
        bookticker: lastBookTickerAt,
        scan: lastScanAt,
      },
      streams: Object.fromEntries(
        streamTypes.map((t) => [
          t,
          {
            connected: streamConnectedByType.get(t) ?? 0,
            reconnects: streamReconnectCountByType.get(t) ?? 0,
          },
        ]),
      ),
      alerts_count: latestAlerts.length,
      candidates_count: latestEarlyCandidates.length,
    });
  }

  if (req.method === "GET" && url.pathname === "/alerts") {
    return jsonResponse({
      scanned_pairs: lastScannedPairs,
      last_scan_at: lastScanAt,
      run_id: RUN_ID,
      run_started_at: RUN_STARTED_AT,
      runs: buildRunsPayload(),
      alerts_count: latestAlerts.length,
      candidates_count: latestEarlyCandidates.length,
      alerts: latestAlerts,
      early_candidates: latestEarlyCandidates,
      performance: buildPerformancePayload(),
    });
  }

  if (req.method === "GET" && url.pathname === "/runs") {
    return jsonResponse({
      run_id: RUN_ID,
      run_started_at: RUN_STARTED_AT,
      runs: buildRunsPayload(),
    });
  }

  if (req.method === "GET" && url.pathname === "/performance") {
    return jsonResponse(buildPerformancePayload());
  }

  if (req.method === "GET" && url.pathname === "/") {
    return Response.redirect(new URL(DASHBOARD_PATH, req.url), 302);
  }

  return jsonResponse({ error: "Not found" }, 404);
});

async function appMain(): Promise<void> {
  console.log(`Scanner API listening on http://localhost:${_port}`);
  if (!isRunningAsService) {
    void openDashboardInBrowser(`http://localhost:${_port}${DASHBOARD_PATH}`);
  }
  if (_dashboardOnly) {
    console.log("Dashboard-only mode: live scanning disabled.");
  } else {
    await startStreamingScanner(_pairsLimit);
  }
}

if (_runAsService) {
  await runAsWindowsService(appMain);
} else {
  try {
    await appMain();
  } catch (error) {
    console.error("Scanner failed to start:", error);
  }
}

/*
Run (Windows):
  deno run --allow-net --allow-run=cmd.exe,powershell.exe --allow-read --allow-write=alerts_history.json scanner.ts
  deno run --allow-net --allow-run=cmd.exe,powershell.exe --allow-read --allow-write=alerts_history.json scanner.ts --port 9000 --pairs-limit 50

Run (Linux/macOS):
  deno run --allow-net --allow-run=xdg-open --allow-read --allow-write=alerts_history.json scanner.ts
  deno run --allow-net --allow-run=open     --allow-read --allow-write=alerts_history.json scanner.ts

Service commands (requires admin/root + --allow-ffi on Windows):
  scanner install / uninstall / start / stop
*/
