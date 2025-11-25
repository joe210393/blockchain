const express = require('express');
const path = require('path');
const axios = require('axios');
const mysql = require('mysql2/promise');
const cron = require('node-cron');
const rateLimit = require('express-rate-limit');
const compression = require('compression');
const cors = require('cors');
const morgan = require('morgan');
require('dotenv').config();

// Remote MySQL defaults (Zeabur). Override via MYSQL_* env vars if needed.
const DEFAULT_DB = {
  host: '150.109.72.98',
  port: 30170,
  user: 'root',
  password: '9Ff2TP786Wx4gIy0Q5GSEc1bBa3mRrw',
  database: 'blackchain'
};

function isoDateUTC(ts) { return new Date(ts).toISOString().slice(0,10); }
function isEthAddress(a){ return /^0x[0-9a-fA-F]{40}$/.test(a); }

// Binance symbol map (only where pairs exist). CRO often not on Binance; fallback to CoinGecko for CRO.
const BINANCE_SYMBOL = {
  BTC: 'BTCUSDT',
  ETH: 'ETHUSDT',
  ADA: 'ADAUSDT',
  PEPE: 'PEPEUSDT',
  LUNC: 'LUNCUSDT',
  TRX: 'TRXUSDT'
};

// Token map for ETH chain (MVP)
const ETH_TOKEN_MAP = {
  PEPE: '0x6982508145454Ce325dDbE47a25d4ec3d2311933'.toLowerCase()
};

// CronosScan API key (optional)
const CRONOSCAN_KEY = process.env.CRONOSCAN_KEY || '';
const BLOCKFROST_KEY = process.env.BLOCKFROST_KEY || '';

const app = express();
// Behind reverse proxies (e.g., Zeabur/NGINX), trust X-Forwarded-* headers
app.set('trust proxy', 1);
app.use(express.json());
app.use(cors());
app.use(compression());
app.use(morgan('dev'));
app.use(express.static(path.join(__dirname, 'public')));

// Simple in-memory cache
const memoryCache = {
  fng: { ts: 0, data: null }
};

// Whitelist and mapping
const COIN_MAP = [
  { symbol: 'BTC', coingecko_id: 'bitcoin' },
  { symbol: 'ETH', coingecko_id: 'ethereum' },
  { symbol: 'ADA', coingecko_id: 'cardano' },
  { symbol: 'CRO', coingecko_id: 'cronos' },
  { symbol: 'PEPE', coingecko_id: 'pepe' },
  { symbol: 'LUNC', coingecko_id: 'terra-luna' },
  { symbol: 'TRX', coingecko_id: 'tron' },
  { symbol: 'SNEK', coingecko_id: 'snek' }
];
// Allow LUNAC alias at API level, but store as LUNC in DB
const API_SYMBOLS = new Set([...COIN_MAP.map(c => c.symbol), 'LUNAC', 'TRON']);
// Canonical symbols used for data bootstrapping and cron jobs (exclude aliases like LUNAC)
const CANONICAL_SYMBOLS = new Set(COIN_MAP.map(c => c.symbol));
const SYMBOL_TO_ID = COIN_MAP.reduce((acc, c) => { acc[c.symbol] = c.coingecko_id; return acc; }, {});
function candidateIdsForSymbol(symbol) {
  if (symbol === 'CRO') return ['cronos', 'crypto-com-chain'];
  return [SYMBOL_TO_ID[symbol]];
}

// MySQL pool
let pool;
async function getPool() {
  if (!pool) {
    const socketPath = process.env.MYSQL_SOCKET;
    const urlEnv = process.env.MYSQL_URL || process.env.DATABASE_URL || process.env.ZEABUR_DATABASE_URL || '';
    let resolved = { source: 'env-fields' };
    if (urlEnv) {
      try {
        const u = new URL(urlEnv);
        // e.g., mysql://user:pass@host:port/db
        resolved = {
          source: 'env-url',
          user: decodeURIComponent(u.username || ''), 
          password: decodeURIComponent(u.password || ''),
          host: u.hostname || 'localhost',
          port: Number(u.port || 3306),
          database: (u.pathname || '').replace(/^\//, '') || process.env.MYSQL_DATABASE || 'web3_mvp'
        };
        // Fill any missing pieces from env fields or hardcoded defaults
        const fx = {
          user: process.env.MYSQL_USER || DEFAULT_DB.user,
          password: process.env.MYSQL_PASSWORD || DEFAULT_DB.password,
          host: process.env.MYSQL_HOST || DEFAULT_DB.host,
          port: Number(process.env.MYSQL_PORT || DEFAULT_DB.port),
          database: process.env.MYSQL_DATABASE || DEFAULT_DB.database
        };
        resolved.user = resolved.user || fx.user;
        resolved.password = (resolved.password !== undefined && resolved.password !== '') ? resolved.password : fx.password;
        resolved.host = resolved.host || fx.host;
        resolved.port = resolved.port || fx.port;
        resolved.database = resolved.database || fx.database;
      } catch {}
    }
    if (!urlEnv) {
      resolved = { source: 'hardcoded', ...DEFAULT_DB };
    }
    const base = {
      user: resolved.user,
      password: resolved.password,
      database: resolved.database,
      waitForConnections: true,
      connectionLimit: 10,
      namedPlaceholders: true,
      connectTimeout: 15000
    };
    const host = resolved.host;
    const port = Number(resolved.port);
    const cfg = socketPath ? { ...base, socketPath } : { ...base, host, port };
    console.log('MySQL config =>', { source: resolved.source, host: cfg.host || '(socket)', port: cfg.port || '(socket)', database: cfg.database });
    pool = mysql.createPool(cfg);
  }
  return pool;
}

// FX cache
let fxCache = { rate: 1, base: 'USD', quote: 'USD', ts: 0 };
async function getUsdTo(currency) {
  const cur = (currency || 'USD').toUpperCase();
  if (cur === 'USD') return 1;
  const now = Date.now();
  if (fxCache.quote === cur && now - fxCache.ts < 60 * 60 * 1000) return fxCache.rate;
  try {
    // Provider A: open.er-api.com
    const urlA = `https://open.er-api.com/v6/latest/USD`;
    const { data: dataA } = await axios.get(urlA, { timeout: 8000 });
    const rateA = dataA && dataA.rates && dataA.rates[cur];
    if (rateA) { fxCache = { rate: rateA, base: 'USD', quote: cur, ts: now }; return rateA; }
  } catch {}
  try {
    // Provider B: exchangerate.host
    const urlB = `https://api.exchangerate.host/latest?base=USD&symbols=${encodeURIComponent(cur)}`;
    const { data: dataB } = await axios.get(urlB, { timeout: 8000 });
    const rateB = dataB && dataB.rates && dataB.rates[cur];
    if (rateB) { fxCache = { rate: rateB, base: 'USD', quote: cur, ts: now }; return rateB; }
  } catch {}
  // Fallback to cached if exists, otherwise 1
  return fxCache.quote === cur ? fxCache.rate : 1;
}

// --- Ensure core tables exist (for fresh DBs on cloud deploy) ---
async function ensureCandlesTable() {
  const pool = await getPool();
  await pool.query(`CREATE TABLE IF NOT EXISTS candles (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    ts BIGINT NOT NULL,
    timeframe ENUM('1h','4h','1d') NOT NULL DEFAULT '1d',
    open DECIMAL(24,8) NOT NULL,
    high DECIMAL(24,8) NOT NULL,
    low DECIMAL(24,8) NOT NULL,
    close DECIMAL(24,8) NOT NULL,
    volume DECIMAL(28,8) NOT NULL,
    UNIQUE KEY uniq_symbol_ts_interval (symbol, ts, timeframe),
    INDEX idx_symbol_ts (symbol, ts)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
}

async function ensureLevelsTable() {
  const pool = await getPool();
  await pool.query(`CREATE TABLE IF NOT EXISTS levels (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    ts BIGINT NOT NULL,
    method ENUM('pivot','swing','vbp') NOT NULL,
    bands_json JSON NOT NULL,
    INDEX idx_levels_symbol_ts (symbol, ts, method)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
}

async function ensureProbTable() {
  const pool = await getPool();
  await pool.query(`CREATE TABLE IF NOT EXISTS prob_signal (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    ts BIGINT NOT NULL,
    horizon ENUM('4h','24h','1d') NOT NULL DEFAULT '1d',
    p_up DECIMAL(6,4) NOT NULL,
    p_down DECIMAL(6,4) NOT NULL,
    verdict ENUM('bull','neutral','bear') NOT NULL,
    features_json JSON NULL,
    UNIQUE KEY uniq_symbol_ts_horizon (symbol, ts, horizon),
    INDEX idx_prob_symbol_ts (symbol, ts)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
}

async function ensureOnchainTable() {
  const pool = await getPool();
  await pool.query(`CREATE TABLE IF NOT EXISTS onchain_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    ts BIGINT NOT NULL,
    active_addr BIGINT DEFAULT NULL,
    tx_count BIGINT DEFAULT NULL,
    gas_used BIGINT DEFAULT NULL,
    stable_netflow DECIMAL(28,8) DEFAULT NULL,
    whale_tx BIGINT DEFAULT NULL,
    UNIQUE KEY uniq_symbol_ts (symbol, ts),
    INDEX idx_onchain_symbol_ts (symbol, ts)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
}

// Utils
function validateSymbol(req, res, next) {
  const raw = (req.query.symbol || req.params.symbol || '').toUpperCase();
  if (!API_SYMBOLS.has(raw)) {
    return res.status(400).json({ ok: false, message: 'symbol not allowed' });
  }
  req.symbol = raw === 'LUNAC' ? 'LUNC' : (raw === 'TRON' ? 'TRX' : raw);
  next();
}

const limiter = rateLimit({ windowMs: 60 * 1000, max: 120 });
app.use('/api/', limiter);

// Indicators
function calcEMA(values, period) {
  if (values.length === 0) return [];
  const k = 2 / (period + 1);
  const ema = [];
  let prev;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (i === 0) {
      prev = v;
    } else {
      prev = v * k + prev * (1 - k);
    }
    ema.push(prev);
  }
  return ema;
}

function calcRSI(closes, period = 14) {
  if (closes.length < period + 1) return Array(closes.length).fill(null);
  const rsi = Array(closes.length).fill(null);
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const ch = closes[i] - closes[i - 1];
    if (ch >= 0) gains += ch; else losses -= ch;
  }
  let avgGain = gains / period;
  let avgLoss = losses / period;
  rsi[period] = avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss)));
  for (let i = period + 1; i < closes.length; i++) {
    const ch = closes[i] - closes[i - 1];
    const gain = Math.max(ch, 0);
    const loss = Math.max(-ch, 0);
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
    rsi[i] = avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss)));
  }
  return rsi;
}

function calcMACD(closes, fast = 12, slow = 26, signal = 9) {
  const emaFast = calcEMA(closes, fast);
  const emaSlow = calcEMA(closes, slow);
  const macdLine = closes.map((_, i) => (emaFast[i] ?? 0) - (emaSlow[i] ?? 0));
  const signalLine = calcEMA(macdLine.map(v => v ?? 0), signal);
  const hist = macdLine.map((v, i) => (v ?? 0) - (signalLine[i] ?? 0));
  return { macdLine, signalLine, hist };
}

function zScore(series, window = 20) {
  const out = Array(series.length).fill(null);
  for (let i = 0; i < series.length; i++) {
    const start = Math.max(0, i - window + 1);
    const slice = series.slice(start, i + 1).filter(v => v != null);
    if (slice.length < 2) { out[i] = null; continue; }
    const mean = slice.reduce((a, b) => a + b, 0) / slice.length;
    const sd = Math.sqrt(slice.reduce((a, b) => a + (b - mean) ** 2, 0) / (slice.length - 1));
    out[i] = sd === 0 ? 0 : (series[i] - mean) / sd;
  }
  return out;
}

function calcATR(candles, period = 14) {
  if (!candles || candles.length < period + 1) return null;
  const TR = [];
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    const high = Number(c.high), low = Number(c.low);
    if (i === 0) {
      TR.push(high - low);
    } else {
      const prevClose = Number(candles[i - 1].close);
      const tr = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));
      TR.push(tr);
    }
  }
  // Wilder's smoothing
  let atr = TR.slice(1, period + 1).reduce((a, b) => a + b, 0) / period;
  for (let i = period + 1; i < TR.length; i++) atr = (atr * (period - 1) + TR[i]) / period;
  return atr;
}

// --- Fallback helpers when DB has no candles yet (e.g., fresh deploy) ---
async function getCandlesFallback(symbol, days = 120, tf = '1d') {
  try {
    if (tf === '1d' && BINANCE_SYMBOL[symbol]) {
      return await fetchBinanceDailyKlines(BINANCE_SYMBOL[symbol], Math.min(days, 500));
    }
  } catch {}
  try {
    if (tf === '1d') {
      const idCandidates = candidateIdsForSymbol(symbol);
      const [ohlc, vols] = await Promise.all([
        fetchDailyOHLC(idCandidates, Math.min(days, 90)),
        fetchDailyVolumes(idCandidates, Math.min(days, 90))
      ]);
      const volMap = new Map(vols.map(v => [isoDateUTC(v.ts), v.volume]));
      return ohlc.map(c => ({ ...c, volume: volMap.get(isoDateUTC(c.ts)) || 0 }));
    }
  } catch {}
  return [];
}

// Levels
function computePivotLevels(latestCandle) {
  const H = Number(latestCandle.high), L = Number(latestCandle.low), C = Number(latestCandle.close);
  const P = (H + L + C) / 3;
  const R1 = 2 * P - L; const S1 = 2 * P - H;
  const R2 = P + (H - L); const S2 = P - (H - L);
  const width = C * 0.003; // 0.3%
  return [
    { min: S2 - width, max: S2 + width, hits: 0 },
    { min: S1 - width, max: S1 + width, hits: 0 },
    { min: P - width,  max: P + width,  hits: 0 },
    { min: R1 - width, max: R1 + width, hits: 0 },
    { min: R2 - width, max: R2 + width, hits: 0 },
  ];
}

function computeSwingBands(candles, lookback = 5, proximity = 0.01) {
  const highs = []; const lows = [];
  for (let i = lookback; i < candles.length - lookback; i++) {
    const h = Number(candles[i].high);
    const l = Number(candles[i].low);
    let isHigh = true, isLow = true;
    for (let k = i - lookback; k <= i + lookback; k++) {
      if (Number(candles[k].high) > h) isHigh = false;
      if (Number(candles[k].low) < l) isLow = false;
    }
    if (isHigh) highs.push(h);
    if (isLow) lows.push(l);
  }
  const points = highs.concat(lows).sort((a,b)=>a-b);
  const bands = [];
  for (const p of points) {
    if (bands.length === 0) { bands.push({ min: p, max: p, hits: 1 }); continue; }
    const last = bands[bands.length - 1];
    if ((p - last.max) / last.max <= proximity) {
      last.max = Math.max(last.max, p);
      last.min = Math.min(last.min, p);
      last.hits += 1;
    } else {
      bands.push({ min: p, max: p, hits: 1 });
    }
  }
  return bands.map(b => ({ min: b.min, max: b.max, hits: b.hits }));
}

function computeVbpBands(candles, buckets = 12) {
  const minP = Math.min(...candles.map(c => Number(c.low)));
  const maxP = Math.max(...candles.map(c => Number(c.high)));
  const width = (maxP - minP) / buckets;
  if (width <= 0) return [];
  const vols = Array(buckets).fill(0);
  for (const c of candles) {
    const price = Number(c.close);
    let idx = Math.floor((price - minP) / width);
    if (idx >= buckets) idx = buckets - 1;
    if (idx < 0) idx = 0;
    vols[idx] += Number(c.volume);
  }
  const bands = vols.map((v, i) => ({ min: minP + i * width, max: minP + (i + 1) * width, hits: Math.round(v) }));
  bands.sort((a, b) => b.hits - a.hits);
  return bands.slice(0, 5).sort((a, b) => a.min - b.min);
}

// Probability baseline
function computeProbabilitySnapshot(candles, onchainRows) {
  if (!candles || candles.length < 40) return { p_up: 0.5, p_down: 0.5, verdict: 'neutral', features: {} };
  const closes = candles.map(c => Number(c.close));
  const volumes = candles.map(c => Number(c.volume));
  const rsi = calcRSI(closes, 14);
  const { macdLine, signalLine, hist } = calcMACD(closes, 12, 26, 9);

  const ma20 = calcEMA(closes, 20);
  const ma50 = calcEMA(closes, 50);
  const ma20Slope = ma20[ma20.length - 1] - ma20[ma20.length - 2];
  const ma50Slope = ma50[ma50.length - 1] - ma50[ma50.length - 2];
  const volZ = zScore(volumes, 20);

  const last = closes.length - 1;
  const features = {
    rsi: rsi[last],
    macd: macdLine[last],
    macd_signal: signalLine[last],
    macd_hist: hist[last],
    ma20_slope: ma20Slope,
    ma50_slope: ma50Slope,
    volume_z: volZ[last]
  };

  // On-chain mock features contribution
  if (onchainRows && onchainRows.length > 5) {
    const growthActive = (onchainRows[onchainRows.length - 1].active_addr || 0) - (onchainRows[onchainRows.length - 6].active_addr || 0);
    const growthTx = (onchainRows[onchainRows.length - 1].tx_count || 0) - (onchainRows[onchainRows.length - 6].tx_count || 0);
    features.active_growth_5d = growthActive;
    features.tx_growth_5d = growthTx;
  }

  // Rule-based scoring
  let score = 0;
  if (features.rsi != null) {
    if (features.rsi > 55) score += 0.15;
    else if (features.rsi < 45) score -= 0.15;
  }
  if (features.macd != null && features.macd_signal != null) {
    if (features.macd > features.macd_signal) score += 0.2; else score -= 0.2;
  }
  if (features.ma20_slope > 0) score += 0.2; else score -= 0.1;
  if (features.ma50_slope > 0) score += 0.1; else score -= 0.1;
  if (features.volume_z != null) {
    if (features.volume_z > 1) score += 0.1;
  }
  if (features.active_growth_5d && features.active_growth_5d > 0) score += 0.1;
  if (features.tx_growth_5d && features.tx_growth_5d > 0) score += 0.05;

  // map score (-inf..inf) to probability via sigmoid-ish transform
  const clamp = (x, min, max) => Math.max(min, Math.min(max, x));
  const p_up = clamp(0.5 + score, 0.01, 0.99);
  const p_down = 1 - p_up;
  const verdict = p_up > 0.6 ? 'bull' : (p_up < 0.4 ? 'bear' : 'neutral');
  return { p_up, p_down, verdict, features };
}

// CoinGecko helpers
const COINGECKO_BASE = process.env.COINGECKO_BASE || 'https://api.coingecko.com/api/v3';
async function fetchDailyOHLCById(id, days = 90) {
  const url = `${COINGECKO_BASE}/coins/${id}/ohlc?vs_currency=usd&days=${days}`;
  const { data } = await axios.get(url, { timeout: 20000 });
  return data.map(row => ({ ts: row[0], open: row[1], high: row[2], low: row[3], close: row[4] }));
}
async function fetchMarketChartById(id, days = 90) {
  const url = `${COINGECKO_BASE}/coins/${id}/market_chart?vs_currency=usd&days=${days}&interval=daily`;
  const { data } = await axios.get(url, { timeout: 20000 });
  return data;
}
async function fetchDailyOHLC(idOrIds, days = 90) {
  const ids = Array.isArray(idOrIds) ? idOrIds : [idOrIds];
  for (const id of ids) {
    try {
      return await fetchDailyOHLCById(id, days);
    } catch (e) {
      if (e.response && e.response.status === 404) {
        try {
          const data = await fetchMarketChartById(id, days);
          const prices = (data.prices || []).map(p => ({ ts: p[0], close: p[1] }));
          const out = prices.map((p, idx) => {
            const prevClose = idx > 0 ? prices[idx - 1].close : p.close;
            return { ts: p.ts, open: prevClose, high: p.close, low: p.close, close: p.close };
          });
          return out;
        } catch (e2) {
          if (!(e2.response && e2.response.status === 404)) throw e2;
        }
      } else {
        throw e;
      }
    }
  }
  throw new Error('All CoinGecko ID candidates failed');
}

async function fetchDailyVolumes(idOrIds, days = 90) {
  const ids = Array.isArray(idOrIds) ? idOrIds : [idOrIds];
  for (const id of ids) {
    try {
      const data = await fetchMarketChartById(id, days);
      return (data.total_volumes || []).map(row => ({ ts: row[0], volume: row[1] }));
    } catch (e) {
      if (!(e.response && e.response.status === 404)) throw e;
    }
  }
  return [];
}

async function fetchBinanceDailyKlines(binanceSymbol, limit = 90) {
  const url = `https://api.binance.com/api/v3/klines?symbol=${encodeURIComponent(binanceSymbol)}&interval=1d&limit=${limit}`;
  const { data } = await axios.get(url, { timeout: 20000 });
  // [ openTime, open, high, low, close, volume, closeTime, ... ]
  return data.map(k => ({
    ts: Number(k[6]), // use close time in ms
    open: Number(k[1]),
    high: Number(k[2]),
    low: Number(k[3]),
    close: Number(k[4]),
    volume: Number(k[5])
  }));
}

async function fetchBinanceTickerPrice(binanceSymbol) {
  const url = `https://api.binance.com/api/v3/ticker/price?symbol=${encodeURIComponent(binanceSymbol)}`;
  const { data } = await axios.get(url, { timeout: 10000 });
  return Number(data.price);
}

// Crypto.com Exchange providers (primary for CRO; extended to SNEK)
async function fetchCryptoComCandlesGeneric(symbol, timeframe = '1D') {
  const inst = `${symbol}_USDT`;
  const url = `https://api.crypto.com/v2/public/get-candlestick?instrument_name=${encodeURIComponent(inst)}&timeframe=${encodeURIComponent(timeframe)}`;
  const { data } = await axios.get(url, { timeout: 20000 });
  const arr = (data && data.result && data.result.data) || (data && data.data && data.data.candlestick) || (data && data.data) || [];
  return arr.map(k => {
    const tsRaw = Number(k.t ?? k.time ?? k.T ?? 0);
    const ts = tsRaw < 1e12 ? tsRaw * 1000 : tsRaw;
    const open = Number(k.o ?? k.open ?? 0);
    const high = Number(k.h ?? k.high ?? 0);
    const low = Number(k.l ?? k.low ?? 0);
    const close = Number(k.c ?? k.close ?? 0);
    const volume = Number(k.v ?? k.volume ?? 0);
    return { ts, open, high, low, close, volume };
  }).sort((a,b)=>a.ts-b.ts);
}

async function fetchCryptoComCandlesCRO(timeframe = '1D') {
  return fetchCryptoComCandlesGeneric('CRO', timeframe);
}

async function fetchCryptoComTickerGeneric(symbol) {
  const url = `https://api.crypto.com/v2/public/get-ticker?instrument_name=${encodeURIComponent(symbol + '_USDT')}`;
  const { data } = await axios.get(url, { timeout: 10000 });
  const obj = (data && data.result) || data;
  // result.data may be array or object depending on API version
  const item = (obj && (obj.data && (Array.isArray(obj.data) ? obj.data[0] : obj.data))) || obj;
  const price = Number(item && (item.a ?? item.price ?? item.p ?? item.last_price));
  if (!price || Number.isNaN(price)) throw new Error('cryptocom ticker unavailable');
  return price;
}

async function fetchCryptoComTickerCRO() { return fetchCryptoComTickerGeneric('CRO'); }

async function fetchCoingeckoSimpleUsd(symbol) {
  const idCandidates = candidateIdsForSymbol(symbol);
  for (const id of idCandidates) {
    try {
      const url = `${COINGECKO_BASE}/simple/price?ids=${encodeURIComponent(id)}&vs_currencies=usd`;
      const { data } = await axios.get(url, { timeout: 10000 });
      const price = data && data[id] && data[id].usd;
      if (price) return Number(price);
    } catch (e) {
      if (!(e.response && e.response.status === 404)) throw e;
    }
  }
  throw new Error('live price unavailable');
}

async function upsertCandles(symbol, candles) {
  if (candles.length === 0) return;
  const pool = await getPool();
  const sql = `INSERT INTO candles (symbol, ts, timeframe, open, high, low, close, volume)
               VALUES ${candles.map(()=> '(?,?,?,?,?,?,?,?)').join(',')}
               ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), volume=VALUES(volume)`;
  const params = [];
  for (const c of candles) {
    params.push(symbol, c.ts, '1d', c.open, c.high, c.low, c.close, c.volume ?? 0);
  }
  await pool.query(sql, params);
}

async function upsertLevels(symbol, ts, method, bands) {
  const pool = await getPool();
  await pool.query(
    `INSERT INTO levels (symbol, ts, method, bands_json) VALUES (?,?,?,?)`,
    [symbol, ts, method, JSON.stringify(bands)]
  );
}

async function upsertProbability(symbol, ts, horizon, result) {
  const pool = await getPool();
  await pool.query(
    `INSERT INTO prob_signal (symbol, ts, horizon, p_up, p_down, verdict, features_json)
     VALUES (?,?,?,?,?,?,?)
     ON DUPLICATE KEY UPDATE p_up=VALUES(p_up), p_down=VALUES(p_down), verdict=VALUES(verdict), features_json=VALUES(features_json)`,
    [symbol, ts, horizon, result.p_up, result.p_down, result.verdict, JSON.stringify(result.features)]
  );
}

async function upsertOnchainMock(symbol, rows) {
  if (!rows || rows.length === 0) return;
  const pool = await getPool();
  const sql = `INSERT INTO onchain_daily (symbol, ts, active_addr, tx_count, gas_used, stable_netflow, whale_tx)
               VALUES ${rows.map(()=> '(?,?,?,?,?,?,?)').join(',')}
               ON DUPLICATE KEY UPDATE active_addr=VALUES(active_addr), tx_count=VALUES(tx_count), gas_used=VALUES(gas_used), stable_netflow=VALUES(stable_netflow), whale_tx=VALUES(whale_tx)`;
  const params = [];
  for (const r of rows) {
    params.push(symbol, r.ts, r.active_addr, r.tx_count, r.gas_used, r.stable_netflow, r.whale_tx);
  }
  await pool.query(sql, params);
}

function genOnchainMock(tsArray, seed = 1) {
  // deterministic pseudo-random with trend
  let s = seed;
  function rnd() { s = (s * 9301 + 49297) % 233280; return s / 233280; }
  let baseActive = 500000 + rnd() * 200000;
  let baseTx = 200000 + rnd() * 100000;
  let baseGas = 1e12 + rnd() * 2e12;
  const rows = [];
  for (const ts of tsArray) {
    baseActive *= 0.999 + rnd() * 0.002;
    baseTx *= 0.999 + rnd() * 0.002;
    baseGas *= 0.999 + rnd() * 0.002;
    const stableNet = (rnd() - 0.5) * 5e7; // +/- 50M
    const whales = Math.floor(50 + rnd() * 100);
    rows.push({ ts, active_addr: Math.round(baseActive), tx_count: Math.round(baseTx), gas_used: Math.round(baseGas), stable_netflow: Math.round(stableNet), whale_tx: whales });
  }
  return rows;
}

// --- BTC on-chain via blockchain.info public charts ---
async function fetchBlockchainChart(name, days = 120) {
  const url = `https://api.blockchain.info/charts/${encodeURIComponent(name)}?timespan=${days}days&format=json`;
  const { data } = await axios.get(url, { timeout: 15000 });
  const arr = (data && data.values) || [];
  return arr.map(p => ({ ts: Number(p.x) * 1000, value: Number(p.y) }));
}

async function fetchBtcOnchainDaily(days = 120) {
  const [act, tx] = await Promise.all([
    fetchBlockchainChart('active_addresses', days).catch(()=>[]),
    fetchBlockchainChart('n-transactions', days).catch(()=>[])
  ]);
  const map = new Map();
  for (const a of act) { const m = map.get(a.ts) || {}; m.ts = a.ts; m.active_addr = Math.round(a.value); map.set(a.ts, m); }
  for (const t of tx) { const m = map.get(t.ts) || {}; m.ts = t.ts; m.tx_count = Math.round(t.value); map.set(t.ts, m); }
  const rows = Array.from(map.values()).sort((a,b)=>a.ts-b.ts).map(r => ({
    ts: r.ts,
    active_addr: r.active_addr ?? null,
    tx_count: r.tx_count ?? null,
    gas_used: null,
    stable_netflow: null,
    whale_tx: null
  }));
  return rows;
}

async function fetchAndUpdateSymbol(symbol) {
  let merged;
  try {
    if (BINANCE_SYMBOL[symbol]) {
      const b = await fetchBinanceDailyKlines(BINANCE_SYMBOL[symbol], 90);
      merged = b;
    }
  } catch (e) {
    // fall through to other providers
  }
  if (!merged && symbol === 'CRO') {
    try {
      merged = await fetchCryptoComCandlesCRO('1D');
    } catch (e) {
      // continue to CoinGecko fallback
    }
  }
  if (!merged && symbol === 'SNEK') {
    try {
      merged = await fetchCryptoComCandles('SNEK', '1D');
    } catch (e) {
      // continue to CoinGecko fallback
    }
  }
  if (!merged) {
    const idCandidates = candidateIdsForSymbol(symbol);
    const [ohlc, vols] = await Promise.all([
      fetchDailyOHLC(idCandidates, 90),
      fetchDailyVolumes(idCandidates, 90),
    ]);
    const volMap = new Map(vols.map(v => [isoDateUTC(v.ts), v.volume]));
    merged = ohlc.map(c => {
      const key = isoDateUTC(c.ts);
      return { ...c, volume: volMap.get(key) || 0 };
    });
  }

  await upsertCandles(symbol, merged);

  const pool = await getPool();
  const [rows] = await pool.query(`SELECT * FROM candles WHERE symbol=? AND timeframe='1d' ORDER BY ts ASC`, [symbol]);
  if (!rows || rows.length === 0) return;
  const latest = rows[rows.length - 1];

  const pivot = computePivotLevels(latest);
  const swing = computeSwingBands(rows);
  const vbp = computeVbpBands(rows);

  await upsertLevels(symbol, latest.ts, 'pivot', pivot);
  await upsertLevels(symbol, latest.ts, 'swing', swing);
  await upsertLevels(symbol, latest.ts, 'vbp', vbp);

  const tsArray = rows.map(r => r.ts);
  let onchainRows;
  if (symbol === 'BTC') {
    try { onchainRows = await fetchBtcOnchainDaily(Math.min(180, rows.length)); }
    catch { onchainRows = genOnchainMock(tsArray, symbol.charCodeAt(0)); }
  } else {
    onchainRows = genOnchainMock(tsArray, symbol.charCodeAt(0));
  }
  await upsertOnchainMock(symbol, onchainRows);

  const prob = computeProbabilitySnapshot(rows, onchainRows);
  await upsertProbability(symbol, latest.ts, '1d', prob);
}

async function ensureInitialData() {
  for (const sym of CANONICAL_SYMBOLS) {
    try {
      const pool = await getPool();
      const [rows] = await pool.query(`SELECT COUNT(*) as cnt FROM candles WHERE symbol=? AND timeframe='1d'`, [sym]);
      const cnt = rows[0].cnt;
      if (cnt < 10) {
        console.log(`Bootstrapping ${sym} candles...`);
        // Try quick fallback fetch to warm DB without blocking startup
        (async () => {
          try {
            const raw = await getCandlesFallback(sym, 120, '1d');
            await upsertCandles(sym, raw);
          } catch {}
          try { await fetchAndUpdateSymbol(sym); } catch {}
        })();
      }
    } catch (e) {
      console.error(`Init data error for ${sym}:`, e.message || String(e));
    }
  }
}

// Cron
const cronExpr = process.env.CRON_FETCH || '*/15 * * * *';
cron.schedule(cronExpr, async () => {
  console.log('Cron tick:', new Date().toISOString());
  for (const sym of CANONICAL_SYMBOLS) {
    try { await fetchAndUpdateSymbol(sym); }
    catch (e) { console.error(`Cron update failed for ${sym}:`, e.message); }
  }
});

// APIs
app.get('/api/market/summary', validateSymbol, async (req, res) => {
  try {
    const interval = (req.query.interval || '1d');
    const days = Math.max(1, Math.min(365, Number(req.query.days || 90)));
    const currency = (req.query.currency || 'USD').toUpperCase();
    const rate = await getUsdTo(currency);
    const pool = await getPool();
    await ensureCandlesTable();
    let candles = [];
    if (interval === '1d') {
      const [rows] = await pool.query(
        `SELECT ts, open, high, low, close, volume FROM candles WHERE symbol=? AND timeframe='1d' ORDER BY ts DESC LIMIT ?`,
        [req.symbol, days]
      );
      if (!rows.length) {
        // Fresh DB fallback: fetch live candles and return (also async backfill)
        const raw = await getCandlesFallback(req.symbol, days, '1d');
        candles = raw.map(r => ({ ts: r.ts, open: r.open * rate, high: r.high * rate, low: r.low * rate, close: r.close * rate, volume: r.volume }));
        // Async persist in USD to DB (no await to keep response fast)
        (async () => { try { await upsertCandles(req.symbol, raw); await fetchAndUpdateSymbol(req.symbol); } catch {} })();
      } else {
        candles = rows.reverse().map(r => ({ ts: r.ts, open: Number(r.open) * rate, high: Number(r.high) * rate, low: Number(r.low) * rate, close: Number(r.close) * rate, volume: Number(r.volume) }));
      }
    } else if (interval === '4h') {
      const arr = await getCandlesFor(req.symbol, '4h');
      candles = arr.slice(-days).map(r => ({ ts: r.ts, open: r.open * rate, high: r.high * rate, low: r.low * rate, close: r.close * rate, volume: r.volume }));
    } else if (interval === '1h') {
      // derive from 4h or fetch binance 1h quickly (fallback: reuse 4h expanded)
      try {
        const url = `https://api.binance.com/api/v3/klines?symbol=${encodeURIComponent(BINANCE_SYMBOL[req.symbol]||'')}&interval=1h&limit=${Math.min(1000, days)}`;
        if (!BINANCE_SYMBOL[req.symbol]) throw new Error('no binance pair');
        const { data } = await axios.get(url, { timeout: 20000 });
        candles = data.map(k => ({ ts: Number(k[6]), open: Number(k[1])*rate, high: Number(k[2])*rate, low: Number(k[3])*rate, close: Number(k[4])*rate, volume: Number(k[5]) }));
      } catch {
        const arr = await getCandlesFor(req.symbol, '4h');
        candles = arr.reduce((acc, c) => acc.concat([{ ts: c.ts - 3*60*60*1000, open: c.open*rate, high: c.high*rate, low: c.low*rate, close: c.close*rate, volume: c.volume }]), []).slice(-days);
      }
    } else if (interval === '1w') {
      // aggregate from daily
      const [rows] = await pool.query(`SELECT ts, open, high, low, close, volume FROM candles WHERE symbol=? AND timeframe='1d' ORDER BY ts ASC`, [req.symbol]);
      const d = rows.map(r => ({ ts: r.ts, open: Number(r.open), high: Number(r.high), low: Number(r.low), close: Number(r.close), volume: Number(r.volume) }));
      const weeks = [];
      let cur = null, curWeek = null;
      for (const c of d) {
        const dt = new Date(c.ts);
        const wk = `${dt.getUTCFullYear()}-${Math.ceil((dt.getUTCDate() + (new Date(Date.UTC(dt.getUTCFullYear(),0,1)).getUTCDay()||7)) / 7)}`;
        if (curWeek !== wk) {
          if (cur) weeks.push(cur);
          curWeek = wk;
          cur = { ts: Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate()), open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume };
        } else {
          cur.high = Math.max(cur.high, c.high);
          cur.low = Math.min(cur.low, c.low);
          cur.close = c.close;
          cur.volume += c.volume;
        }
      }
      if (cur) weeks.push(cur);
      candles = weeks.slice(-days).map(r => ({ ts: r.ts, open: r.open*rate, high: r.high*rate, low: r.low*rate, close: r.close*rate, volume: r.volume }));
    }

    // Add provisional today candle for 1d if today's daily close hasn't been stored yet
    if (interval === '1d' && candles.length) {
      const now = new Date();
      const utcStart = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
      const lastTsLocal = Number(candles[candles.length - 1].ts || 0);
      if (lastTsLocal < utcStart) {
        let todayCandle = null;
        try {
          // Use live price to form a provisional flat candle in current currency
          const liveUsd = await fetchCoingeckoSimpleUsd(req.symbol);
          const prevCloseLocal = Number(candles[candles.length - 1].close) || (liveUsd * rate);
          todayCandle = { ts: utcStart, open: prevCloseLocal, high: liveUsd * rate, low: liveUsd * rate, close: liveUsd * rate, volume: 0 };
        } catch {}
        if (todayCandle) candles = candles.concat([todayCandle]);
      }
    }

    const price = candles.length ? Number(candles[candles.length - 1].close) : null;
    const change24h = candles.length > 1 ? (price - Number(candles[candles.length - 2].close)) / Number(candles[candles.length - 2].close) : null;
    res.json({ ok: true, data: { candles, price, change24h, currency } });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

app.get('/api/market/top', async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(250, Number(req.query.limit || 100)));
    const currency = (req.query.currency || 'USD').toUpperCase();
    const url = `${COINGECKO_BASE}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=${limit}&page=1&sparkline=false&price_change_percentage=24h`;
    const { data } = await axios.get(url, { timeout: 20000 });
    const rate = await getUsdTo(currency);
    const mapped = (data || []).map((c, idx) => ({
      rank: idx + 1,
      id: c.id,
      symbol: (c.symbol || '').toUpperCase(),
      name: c.name,
      price: Number(c.current_price) * rate,
      change24h: c.price_change_percentage_24h,
      market_cap: Number(c.market_cap || 0) * rate,
      volume24h: Number(c.total_volume || 0) * rate,
      image: c.image
    }));
    res.json({ ok: true, data: { currency, list: mapped } });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

app.get('/api/market/top_gainers', async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(250, Number(req.query.limit || 100)));
    const currency = (req.query.currency || 'USD').toUpperCase();
    // Fetch first 250 by market cap and then sort by 24h change
    const url = `${COINGECKO_BASE}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false&price_change_percentage=24h`;
    const { data } = await axios.get(url, { timeout: 20000 });
    const rate = await getUsdTo(currency);
    const list = (data || []).map(c => ({
      id: c.id,
      symbol: (c.symbol || '').toUpperCase(),
      name: c.name,
      price: Number(c.current_price) * rate,
      change24h: c.price_change_percentage_24h,
      market_cap: Number(c.market_cap || 0) * rate,
      volume24h: Number(c.total_volume || 0) * rate,
      image: c.image
    })).sort((a, b) => (Number(b.change24h || -Infinity)) - (Number(a.change24h || -Infinity)));
    const top = list.slice(0, limit).map((x, idx) => ({ rank: idx + 1, ...x }));
    res.json({ ok: true, data: { currency, list: top } });
  } catch (e) {
    // Fallback: return empty list to avoid UI 500
    const currency = (req.query.currency || 'USD').toUpperCase();
    res.json({ ok: true, data: { currency, list: [] }, message: 'top_gainers unavailable' });
  }
});

app.get('/api/levels', validateSymbol, async (req, res) => {
  try {
    const currency = (req.query.currency || 'USD').toUpperCase();
    const rate = await getUsdTo(currency);
    const pool = await getPool();
    await ensureLevelsTable();
    const [rows] = await pool.query(
      `SELECT method, bands_json, ts FROM levels WHERE symbol=? ORDER BY ts DESC`,
      [req.symbol]
    );
    const out = { pivot: null, swing: null, vbp: null, ts: null, currency };
    for (const r of rows) {
      const bands = (typeof r.bands_json === 'string' ? JSON.parse(r.bands_json) : r.bands_json) || [];
      const converted = bands.map(b => ({ min: Number(b.min) * rate, max: Number(b.max) * rate, hits: b.hits }));
      if (!out[r.method]) { out[r.method] = converted; out.ts = out.ts || r.ts; }
      if (out.pivot && out.swing && out.vbp) break;
    }
    res.json({ ok: true, data: out });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

app.get('/api/probability', validateSymbol, async (req, res) => {
  try {
    const horizon = (req.query.horizon || '1d');
    const currency = (req.query.currency || 'USD').toUpperCase();
    const rate = await getUsdTo(currency);
    const pool = await getPool();
    await Promise.all([ensureCandlesTable(), ensureProbTable(), ensureLevelsTable(), ensureOnchainTable()]);
    const [rows] = await pool.query(
      `SELECT ts, p_up, p_down, verdict, features_json FROM prob_signal WHERE symbol=? AND horizon=? ORDER BY ts DESC LIMIT 1`,
      [req.symbol, horizon]
    );

    // Always compute forecast targets fresh based on latest candles + levels
    const [candlesRows] = await pool.query(`SELECT * FROM candles WHERE symbol=? AND timeframe='1d' ORDER BY ts ASC`, [req.symbol]);
    const [levelRows] = await pool.query(`SELECT method, bands_json, ts FROM levels WHERE symbol=? ORDER BY ts DESC`, [req.symbol]);
    const latest = candlesRows[candlesRows.length - 1];
    let bandsAll = [];
    const seen = { pivot: false, swing: false, vbp: false };
    for (const r of levelRows) {
      if (seen[r.method]) continue;
      const bands = (typeof r.bands_json === 'string' ? JSON.parse(r.bands_json) : r.bands_json) || [];
      bandsAll = bandsAll.concat(bands.map(b => ({ ...b, method: r.method })));
      seen[r.method] = true;
      if (seen.pivot && seen.swing && seen.vbp) break;
    }
    const close = latest ? Number(latest.close) : null;
    const atr = calcATR(candlesRows);

    function pickTargets(direction) {
      const out = [];
      if (!close) return out;
      const upper = bandsAll.filter(b => ((b.min + b.max) / 2) > close).sort((a, b) => ((a.min + a.max) / 2) - ((b.min + b.max) / 2));
      const lower = bandsAll.filter(b => ((b.min + b.max) / 2) < close).sort((a, b) => ((b.min + b.max) / 2) - ((a.min + a.max) / 2));
      const pick = direction === 'bull' ? upper : lower;
      for (let i = 0; i < Math.min(3, pick.length); i++) {
        const b = pick[i];
        const mid = (Number(b.min) + Number(b.max)) / 2;
        const dist = Math.abs((mid - close) / close);
        out.push({ type: 'band', method: b.method, min: Number(b.min) * rate, max: Number(b.max) * rate, mid: mid * rate, distance_pct: dist });
      }
      // ATR projections
      if (atr && atr > 0) {
        if (direction === 'bull') {
          out.push({ type: 'atr', label: '+0.5ATR', price: (close + 0.5 * atr) * rate });
          out.push({ type: 'atr', label: '+1.0ATR', price: (close + 1.0 * atr) * rate });
        } else {
          out.push({ type: 'atr', label: '-0.5ATR', price: (close - 0.5 * atr) * rate });
          out.push({ type: 'atr', label: '-1.0ATR', price: (close - 1.0 * atr) * rate });
        }
      }
      return out;
    }

    let base = null;
    if (rows.length) {
      const r = rows[0];
      base = { ts: r.ts, p_up: Number(r.p_up), p_down: Number(r.p_down), verdict: r.verdict, features_snapshot: r.features_json };
    } else {
      // compute on-demand and persist
      const [onchainRows] = await pool.query(`SELECT * FROM onchain_daily WHERE symbol=? ORDER BY ts ASC`, [req.symbol]);
      if (!candlesRows.length) return res.status(404).json({ ok: false, message: 'no candle data' });
      const latestC = candlesRows[candlesRows.length - 1];
      const prob = computeProbabilitySnapshot(candlesRows, onchainRows);
      await upsertProbability(req.symbol, latestC.ts, horizon, prob);
      base = { ts: latestC.ts, p_up: prob.p_up, p_down: prob.p_down, verdict: prob.verdict, features_snapshot: prob.features };
    }

    const targets = pickTargets(base.verdict);
    return res.json({ ok: true, data: { ...base, currency, targets } });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

app.get('/api/onchain/metrics', validateSymbol, async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(365, Number(req.query.limit || 90)));
    const pool = await getPool();
    await ensureOnchainTable();
    let [rows] = await pool.query(
      `SELECT ts, active_addr, tx_count, gas_used, stable_netflow, whale_tx FROM onchain_daily WHERE symbol=? ORDER BY ts DESC LIMIT ?`,
      [req.symbol, limit]
    );
    if (!rows.length && req.symbol === 'BTC') {
      try {
        const fresh = await fetchBtcOnchainDaily(Math.min(180, limit));
        if (fresh && fresh.length) {
          const sql = `INSERT IGNORE INTO onchain_daily (symbol, ts, active_addr, tx_count, gas_used, stable_netflow, whale_tx) VALUES ${fresh.map(()=> '(?,?,?,?,?,?,?)').join(',')}`;
          const params = [];
          for (const r of fresh) params.push('BTC', r.ts, r.active_addr, r.tx_count, r.gas_used, r.stable_netflow, r.whale_tx);
          await pool.query(sql, params);
          ;[rows] = await pool.query(
            `SELECT ts, active_addr, tx_count, gas_used, stable_netflow, whale_tx FROM onchain_daily WHERE symbol=? ORDER BY ts DESC LIMIT ?`,
            [req.symbol, limit]
          );
        }
      } catch {}
    }
    const data = (rows || []).reverse();
    res.json({ ok: true, data });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

app.get('/api/onchain/overview', async (req, res) => {
  try {
    const pool = await getPool();
    await ensureOnchainTable();
    const symbols = Array.from(CANONICAL_SYMBOLS);
    const out = [];
    for (const s of symbols) {
      const [[row]] = await pool.query(`SELECT ts, active_addr, tx_count, gas_used, stable_netflow, whale_tx FROM onchain_daily WHERE symbol=? ORDER BY ts DESC LIMIT 1`, [s]);
      if (row) out.push({ symbol: s, ...row });
    }
    res.json({ ok: true, data: out });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

app.get('/api/price', validateSymbol, async (req, res) => {
  try {
    const currency = (req.query.currency || 'USD').toUpperCase();
    let usdPrice;
    try {
      if (BINANCE_SYMBOL[req.symbol]) {
        try { usdPrice = await fetchBinanceTickerPrice(BINANCE_SYMBOL[req.symbol]); }
        catch { usdPrice = await fetchCoingeckoSimpleUsd(req.symbol); }
      } else if (req.symbol === 'CRO' || req.symbol === 'SNEK') {
        try { usdPrice = req.symbol === 'CRO' ? await fetchCryptoComTickerCRO() : await fetchCryptoComTickerGeneric('SNEK'); }
        catch { usdPrice = await fetchCoingeckoSimpleUsd(req.symbol); }
      } else {
        usdPrice = await fetchCoingeckoSimpleUsd(req.symbol);
      }
    } catch (e) {
      // Fallback to latest candle close
      try {
        const pool = await getPool();
        const [rows] = await pool.query(`SELECT close FROM candles WHERE symbol=? AND timeframe='1d' ORDER BY ts DESC LIMIT 1`, [req.symbol]);
        if (rows && rows.length) {
          usdPrice = Number(rows[0].close);
        } else {
          throw e;
        }
      } catch (e2) {
        return res.status(500).json({ ok: false, message: 'live price unavailable' });
      }
    }
    const rate = await getUsdTo(currency);
    const px = usdPrice * rate;
    res.json({ ok: true, data: { symbol: req.symbol, price: px, currency } });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

app.get('/api/health', async (req, res) => {
  try {
    const pool = await getPool();
    const [rows] = await pool.query('SELECT 1');
    res.json({ ok: true, db: process.env.MYSQL_DATABASE || null, rows });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message, config: { host: process.env.MYSQL_HOST, port: process.env.MYSQL_PORT, db: process.env.MYSQL_DATABASE } });
  }
});

// --- Market: Fear & Greed Index ---
// Source: alternative.me public API
app.get('/api/market/fear_greed', async (req, res) => {
  try {
    const now = Date.now();
    // 30 minutes cache
    if (memoryCache.fng.data && (now - memoryCache.fng.ts) < 30 * 60 * 1000) {
      return res.json({ ok: true, data: memoryCache.fng.data, cached: true });
    }
    const url = 'https://api.alternative.me/fng/?limit=1&format=json';
    const { data } = await axios.get(url, { timeout: 15000 });
    const item = (data && data.data && data.data[0]) || {};
    const value = Number(item.value || 0);
    const classification = item.value_classification || (
      value >= 75 ? 'Extreme Greed' :
      value >= 55 ? 'Greed' :
      value >  45 ? 'Neutral' :
      value >= 25 ? 'Fear' : 'Extreme Fear'
    );
    const ts = item.timestamp ? Number(item.timestamp) * 1000 : Date.now();
    const out = { value, classification, ts };
    memoryCache.fng = { ts: now, data: out };
    res.json({ ok: true, data: out });
  } catch (e) {
    // fallback graceful to avoid UI errors
    res.json({ ok: true, data: { value: null, classification: 'N/A', ts: Date.now() }, message: 'fng unavailable' });
  }
});

// --- Wallet holdings (MVP: ETH via Ethplorer; BTC via Blockstream; Cronos via CronosScan) ---
async function fetchEthRpcBalance(address){
  // Cloudflare public endpoint
  const url = 'https://cloudflare-eth.com';
  const payload = { jsonrpc: '2.0', id: 1, method: 'eth_getBalance', params: [address, 'latest'] };
  const { data } = await axios.post(url, payload, { timeout: 15000, headers: { 'Content-Type': 'application/json' } });
  const hex = data && data.result;
  if (!hex) throw new Error('rpc no result');
  const wei = BigInt(hex);
  const eth = Number(wei) / 1e18;
  return eth;
}

async function fetchEthHoldings(address) {
  if (!isEthAddress(address)) throw new Error('invalid eth address');
  const out = [];
  // Try Ethplorer
  try {
    const key = process.env.ETHPLORER_KEY || 'freekey';
    const url = `https://api.ethplorer.io/getAddressInfo/${address}?apiKey=${encodeURIComponent(key)}`;
    const { data } = await axios.get(url, { timeout: 15000 });
    const ethBalance = data.ETH && data.ETH.balance != null ? Number(data.ETH.balance) : 0;
    if (ethBalance > 0) out.push({ symbol: 'ETH', balance: ethBalance });
    const tokens = Array.isArray(data.tokens) ? data.tokens : [];
    for (const t of tokens) {
      try {
        const info = t.tokenInfo || {};
        const addr = (info.address || '').toLowerCase();
        const symbol = Object.keys(ETH_TOKEN_MAP).find(sym => ETH_TOKEN_MAP[sym] === addr);
        if (!symbol) continue;
        const decimals = Number(info.decimals || 18);
        const raw = Number(t.balance || 0);
        const bal = raw / Math.pow(10, decimals);
        if (bal > 0) out.push({ symbol, balance: bal });
      } catch {}
    }
    if (out.length) return out;
  } catch {}
  // Fallback: public RPC for ETH only
  try {
    const eth = await fetchEthRpcBalance(address);
    if (eth > 0) out.push({ symbol: 'ETH', balance: eth });
  } catch {}
  return out;
}

async function fetchBtcHoldings(address) {
  const url = `https://blockstream.info/api/address/${encodeURIComponent(address)}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  const chain = data.chain_stats || {};
  const mempool = data.mempool_stats || {};
  const funded = Number(chain.funded_txo_sum || 0) + Number(mempool.funded_txo_sum || 0);
  const spent = Number(chain.spent_txo_sum || 0) + Number(mempool.spent_txo_sum || 0);
  const sats = Math.max(0, funded - spent);
  const btc = sats / 1e8;
  return [{ symbol: 'BTC', balance: btc }];
}

async function fetchAdaHoldings(address) {
  // Prefer Blockfrost if key provided
  if (BLOCKFROST_KEY) {
    const url = `https://cardano-mainnet.blockfrost.io/api/v0/addresses/${encodeURIComponent(address)}`;
    const { data } = await axios.get(url, { timeout: 15000, headers: { project_id: BLOCKFROST_KEY } });
    const amt = Array.isArray(data.amount) ? data.amount : [];
    const lovelace = amt.find(a => a.unit === 'lovelace');
    const ada = lovelace ? Number(lovelace.quantity || '0') / 1e6 : 0;
    return [{ symbol: 'ADA', balance: ada }];
  }
  // Fallback to Koios public API
  try {
    const url = `https://api.koios.rest/api/v1/address_info`;
    const { data } = await axios.post(url, { _addresses: [address] }, { timeout: 15000, headers: { 'Content-Type': 'application/json' } });
    const row = Array.isArray(data) && data[0] ? data[0] : null;
    const lovelaceStr = row && (row.balance || row.value || '0');
    const lovelace = Number(lovelaceStr || '0');
    const ada = lovelace / 1e6;
    return [{ symbol: 'ADA', balance: ada }];
  } catch (e) {
    throw new Error('cardano balance unavailable');
  }
}

async function fetchCronosHoldings(address) {
  if (!CRONOSCAN_KEY) throw new Error('CronosScan API key missing');
  const url = `https://api.cronoscan.com/api?module=account&action=balance&address=${encodeURIComponent(address)}&apikey=${encodeURIComponent(CRONOSCAN_KEY)}`;
  const { data } = await axios.get(url, { timeout: 15000 });
  if (!data || data.status !== '1') throw new Error('cronos balance unavailable');
  const wei = BigInt(data.result || '0');
  const cro = Number(wei) / 1e18;
  return [{ symbol: 'CRO', balance: cro }];
}

app.get('/api/wallet/holdings', async (req, res) => {
  try {
    const chain = (req.query.chain || 'eth').toLowerCase();
    const address = String(req.query.address || '').trim();
    if (!address) return res.status(400).json({ ok: false, message: 'address required' });
    if (chain === 'eth') {
      const holdings = await fetchEthHoldings(address);
      const filtered = holdings.filter(h => ['ETH','PEPE'].includes(h.symbol));
      return res.json({ ok: true, data: filtered });
    }
    if (chain === 'btc') {
      const data = await fetchBtcHoldings(address);
      return res.json({ ok: true, data });
    }
    if (chain === 'ada') {
      const data = await fetchAdaHoldings(address);
      return res.json({ ok: true, data });
    }
    if (chain === 'cronos') {
      const data = await fetchCronosHoldings(address);
      return res.json({ ok: true, data });
    }
    return res.status(400).json({ ok: false, message: 'supported chains: eth, btc, ada, cronos' });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

async function getLatestProb(symbol, horizon = '1d') {
  const pool = await getPool();
  const [rows] = await pool.query(
    `SELECT ts, p_up, p_down, verdict, features_json FROM prob_signal WHERE symbol=? AND horizon=? ORDER BY ts DESC LIMIT 1`,
    [symbol, horizon]
  );
  if (rows.length) {
    const r = rows[0];
    return { ts: r.ts, p_up: Number(r.p_up), p_down: Number(r.p_down), verdict: r.verdict, features: r.features_json };
  }
  const [candlesRows] = await pool.query(`SELECT * FROM candles WHERE symbol=? AND timeframe='1d' ORDER BY ts ASC`, [symbol]);
  const [onchainRows] = await pool.query(`SELECT * FROM onchain_daily WHERE symbol=? ORDER BY ts ASC`, [symbol]);
  if (!candlesRows.length) return null;
  const latest = candlesRows[candlesRows.length - 1];
  const prob = computeProbabilitySnapshot(candlesRows, onchainRows);
  await upsertProbability(symbol, latest.ts, '1d', prob);
  return { ts: latest.ts, p_up: prob.p_up, p_down: prob.p_down, verdict: prob.verdict, features: prob.features };
}

async function getLatestBands(symbol) {
  const pool = await getPool();
  const [levelRows] = await pool.query(`SELECT method, bands_json, ts FROM levels WHERE symbol=? ORDER BY ts DESC`, [symbol]);
  const out = [];
  const seen = { pivot: false, swing: false, vbp: false };
  for (const r of levelRows) {
    if (seen[r.method]) continue;
    const bands = (typeof r.bands_json === 'string' ? JSON.parse(r.bands_json) : r.bands_json) || [];
    out.push(...bands.map(b => ({ ...b, method: r.method })));
    seen[r.method] = true;
    if (seen.pivot && seen.swing && seen.vbp) break;
  }
  return out;
}

function nearestBandsRelative(bands, price, direction = 'up') {
  const mid = b => (Number(b.min) + Number(b.max)) / 2;
  const arr = bands.filter(b => direction === 'up' ? mid(b) > price : mid(b) < price)
                   .sort((a, b) => Math.abs(mid(a) - price) - Math.abs(mid(b) - price));
  return arr.slice(0, 2);
}

async function fetchBinance4hKlines(binanceSymbol, limit = 120) {
  const url = `https://api.binance.com/api/v3/klines?symbol=${encodeURIComponent(binanceSymbol)}&interval=4h&limit=${limit}`;
  const { data } = await axios.get(url, { timeout: 20000 });
  return data.map(k => ({
    ts: Number(k[6]),
    open: Number(k[1]),
    high: Number(k[2]),
    low: Number(k[3]),
    close: Number(k[4]),
    volume: Number(k[5])
  }));
}

async function fetchCryptoComCandles(symbol, timeframe) {
  if (symbol !== 'CRO' && symbol !== 'SNEK') return [];
  if (symbol === 'CRO') return await fetchCryptoComCandlesCRO(timeframe);
  return await fetchCryptoComCandlesGeneric(symbol, timeframe);
}

async function getCandlesFor(symbol, tf) {
  if (tf === '1d') {
    const pool = await getPool();
    const [rows] = await pool.query(`SELECT ts, open, high, low, close, volume FROM candles WHERE symbol=? AND timeframe='1d' ORDER BY ts ASC`, [symbol]);
    return rows.map(r => ({ ts: r.ts, open: Number(r.open), high: Number(r.high), low: Number(r.low), close: Number(r.close), volume: Number(r.volume) }));
  }
  if (tf === '4h') {
    try {
      if (BINANCE_SYMBOL[symbol]) return await fetchBinance4hKlines(BINANCE_SYMBOL[symbol], 120);
    } catch {}
    try {
      const cc = await fetchCryptoComCandles(symbol, '4H');
      return cc;
    } catch {}
  }
  return [];
}

function computeProbabilityFromCandlesOnly(candles) {
  if (!candles || candles.length < 40) return { p_up: 0.5, p_down: 0.5, verdict: 'neutral', features: {} };
  // Reuse existing snapshot without onchain
  return computeProbabilitySnapshot(candles, null);
}

function buildTpsl(usdPrice, rate, bands, atr) {
  const out = { tp: [], sl: [] };
  const mid = b => (Number(b.min) + Number(b.max)) / 2;
  const upper = bands.filter(b => mid(b) > usdPrice).sort((a,b)=>mid(a)-mid(b));
  const lower = bands.filter(b => mid(b) < usdPrice).sort((a,b)=>mid(b)-mid(a));
  if (upper[0]) out.tp.push({ label: 'Band-TP1', price: mid(upper[0]) * rate });
  if (upper[1]) out.tp.push({ label: 'Band-TP2', price: mid(upper[1]) * rate });
  if (atr && atr > 0) {
    out.tp.push({ label: '+0.5ATR', price: (usdPrice + 0.5 * atr) * rate });
    out.tp.push({ label: '+1.0ATR', price: (usdPrice + 1.0 * atr) * rate });
  }
  if (lower[0]) out.sl.push({ label: 'Band-SL1', price: mid(lower[0]) * rate });
  if (atr && atr > 0) out.sl.push({ label: '-1.0ATR', price: (usdPrice - 1.0 * atr) * rate });
  return out;
}

function estimateFeePct(chainOrSymbol) {
  // Rough MVP assumed total cost (fees+slippage)
  // ETH: 0.4%, BTC: 0.15%, ADA: 0.15%, CRO(Cronos): 0.2%, others: 0.2%
  const m = { ETH: 0.004, BTC: 0.0015, ADA: 0.0015, CRO: 0.002, PEPE: 0.004, LUNC: 0.002 };
  return m[chainOrSymbol] ?? 0.002;
}

function smallestTpDistancePct(usdPrice, tps) {
  const deltas = tps.map(t => ((t.price / tps[0].price /* wrong */)));
  let minPct = null;
  for (const t of tps) {
    const pct = usdPrice ? ((t.price / (usdPrice)) - 1) * 100 : null;
    if (pct == null) continue;
    if (minPct == null || pct < minPct) minPct = pct;
  }
  return minPct;
}

app.get('/api/wallet/advice', async (req, res) => {
  try {
    const chain = (req.query.chain || 'eth').toLowerCase();
    const address = String(req.query.address || '').trim();
    const currency = (req.query.currency || 'USD').toUpperCase();
    if (!address) return res.status(400).json({ ok: false, message: 'address required' });

    const rate = await getUsdTo(currency);
    let holdings = [];
    if (chain === 'eth') holdings = (await fetchEthHoldings(address)).filter(h => ['ETH','PEPE'].includes(h.symbol));
    else if (chain === 'btc') holdings = await fetchBtcHoldings(address);
    else if (chain === 'ada') holdings = await fetchAdaHoldings(address);
    else if (chain === 'cronos') holdings = await fetchCronosHoldings(address);
    else return res.status(400).json({ ok: false, message: 'supported chains: eth, btc, ada, cronos' });

    const results = [];
    for (const h of holdings) {
      const symbol = h.symbol;
      if (!['BTC','ETH','PEPE','CRO','ADA','LUNC'].includes(symbol)) continue;
      const prob1d = await getLatestProb(symbol, '1d');
      if (!prob1d) { results.push({ symbol, advice: '資料不足', reason: '尚無 K 線/機率', balance: h.balance }); continue; }

      // Price
      let usdPrice;
      try {
        if (symbol === 'CRO') usdPrice = await fetchCryptoComTickerCRO();
        else if (BINANCE_SYMBOL[symbol]) usdPrice = await fetchBinanceTickerPrice(BINANCE_SYMBOL[symbol]);
        else usdPrice = await fetchCoingeckoSimpleUsd(symbol);
      } catch {
        const pool = await getPool();
        const [r] = await pool.query(`SELECT close FROM candles WHERE symbol=? AND timeframe='1d' ORDER BY ts DESC LIMIT 1`, [symbol]);
        usdPrice = r && r.length ? Number(r[0].close) : null;
      }
      const price = (usdPrice || 0) * rate;

      // Bands & ATR (1d)
      const bands = await getLatestBands(symbol);
      const candles1d = await getCandlesFor(symbol, '1d');
      const atr1d = calcATR(candles1d);

      // Multi-timeframe prob (4h)
      const candles4h = await getCandlesFor(symbol, '4h');
      const prob4h = candles4h.length ? computeProbabilityFromCandlesOnly(candles4h) : { p_up: 0.5, p_down: 0.5, verdict: 'neutral' };
      const upMid = bands.filter(b => ((Number(b.min)+Number(b.max))/2) > (usdPrice||0)).sort((a,b)=>((a.min+a.max)-(b.min+b.max)))[0];
      const upDistPct = upMid && usdPrice ? ((((Number(upMid.min)+Number(upMid.max))/2) - usdPrice) / usdPrice) * 100 : null;

      // Advice
      let advice = '觀望';
      let reason = '';
      if (prob1d.p_up > 0.65 && upMid) { advice = '加倉'; reason = `p_up=${prob1d.p_up.toFixed(2)}，上方價帶距離 ${upDistPct!=null?upDistPct.toFixed(1)+'%':'--'}`; }
      if (prob1d.p_down > 0.6 || (upDistPct!=null && upDistPct < 1.0)) { advice = '獲利了結'; reason = reason ? reason + '；靠近壓力帶' : '靠近壓力帶/看空機率高'; }

      // TP/SL and fee adjust
      const tpsl = buildTpsl(usdPrice || 0, rate, bands, atr1d || 0);
      const minTpPct = smallestTpDistancePct(usdPrice || 0, tpsl.tp);
      const feePct = estimateFeePct(symbol);
      const netOk = minTpPct != null ? (minTpPct > (feePct * 100 * 2)) : true; // target should beat round-trip cost

      // Consistency view
      const upPct4h = Math.round(prob4h.p_up * 100);
      const upPct1d = Math.round(prob1d.p_up * 100);
      let view = '不一致';
      if (prob4h.p_up >= 0.55 && prob1d.p_up >= 0.55) view = '一致偏多';
      else if (prob4h.p_up <= 0.45 && prob1d.p_up <= 0.45) view = '一致偏空';

      // Plain text
      const upPct = Math.round(prob1d.p_up * 100);
      let plain;
      if (advice === '加倉') {
        plain = `上漲機率約 ${upPct}%（偏多），${view}。距離上方壓力${upDistPct!=null?`約 ${upDistPct.toFixed(1)}%`:'--'}，建議分批加倉並設停損。`;
      } else if (advice === '獲利了結') {
        plain = `上漲機率約 ${upPct}%。${upDistPct!=null && upDistPct<1 ? '已非常接近壓力，' : ''}建議分批減碼。`;
      } else {
        plain = `上漲機率約 ${upPct}%（訊號不明顯，${view}）。觀望，等突破壓力或回到支撐再動作。`;
      }
      if (minTpPct != null) plain += ` 估計往上最近目標距離約 ${minTpPct.toFixed(1)}%，假設交易成本約 ${(feePct*100).toFixed(2)}%/次，${netOk? '目標大於成本，具可行性':'可能被費用吃掉，保守觀望' }。`;

      results.push({
        symbol,
        balance: h.balance,
        currency,
        price,
        verdict: prob1d.verdict,
        p_up: prob1d.p_up,
        p_down: prob1d.p_down,
        advice,
        reason,
        plain,
        tpsl,
        consistency: { p_up_4h: prob4h.p_up, p_up_1d: prob1d.p_up, view },
        fee: { est_pct: feePct, min_tp_dist_pct: minTpPct, pass: netOk }
      });
    }

    res.json({ ok: true, data: results });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

// --- News aggregator ---
async function ensureNewsTable() {
  const pool = await getPool();
  await pool.query(`CREATE TABLE IF NOT EXISTS news (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    title VARCHAR(512) NOT NULL,
    link VARCHAR(512) NOT NULL,
    source VARCHAR(128) NOT NULL,
    summary TEXT NULL,
    tags_json JSON NULL,
    title_zh VARCHAR(512) NULL,
    UNIQUE KEY uniq_link (link),
    INDEX idx_ts (ts)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // Ensure columns exist for older installs
  try { await pool.query(`ALTER TABLE news ADD COLUMN IF NOT EXISTS tags_json JSON NULL`); } catch {}
  try { await pool.query(`ALTER TABLE news ADD COLUMN IF NOT EXISTS title_zh VARCHAR(512) NULL`); } catch {}
}

function detectNewsTags(title, summary) {
  const text = `${title || ''} ${summary || ''}`.toLowerCase();
  const hasAny = (arr) => arr.some(k => text.includes(k));
  const tags = [];
  if (hasAny(['btc','bitcoin','比特幣'])) tags.push('BTC');
  if (hasAny(['eth','ethereum','以太','以太坊'])) tags.push('ETH');
  if (hasAny(['ada','cardano','艾達','卡達諾'])) tags.push('ADA');
  if (hasAny(['cro','cronos','crypto.com'])) tags.push('CRO');
  return tags;
}

function isChineseText(text) {
  if (!text) return false;
  return /[\u4e00-\u9FFF]/.test(String(text));
}

async function translateToZhTw(text) {
  if (!text) return '';
  const payload = { q: text, source: 'auto', target: 'zh-TW', format: 'text' };
  const headers = { 'Content-Type': 'application/json' };
  const endpoints = [
    'https://translate.astian.org/translate',
    'https://libretranslate.de/translate',
    'https://translate.argosopentech.com/translate'
  ];
  for (const url of endpoints) {
    try {
      const { data } = await axios.post(url, payload, { headers, timeout: 5000 });
      const out = data && (data.translatedText || data.translation || data.translated_text);
      if (out) return String(out);
    } catch {}
  }
  return text;
}

async function translateNewsRowsZh(rows) {
  if (!rows || !rows.length) return;
  const pool = await getPool();
  // translate at most 50 to bound latency
  const pending = rows.filter(r => !r.title_zh).slice(0, 50);
  for (const r of pending) {
    try {
      const zh = await translateToZhTw(r.title);
      if (zh && zh !== r.title) {
        const clipped = zh.slice(0, 512);
        await pool.query(`UPDATE news SET title_zh=? WHERE link=?`, [clipped, r.link]);
        r.title_zh = clipped;
      }
    } catch {}
  }
}

async function backfillNewsTags(limit = 500) {
  try {
    const pool = await getPool();
    const [rows] = await pool.query(`SELECT id, title, summary, tags_json FROM news ORDER BY ts DESC LIMIT ?`, [limit]);
    for (const r of rows) {
      const tags = r.tags_json ? (typeof r.tags_json === 'string' ? JSON.parse(r.tags_json) : r.tags_json) : [];
      if (tags && tags.length) continue;
      const newTags = detectNewsTags(r.title, r.summary);
      await pool.query(`UPDATE news SET tags_json=? WHERE id=?`, [JSON.stringify(newTags), r.id]);
    }
  } catch {}
}

async function insertNews(items) {
  if (!items.length) return;
  const pool = await getPool();
  const sql = `INSERT IGNORE INTO news (ts, title, link, source, summary, tags_json) VALUES ${items.map(()=> '(?,?,?,?,?,?)').join(',')}`;
  const params = [];
  for (const it of items) params.push(it.ts, it.title, it.link, it.source, it.summary || null, JSON.stringify(it.tags || []));
  await pool.query(sql, params);
}

async function fetchRssFeed(url, source) {
  const Parser = (await import('rss-parser')).default;
  const parser = new Parser();
  const feed = await parser.parseURL(url);
  return (feed.items || []).map(i => ({
    ts: i.isoDate ? Date.parse(i.isoDate) : (i.pubDate ? Date.parse(i.pubDate) : Date.now()),
    title: i.title || '(no title)',
    link: i.link || '',
    source,
    summary: (i.contentSnippet || i.content || '').slice(0, 500)
  }));
}

async function fetchFirstRss(urlCandidates, sourceLabel) {
  for (const u of urlCandidates) {
    try { return await fetchRssFeed(u, sourceLabel); } catch { /* try next */ }
  }
  return [];
}

async function fetchCryptoNews() {
  // 中文來源優先
  const tasks = [
    fetchFirstRss([
      'https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml&language=zh',
      'https://www.coindesk.com/zh/arc/outboundfeeds/rss/?outputType=xml',
      'https://www.coindesk.com/zh/feed'
    ], 'CoinDesk(中文)'),
    fetchFirstRss([
      'https://cn.cointelegraph.com/rss'
    ], 'Cointelegraph(中文)'),
    fetchFirstRss([
      'https://news.bitcoin.com/zh-hant/feed/',
      'https://news.bitcoin.com/zh/feed/',
      'https://news.bitcoin.com/feed/'
    ], 'Bitcoin.com(中文)')
  ];
  let all = [];
  try {
    const results = await Promise.all(tasks.map(p => p.catch(()=>[])));
    for (const arr of results) all = all.concat(arr);
  } catch {}
  all = all
    .map(n => ({ ...n, tags: detectNewsTags(n.title, n.summary) }))
    .filter(n => n.link)
    .filter(n => isChineseText(n.title) || isChineseText(n.summary) || /\b(zh|zh-hant)\b/i.test(n.link) || /cn\./i.test(n.link));
  all.sort((a,b)=>b.ts-a.ts);
  all = all.slice(0, 300);
  await insertNews(all);
  await backfillNewsTags(800);
}

app.get('/api/news', async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(300, Number(req.query.limit || 80)));
    const grouped = (req.query.grouped || '0') === '1';
    const pool = await getPool();
    await ensureNewsTable();
    let rowsRaw = [];
    try {
      const [r] = await pool.query(`SELECT ts, title, link, source, summary, tags_json FROM news ORDER BY ts DESC LIMIT ?`, [limit]);
      rowsRaw = r;
    } catch (e) {
      // On fresh deploy with no table yet fully ready, return empty list gracefully
      rowsRaw = [];
    }
    const rows = rowsRaw.filter(r => isChineseText(r.title) || isChineseText(r.summary) || /\b(zh|zh-hant)\b/i.test(r.link) || /cn\./i.test(r.link) || /中文/.test(r.source || ''));
    if (!grouped) {
      return res.json({ ok: true, data: rows });
    }
    const out = { BTC: [], ETH: [], ADA: [], CRO: [], OTHER: [] };
    for (const r of rows) {
      const tags = typeof r.tags_json === 'string' ? JSON.parse(r.tags_json) : (r.tags_json || []);
      let pushed = false;
      for (const t of ['BTC','ETH','ADA','CRO']) {
        if (tags && tags.includes(t)) { out[t].push({ ts: r.ts, title: r.title, link: r.link, source: r.source, summary: r.summary }); pushed = true; }
      }
      if (!pushed) out.OTHER.push({ ts: r.ts, title: r.title, link: r.link, source: r.source, summary: r.summary });
    }
    return res.json({ ok: true, data: out });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

app.get('/api/news/refresh', async (req, res) => {
  try {
    await ensureNewsTable();
    await fetchCryptoNews();
    await backfillNewsTags(800);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

// Schedule: Every day 08:00 Asia/Taipei
aSyncInitNews();
async function aSyncInitNews(){ try { await ensureNewsTable(); await backfillNewsTags(800); } catch {} }
cron.schedule('0 8 * * *', async () => {
  try { await fetchCryptoNews(); } catch (e) { console.error('News cron error:', e.message); }
}, { timezone: 'Asia/Taipei' });
// also refresh every 15 minutes
cron.schedule('*/15 * * * *', async () => {
  try { await fetchCryptoNews(); } catch (e) { console.error('News cron error (15m):', e.message); }
}, { timezone: 'Asia/Taipei' });
// also first boot fetch to populate
(async () => { try { await fetchCryptoNews(); } catch {} })();
// --- end news ---

// --- Recommendations (daily top 10) ---
async function ensureRecomTable() {
  const pool = await getPool();
  await pool.query(`CREATE TABLE IF NOT EXISTS recommendations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    cg_id VARCHAR(128) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(128) NOT NULL,
    score DECIMAL(10,6) NOT NULL,
    price_usd DECIMAL(28,10) NOT NULL,
    change24h DECIMAL(10,4) NULL,
    change7d DECIMAL(10,4) NULL,
    market_cap_usd DECIMAL(28,2) NULL,
    volume24h_usd DECIMAL(28,2) NULL,
    image VARCHAR(512) NULL,
    UNIQUE KEY uniq_ts_id (ts, cg_id),
    INDEX idx_ts_score (ts, score)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
}

function normalizePct(p) { if (p == null || !isFinite(p)) return 0; return Math.max(-50, Math.min(50, p)) / 50; }
function clamp01(x) { return Math.max(0, Math.min(1, x)); }

async function computeRecommendations() {
  // Fetch top 250 markets to score
  const url = `${COINGECKO_BASE}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false&price_change_percentage=24h,7d,30d`;
  const { data } = await axios.get(url, { timeout: 25000 });
  const now = Date.now();
  const excludeIds = new Set(['tether','usd-coin','binance-usd','dai','first-digital-usd','true-usd','usdd','frax','paxos-standard']);
  const scored = (data || [])
    .filter(c => c && c.current_price && c.market_cap && !excludeIds.has(c.id))
    .map((c, idx) => {
      const ch24 = c.price_change_percentage_24h;
      const ch7 = (c.price_change_percentage_7d_in_currency ?? c.price_change_percentage_7d) || 0;
      const vol = Number(c.total_volume || 0);
      const mcap = Number(c.market_cap || 1);
      const volRatio = Math.min(1, vol / Math.max(1, mcap)); // 0..1
      const sizeScore = 1 - clamp01(Math.log10(mcap + 1) / 12); // 小市值略加權，但避免極端
      const momentum = 0.55 * normalizePct(ch7) + 0.35 * normalizePct(ch24);
      const quality = 0.10 * volRatio;
      const score = momentum + quality + 0.05 * sizeScore;
      return {
        ts: now,
        cg_id: c.id,
        symbol: String(c.symbol || '').toUpperCase(),
        name: c.name,
        score,
        price_usd: Number(c.current_price || 0),
        change24h: ch24 ?? null,
        change7d: ch7 ?? null,
        market_cap_usd: mcap,
        volume24h_usd: vol,
        image: c.image
      };
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, 50);
  const pool = await getPool();
  await ensureRecomTable();
  if (scored.length) {
    const sql = `INSERT INTO recommendations (ts, cg_id, symbol, name, score, price_usd, change24h, change7d, market_cap_usd, volume24h_usd, image)
                 VALUES ${scored.map(()=> '(?,?,?,?,?,?,?,?,?,?,?)').join(',')}`;
    const params = [];
    for (const r of scored) params.push(r.ts, r.cg_id, r.symbol, r.name, r.score, r.price_usd, r.change24h, r.change7d, r.market_cap_usd, r.volume24h_usd, r.image);
    await pool.query(sql, params);
  }
}

app.get('/api/recommendations', async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(50, Number(req.query.limit || 10)));
    const currency = (req.query.currency || 'USD').toUpperCase();
    const rate = await getUsdTo(currency);
    const pool = await getPool();
    await ensureRecomTable();
    const [rows] = await pool.query(`SELECT ts, cg_id, symbol, name, score, price_usd, change24h, change7d, market_cap_usd, volume24h_usd, image FROM recommendations ORDER BY ts DESC, score DESC LIMIT ?`, [limit]);
    const data = rows.map(r => ({
      ts: r.ts,
      id: r.cg_id,
      symbol: r.symbol,
      name: r.name,
      score: Number(r.score),
      price: Number(r.price_usd) * rate,
      change24h: r.change24h != null ? Number(r.change24h) : null,
      change7d: r.change7d != null ? Number(r.change7d) : null,
      market_cap: Number(r.market_cap_usd || 0) * rate,
      volume24h: Number(r.volume24h_usd || 0) * rate,
      image: r.image
    }));
    res.json({ ok: true, data: { currency, list: data } });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

// schedule daily recompute
cron.schedule('5 8 * * *', async () => {
  try { await computeRecommendations(); } catch (e) { console.error('Recom cron error:', e.message); }
}, { timezone: 'Asia/Taipei' });
// prime on boot
(async () => { try { await computeRecommendations(); } catch {} })();
// --- end recommendations ---

// --- Paper trading (no auth, local MVP) ---
async function ensurePaperTable() {
  const pool = await getPool();
  await pool.query(`CREATE TABLE IF NOT EXISTS paper_trades (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_open BIGINT NOT NULL,
    ts_close BIGINT NULL,
    symbol VARCHAR(10) NOT NULL,
    side ENUM('long','short') NOT NULL,
    entry DECIMAL(28,10) NOT NULL,
    qty DECIMAL(28,10) NOT NULL,
    tp DECIMAL(28,10) NULL,
    sl DECIMAL(28,10) NULL,
    close_price DECIMAL(28,10) NULL,
    pnl DECIMAL(28,10) NULL,
    room VARCHAR(64) NULL,
    status ENUM('open','closed') NOT NULL DEFAULT 'open',
    INDEX idx_open (status, ts_open)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
  // backfill for older installs
  try { await pool.query(`ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS room VARCHAR(64) NULL`); } catch {}
}

function calcPnl(side, entry, close, qty) {
  const diff = side === 'long' ? (close - entry) : (entry - close);
  return diff * qty;
}

app.get('/api/paper/trades', async (req, res) => {
  try {
    const currency = (req.query.currency || 'USD').toUpperCase();
    const rate = await getUsdTo(currency);
    const pool = await getPool();
    await ensurePaperTable();
    const room = String(req.query.room || '').trim();
    let rows;
    if (room) {
      [rows] = await pool.query(`SELECT * FROM paper_trades WHERE room=? ORDER BY ts_open DESC LIMIT 200`, [room]);
    } else {
      [rows] = await pool.query(`SELECT * FROM paper_trades ORDER BY ts_open DESC LIMIT 200`);
    }
    const out = [];
    for (const r of rows) {
      const row = { ...r };
      const entry = Number(r.entry);
      const qty = Number(r.qty);
      if (r.status === 'open') {
        // get live price in USD
        let usdPrice;
        const sym = r.symbol;
        try {
          usdPrice = sym === 'CRO' ? await fetchCryptoComTickerCRO() : (BINANCE_SYMBOL[sym] ? await fetchBinanceTickerPrice(BINANCE_SYMBOL[sym]) : await fetchCoingeckoSimpleUsd(sym));
        } catch { usdPrice = entry; }
        const uPnL = calcPnl(r.side, entry, usdPrice, qty);
        row.current_price = usdPrice;
        row.unrealized_pnl = uPnL;
        row.unrealized_pnl_conv = uPnL * rate;
      } else {
        const realized = Number(r.pnl || 0);
        row.pnl_conv = realized * rate;
        row.close_price_conv = (r.close_price != null) ? Number(r.close_price) * rate : null;
      }
      row.entry_conv = entry * rate;
      row.currency = currency;
      out.push(row);
    }
    res.json({ ok: true, data: out, currency });
  } catch (e) { res.status(500).json({ ok: false, message: e.message }); }
});

app.post('/api/paper/trades', async (req, res) => {
  try {
    const { symbol, side, entry, qty, tp, sl, room } = req.body || {};
    if (!symbol || !side || entry == null || !qty) return res.status(400).json({ ok: false, message: 'missing fields' });
    const pool = await getPool();
    await ensurePaperTable();
    const ts = Date.now();
    await pool.query(`INSERT INTO paper_trades (ts_open, symbol, side, entry, qty, tp, sl, room) VALUES (?,?,?,?,?,?,?,?)`, [ts, symbol.toUpperCase(), side, entry, qty, tp ?? null, sl ?? null, room ?? null]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, message: e.message }); }
});

app.post('/api/paper/close', async (req, res) => {
  try {
    const { id, price } = req.body || {};
    if (!id) return res.status(400).json({ ok: false, message: 'id required' });
    const pool = await getPool();
    await ensurePaperTable();
    const [[row]] = await pool.query(`SELECT * FROM paper_trades WHERE id=?`, [id]);
    if (!row || row.status !== 'open') return res.status(404).json({ ok: false, message: 'not open' });
    let closePrice = price;
    if (closePrice == null) {
      const sym = row.symbol;
      let usdPrice;
      try { usdPrice = sym === 'CRO' ? await fetchCryptoComTickerCRO() : (BINANCE_SYMBOL[sym] ? await fetchBinanceTickerPrice(BINANCE_SYMBOL[sym]) : await fetchCoingeckoSimpleUsd(sym)); }
      catch { usdPrice = Number(row.entry); }
      closePrice = usdPrice;
    }
    const pnl = calcPnl(row.side, Number(row.entry), Number(closePrice), Number(row.qty));
    await pool.query(`UPDATE paper_trades SET ts_close=?, close_price=?, pnl=?, status='closed' WHERE id=?`, [Date.now(), closePrice, pnl, id]);
    res.json({ ok: true, data: { id, close_price: closePrice, pnl } });
  } catch (e) { res.status(500).json({ ok: false, message: e.message }); }
});

app.get('/api/paper/metrics', async (req, res) => {
  try {
    const currency = (req.query.currency || 'USD').toUpperCase();
    const rate = await getUsdTo(currency);
    const pool = await getPool();
    await ensurePaperTable();
    const [closed] = await pool.query(`SELECT * FROM paper_trades WHERE status='closed' ORDER BY ts_close ASC`);
    const [open] = await pool.query(`SELECT * FROM paper_trades WHERE status='open' ORDER BY ts_open ASC`);
    let wins = 0, equity = 0, peak = 0, maxdd = 0; const curve = [];
    for (const r of closed) {
      const realized = Number(r.pnl || 0);
      equity += realized;
      if (realized >= 0) wins++; 
      peak = Math.max(peak, equity);
      maxdd = Math.min(maxdd, equity - peak);
      curve.push({ ts: r.ts_close, equity: equity * rate });
    }
    // unrealized sum for info
    let unrealized = 0;
    for (const r of open) {
      const entry = Number(r.entry), qty = Number(r.qty);
      let usdPrice = entry;
      const sym = r.symbol;
      try { usdPrice = sym === 'CRO' ? await fetchCryptoComTickerCRO() : (BINANCE_SYMBOL[sym] ? await fetchBinanceTickerPrice(BINANCE_SYMBOL[sym]) : await fetchCoingeckoSimpleUsd(sym)); } catch {}
      unrealized += calcPnl(r.side, entry, usdPrice, qty);
    }
    const trades = closed.length;
    const winrate = trades ? wins / trades : 0;
    res.json({ ok: true, data: { currency, winrate, trades, pnl: equity * rate, max_drawdown: maxdd * rate, unrealized_pnl: unrealized * rate, curve } });
  } catch (e) { res.status(500).json({ ok: false, message: e.message }); }
});
// --- end Paper trading & DCA ---

// DCA calculator: equal amount buy on each step, compute avg cost, units, current value
app.get('/api/tools/dca', async (req, res) => {
  try {
    const symbol = String(req.query.symbol || 'BTC').toUpperCase();
    const currency = (req.query.currency || 'USD').toUpperCase();
    const periods = Math.max(1, Math.min(365, Number(req.query.periods || 12)));
    const amount = Math.max(0, Number(req.query.amount || 100)); // per period amount in USD
    const freq = String(req.query.freq || '1w'); // '1d' | '1w' | '1m'
    const step = freq === '1d' ? 1 : (freq === '1m' ? 30 : 7);

    const pool = await getPool();
    const [rows] = await pool.query(`SELECT ts, close FROM candles WHERE symbol=? AND timeframe='1d' ORDER BY ts DESC LIMIT ?`, [symbol, periods * step + 1]);
    const listDesc = rows; // already DESC
    if (!listDesc.length) return res.status(404).json({ ok: false, message: 'no candles' });

    // pick every 'step' from the tail so oldest first
    const picked = [];
    for (let i = listDesc.length - 1; i >= 0 && picked.length < periods; i -= step) {
      picked.push({ ts: listDesc[i].ts, close: Number(listDesc[i].close) });
    }
    picked.reverse();
    if (!picked.length) return res.status(404).json({ ok: false, message: 'not enough data' });

    let totalInvest = 0;
    let totalUnits = 0;
    for (const p of picked) {
      const units = amount / p.close;
      totalInvest += amount;
      totalUnits += units;
    }
    const avgCost = totalInvest / totalUnits; // USD per unit

    const latestClose = Number(listDesc[0].close);
    const currentValue = totalUnits * latestClose;
    const pnlAmt = currentValue - totalInvest;
    const retPct = totalInvest > 0 ? (pnlAmt / totalInvest) * 100 : 0;

    const rate = await getUsdTo(currency);
    res.json({ ok: true, data: {
      symbol, freq, periods,
      amount_per_period: amount * rate,
      currency,
      buys: picked.length,
      avg_cost: avgCost * rate,
      total_invested: totalInvest * rate,
      total_units: totalUnits,
      latest_price: latestClose * rate,
      current_value: currentValue * rate,
      pnl_amount: pnlAmt * rate,
      return_pct: retPct
    }});
  } catch (e) { res.status(500).json({ ok: false, message: e.message }); }
});
// --- end Paper trading & DCA ---

const PORT = Number(process.env.PORT || 8080);
app.listen(PORT, async () => {
  console.log(`Server running on http://localhost:${PORT}`);
  try { await ensureInitialData(); } catch (e) { console.error('Initial data task failed:', e.message); }
}); 