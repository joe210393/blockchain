const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });
const axios = require('axios');
const mysql = require('mysql2/promise');

const COIN_MAP = [
  { symbol: 'BTC', coingecko_id: 'bitcoin' },
  { symbol: 'ETH', coingecko_id: 'ethereum' },
  { symbol: 'ADA', coingecko_id: 'cardano' },
  { symbol: 'CRO', coingecko_id: 'cronos' },
  { symbol: 'PEPE', coingecko_id: 'pepe' },
  { symbol: 'LUNC', coingecko_id: 'terra-luna' }
];
const SYMBOLS = new Set(COIN_MAP.map(c => c.symbol));
const SYMBOL_TO_ID = COIN_MAP.reduce((acc, c) => { acc[c.symbol] = c.coingecko_id; return acc; }, {});

let pool;
async function getPool() {
  if (!pool) {
    const socketPath = process.env.MYSQL_SOCKET;
    const base = {
      user: process.env.MYSQL_USER || 'root',
      password: process.env.MYSQL_PASSWORD || '',
      database: process.env.MYSQL_DATABASE || 'web3_mvp',
      waitForConnections: true,
      connectionLimit: 10,
      namedPlaceholders: true
    };
    pool = mysql.createPool(
      socketPath
        ? { ...base, socketPath }
        : { ...base, host: process.env.MYSQL_HOST || 'localhost', port: Number(process.env.MYSQL_PORT || 3306) }
    );
  }
  return pool;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fetchWithRetry(reqFn, retries = 2, delayMs = 12000) {
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try { return await reqFn(); } catch (e) {
      lastErr = e;
      const status = e.response?.status;
      if (status === 429 || status === 502 || status === 503) {
        const wait = delayMs * (i + 1);
        console.log(`Got ${status}, retry in ${wait}ms...`);
        await sleep(wait);
      } else {
        break;
      }
    }
  }
  throw lastErr;
}

const COINGECKO_BASE = process.env.COINGECKO_BASE || 'https://api.coingecko.com/api/v3';
async function fetchDailyOHLC(idOrIds, days = 90) {
  const ids = Array.isArray(idOrIds) ? idOrIds : [idOrIds];
  for (const id of ids) {
    try {
      const url = `${COINGECKO_BASE}/coins/${id}/ohlc?vs_currency=usd&days=${days}`;
      const { data } = await fetchWithRetry(() => axios.get(url, { timeout: 20000 }));
      return data.map(row => ({ ts: row[0], open: row[1], high: row[2], low: row[3], close: row[4] }));
    } catch (e) {
      if (e.response && e.response.status === 404) {
        const mc = `${COINGECKO_BASE}/coins/${id}/market_chart?vs_currency=usd&days=${days}&interval=daily`;
        try {
          const { data } = await fetchWithRetry(() => axios.get(mc, { timeout: 20000 }));
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
      const url = `${COINGECKO_BASE}/coins/${id}/market_chart?vs_currency=usd&days=${days}&interval=daily`;
      const { data } = await fetchWithRetry(() => axios.get(url, { timeout: 20000 }));
      return (data.total_volumes || []).map(row => ({ ts: row[0], volume: row[1] }));
    } catch (e) {
      if (!(e.response && e.response.status === 404)) throw e;
    }
  }
  return [];
}

async function fetchCryptoComCandlesCRO(timeframe = '1D') {
  const url = `https://api.crypto.com/v2/public/get-candlestick?instrument_name=CRO_USDT&timeframe=${encodeURIComponent(timeframe)}`;
  const { data } = await fetchWithRetry(() => axios.get(url, { timeout: 20000 }));
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

function calcEMA(values, period) {
  if (values.length === 0) return [];
  const k = 2 / (period + 1);
  const ema = [];
  let prev;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (i === 0) prev = v; else prev = v * k + prev * (1 - k);
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

function computePivotLevels(latestCandle) {
  const H = Number(latestCandle.high), L = Number(latestCandle.low), C = Number(latestCandle.close);
  const P = (H + L + C) / 3;
  const R1 = 2 * P - L; const S1 = 2 * P - H;
  const R2 = P + (H - L); const S2 = P - (H - L);
  const width = C * 0.003;
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
    if ((p - last.max) / last.max <= 0.01) {
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
    rsi: rsi[last], macd: macdLine[last], macd_signal: signalLine[last], macd_hist: hist[last],
    ma20_slope: ma20Slope, ma50_slope: ma50Slope, volume_z: volZ[last]
  };
  if (onchainRows && onchainRows.length > 5) {
    const growthActive = (onchainRows[onchainRows.length - 1].active_addr || 0) - (onchainRows[onchainRows.length - 6].active_addr || 0);
    const growthTx = (onchainRows[onchainRows.length - 1].tx_count || 0) - (onchainRows[onchainRows.length - 6].tx_count || 0);
    features.active_growth_5d = growthActive;
    features.tx_growth_5d = growthTx;
  }
  let score = 0;
  if (features.rsi != null) { if (features.rsi > 55) score += 0.15; else if (features.rsi < 45) score -= 0.15; }
  if (features.macd != null && features.macd_signal != null) { if (features.macd > features.macd_signal) score += 0.2; else score -= 0.2; }
  if (features.ma20_slope > 0) score += 0.2; else score -= 0.1;
  if (features.ma50_slope > 0) score += 0.1; else score -= 0.1;
  if (features.volume_z != null) { if (features.volume_z > 1) score += 0.1; }
  if (features.active_growth_5d && features.active_growth_5d > 0) score += 0.1;
  if (features.tx_growth_5d && features.tx_growth_5d > 0) score += 0.05;
  const clamp = (x, min, max) => Math.max(min, Math.min(max, x));
  const p_up = clamp(0.5 + score, 0.01, 0.99);
  const p_down = 1 - p_up;
  const verdict = p_up > 0.6 ? 'bull' : (p_up < 0.4 ? 'bear' : 'neutral');
  return { p_up, p_down, verdict, features };
}

async function upsertLevels(symbol, ts, method, bands) {
  const pool = await getPool();
  await pool.query(`INSERT INTO levels (symbol, ts, method, bands_json) VALUES (?,?,?,?)`, [symbol, ts, method, JSON.stringify(bands)]);
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
  for (const r of rows) params.push(symbol, r.ts, r.active_addr, r.tx_count, r.gas_used, r.stable_netflow, r.whale_tx);
  await pool.query(sql, params);
}
function genOnchainMock(tsArray, seed = 1) {
  let s = seed; function rnd() { s = (s * 9301 + 49297) % 233280; return s / 233280; }
  let baseActive = 500000 + rnd() * 200000;
  let baseTx = 200000 + rnd() * 100000;
  let baseGas = 1e12 + rnd() * 2e12;
  const rows = [];
  for (const ts of tsArray) {
    baseActive *= 0.999 + rnd() * 0.002;
    baseTx *= 0.999 + rnd() * 0.002;
    baseGas *= 0.999 + rnd() * 0.002;
    const stableNet = (rnd() - 0.5) * 5e7;
    const whales = Math.floor(50 + rnd() * 100);
    rows.push({ ts, active_addr: Math.round(baseActive), tx_count: Math.round(baseTx), gas_used: Math.round(baseGas), stable_netflow: Math.round(stableNet), whale_tx: whales });
  }
  return rows;
}

function candidateIdsForSymbol(symbol) {
  if (symbol === 'CRO') return ['cronos', 'crypto-com-chain'];
  return [SYMBOL_TO_ID[symbol]];
}

async function syncSymbol(symbol) {
  const ids = candidateIdsForSymbol(symbol);
  console.log(`Syncing ${symbol}...`);
  let ohlc;
  let vols = [];
  if (symbol === 'CRO') {
    try { ohlc = await fetchCryptoComCandlesCRO('1D'); } catch {}
  }
  if (!ohlc) {
    ohlc = await fetchDailyOHLC(ids, 90);
    await sleep(1200);
    vols = await fetchDailyVolumes(ids, 90);
  }
  const volMap = new Map(vols.map(v => [new Date(v.ts).toDateString(), v.volume]));
  const merged = ohlc.map(c => ({ ...c, volume: volMap.get(new Date(c.ts).toDateString()) || c.volume || 0 }));
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
  const onchainRows = genOnchainMock(tsArray, symbol.charCodeAt(0));
  await upsertOnchainMock(symbol, onchainRows);
  const prob = computeProbabilitySnapshot(rows, onchainRows);
  await upsertProbability(symbol, latest.ts, '1d', prob);
}

(async () => {
  try {
    const args = process.argv.slice(2).map(s => s.toUpperCase());
    const list = args.length ? args.filter(s => SYMBOLS.has(s)) : Array.from(SYMBOLS);
    if (!list.length) { console.log('No valid symbols provided.'); process.exit(1); }
    for (const sym of list) {
      try {
        await syncSymbol(sym);
        await sleep(2000);
      } catch (e) {
        console.error(`Sync failed for ${sym}:`, e.message);
      }
    }
    console.log('Sync finished.');
    process.exit(0);
  } catch (e) {
    console.error('Fatal:', e.message);
    process.exit(1);
  }
})();
