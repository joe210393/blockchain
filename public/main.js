const symbolEl = document.getElementById('symbol');
const intervalEl = document.getElementById('interval');
const currencyEl = document.getElementById('currency');
const chartDom = document.getElementById('kchart');
const gaugeDom = document.getElementById('gauge');
const verdictEl = document.getElementById('verdict');
const livebarEl = document.getElementById('livebar');

const kChart = echarts.init(chartDom);
const gaugeChart = echarts.init(gaugeDom);
const paperSparkDom = document.getElementById('paperSpark');
const perfSparkDom = document.getElementById('perfSpark');
let paperSparkChart = paperSparkDom ? echarts.init(paperSparkDom) : null;
let perfSparkChart = perfSparkDom ? echarts.init(perfSparkDom) : null;
const rainbowDom = document.getElementById('rainbowChart');
let rainbowChart = rainbowDom ? echarts.init(rainbowDom) : null;
const fngDom = document.getElementById('fngGauge');
let fngChart = fngDom ? echarts.init(fngDom) : null;

async function fetchJson(url, opts = {}) {
  const res = await fetch(url, opts);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

async function postJson(url, body) {
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function renderGauge(pUp) {
  gaugeChart.setOption({
    series: [{
      type: 'gauge',
      min: 0,
      max: 100,
      detail: { valueAnimation: true, formatter: '{value}%'},
      data: [{ value: Math.round(pUp * 100) }]
    }]
  });
}

function renderFng(value, classification) {
  if (!fngChart) { const dom = document.getElementById('fngGauge'); if (dom) fngChart = echarts.init(dom); else return; }
  const v = Number(value || 0);
  const bands = [
    { max: 25, color: '#ef4444' },   // Extreme Fear
    { max: 45, color: '#f59e0b' },   // Fear
    { max: 55, color: '#10b981' },   // Neutral
    { max: 75, color: '#3b82f6' },   // Greed
    { max: 100, color: '#7c3aed' }   // Extreme Greed
  ];
  const colors = [];
  let last = 0;
  for (const b of bands) {
    const span = (b.max - last) / 100;
    colors.push([b.max/100, b.color]);
    last = b.max;
  }
  fngChart.setOption({
    series: [{
      type: 'gauge',
      startAngle: 200,
      endAngle: -20,
      min: 0,
      max: 100,
      splitNumber: 5,
      axisLine: { lineStyle: { width: 10, color: colors } },
      pointer: { itemStyle: { color: '#111827' } },
      axisTick: { show: false },
      splitLine: { length: 10, lineStyle: { color: '#999' } },
      axisLabel: { distance: 12, color: '#666', fontSize: 10 },
      detail: { valueAnimation: true, formatter: '{value}', offsetCenter: [0, '60%'], color: '#111827' },
      title: { show: true, offsetCenter: [0, '40%'], color: '#6b7280', fontSize: 12 },
      data: [{ value: Math.round(isFinite(v) ? v : 0), name: classification || '--' }]
    }]
  });
  const badge = document.getElementById('fngBadge');
  if (badge) badge.textContent = isFinite(v) ? `Index ${Math.round(v)} · ${classification}` : 'N/A';
}

async function loadFng() {
  try {
    const res = await fetchJson('/api/market/fear_greed');
    const d = res.data || {};
    renderFng(d.value, d.classification);
  } catch (e) {
    renderFng(null, 'N/A');
  }
}


function renderK(candles, levelData) {
  const category = candles.map(c => new Date(c.ts).toISOString().substring(0, 10));
  const values = candles.map(c => [Number(c.open), Number(c.close), Number(c.low), Number(c.high)]);
  const series = [{ type: 'candlestick', data: values, name: 'OHLC' }];
  const bands = [];
  const showPivot = document.getElementById('togglePivot')?.checked !== false;
  const showSwing = document.getElementById('toggleSwing')?.checked !== false;
  const showVbp = document.getElementById('toggleVbp')?.checked !== false;
  if (levelData) {
    const all = [];
    if (levelData.pivot && showPivot) all.push(...levelData.pivot.map(b => ({ ...b, method: 'pivot' })));
    if (levelData.swing && showSwing) all.push(...levelData.swing.map(b => ({ ...b, method: 'swing' })));
    if (levelData.vbp && showVbp) all.push(...levelData.vbp.map(b => ({ ...b, method: 'vbp' })));
    for (const b of all) {
      const y = (Number(b.min) + Number(b.max)) / 2;
      bands.push({ yAxis: y, lineStyle: { color: b.method === 'pivot' ? '#3b82f6' : b.method === 'swing' ? '#22c55e' : '#f59e0b', width: 1, type: 'dashed' } });
    }
  }
  series[0].markLine = { data: bands };

  kChart.setOption({
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category', data: category, boundaryGap: true },
    yAxis: { scale: true },
    series,
    axisPointer: { link: [{ xAxisIndex: [0] }] },
    dataZoom: [{ type: 'inside', start: Math.max(0, 100 - (50 / Math.max(candles.length,1)) * 100), end: 100 }, { start: Math.max(0, 100 - (50 / Math.max(candles.length,1)) * 100), end: 100 }]
  });
}

function verdictText(verdict) {
  if (verdict === 'bull') return '上漲';
  if (verdict === 'bear') return '下跌';
  return '中立';
}

function renderTargets(targets) {
  const wrapId = 'targets';
  let wrap = document.getElementById(wrapId);
  if (!wrap) {
    wrap = document.createElement('div');
    wrap.id = wrapId;
    wrap.style.marginTop = '8px';
    gaugeDom.parentElement.appendChild(wrap);
  }
  if (!targets || !targets.length) { wrap.innerHTML = ''; return; }
  wrap.innerHTML = targets.map(t => {
    if (t.type === 'band') {
      const cls = t.method === 'pivot' ? 'band-pivot' : t.method === 'swing' ? 'band-swing' : 'band-vbp';
      return `<div class="badge ${cls}">價帶(${t.method})：${t.min.toFixed(2)} ~ ${t.max.toFixed(2)}</div>`;
    } else {
      return `<div class="badge atr">ATR 目標 ${t.label}：${t.price.toFixed(2)}</div>`;
    }
  }).join('');
}

async function updateLivebar() {
  const sym = symbolEl.value;
  const currency = currencyEl.value;
  try {
    const res = await fetchJson(`/api/price?symbol=${sym}&currency=${currency}`);
    const px = Number(res.data.price);
    const formatted = px >= 1 ? px.toLocaleString(undefined, { maximumFractionDigits: 2 }) : px.toPrecision(6);
    livebarEl.innerHTML = `${sym} 現價：${formatted} ${res.data.currency}<small>每30秒自動更新</small>`;
  } catch (e) {
    livebarEl.textContent = '即時價取得失敗';
  }
}

function bindHelp() {
  const helpbox = document.getElementById('helpbox');
  document.querySelectorAll('.q').forEach(el => {
    el.addEventListener('click', () => {
      const msg = el.getAttribute('data-help') || '';
      if (!msg) return;
      helpbox.textContent = msg;
      helpbox.style.display = 'block';
    });
  });
}

let liveTimer = null;
function startLivebar() {
  if (liveTimer) clearInterval(liveTimer);
  updateLivebar();
  liveTimer = setInterval(updateLivebar, 30000);
}

function formatThousands(val, maxFractionDigits = 0) {
  if (val === null || val === undefined) return '--';
  const num = Number(val);
  if (!Number.isFinite(num)) return String(val);
  const opts = { maximumFractionDigits: maxFractionDigits };
  return num.toLocaleString(undefined, opts);
}

function fmt(val, digits = 2) {
  const n = Number(val);
  if (!Number.isFinite(n)) return String(val);
  return n >= 1 ? n.toLocaleString(undefined, { maximumFractionDigits: digits }) : n.toPrecision(6);
}

function signedPct(v, digits = 1) {
  if (v == null || !isFinite(v)) return '--';
  const s = v >= 0 ? '+' : '';
  return `${s}${v.toFixed(digits)}%`;
}

async function loadWallet() {
  const chain = document.getElementById('walletChain').value;
  const address = document.getElementById('walletAddress').value.trim();
  const currency = currencyEl.value;
  const holdBox = document.getElementById('walletHoldings');
  const adviceBox = document.getElementById('walletAdvice');
  if (!address) { holdBox.textContent = '請輸入地址'; adviceBox.textContent = ''; return; }
  try {
    holdBox.textContent = '讀取中...'; adviceBox.textContent = '';
    const [h, a] = await Promise.all([
      fetchJson(`/api/wallet/holdings?chain=${chain}&address=${encodeURIComponent(address)}`),
      fetchJson(`/api/wallet/advice?chain=${chain}&address=${encodeURIComponent(address)}&currency=${currency}`)
    ]);
    const holdings = h.data || [];
    if (!holdings.length) holdBox.textContent = '無持倉或不支援的資產';
    else holdBox.innerHTML = holdings.map(x => `${x.symbol}: ${fmt(x.balance, 6)}`).join(' · ');

    const adv = a.data || [];
    if (!adv.length) { adviceBox.textContent = '無可用建議'; return; }
    adviceBox.innerHTML = adv.map(x => {
      const tps = (x.tpsl && x.tpsl.tp || []).map(t => `${t.label}:${fmt(t.price)}`).join(' / ');
      const sls = (x.tpsl && x.tpsl.sl || []).map(t => `${t.label}:${fmt(t.price)}`).join(' / ');
      const cons = x.consistency ? `多時窗一致性：4h上漲${(x.consistency.p_up_4h*100).toFixed(0)}%，1d上漲${(x.consistency.p_up_1d*100).toFixed(0)}%，結論：${x.consistency.view}` : '';
      const fee = x.fee ? `費用校正：目標距離≈${x.fee.min_tp_dist_pct!=null?x.fee.min_tp_dist_pct.toFixed(1)+'%':'--'}，單次費用≈${(x.fee.est_pct*100).toFixed(2)}%，${x.fee.pass?'可行':'恐被費用吃掉'}` : '';
      return `<div class="badge">${x.symbol}: 現價 ${fmt(x.price)} ${x.currency}，p_up=${(x.p_up*100).toFixed(0)}%，建議：${x.advice}（${x.reason||''}）</div>
              <div style="margin:4px 0 2px;color:#555;">${x.plain || ''}</div>
              <div style="margin:2px 0;color:#666;">TP：${tps || '--'}；SL：${sls || '--'}</div>
              <div style="margin:2px 0;color:#666;">${cons}</div>
              <div style="margin:2px 0 8px;color:#666;">${fee}</div>`;
    }).join('');
  } catch (e) {
    holdBox.textContent = '查詢失敗'; adviceBox.textContent = '';
  }
}

function bindWallet() {
  const btn = document.getElementById('walletCheck');
  btn.addEventListener('click', loadWallet);
}

function computeAtrFromCandles(candles, period = 14){
  if (!candles || candles.length < period + 1) return null;
  const TR = [];
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    const high = Number(c.high), low = Number(c.low);
    if (i === 0) TR.push(high - low); else {
      const prevClose = Number(candles[i-1].close);
      TR.push(Math.max(high-low, Math.abs(high-prevClose), Math.abs(low-prevClose)));
    }
  }
  let atr = TR.slice(1, period+1).reduce((a,b)=>a+b,0)/period;
  for (let i=period+1;i<TR.length;i++) atr = (atr*(period-1)+TR[i])/period;
  return atr;
}

let lastCandlesCache = null;

function updateBeginnerAndPlan(candles, levels, prob, currency) {
  lastCandlesCache = candles;
  const price = candles.length ? Number(candles[candles.length - 1].close) : null;
  const upPct = Math.round(prob.data.p_up * 100);
  const verdict = verdictText(prob.data.verdict);
  const summary = document.getElementById('beginnerSummary');
  const planEntry = document.getElementById('planEntry');
  const planTP1 = document.getElementById('planTP1');
  const planTP2 = document.getElementById('planTP2');
  const planSL = document.getElementById('planSL');
  const planSideEl = document.getElementById('planSide');

  const all = [];
  if (levels.data.pivot) all.push(...levels.data.pivot.map(b => ({ ...b, method: 'pivot' })));
  if (levels.data.swing) all.push(...levels.data.swing.map(b => ({ ...b, method: 'swing' })));
  if (levels.data.vbp) all.push(...levels.data.vbp.map(b => ({ ...b, method: 'vbp' })));
  const mid = b => (Number(b.min)+Number(b.max))/2;
  const upper = all.filter(b => mid(b) > price).sort((a,b)=>mid(a)-mid(b));
  const lower = all.filter(b => mid(b) < price).sort((a,b)=>mid(b)-mid(a));
  const msg = verdict === '上漲'
    ? `偏多（p_up≈${upPct}%）。最近壓力約 ${upper[0]?fmt(mid(upper[0])):'--'} ${currency}。靠近壓力可減碼，拉回支撐再續攻。`
    : verdict === '下跌'
      ? `偏空（p_up≈${upPct}%）。最近支撐約 ${lower[0]?fmt(mid(lower[0])):'--'} ${currency}。反彈遇壓力可減碼。`
      : `中立（p_up≈${upPct}%）。等待突破壓力或回測支撐。`;
  summary.textContent = msg;

  planEntry.textContent = price!=null ? `${fmt(price)} ${currency}` : '--';
  // --- Favor R:R with max 10% stop tolerance, align to bands when possible ---
  let tp1Val = null, tp2Val = null, slVal = null;
  const MAX_RISK_PCT = 0.10; // 可忍受最大停損 10%
  if (price != null) {
    if (verdict === '上漲') {
      const bandSL = lower[0] ? mid(lower[0]) : null;
      const bandRiskPct = bandSL ? (price - bandSL) / price : Infinity;
      const riskPct = Math.min(MAX_RISK_PCT, isFinite(bandRiskPct) ? bandRiskPct : MAX_RISK_PCT);
      const slCandidate = price * (1 - riskPct);
      // 若最近支撐接近目標停損，優先錨定在支撐
      slVal = (bandSL && bandSL < price && Math.abs(bandSL - slCandidate)/price < 0.01) ? bandSL : slCandidate;

      const riskAbs = price - slVal;
      const target1 = price + 1.8 * riskAbs;
      const target2 = price + 3.0 * riskAbs;
      const uppers = upper.map(b => mid(b)).filter(v=>v>price).sort((a,b)=>a-b);
      tp1Val = uppers.find(v => v >= target1) || target1;
      tp2Val = uppers.find(v => v >= target2) || target2;
      if (tp2Val <= tp1Val) {
        const next = uppers.find(v => v > tp1Val);
        if (next) tp2Val = next; else tp2Val = price + Math.max(2.5, 3.0) * riskAbs;
      }
    } else if (verdict === '下跌') {
      const bandSL = upper[0] ? mid(upper[0]) : null;
      const bandRiskPct = bandSL ? (bandSL - price) / price : Infinity;
      const riskPct = Math.min(MAX_RISK_PCT, isFinite(bandRiskPct) ? bandRiskPct : MAX_RISK_PCT);
      const slCandidate = price * (1 + riskPct);
      slVal = (bandSL && bandSL > price && Math.abs(bandSL - slCandidate)/price < 0.01) ? bandSL : slCandidate;

      const riskAbs = slVal - price;
      const target1 = price - 1.8 * riskAbs;
      const target2 = price - 3.0 * riskAbs;
      const lowers = lower.map(b => mid(b)).filter(v=>v<price).sort((a,b)=>b-a);
      tp1Val = lowers.find(v => v <= target1) || target1;
      tp2Val = lowers.find(v => v <= target2) || target2;
      if (tp2Val >= tp1Val) {
        const next = lowers.find(v => v < tp1Val);
        if (next) tp2Val = next; else tp2Val = price - Math.max(2.5, 3.0) * riskAbs;
      }
    } else {
      // 中立：不提供計畫
      tp1Val = null; tp2Val = null; slVal = null;
    }
  }
  // Display percents based on預設方向：多頭/空頭（以 verdict 決定）
  const displaySide = verdict === '上漲' ? 'long' : (verdict === '下跌' ? 'short' : 'none');
  if (planSideEl) {
    if (displaySide === 'long') planSideEl.textContent = '做多 ↑';
    else if (displaySide === 'short') planSideEl.textContent = '做空 ↓';
    else planSideEl.textContent = '不建議交易 ✕';
  }
  let tp1DispPct = null, tp2DispPct = null, slDispPct = null;
  if (price && displaySide !== 'none') {
    if (displaySide === 'long') {
      tp1DispPct = (tp1Val!=null) ? ((tp1Val - price) / price) * 100 : null;
      tp2DispPct = (tp2Val!=null) ? ((tp2Val - price) / price) * 100 : null;
      slDispPct = (slVal!=null) ? (-(price - slVal) / price) * 100 : null; // 風險顯示為負
    } else { // short
      tp1DispPct = (tp1Val!=null) ? ((price - tp1Val) / price) * 100 : null;
      tp2DispPct = (tp2Val!=null) ? ((price - tp2Val) / price) * 100 : null;
      slDispPct = (slVal!=null) ? (-(slVal - price) / price) * 100 : null; // 風險顯示為負
    }
  }
  if (displaySide === 'none') {
    planTP1.textContent = '--';
    planTP2.textContent = '--';
    planSL.textContent = '--';
  } else {
    // Guard against sign confusion: for long, TP must be > entry; for short, TP must be < entry
    const tp1Ok = (tp1Val!=null) && ((displaySide==='long' && tp1Val>price) || (displaySide==='short' && tp1Val<price));
    const tp2Ok = (tp2Val!=null) && ((displaySide==='long' && tp2Val>price) || (displaySide==='short' && tp2Val<price));
    const slOk = (slVal!=null) && ((displaySide==='long' && slVal<price) || (displaySide==='short' && slVal>price));
    planTP1.textContent = tp1Ok ? `${fmt(tp1Val)} ${currency} (${signedPct(tp1DispPct)})` : '--';
    planTP2.textContent = tp2Ok ? `${fmt(tp2Val)} ${currency} (${signedPct(tp2DispPct)})` : '--';
    planSL.textContent = slOk ? `${fmt(slVal)} ${currency} (${signedPct(slDispPct)})` : '--';
  }
}

function feeEstimatePct(symbol) {
  const m = { ETH: 0.004, BTC: 0.0015, ADA: 0.0015, CRO: 0.002, PEPE: 0.004, LUNC: 0.002 };
  return m[symbol] ?? 0.002;
}

function bindRiskCalc() {
  const btn = document.getElementById('calcBtn');
  if (!btn) return; // UI section removed or not rendered
  btn.addEventListener('click', () => {
    const cap = Number(document.getElementById('capitalInput').value || 0);
    const riskPct = Number(document.getElementById('riskInput').value || 0);
    const currency = currencyEl.value;
    const sym = symbolEl.value;
    const lastClose = kChart.getOption()?.series?.[0]?.data?.slice(-1)?.[0]?.[1];
    const entry = Number(lastClose || 0);

    // Read TP1/TP2
    const tp1Text = document.getElementById('planTP1').textContent.split(' ')[0];
    const tp2Text = document.getElementById('planTP2').textContent.split(' ')[0];
    const tp1 = Number((tp1Text || '').replace(/,/g,'')) || 0;
    const tp2 = Number((tp2Text || '').replace(/,/g,'')) || 0;

    // Stop mode
    const slMode = document.getElementById('slMode').value;
    const atrMult = Number(document.getElementById('atrMult').value || 1.0);
    let sl;
    if (slMode === 'atr') {
      const atr = computeAtrFromCandles(lastCandlesCache||[], 14) || (entry*0.01);
      sl = entry - atrMult * atr;
      const slPctNow = entry ? ((sl - entry) / entry) * 100 : null;
      document.getElementById('planSL').textContent = isFinite(sl) ? `${fmt(sl)} ${currency} (${signedPct(slPctNow)})` : '--';
    } else {
      const slText = document.getElementById('planSL').textContent.split(' ')[0];
      sl = Number(slText.replace(/,/g,'')) || 0;
    }

    // Allocations
    const a1 = Number(document.getElementById('tp1Alloc').value || 50);
    const a2 = Number(document.getElementById('tp2Alloc').value || 50);
    const totalAlloc = Math.max(1, a1+a2);
    const w1 = a1/totalAlloc, w2 = a2/totalAlloc;

    // Fee
    const feeOverride = Number(document.getElementById('feeOverride').value || 0);
    const feePct = feeOverride>0 ? (feeOverride/100) : feeEstimatePct(sym);
    const roundTripFeePct = feePct * 2;

    // Position sizing
    const perUnitRisk = entry && sl ? Math.max(entry - sl, entry*0.01) : 0; // 至少 1%
    const riskAmount = cap * (riskPct/100);
    const qty = perUnitRisk ? riskAmount / perUnitRisk : 0;
    document.getElementById('planQty').textContent = qty>0 ? `建議數量：約 ${fmt(qty, 4)} 單位（投入 ≈ ${fmt(entry*qty)} ${currency}）` : '資料不足無法計算';

    // PnL for TP1 / TP2
    const grossTp1Pct = (tp1 && entry) ? ((tp1 - entry) / entry) * 100 : null;
    const netTp1Pct = grossTp1Pct!=null ? (grossTp1Pct - (roundTripFeePct * 100)) : null;
    const grossTp2Pct = (tp2 && entry) ? ((tp2 - entry) / entry) * 100 : null;
    const netTp2Pct = grossTp2Pct!=null ? (grossTp2Pct - (roundTripFeePct * 100)) : null;

    const pnl1 = (tp1 && entry && qty) ? (tp1 - entry) * qty * w1 : 0;
    const pnl2 = (tp2 && entry && qty) ? (tp2 - entry) * qty * w2 : 0;
    const feeCostEst = (entry && qty) ? (entry * qty * feePct + ((tp1||entry) * qty * w1 + (tp2||entry) * qty * w2) * feePct) : 0;
    const netPnl = (pnl1 + pnl2) - feeCostEst;

    const pnlLine = (qty>0)
      ? `目標潛在淨利 約 ${fmt(netPnl)} ${currency}（TP1 淨 ${netTp1Pct!=null?netTp1Pct.toFixed(1)+'%':'--'} · 配重 ${Math.round(w1*100)}%｜TP2 淨 ${netTp2Pct!=null?netTp2Pct.toFixed(1)+'%':'--'} · 配重 ${Math.round(w2*100)}%｜單邊費率 ${ (feePct*100).toFixed(2) }%）`
      : '--';
    document.getElementById('planPnL').textContent = pnlLine;

    // Risk-Reward & Break-even
    const grossSlPct = (sl && entry) ? ((entry - sl) / entry) * 100 : null;
    const netSlPct = grossSlPct!=null ? (grossSlPct + (roundTripFeePct * 100)) : null;
    document.getElementById('planRR').textContent = (netTp1Pct!=null && netSlPct!=null)
      ? `風險報酬比（以 TP1 淨）：${(netTp1Pct/Math.max(netSlPct,0.01)).toFixed(2)} 倍`
      : '--';
    const bePct = (roundTripFeePct*100); // 粗略：需超過費用
    document.getElementById('planBE').textContent = `損益兩平門檻（估）：約 ${bePct.toFixed(2)}% 漲幅`;
  });
}

function formatTime(ts) {
  try { return new Date(ts).toLocaleString(); } catch { return ''; }
}

async function loadNews() {
  try {
    const res = await fetchJson('/api/news?limit=120&grouped=1');
    const data = res.data || {};
    const sections = [
      { key: 'BTC', name: 'BTC' },
      { key: 'ETH', name: 'ETH' },
      { key: 'ADA', name: 'ADA' },
      { key: 'CRO', name: 'CRO' },
      { key: 'OTHER', name: '其他' }
    ];
    const tabs = `<div class="tabs">${sections.map((s,i)=>`<div class="tab ${i===0?'active':''}" data-tab="${s.key}">${s.name}</div>`).join('')}</div>`;
    const panels = sections.map((s,i)=>{
      const list = (data[s.key] || []).slice(0,5);
      const items = list.map(n => {
        const t = formatTime(n.ts);
        const title = (n.title || '').replace(/</g,'&lt;');
        const src = (n.source || '').replace(/</g,'&lt;');
        const sum = (n.summary || '').replace(/</g,'&lt;');
        return `<div style="padding:6px 0;border-bottom:1px solid #eee;">
          <div><a href="${n.link}" target="_blank" rel="noreferrer">${title}</a></div>
          <div style="color:#666;font-size:12px;">${src} · ${t}</div>
          <div style="color:#444;font-size:13px;">${sum}</div>
        </div>`;
      }).join('');
      const more = `<a class="viewmore" href="/news.html?tag=${s.key}" target="_blank" rel="noreferrer">View more</a>`;
      return `<div class="tabpanel ${i===0?'active':''}" id="tab_${s.key}">${items || '<div>暫無資料</div>'}${more}</div>`;
    }).join('');
    document.getElementById('newsList').innerHTML = tabs + panels;
    document.querySelectorAll('.tab').forEach(tab => {
      tab.addEventListener('click', () => {
        const key = tab.getAttribute('data-tab');
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tabpanel').forEach(p => p.classList.remove('active'));
        tab.classList.add('active');
        const panel = document.getElementById(`tab_${key}`);
        if (panel) panel.classList.add('active');
      });
    });
  } catch (e) {
    document.getElementById('newsList').textContent = '新聞載入失敗';
  }
}

async function loadAllChains(){
  try {
    const res = await fetchJson('/api/onchain/overview');
    const list = res.data || [];
    const box = document.getElementById('allChains');
    if (!box) return;
    box.innerHTML = list.map(r => {
      const aa = formatThousands(r.active_addr, 0);
      const tx = formatThousands(r.tx_count, 0);
      const wh = formatThousands(r.whale_tx, 0);
      const ts = r.ts ? new Date(r.ts).toLocaleDateString() : '';
      return `<div class="card">
        <div class="card-title">${r.symbol}</div>
        <div class="card-value" style="font-size:13px; line-height:1.5; color:#444;">
          活躍地址：${aa}<br/>交易數：${tx}<br/>鯨魚轉帳：${wh}<br/><span style="color:#777;">${ts}</span>
        </div>
      </div>`;
    }).join('');
  } catch {}
}

let topCache = { list: [], currency: 'USD', page: 1 };

async function loadToplist() {
  try {
    const currency = currencyEl.value;
    const res = await fetchJson(`/api/market/top?limit=100&currency=${currency}`);
    topCache = { list: (res.data && res.data.list) || [], currency, page: 1 };
    renderToplistPage();
  } catch (e) {
    document.getElementById('toplist').textContent = '排行載入失敗';
  }
}

function renderToplistPage() {
  const { list, currency, page } = topCache;
  const pageSize = 10;
  const totalPages = Math.max(1, Math.ceil(list.length / pageSize));
  const p = Math.max(1, Math.min(page, totalPages));
  topCache.page = p;
  const start = (p - 1) * pageSize;
  const items = list.slice(start, start + pageSize);
  const rows = items.map(x => {
    const px = x.price >= 1 ? x.price.toLocaleString(undefined, { maximumFractionDigits: 2 }) : x.price.toPrecision(6);
    const ch = x.change24h!=null ? `${x.change24h.toFixed(2)}%` : '--';
    return `<div style="display:flex; gap:8px; align-items:center; padding:4px 0; border-bottom:1px solid #eee;">
      <div style="width:28px; text-align:right; color:#666;">${x.rank}</div>
      <img src="${x.image}" alt="" style="width:18px; height:18px; border-radius:50%;" />
      <div style="width:70px; font-weight:600;">${x.symbol}</div>
      <div style="flex:1; color:#444;">${x.name}</div>
      <div style="width:140px; text-align:right;">${px} ${currency}</div>
      <div style="width:90px; text-align:right; color:${x.change24h>=0?'#16a34a':'#dc2626'};">${ch}</div>
    </div>`;
  }).join('');
  document.getElementById('toplist').innerHTML = rows || '暫無資料';
  document.getElementById('topPage').textContent = `${p} / ${totalPages}`;
  document.getElementById('topPrev').disabled = p<=1;
  document.getElementById('topNext').disabled = p>=totalPages;
}

function bindToplistPager() {
  document.getElementById('topPrev').addEventListener('click', () => { topCache.page = Math.max(1, topCache.page - 1); renderToplistPage(); });
  document.getElementById('topNext').addEventListener('click', () => { const size = Math.max(1, Math.ceil(topCache.list.length/10)); topCache.page = Math.min(size, topCache.page + 1); renderToplistPage(); });
  currencyEl.addEventListener('change', () => { loadToplist(); });
  const toggleIds = ['togglePivot','toggleSwing','toggleVbp'];
  toggleIds.forEach(id => {
    const el = document.getElementById(id);
    if (el && !el._bound) { el._bound = true; el.addEventListener('change', () => { refresh(); }); }
  });
}

let gainCache = { list: [], currency: 'USD', page: 1 };

async function loadGainers() {
  try {
    const currency = currencyEl.value;
    const res = await fetchJson(`/api/market/top_gainers?limit=100&currency=${currency}`);
    gainCache = { list: (res.data && res.data.list) || [], currency, page: 1 };
    renderGainersPage();
  } catch (e) {
    document.getElementById('gainlist').textContent = '漲幅排行載入失敗';
  }
}

function renderGainersPage() {
  const { list, currency, page } = gainCache;
  const pageSize = 10;
  const totalPages = Math.max(1, Math.ceil(list.length / pageSize));
  const p = Math.max(1, Math.min(page, totalPages));
  gainCache.page = p;
  const start = (p - 1) * pageSize;
  const items = list.slice(start, start + pageSize);
  const rows = items.map(x => {
    const px = x.price >= 1 ? x.price.toLocaleString(undefined, { maximumFractionDigits: 2 }) : x.price.toPrecision(6);
    const ch = x.change24h!=null ? `${x.change24h.toFixed(2)}%` : '--';
    return `<div style="display:flex; gap:8px; align-items:center; padding:4px 0; border-bottom:1px solid #eee;">
      <div style="width:28px; text-align:right; color:#666;">${x.rank}</div>
      <img src="${x.image}" alt="" style="width:18px; height:18px; border-radius:50%;" />
      <div style="width:70px; font-weight:600;">${x.symbol}</div>
      <div style="flex:1; color:#444;">${x.name}</div>
      <div style="width:140px; text-align:right;">${px} ${currency}</div>
      <div style="width:90px; text-align:right; color:${x.change24h>=0?'#16a34a':'#dc2626'};">${ch}</div>
    </div>`;
  }).join('');
  document.getElementById('gainlist').innerHTML = rows || '暫無資料';
  document.getElementById('gainPage').textContent = `${p} / ${totalPages}`;
  document.getElementById('gainPrev').disabled = p<=1;
  document.getElementById('gainNext').disabled = p>=totalPages;
}

function bindGainersPager() {
  document.getElementById('gainPrev').addEventListener('click', () => { gainCache.page = Math.max(1, gainCache.page - 1); renderGainersPage(); });
  document.getElementById('gainNext').addEventListener('click', () => { const size = Math.max(1, Math.ceil(gainCache.list.length/10)); gainCache.page = Math.min(size, gainCache.page + 1); renderGainersPage(); });
  currencyEl.addEventListener('change', () => { loadGainers(); });
}

async function loadRecom() {
  try {
    const currency = currencyEl.value;
    const res = await fetchJson(`/api/recommendations?limit=10&currency=${currency}`);
    const list = (res.data && res.data.list) || [];
    const rows = list.map(x => {
      const px = x.price >= 1 ? x.price.toLocaleString(undefined, { maximumFractionDigits: 2 }) : x.price.toPrecision(6);
      const ch7 = x.change7d!=null ? `${x.change7d.toFixed(2)}%` : '--';
      const score = x.score!=null ? x.score.toFixed(3) : '--';
      return `<div style="display:flex; gap:8px; align-items:center; padding:4px 0; border-bottom:1px solid #eee;">
        <img src="${x.image}" alt="" style="width:18px; height:18px; border-radius:50%;" />
        <div style="width:70px; font-weight:600;">${x.symbol}</div>
        <div style="flex:1; color:#444;">${x.name}</div>
        <div style="width:140px; text-align:right;">${px} ${currency}</div>
        <div style="width:90px; text-align:right;">7d ${ch7}</div>
        <div style="width:80px; text-align:right; color:#6b21a8;">Score ${score}</div>
      </div>`;
    }).join('');
    document.getElementById('recomList').innerHTML = rows || '暫無資料';
  } catch (e) {
    document.getElementById('recomList').textContent = '推薦載入失敗';
  }
}

// Frontend micro-cache for quicker perceived updates
const frontCache = { summary: new Map(), levels: new Map(), ttlMs: 30000 };
let currentRefreshCtrl = null;
let refreshSeq = 0;
function daysForInterval(interval) {
  if (interval === '1h') return 14;   // ~336 bars
  if (interval === '4h') return 60;   // ~360 bars
  if (interval === '1w') return 520;  // ~10 years
  return 180;                         // 1d default
}
async function refresh() {
  const sym = symbolEl.value;
  const interval = intervalEl.value;
  const currency = currencyEl.value;
  // Abort any in-flight previous refresh
  if (currentRefreshCtrl) { try { currentRefreshCtrl.abort(); } catch {} }
  const ctrl = new AbortController();
  currentRefreshCtrl = ctrl;
  const seq = ++refreshSeq;
  try {
    const days = daysForInterval(interval);
    const key = `${sym}:${interval}:${currency}`;
    // Instant render from cache if available
    const cachedSum = frontCache.summary.get(key);
    const cachedLvl = frontCache.levels.get(`${sym}:${currency}`);
    const now = Date.now();
    if (cachedSum && (now - cachedSum.ts) < frontCache.ttlMs && cachedLvl && (now - cachedLvl.ts) < frontCache.ttlMs) {
      try { renderK(cachedSum.data, cachedLvl.data); } catch {}
    }
    // Phase 1: fetch market + levels fast, render chart ASAP
    const [mkt, levels] = await Promise.all([
      fetchJson(`/api/market/summary?symbol=${sym}&interval=${interval}&days=${days}&currency=${currency}`, { signal: ctrl.signal }),
      fetchJson(`/api/levels?symbol=${sym}&currency=${currency}`, { signal: ctrl.signal }),
    ]);
    if (seq !== refreshSeq) return; // superseded
    const candles = mkt.data.candles;
    frontCache.summary.set(key, { ts: now, data: candles });
    frontCache.levels.set(`${sym}:${currency}`, { ts: now, data: levels.data });
    renderK(candles, levels.data);
    // Phase 2: fetch probability + on-chain, then update gauges/cards
    const [prob, onchain] = await Promise.all([
      fetchJson(`/api/probability?symbol=${sym}&horizon=1d&currency=${currency}`, { signal: ctrl.signal }),
      fetchJson(`/api/onchain/metrics?symbol=${sym}&limit=90`, { signal: ctrl.signal }),
    ]);
    if (seq !== refreshSeq) return;
    renderGauge(prob.data.p_up);
    verdictEl.textContent = verdictText(prob.data.verdict);
    renderTargets(prob.data.targets);
    bindHelp();
    const last = onchain.data[onchain.data.length - 1] || {};
    const nf = last.stable_netflow;
    const nfDigits = nf != null && Number(nf) % 1 !== 0 ? 2 : 0;
    document.getElementById('active_addr').textContent = formatThousands(last.active_addr, 0);
    document.getElementById('stable_netflow').textContent = formatThousands(nf, nfDigits);
    document.getElementById('whale_tx').textContent = formatThousands(last.whale_tx, 0);
    updateBeginnerAndPlan(candles, levels, prob, currency);
    startLivebar();
    // Rainbow chart: native render for BTC
    const box = document.getElementById('rainbowBox');
    const ph = document.getElementById('rainbowPlaceholder');
    if (box && ph) {
      if (sym === 'BTC') {
        box.style.display = '';
        ph.style.display = 'none';
        await renderRainbowBTC();
      } else {
        box.style.display = 'none';
        ph.style.display = '';
      }
    }
  } catch (e) {
    if (e.name === 'AbortError') return;
    console.error(e);
    livebarEl.textContent = '即時價取得失敗';
  }
}

function linearRegression(x, y) {
  const n = x.length;
  if (n === 0) return { a: 0, b: 0 };
  let sumX=0, sumY=0, sumXY=0, sumXX=0;
  for (let i=0;i<n;i++){ sumX+=x[i]; sumY+=y[i]; sumXY+=x[i]*y[i]; sumXX+=x[i]*x[i]; }
  const denom = (n*sumXX - sumX*sumX) || 1e-9;
  const b = (n*sumXY - sumX*sumY) / denom;
  const a = (sumY - b*sumX)/n;
  return { a, b };
}

function hexToRgba(hex, alpha){
  const h = hex.replace('#','');
  const r = parseInt(h.substring(0,2),16);
  const g = parseInt(h.substring(2,4),16);
  const b = parseInt(h.substring(4,6),16);
  return `rgba(${r},${g},${b},${alpha})`;
}

async function renderRainbowBTC(){
  try {
    if (!rainbowChart) { const dom = document.getElementById('rainbowChart'); if (dom) rainbowChart = echarts.init(dom); else return; }
    // Long history daily data in USD (extend to ~13.5y)
    const res = await fetchJson(`/api/market/summary?symbol=BTC&interval=1d&days=5000&currency=USD`);
    const candles = (res && res.data && res.data.candles) ? res.data.candles : [];
    if (!candles.length) return;
    const ts = candles.map(c => new Date(c.ts));
    const closes = candles.map(c => Math.max(1e-9, Number(c.close)));
    const t = candles.map((c,i)=> i); // simple index as time variable
    const logP = closes.map(p => Math.log(p));
    const { a, b } = linearRegression(t, logP);
    const fit = t.map(tt => a + b*tt);
    const resid = logP.map((y,i)=> y - fit[i]);
    const sd = Math.sqrt(resid.reduce((s,r)=>s + r*r, 0) / Math.max(resid.length-1,1));
    // Bands centered around 0 with symmetric steps; include mid as fair value
    const kLevels = [-2.5,-1.5,-0.5,0,0.5,1.5,2.5];
    const lines = kLevels.map(k => fit.map(f => Math.exp(f + k*sd)));
    const priceSeries = closes;
    const colors = ['#7e57c2','#42a5f5','#66bb6a','#ffd400','#ffaa00','#ff7f00','#ef4444'];
    const fillColors = ['#7e57c2','#42a5f5','#66bb6a','#ffd400','#ffaa00','#ff7f00'];

    // Future projection (next 365 days)
    const FUTURE_DAYS = 365;
    const lastTs = ts[ts.length-1].getTime();
    const tsFuture = Array.from({length: FUTURE_DAYS}, (_,i)=> new Date(lastTs + (i+1)*86400000));
    const tFuture = Array.from({length: FUTURE_DAYS}, (_,i)=> t[t.length-1] + (i+1));
    const fitFuture = tFuture.map(tt => a + b*tt);
    const linesFuture = kLevels.map(k => fitFuture.map(f => Math.exp(f + k*sd)));

    const series = [];
    // Helper to build stacked area between lower and upper lines
    const buildBand = (idx, tsArr, lowerArr, upperArr, colorHex, stackSuffix, alphaFill = 0.22, dashed = false) => {
      const stackName = `BAND_${idx}_${stackSuffix}`;
      // base (lower)
      series.push({ type: 'line', name: '', stack: stackName, data: lowerArr.map((y,i)=>[tsArr[i], y]), showSymbol: false, smooth: true, lineStyle: { width: 0, opacity: 0 }, areaStyle: { opacity: 0 }, z: 0 });
      // height (upper-lower) with area fill
      series.push({ type: 'line', name: `區間 ${idx+1} ${stackSuffix}`, stack: stackName, data: upperArr.map((y,i)=>[tsArr[i], Math.max(upperArr[i]-lowerArr[i], 0)]), showSymbol: false, smooth: true, lineStyle: { width: 0 }, areaStyle: { color: hexToRgba(colorHex, alphaFill) }, z: 0 });
      // boundary lines for arc feel
      series.push({ type: 'line', name: '', data: lowerArr.map((y,i)=>[tsArr[i], y]), showSymbol: false, smooth: true, lineStyle: { width: 0.8, color: hexToRgba(colorHex, dashed?0.35:0.7), type: dashed?'dashed':'solid' }, z: 1, silent: true });
      series.push({ type: 'line', name: '', data: upperArr.map((y,i)=>[tsArr[i], y]), showSymbol: false, smooth: true, lineStyle: { width: 0.8, color: hexToRgba(colorHex, dashed?0.35:0.7), type: dashed?'dashed':'solid' }, z: 1, silent: true });
    };
    // Draw past bands (filled)
    for (let i=0;i<kLevels.length-1;i++) {
      buildBand(i, ts, lines[i], lines[i+1], fillColors[i], 'PAST', 0.22, false);
    }
    // Draw future bands (lighter fill)
    for (let i=0;i<kLevels.length-1;i++) {
      buildBand(i, tsFuture, linesFuture[i], linesFuture[i+1], fillColors[i], 'FUT', 0.12, true);
    }
    // center regression line (fair value)
    const fitAllTs = ts.concat(tsFuture);
    const fitAll = fit.concat(fitFuture).map(v=>Math.exp(v));
    series.push({ type: 'line', name: '合理價（回歸）', data: fitAllTs.map((d,i)=>[d, fitAll[i]]), showSymbol: false, smooth: true, lineStyle: { width: 1.2, color: '#111827', type: 'dashed', opacity: 0.8 }, z: 2 });
    // price
    const priceSer = { type: 'line', name: 'BTC 價格', data: priceSeries.map((y,idx)=>[ts[idx], y]), showSymbol: false, smooth: true, lineStyle: { width: 2, color: '#ef4444' }, z: 3 };
    // mark today split
    priceSer.markLine = { symbol: ['none','none'], label: { formatter: '今天' }, lineStyle: { color: '#9ca3af', type: 'dashed' }, data: [{ xAxis: ts[ts.length-1] }] };
    series.push(priceSer);

    rainbowChart.setOption({
      tooltip: { trigger: 'axis', valueFormatter: v => (Number(v)>=1?Number(v).toLocaleString(undefined,{maximumFractionDigits:2}):Number(v).toPrecision(6)) },
      grid: { left: 56, right: 24, top: 28, bottom: 48 },
      xAxis: { type: 'time' },
      yAxis: { type: 'log', min: 'dataMin', max: 'dataMax', splitLine: { lineStyle: { color: '#e5e7eb' } } },
      legend: { top: 0 },
      series
    });
    rainbowChart.resize();

    // Price range summary for current date
    const lastIdx = closes.length - 1;
    const lastPrice = closes[lastIdx];
    // find band it sits in
    const bandEdges = kLevels.map((k,i)=>({k, v: (i<lines.length ? lines[i][lastIdx] : Math.exp(fit[lastIdx]))}));
    let bandLabel = '合理價附近';
    if (lastPrice <= bandEdges[0].v) bandLabel = '極度低估';
    else if (lastPrice <= bandEdges[1].v) bandLabel = '低估';
    else if (lastPrice <= bandEdges[2].v) bandLabel = '略低估';
    else if (lastPrice <= bandEdges[3].v) bandLabel = '合理價';
    else if (lastPrice <= bandEdges[4].v) bandLabel = '略偏高';
    else if (lastPrice <= bandEdges[5].v) bandLabel = '偏高';
    else bandLabel = '過熱';

    const rangeEl = document.getElementById('rainbowRanges');
    if (rangeEl) {
      const f = (v)=> v>=1 ? v.toLocaleString(undefined,{maximumFractionDigits:2}) : v.toPrecision(6);
      const fvNow = Math.exp(fit[lastIdx]);
      const fv1y = Math.exp(fitFuture[fitFuture.length-1]);
      rangeEl.innerHTML = `今日區間：${bandLabel} · 當前 ${f(lastPrice)} USD · 合理價 ${f(fvNow)} USD｜一年後合理價預估：${f(fv1y)} USD`;
    }
  } catch (e) {
    // fail silently
  }
}

function renderSparkline(chart, series, color = '#3b82f6') {
  if (!chart) return;
  chart.setOption({
    grid: { left: 8, right: 8, top: 8, bottom: 16 },
    xAxis: { type: 'category', data: series.map(s=>s.t), axisLabel: { show: false }, axisTick: { show: false }, axisLine: { show: false } },
    yAxis: { type: 'value', axisLabel: { show: false }, axisTick: { show: false }, splitLine: { show: false }, axisLine: { show: false } },
    series: [{ type: 'line', data: series.map(s=>s.v), smooth: true, showSymbol: false, lineStyle: { color } }],
    tooltip: { trigger: 'axis' }
  });
}

function renderPaperTable(rows, currency, view = 'all') {
  const body = document.getElementById('paperBody');
  if (!body) return;
  const filtered = rows.filter(r => view==='all' ? true : r.status === view);
  body.innerHTML = filtered.map(r => {
    const status = r.status;
    const entry = r.entry_conv!=null ? Number(r.entry_conv) : Number(r.entry);
    const qty = Number(r.qty);
    const invested = (isFinite(entry) && isFinite(qty)) ? (entry * qty) : null;
    const latest = status==='open' ? (r.mark_price_conv!=null ? Number(r.mark_price_conv) : null) : (r.close_price_conv!=null ? Number(r.close_price_conv) : null);
    const entryTxt = entry>=1 ? entry.toLocaleString(undefined,{maximumFractionDigits:2}) : entry.toPrecision(6);
    const priceTxt = latest!=null ? (latest>=1 ? latest.toLocaleString(undefined,{maximumFractionDigits:2}) : latest.toPrecision(6)) : '--';
    let pnlTxt = '--', pnlClass = '';
    if (status==='open' && r.unrealized_pnl_conv!=null) {
      const v = Number(r.unrealized_pnl_conv);
      const pct = (invested && invested!==0) ? (v / invested) * 100 : null;
      pnlClass = v>=0 ? 'pos' : 'neg';
      pnlTxt = `${v>=0?'+':''}${v.toFixed(2)} ${currency}${pct!=null?` (${signedPct(pct)})`:''}`;
    }
    if (status==='closed' && r.pnl_conv!=null) {
      const v = Number(r.pnl_conv);
      const pct = (invested && invested!==0) ? (v / invested) * 100 : null;
      pnlClass = v>=0 ? 'pos' : 'neg';
      pnlTxt = `${v>=0?'+':''}${v.toFixed(2)} ${currency}${pct!=null?` (${signedPct(pct)})`:''}`;
    }
    const tsOpen = new Date(r.ts_open).toLocaleString();
    const action = status==='open' ? `<button data-id="${r.id}" class="closeBtn btn">平倉</button>` : '';
    return `<tr>
      <td>${r.symbol}</td>
      <td>${r.side}</td>
      <td class="num">${entryTxt}</td>
      <td class="num">${priceTxt}</td>
      <td class="num ${pnlClass}">${pnlTxt}</td>
      <td>${tsOpen}</td>
      <td>${action}</td>
    </tr>`;
  }).join('');
  body.querySelectorAll('.closeBtn').forEach(btn => {
    btn.addEventListener('click', async () => {
      try { await postJson('/api/paper/close', { id: Number(btn.getAttribute('data-id')) }); await loadPaperTrades(); await loadPaperPerf(); } catch {}
    });
  });
}

let paperCache = { rows: [], currency: 'USD', view: 'all' };

async function loadPaperTrades() {
  try {
    const currency = currencyEl.value;
    const room = (localStorage.getItem('paper_room')||'').trim();
    const qp = room ? `&room=${encodeURIComponent(room)}` : '';
    const res = await fetchJson(`/api/paper/trades?currency=${currency}${qp}`);
    const rows = res.data || [];
    paperCache = { rows, currency, view: paperCache.view || 'all' };
    renderPaperTable(rows, currency, paperCache.view);
    // Sparkline: equity curve approximation (sum pnl by time for closed, cum with mark for open)
    const series = [];
    let cum = 0;
    const sorted = [...rows].sort((a,b)=> (a.ts_open||0)-(b.ts_open||0));
    for (const r of sorted) {
      if (r.status === 'closed' && r.pnl_conv != null) cum += Number(r.pnl_conv);
      if (r.status === 'open' && r.unrealized_pnl_conv != null) {}
      series.push({ t: new Date(r.ts_open).toLocaleDateString(), v: cum });
    }
    renderSparkline(paperSparkChart, series);
    // bind segmented control
    const seg = document.getElementById('paperViewSeg');
    if (seg && !seg._bound) {
      seg._bound = true;
      seg.querySelectorAll('button').forEach(b => b.addEventListener('click', () => {
        seg.querySelectorAll('button').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        paperCache.view = b.getAttribute('data-view');
        renderPaperTable(paperCache.rows, paperCache.currency, paperCache.view);
      }));
    }
  } catch (e) {
    const body = document.getElementById('paperBody');
    if (body) body.innerHTML = '<tr><td colspan="7">載入失敗</td></tr>';
  }
}

async function openPaperTrade() {
  try {
    const sym = symbolEl.value;
    const side = document.getElementById('paperSide').value;
    const amountInput = document.getElementById('paperAmount');
    const tp = Number(document.getElementById('paperTP').value || 0) || undefined;
    const sl = Number(document.getElementById('paperSL').value || 0) || undefined;
    const currency = currencyEl.value;
    const amount = Number(amountInput && amountInput.value || 0);
    if (!amount) return;
    // 以目前顯示的貨幣價格換算數量；下單記錄統一用 USD 價
    const priceLocalRes = await fetchJson(`/api/price?symbol=${sym}&currency=${currency}`);
    const pxLocal = Number(priceLocalRes.data.price);
    if (!pxLocal) return;
    const qty = amount / pxLocal;
    if (qty <= 0) return;
    const priceUsdRes = await fetchJson(`/api/price?symbol=${sym}&currency=USD`);
    const pxUsd = Number(priceUsdRes.data.price);
    const room = (localStorage.getItem('paper_room')||'').trim();
    await postJson('/api/paper/trades', { symbol: sym, side, entry: pxUsd, qty, tp, sl, room: room||undefined });
    await loadPaperTrades();
  } catch {}
}

function tableHtmlFromKV(rows){
  if (!rows || !rows.length) return '<tbody><tr><td>--</td></tr></tbody>';
  return '<tbody>' + rows.map(r=>`<tr><th style="width:180px; color:#6b7280;">${r.k}</th><td class="num">${r.v}</td></tr>`).join('') + '</tbody>';
}

function renderPerfDetailsFromTrades(trades, currency){
  const closed = trades.filter(t=>t.status==='closed');
  const wins = closed.filter(t=>Number(t.pnl_conv||t.pnl||0) > 0);
  const losses = closed.filter(t=>Number(t.pnl_conv||t.pnl||0) <= 0);
  const sum = arr => arr.reduce((a,b)=>a + Number(b.pnl_conv||b.pnl||0), 0);
  const sumAbs = arr => arr.reduce((a,b)=>a + Math.abs(Number(b.pnl_conv||b.pnl||0)), 0);
  const pf = (sum(wins) / Math.max(-sum(losses), 1e-9));
  const avgWin = wins.length ? (sum(wins)/wins.length) : 0;
  const avgLoss = losses.length ? (sum(losses)/losses.length) : 0;
  const expectancy = (wins.length + losses.length) ? ((wins.length/(wins.length+losses.length))*avgWin + (losses.length/(wins.length+losses.length))*avgLoss) : 0;
  // equity and drawdown for Sharpe/MaxDD proxy
  const ordered = closed.sort((a,b)=>(a.close_time||0)-(b.close_time||0));
  const equ = []; let cum=0, peak=0, maxDD=0;
  for (const t of ordered){ cum += Number(t.pnl_conv||t.pnl||0); peak=Math.max(peak,cum); maxDD=Math.max(maxDD, peak-cum); equ.push(cum); }
  const mean = equ.length? equ.reduce((a,b)=>a+b,0)/equ.length : 0;
  const sd = equ.length>1 ? Math.sqrt(equ.reduce((a,b)=>a+(b-mean)**2,0)/(equ.length-1)) : 0;
  const sharpeProxy = sd? (mean / sd) : 0;
  // streaks
  let curW=0,curL=0,maxW=0,maxL=0; for(const t of ordered){ const p=Number(t.pnl_conv||t.pnl||0); if(p>0){curW++;maxW=Math.max(maxW,curW);curL=0;} else {curL++;maxL=Math.max(maxL,curL);curW=0;} }
  // leaders
  const best = closed.slice().sort((a,b)=>Number(b.pnl_conv||b.pnl||0)-Number(a.pnl_conv||a.pnl||0))[0];
  const worst = closed.slice().sort((a,b)=>Number(a.pnl_conv||a.pnl||0)-Number(b.pnl_conv||b.pnl||0))[0];

  const detailsRows = [
    { k: 'Profit Factor', v: pf.toFixed(2) },
    { k: '平均獲利', v: (avgWin).toFixed(2) + ' ' + currency },
    { k: '平均虧損', v: (avgLoss).toFixed(2) + ' ' + currency },
    { k: '期望值(Expectancy)', v: (expectancy).toFixed(2) + ' ' + currency },
    { k: 'Sharpe(近似)', v: sharpeProxy.toFixed(2) },
    { k: '連勝', v: maxW },
    { k: '連敗', v: maxL },
  ];
  const det = document.getElementById('perfDetails');
  if (det) det.innerHTML = tableHtmlFromKV(detailsRows);

  const byKey = {};
  for(const t of trades){ const key = `${t.symbol}-${t.side}`; byKey[key] = byKey[key] || {n:0,p:0}; byKey[key].n++; byKey[key].p += Number((t.pnl_conv??t.unrealized_pnl_conv) || 0); }
  const brRows = Object.entries(byKey).map(([k,v])=>({k, v: (v.p).toFixed(2) + ' ' + currency + `（${v.n} 筆）`}));
  const br = document.getElementById('perfBreakdown');
  if (br) br.innerHTML = tableHtmlFromKV(brRows);

  // monthly
  const monthKey = ts => { const d = new Date(ts || Date.now()); return `${d.getFullYear()}-${(d.getMonth()+1).toString().padStart(2,'0')}`; };
  const monthly = {};
  for(const t of closed){ const k = monthKey(t.close_time || t.ts_open); monthly[k] = (monthly[k]||0) + Number(t.pnl_conv||t.pnl||0); }
  const monRows = Object.entries(monthly).sort().map(([k,v])=>({k, v: (v).toFixed(2) + ' ' + currency }));
  const mon = document.getElementById('perfMonthly');
  if (mon) mon.innerHTML = tableHtmlFromKV(monRows);

  const leadersRows = [
    { k: '最佳交易', v: best ? `${best.symbol} ${best.side} · ${(Number(best.pnl_conv||best.pnl||0)).toFixed(2)} ${currency}` : '--' },
    { k: '最差交易', v: worst ? `${worst.symbol} ${worst.side} · ${(Number(worst.pnl_conv||worst.pnl||0)).toFixed(2)} ${currency}` : '--' }
  ];
  const lead = document.getElementById('perfLeaders');
  if (lead) lead.innerHTML = tableHtmlFromKV(leadersRows);
}

async function loadPaperPerf() {
  try {
    const currency = currencyEl.value;
    const res = await fetchJson(`/api/paper/metrics?currency=${currency}`);
    const d = res.data || {};
    const winEl = document.getElementById('perfWin');
    const pnlEl = document.getElementById('perfPnl');
    const nEl = document.getElementById('perfTrades');
    const mddEl = document.getElementById('perfMdd');
    if (winEl) winEl.textContent = ((d.winrate||0)*100).toFixed(0) + '%';
    if (pnlEl) pnlEl.textContent = (d.pnl||0).toFixed(2) + ' ' + currency;
    if (nEl) nEl.textContent = d.trades || 0;
    if (mddEl) mddEl.textContent = (d.max_drawdown||0).toFixed(2) + ' ' + currency;
    const series = (d.equity_curve||[]).map(p => ({ t: new Date(p.ts).toLocaleDateString(), v: Number(p.equity||0) }));
    renderSparkline(perfSparkChart, series, '#10b981');
    // 取全部交易，做更細的分析
    const tradesRes = await fetchJson(`/api/paper/trades?currency=${currency}`);
    renderPerfDetailsFromTrades(tradesRes.data || [], currency);
  } catch (e) {}
}

async function calcDCA() {
  try {
    const sym = symbolEl.value;
    const currency = currencyEl.value;
    const periods = Number(document.getElementById('dcaPeriods').value || 12);
    const amount = Number(document.getElementById('dcaAmount').value || 100);
    const freq = document.getElementById('dcaFreq').value || '1w';
    const res = await fetchJson(`/api/tools/dca?symbol=${sym}&periods=${periods}&amount=${amount}&freq=${freq}&currency=${currency}`);
    const d = res.data;
    const avgEl = document.getElementById('dcaAvg');
    const invEl = document.getElementById('dcaInvested');
    const valEl = document.getElementById('dcaValue');
    const retEl = document.getElementById('dcaReturn');
    const f = (v) => v>=1 ? v.toLocaleString(undefined,{maximumFractionDigits:2}) : v.toPrecision(6);
    if (avgEl) avgEl.textContent = `${f(d.avg_cost)} ${currency}`;
    if (invEl) invEl.textContent = `${f(d.total_invested)} ${currency}`;
    if (valEl) valEl.textContent = `${f(d.current_value)} ${currency}`;
    if (retEl) retEl.textContent = `${(d.return_pct||0).toFixed(2)}%`;
    const result = document.getElementById('dcaResult');
    if (result) result.innerHTML = `買入次數：${d.buys} 次；頻率：${freq}；每期：${f(d.amount_per_period)} ${currency}<br/>持有數量：${(d.total_units||0).toFixed(6)} · 最新價：${f(d.latest_price)} ${currency}`;
  } catch (e) {
    const result = document.getElementById('dcaResult');
    if (result) result.textContent = '計算失敗';
  }
}

function bindPaperAndTools() {
  const openBtn = document.getElementById('paperOpenBtn');
  if (openBtn) openBtn.addEventListener('click', openPaperTrade);
  const dcaBtn = document.getElementById('dcaBtn');
  if (dcaBtn) dcaBtn.addEventListener('click', calcDCA);
  currencyEl.addEventListener('change', () => { loadPaperTrades(); loadPaperPerf(); });
  const roomInput = document.getElementById('paperRoom');
  const applyBtn = document.getElementById('roomApply');
  const clearBtn = document.getElementById('roomClear');
  if (roomInput) {
    try { roomInput.value = localStorage.getItem('paper_room') || ''; } catch {}
    if (applyBtn && !applyBtn._bound) {
      applyBtn._bound = true;
      applyBtn.addEventListener('click', () => { const v = (roomInput.value||'').trim(); try { if (v) localStorage.setItem('paper_room', v); else localStorage.removeItem('paper_room'); } catch {}; loadPaperTrades(); });
    }
    if (clearBtn && !clearBtn._bound) {
      clearBtn._bound = true;
      clearBtn.addEventListener('click', () => { try { localStorage.removeItem('paper_room'); } catch {}; roomInput.value=''; loadPaperTrades(); });
    }
  }

  // Collapsible gauges: default collapsed; toggle expand/collapse
  document.querySelectorAll('.collapse-toggle').forEach(btn => {
    if (btn._bound) return; btn._bound = true;
    btn.addEventListener('click', () => {
      const id = btn.getAttribute('data-target');
      const gauge = document.getElementById(id);
      if (!gauge) return;
      const isCollapsed = gauge.classList.toggle('collapsed');
      btn.textContent = isCollapsed ? '展開' : '縮小';
      // Re-render small charts when expanding
      if (!isCollapsed) {
        if (paperSparkChart) paperSparkChart.resize();
        if (perfSparkChart) perfSparkChart.resize();
      }
    });
  });
}

symbolEl.addEventListener('change', refresh);
intervalEl.addEventListener('change', refresh);
currencyEl.addEventListener('change', refresh);

refresh();
loadNews();
loadToplist();
bindToplistPager();
loadGainers();
bindGainersPager();
loadRecom();
loadPaperTrades();
loadPaperPerf();
bindPaperAndTools();
currencyEl.addEventListener('change', () => { loadRecom(); calcDCA(); });
bindWallet();
bindRiskCalc(); 
loadAllChains();
loadFng();