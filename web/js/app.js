const CAP_FILTERS = ['all', 'small', 'mega'];
const ETF_FILTERS = ['topAUM', 'topVol', 'spdr', 'spdrSectors', 'vanguard', 'vaneck', 'commodities', 'sector'];

const SOURCE_DEFS = {
  all:        { label: 'All' },
  none:       { label: 'None' },
  small:      { label: '$200B\u2212',  match: (s, r) => r.marketCap > 0 && r.marketCap < 200000000000 },
  mega:       { label: '$200B+',       match: (s, r) => r.marketCap >= 200000000000 },
  topAUM:     { label: 'Top AUM',      set: TOP_AUM },
  topVol:     { label: 'Top Vol',      set: TOP_VOL },
  spdr:       { label: 'SPDR',         set: SPDR },
  spdrSectors:{ label: 'Sectors',     set: SPDR_SECTORS },
  vanguard:   { label: 'Vanguard',     set: VANGUARD },
  vaneck:     { label: 'VanEck',       set: VANECK_ETFS },
  commodities:{ label: 'Commodities', set: COMMODITY_ETFS },
  sector:     { label: 'Other ETFs',   set: SECTOR_ETFS },
};

const SOURCE_ORDER = ['all', 'none', 'small', 'mega', 'topAUM', 'topVol', 'spdr', 'spdrSectors', 'vanguard', 'vaneck', 'commodities', 'sector'];

const DEFAULT_FILTERS = {
  close: { min: 10 },
  count: { min: 5 },
  weeksBelow: { min: 6 },
  weeksAbove: { min: 5 },
  monthsBelow: { min: 4 },
  monthsAbove: { min: 3 },
  pct: { min: 3 },
  wkStatus: { above: true, below: true, ontop: true },
  moStatus: { above: true, below: true, ontop: true },
  qtrStatus: { above: true, below: true, ontop: true },
};

const TAB_DEFS = [
  { id: 'etfDashboard', label: 'ETF Dashboard', special: 'summary', set: ALL_ETFS, cols: ['symbol','name','close','ytdPct','highPct','lowPct','wkStatus','moStatus','qtrStatus'] },
  { id: 'megaDashboard', label: 'Mega Cap Dashboard', special: 'summary', set: MAJOR_TARGETS, cols: ['symbol','name','close','ytdPct','highPct','lowPct','wkStatus','moStatus','qtrStatus'] },
  { id: 'crossovers',  label: 'Wk Cross Up',   file: '/results/latest.json',                      key: 'crossovers',      cols: ['symbol','name','close','weeksBelow','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],   pctField: 'pctAbove' },
  { id: 'crossdowns',  label: 'Wk Cross Down',  file: '/results/latest-crossdown.json',             key: 'crossdowns',      cols: ['symbol','name','close','weeksAbove','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],   pctField: 'pctBelow' },
  { id: 'weekBelow',   label: 'Wk Below',       file: '/results/latest-below.json',                 key: 'weekBelow',       cols: ['symbol','name','close','count','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],        pctField: 'pctBelow' },
  { id: 'weekAbove',   label: 'Wk Above',       file: '/results/latest-above.json',                 key: 'weekAbove',       cols: ['symbol','name','close','count','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],        pctField: 'pctAbove' },
  { id: 'moCrossUp',   label: 'Mo Cross Up',    file: '/results/latest-monthly.json',               key: 'monthCrossovers', cols: ['symbol','name','close','monthsBelow','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],  pctField: 'pctAbove' },
  { id: 'moCrossDown', label: 'Mo Cross Down',  file: '/results/latest-monthly.json',               key: 'monthCrossdowns', cols: ['symbol','name','close','monthsAbove','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],  pctField: 'pctBelow' },
  { id: 'moBelow',     label: 'Mo Below',       file: '/results/latest-monthly-below-above.json',   key: 'monthBelow',      cols: ['symbol','name','close','count','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],        pctField: 'pctBelow' },
  { id: 'moAbove',     label: 'Mo Above',       file: '/results/latest-monthly-below-above.json',   key: 'monthAbove',      cols: ['symbol','name','close','count','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],        pctField: 'pctAbove' },
  { id: 'qCrossUp',   label: 'Q Cross Up',     file: '/results/latest-quarterly.json',              key: 'quarterCrossovers', cols: ['symbol','name','close','quartersBelow','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'], pctField: 'pctAbove' },
  { id: 'qCrossDown', label: 'Q Cross Down',   file: '/results/latest-quarterly.json',              key: 'quarterCrossdowns', cols: ['symbol','name','close','quartersAbove','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'], pctField: 'pctBelow' },
  { id: 'qBelow',     label: 'Q Below',        file: '/results/latest-quarterly-below-above.json',  key: 'quarterBelow',      cols: ['symbol','name','close','count','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],        pctField: 'pctBelow' },
  { id: 'qAbove',     label: 'Q Above',        file: '/results/latest-quarterly-below-above.json',  key: 'quarterAbove',      cols: ['symbol','name','close','count','pct','ema','ytdPct','highPct','lowPct','rsi','forwardPE'],        pctField: 'pctAbove' },
  { id: 'performance', label: 'YTD Stats',        special: 'performance' },
  { id: 'priceBreaks', label: 'Price Breaks',   special: 'priceBreaks' },
  { id: 'sinceQ',      label: 'Since Quarter',  special: 'sinceQuarter' },
  { id: 'duringQ',     label: 'During Quarter', special: 'duringQuarter' },
  { id: 'fwdPE',       label: 'Forward P/E',    special: 'forwardPE' },
  { id: 'vixSpikes',   label: 'VIX Spikes',     special: 'vixSpikes' },
  { id: 'miscStats',   label: 'Misc Stats',     special: 'miscStats' },
];

const COL_LABELS = {
  symbol: 'Symbol', name: 'Name', close: 'Close', ema: 'EMA', pct: '% EMA',
  weeksBelow: 'Weeks Below', weeksAbove: 'Weeks Above',
  monthsBelow: 'Months Below', monthsAbove: 'Months Above',
  quartersBelow: 'Quarters Below', quartersAbove: 'Quarters Above',
  count: 'Count',
  ytdPct: 'YTD', highPct: 'vs High', high3yr: '3yr High', lowPct: 'vs Low', low52wk: '52wk Low',
  return1Y: '1 Year', return5Y: '5 Year',
  rsi: 'RSI', forwardPE: 'Fwd P/E', pctSma200d: '% 200D', pctSma200w: '% 200W',
  breakoutPct: 'Breakout %', breakdownPct: 'Breakdown %',
  breakoutPrice: 'Breakout', breakdownPrice: 'Breakdown',
  breakoutDate: 'Date', breakdownDate: 'Date',
  spikeClose: 'Spike Close', pctGain: '% Gain',
  wkStatus: 'Wk EMA', moStatus: 'Mo EMA', qtrStatus: 'Qtr EMA',
};

const CROSS_UP_TABS = new Set(['crossovers', 'moCrossUp', 'qCrossUp']);
const CROSS_DOWN_TABS = new Set(['crossdowns', 'moCrossDown', 'qCrossDown']);

const TAB_GROUPS = [
  { label: 'Dashboards', tabs: ['etfDashboard', 'megaDashboard'] },
  { label: 'Weekly',    tabs: ['crossovers', 'crossdowns', 'weekBelow', 'weekAbove'] },
  { label: 'Monthly',   tabs: ['moCrossUp', 'moCrossDown', 'moBelow', 'moAbove'] },
  { label: 'Quarterly', tabs: ['qCrossUp', 'qCrossDown', 'qBelow', 'qAbove'] },
  { label: 'Performance', tabs: ['performance', 'priceBreaks', 'sinceQ', 'duringQ', 'fwdPE', 'vixSpikes'] },
];

const GROUPED_TAB_IDS = new Set(TAB_GROUPS.flatMap(g => g.tabs));
const STANDALONE_TABS = TAB_DEFS.filter(t => !GROUPED_TAB_IDS.has(t.id));

const NUM_COLS = new Set(['close','ema','pct','weeksBelow','weeksAbove','monthsBelow','monthsAbove','quartersBelow','quartersAbove','count','ytdPct','highPct','high3yr','lowPct','low52wk','rsi','forwardPE','pctSma200d','pctSma200w','breakoutPct','breakdownPct','breakoutPrice','breakdownPrice','spikeClose','pctGain','return1Y','return5Y']);

const state = {
  activeSourceMode: 'all',
  data: {},
  statsMap: {},
  activeTab: 'etfDashboard',
  sortCol: null,
  sortAsc: true,
  filters: {},
  lowSignalActive: true,
  activeQuickFilters: new Set(),
  activeColorFilters: new Set(),
  manifest: [],
  selectedWeek: 'latest',
  dynPctCols: new Set(),
  dynPECols: new Set(),
  dynPEPrevCol: {},
  tabCountCache: {},
  showAllRows: false,
  isScanning: false,
  latestJson: null,
};

function debounce(fn, delay) {
  let timer;
  return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), delay); };
}

function resetTabState() {
  state.tabCountCache = {};
  state.showAllRows = false;
}

function switchToTab(tabId) {
  state.activeTab = tabId;
  state.sortCol = null;
  state.sortAsc = true;
  resetTabState();
  closeAllDropdowns();
  writeHash();
  renderTabs();
  showTab(state.activeTab);
}

function sortQuarterKeys(keys) {
  // Newest first (e.g. Q4'25, Q3'25, Q2'25, Q1'25, Q4'24...)
  return [...keys].sort((a, b) => {
    const [quarterA, yearA] = [a[1], a.slice(3)];
    const [quarterB, yearB] = [b[1], b.slice(3)];
    if (yearA !== yearB) return Number(yearB) - Number(yearA);
    return Number(quarterB) - Number(quarterA);
  });
}

/* ── Hash persistence ── */

function readHash() {
  const h = location.hash.replace('#', '');
  if (h && TAB_DEFS.some(t => t.id === h)) state.activeTab = h;
}

function writeHash() {
  history.replaceState(null, '', '#' + state.activeTab);
}

/* ── Source toggles ── */

function isSourceActive(id) {
  if (state.activeSourceMode === 'all') return id === 'all';
  if (state.activeSourceMode === 'none') return id === 'none';
  return state.activeSourceMode.has(id);
}

function renderSourceToggles() {
  const el = document.getElementById('source-toggles');
  const capIds = ['all', 'none', 'small', 'mega'];
  const etfIds = SOURCE_ORDER.filter(id => !capIds.includes(id));
  const btnHTML = ids => ids.map(id => {
    const cls = isSourceActive(id) ? ' active' : '';
    return `<button class="source-btn${cls}" data-src="${id}">${SOURCE_DEFS[id].label}</button>`;
  }).join('');
  el.innerHTML =
    `<div class="source-group">${btnHTML(capIds)}</div>` +
    `<div class="source-group">${btnHTML(etfIds)}</div>`;
  el.querySelectorAll('.source-btn').forEach(btn =>
    btn.addEventListener('click', () => {
      const id = btn.dataset.src;
      if (id === 'all') {
        state.activeSourceMode = 'all';
      } else if (id === 'none') {
        state.activeSourceMode = 'none';
      } else if (CAP_FILTERS.includes(id)) {
        state.activeSourceMode = new Set([id]);
      } else {
        let cur = (state.activeSourceMode instanceof Set) ? new Set(state.activeSourceMode) : new Set();
        CAP_FILTERS.forEach(c => cur.delete(c));
        if (cur.has(id)) cur.delete(id);
        else cur.add(id);
        state.activeSourceMode = cur.size > 0 ? cur : 'all';
      }
      resetTabState();
      renderSourceToggles();
      renderTabs();
      showTab(state.activeTab);
    })
  );
  updateSourceDrawerLabel();
}

function matchesSources(symbol, row) {
  if (state.activeSourceMode === 'all') return true;
  if (state.activeSourceMode === 'none') return false;
  return [...state.activeSourceMode].some(id => {
    const def = SOURCE_DEFS[id];
    if (def.match) return def.match(symbol, row);
    if (def.set) return def.set.has(symbol);
    return false;
  });
}

/* ── Default filters ── */

function applyLowSignalFilters() {
  Object.entries(DEFAULT_FILTERS).forEach(([c, def]) => {
    state.filters[c] = { ...def };
  });
}

function initDefaultFilters() {
  state.filters = {};
  state.activeQuickFilters.clear();
  state.lowSignalActive = true;
  applyLowSignalFilters();
}

function clearAllFilters() {
  state.filters = {};
  state.activeQuickFilters.clear();
  state.activeColorFilters.clear();
  state.lowSignalActive = false;
}

/* ── Quick filters ── */

const QUICK_FILTERS = [
  { id: 'low',  label: 'Low $',  col: 'close', values: { min: 3, max: 20 } },
  { id: 'mid',  label: 'Mid $',  col: 'close', values: { min: 20, max: 300 } },
  { id: 'high', label: 'High $', col: 'close', values: { min: 300, max: '' } },
];

const COLOR_FILTERS = [];

const COLOR_FILTERS_MAP = new Map(COLOR_FILTERS.map(cf => [cf.id, cf]));

/* ── Toolbar ── */

function renderToolbar(filteredCount, totalCount) {
  const el = document.getElementById('toolbar');
  const quickBtns = QUICK_FILTERS.map(qf => {
    const cls = state.activeQuickFilters.has(qf.id) ? ' active' : '';
    return `<button class="quick-btn${cls}" data-qf="${qf.id}">${qf.label}</button>`;
  }).join('');

  const colorBtns = COLOR_FILTERS.map(cf => {
    const cls = state.activeColorFilters.has(cf.id) ? ' active' : '';
    return `<button class="quick-btn${cls}" data-cf="${cf.id}">${cf.label}</button>`;
  }).join('');

  const lsCls = state.lowSignalActive ? ' active' : '';
  el.innerHTML =
    `<span class="row-count">Showing <strong>${filteredCount}</strong> of <strong>${totalCount}</strong></span>` +
    `<button class="quick-btn${lsCls}" id="low-signal-btn">Filter Low Signal</button>` +
    quickBtns +
    `<button class="reset-btn" id="clear-filters-btn">Remove Filters</button>`;

  document.getElementById('low-signal-btn').addEventListener('click', () => {
    state.lowSignalActive = !state.lowSignalActive;
    if (state.lowSignalActive) {
      applyLowSignalFilters();
    } else {
      Object.keys(DEFAULT_FILTERS).forEach(c => {
        delete state.filters[c];
      });
    }
    state.activeQuickFilters.clear();
    resetTabState();
    renderTabs();
    showTab(state.activeTab);
  });
  document.getElementById('clear-filters-btn').addEventListener('click', () => {
    clearAllFilters();
    resetTabState();
    renderTabs();
    showTab(state.activeTab);
  });
  el.querySelectorAll('.quick-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const qfId = btn.dataset.qf;
      const cfId = btn.dataset.cf;
      if (qfId) {
        const qf = QUICK_FILTERS.find(q => q.id === qfId);
        if (state.activeQuickFilters.has(qfId)) {
          state.activeQuickFilters.delete(qfId);
          if (DEFAULT_FILTERS[qf.col]) {
            state.filters[qf.col] = { ...DEFAULT_FILTERS[qf.col] };
          } else {
            delete state.filters[qf.col];
          }
        } else {
          QUICK_FILTERS.filter(q => q.col === qf.col && q.id !== qfId)
            .forEach(q => state.activeQuickFilters.delete(q.id));
          state.activeQuickFilters.add(qfId);
          state.filters[qf.col] = { ...qf.values };
        }
      } else if (cfId) {
        if (state.activeColorFilters.has(cfId)) {
          state.activeColorFilters.delete(cfId);
        } else {
          state.activeColorFilters.add(cfId);
        }
      }
      resetTabState();
      renderTabs();
      showTab(state.activeTab);
    });
  });
}

/* ── Sorting ── */

function sortRows(rows) {
  if (!state.sortCol) return;
  const col = state.sortCol;
  const asc = state.sortAsc;
  rows.sort((rowA, rowB) => {
    const valA = rowA[col], valB = rowB[col];
    if (typeof valA === 'number' && typeof valB === 'number') return asc ? valA - valB : valB - valA;
    return asc ? String(valA).localeCompare(String(valB)) : String(valB).localeCompare(String(valA));
  });
}

/* ── Filtering ── */

function applyFilters(rows, cols) {
  return rows.filter(row => {
    if (!matchesSources(row.symbol, row)) return false;
    for (const col of cols) {
      const filter = state.filters[col];
      if (!filter) continue;
      if (col === 'symbol') {
        if (filter.text && !String(row[col]).toLowerCase().includes(filter.text.toLowerCase())) return false;
      } else if (col === 'wkStatus' || col === 'moStatus' || col === 'qtrStatus') {
        const timeframeMap = { wkStatus: 'weekly', moStatus: 'monthly', qtrStatus: 'quarterly' };
        const status = row.emaStatus?.[timeframeMap[col]];
        if (!status || status.above == null) continue;
        
        const isOnTop = Math.abs(status.pctDiff || 0) <= 1.5;
        if (isOnTop) {
          if (!filter.ontop) return false;
        } else if (status.above) {
          if (!filter.above) return false;
        } else {
          if (!filter.below) return false;
        }
      } else {
        const val = row[col];
        if (val == null) return false;
        if (filter.min !== '' && filter.min != null && val < Number(filter.min)) return false;
        if (filter.max !== '' && filter.max != null && val > Number(filter.max)) return false;
      }
    }
    for (const cfId of state.activeColorFilters) {
      const colorFilter = COLOR_FILTERS_MAP.get(cfId);
      if (colorFilter && !colorFilter.test(row)) return false;
    }
    return true;
  });
}

/* ── Row rendering ── */

function renderRowHTML(row, cols, pctField, tabId) {
  return '<tr>' + cols.map(col => {
    const val = row[col];
    const emaCls = col === 'ema' ? ' col-ema' : '';
    if (col === 'symbol') {
      const interval = tabId.startsWith('mo') ? 'M' : 'W';
      const url = `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(val)}&interval=${interval}`;
      return `<td class="symbol"><a href="${url}" target="_blank">${esc(val)}</a></td>`;
    }
    if (col === 'pct') {
      const cls = val > 0 ? 'pct-positive' : val < 0 ? 'pct-negative' : '';
      return `<td class="num ${cls}">${fmtNum(col, val)}</td>`;
    }
    if (col === 'close') {
      const cls = row.close > row.ema ? 'close-above' : row.close < row.ema ? 'close-below' : '';
      return `<td class="num ${cls}">${fmtNum(col, val)}</td>`;
    }
    if (col === 'ytdPct' || col === 'highPct' || col === 'lowPct') {
      const isGreen = col === 'ytdPct' ? val >= 0 : col === 'highPct' ? val >= -5 : val > 5;
      const cls = val != null ? (isGreen ? 'pct-positive' : 'pct-negative') : '';
      return `<td class="num ${cls}">${fmtNum(col, val)}</td>`;
    }
    if (col === 'rsi' && val != null) {
      const cls = val > 70 ? 'pct-negative' : val < 30 ? 'pct-positive' : '';
      return `<td class="num ${cls}">${fmtNum(col, val)}</td>`;
    }
    if (col === 'pctSma200d' || col === 'pctSma200w') {
      const cls = val != null ? (val >= 0 ? 'pct-positive' : 'pct-negative') : '';
      return `<td class="num ${cls}">${fmtNum(col, val)}</td>`;
    }
    if (col === 'breakoutPct' || col === 'breakdownPct' || col === 'pctGain') {
      const cls = val != null ? (val >= 0 ? 'pct-positive' : 'pct-negative') : '';
      return `<td class="num ${cls}">${fmtNum(col, val)}</td>`;
    }
    if (tabId === 'fwdPE' && state.dynPECols.has(col)) {
      if (val == null) return `<td class="num">\u2014</td>`;
      const prevCol = state.dynPEPrevCol[col];
      const prevVal = prevCol ? row[prevCol] : null;
      let cls = '';
      if (prevVal != null) cls = val < prevVal ? 'pct-positive' : val > prevVal ? 'pct-negative' : '';
      return `<td class="num ${cls}">${val.toFixed(1)}</td>`;
    }
    if (state.dynPctCols.has(col) || (state.dynPECols.has(col) && tabId !== 'fwdPE')) {
      if (val == null) return `<td class="num">\u2014</td>`;
      const cls = val >= 0 ? 'pct-positive' : 'pct-negative';
      return `<td class="num ${cls}">${(val > 0 ? '+' : '') + val.toFixed(2)}%</td>`;
    }
    if (NUM_COLS.has(col)) return `<td class="num${emaCls}">${fmtNum(col, val)}</td>`;
    if (col === 'name') return `<td class="name">${esc(val)}</td>`;
    if (col === 'wkStatus' || col === 'moStatus' || col === 'qtrStatus') return `<td>${val}</td>`;
    return `<td>${esc(val)}</td>`;
  }).join('') + '</tr>';
}

const ROW_RENDER_LIMIT = 500;

function renderTbodyHTML(rows, cols, pctField, tabId) {
  if (rows.length === 0) {
    return `<tr><td colspan="${cols.length}" style="text-align:center;padding:32px;color:var(--text-muted);font-family:var(--font-ui)">No data</td></tr>`;
  }
  const limit = state.showAllRows ? rows.length : Math.min(rows.length, ROW_RENDER_LIMIT);
  let html = rows.slice(0, limit).map(row => renderRowHTML(row, cols, pctField, tabId)).join('');
  if (limit < rows.length) {
    html += `<tr class="show-all-row"><td colspan="${cols.length}" style="text-align:center;padding:16px;color:var(--accent);cursor:pointer;font-family:var(--font-ui);font-size:13px">Show all ${rows.length.toLocaleString()} rows (${(rows.length - limit).toLocaleString()} more)</td></tr>`;
  }
  return html;
}

/* ── Tabs ── */

function getTabCount(t) {
  if (t.id === 'miscStats') return '';
  if (t.special) {
    const rows = state.data[t.id] || [];
    return `${rows.filter(r => matchesSources(r.symbol, r)).length}`;
  }
  const total = state.data[t.id] ? state.data[t.id].filter(r => matchesSources(r.symbol, r)).length : 0;
  const filtered = state.data[t.id] ? applyFilters(state.data[t.id], t.cols).length : 0;
  const isActive = t.id === state.activeTab;
  const hasFilter = isActive && Object.keys(state.filters).length > 0 &&
    Object.values(state.filters).some(f => f.text || (f.min !== '' && f.min != null) || (f.max !== '' && f.max != null) || f.above !== undefined);
  return hasFilter ? `${filtered} / ${total}` : `${total}`;
}

function getTabCountCached(t) {
  if (t.id === state.activeTab) {
    const count = getTabCount(t);
    state.tabCountCache[t.id] = count;
    return count;
  }
  if (state.tabCountCache[t.id] != null) return state.tabCountCache[t.id];
  const count = getTabCount(t);
  state.tabCountCache[t.id] = count;
  return count;
}

const isMobile = () => window.innerWidth <= 640;

function renderTabs() {
  const el = document.getElementById('tabs');
  let html = '';

  // Grouped tabs (dropdowns)
  for (const group of TAB_GROUPS) {
    const groupTabDefs = group.tabs.map(id => TAB_DEFS.find(t => t.id === id)).filter(Boolean);
    const activeInGroup = groupTabDefs.find(t => t.id === state.activeTab);
    const activeLabel = activeInGroup ? activeInGroup.label : group.label;

    let btnCls = 'tab-group-btn';
    if (activeInGroup) {
      if (CROSS_UP_TABS.has(activeInGroup.id)) btnCls += ' has-cross-up';
      else if (CROSS_DOWN_TABS.has(activeInGroup.id)) btnCls += ' has-cross-down';
      else btnCls += ' active';
    }

    const countText = activeInGroup ? getTabCountCached(activeInGroup) : '';
    const countSpan = countText ? `<span class="count">${countText}</span>` : '';

    html += `<div class="tab-group" data-group-label="${esc(group.label)}">`;
    html += `<button class="${btnCls}">${activeLabel}${countSpan}</button>`;
    html += `<div class="tab-dropdown">`;
    for (const t of groupTabDefs) {
      const isActive = t.id === state.activeTab;
      const crossCls = CROSS_UP_TABS.has(t.id) ? ' cross-up' : CROSS_DOWN_TABS.has(t.id) ? ' cross-down' : '';
      const tabCount = getTabCountCached(t);
      const tabCountSpan = tabCount ? `<span class="count">${tabCount}</span>` : '';
      html += `<div class="tab${isActive ? ' active' : ''}${crossCls}" data-tab="${t.id}">${t.label}${tabCountSpan}</div>`;
    }
    html += `</div></div>`;
  }

  // Standalone tabs
  for (const t of STANDALONE_TABS) {
    const isActive = t.id === state.activeTab;
    const crossCls = CROSS_UP_TABS.has(t.id) ? ' cross-up' : CROSS_DOWN_TABS.has(t.id) ? ' cross-down' : '';
    const countText = getTabCountCached(t);
    const countSpan = countText ? `<span class="count">${countText}</span>` : '';
    html += `<div class="tab standalone${isActive ? ' active' : ''}${crossCls}" data-tab="${t.id}">${t.label}${countSpan}</div>`;
  }

  el.innerHTML = html;

  // Dropdown toggle — bottom sheet on mobile, position:fixed on desktop
  el.querySelectorAll('.tab-group-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const group = btn.parentElement;

      if (isMobile()) {
        openTabSheet(group);
        return;
      }

      const wasOpen = group.classList.contains('open');
      closeAllDropdowns();
      if (!wasOpen) {
        const rect = btn.getBoundingClientRect();
        const dropdown = group.querySelector('.tab-dropdown');
        group.classList.add('open');
        const ddRect = dropdown.getBoundingClientRect();
        let top = rect.bottom;
        let left = rect.left;
        if (left + ddRect.width > window.innerWidth - 8) left = window.innerWidth - ddRect.width - 8;
        if (left < 8) left = 8;
        if (top + ddRect.height > window.innerHeight - 8) top = rect.top - ddRect.height;
        if (top < 8) top = 8;
        dropdown.style.top = top + 'px';
        dropdown.style.left = left + 'px';
      }
    });
  });

  // Tab clicks (both dropdown and standalone)
  el.querySelectorAll('.tab[data-tab]').forEach(tab => {
    tab.addEventListener('click', () => switchToTab(tab.dataset.tab));
  });
}

function closeAllDropdowns() {
  document.querySelectorAll('.tab-group.open').forEach(g => g.classList.remove('open'));
}

document.addEventListener('click', (e) => {
  if (!e.target.closest('.tab-group')) closeAllDropdowns();
});

/* ── Mobile Bottom Sheet for tab groups ── */

function openTabSheet(groupEl) {
  const overlay = document.getElementById('tab-sheet-overlay');
  const title = document.getElementById('tab-sheet-title');
  const items = document.getElementById('tab-sheet-items');
  const label = groupEl.dataset.groupLabel || '';

  title.textContent = label;

  const groupTabDefs = [];
  const groupDef = TAB_GROUPS.find(g => g.label === label);
  if (!groupDef) return;

  let html = '';
  for (const id of groupDef.tabs) {
    const t = TAB_DEFS.find(td => td.id === id);
    if (!t) continue;
    const isActive = t.id === state.activeTab;
    const crossCls = CROSS_UP_TABS.has(t.id) ? ' cross-up' : CROSS_DOWN_TABS.has(t.id) ? ' cross-down' : '';
    const tabCount = getTabCountCached(t);
    const tabCountSpan = tabCount ? `<span class="count">${tabCount}</span>` : '';
    html += `<div class="tab${isActive ? ' active' : ''}${crossCls}" data-tab="${t.id}">${t.label}${tabCountSpan}</div>`;
  }

  items.innerHTML = html;
  overlay.classList.add('open');

  items.querySelectorAll('.tab[data-tab]').forEach(tab => {
    tab.addEventListener('click', () => {
      closeTabSheet();
      switchToTab(tab.dataset.tab);
    });
  });
}

function closeTabSheet() {
  document.getElementById('tab-sheet-overlay').classList.remove('open');
}

document.getElementById('tab-sheet-overlay').addEventListener('click', (e) => {
  if (e.target === e.currentTarget) closeTabSheet();
});

/* ── Mobile Source Drawer ── */

function updateSourceDrawerLabel() {
  const label = document.getElementById('source-drawer-label');
  if (!label) return;
  if (state.activeSourceMode === 'all') label.textContent = 'All';
  else if (state.activeSourceMode === 'none') label.textContent = 'None';
  else if (state.activeSourceMode instanceof Set) {
    label.textContent = [...state.activeSourceMode].map(id => SOURCE_DEFS[id]?.label || id).join(', ');
  }
}

document.getElementById('source-drawer-toggle').addEventListener('click', () => {
  const btn = document.getElementById('source-drawer-toggle');
  const el = document.getElementById('source-toggles');
  const isOpen = el.classList.contains('drawer-open');
  el.classList.toggle('drawer-open', !isOpen);
  btn.classList.toggle('open', !isOpen);
});

/* ── Table building helpers ── */

function renderTableHeader(def) {
  return def.cols.map(c => {
    const isSorted = state.sortCol === c;
    const arrow = isSorted ? (state.sortAsc ? '\u25B2' : '\u25BC') : '\u25B4';
    const cls = [c === 'symbol' ? 'col-symbol' : '', c === 'name' ? 'col-name' : '', NUM_COLS.has(c) ? 'num' : '', isSorted ? 'sorted' : '', c === 'ema' ? 'col-ema' : ''].filter(Boolean).join(' ');
    return `<th class="${cls}" data-col="${c}">${COL_LABELS[c]}<span class="sort-arrow">${arrow}</span></th>`;
  }).join('');
}

function renderFilterRow(def) {
  return def.cols.map(c => {
    const emaClass = c === 'ema' ? ' col-ema' : '';
    if (c === 'name') return `<td class="col-name"></td>`;
    if (c === 'symbol') {
      const val = state.filters[c]?.text || '';
      return `<td class="col-symbol-filter"><input type="text" class="filter-symbol" data-col="${c}" placeholder="Filter\u2026" value="${esc(val)}"></td>`;
    }
    if (c === 'wkStatus' || c === 'moStatus' || c === 'qtrStatus') {
      const f = state.filters[c] || { above: true, below: true, ontop: true };
      return `<td><div class="ema-checkbox-filters">` +
        `<label class="ema-checkbox-group"><input type="checkbox" data-col="${c}" data-bound="above" ${f.above ? 'checked' : ''}> Above</label>` +
        `<label class="ema-checkbox-group"><input type="checkbox" data-col="${c}" data-bound="below" ${f.below ? 'checked' : ''}> Below</label>` +
        `<label class="ema-checkbox-group"><input type="checkbox" data-col="${c}" data-bound="ontop" ${f.ontop ? 'checked' : ''}> On-Top</label>` +
        `</div></td>`;
    }
    const f = state.filters[c] || {};
    const minVal = f.min != null ? f.min : '';
    const stackClass = c === 'close' ? ' num-filters-stacked' : '';
    const defaultBound = c === 'rsi' ? 'max' : 'min';
    const defaultVal = defaultBound === 'max' ? (f.max != null ? f.max : '') : minVal;
    let cell = `<td class="num${emaClass}"><div class="num-filters${stackClass}">` +
      `<div class="num-filter-group">` +
        `<button class="spin-btn" data-col="${c}" data-bound="${defaultBound}" data-dir="-1">&#x2212;</button>` +
        `<input type="number" class="filter-num" data-col="${c}" data-bound="${defaultBound}" placeholder="${defaultBound}" value="${defaultVal}">` +
        `<button class="spin-btn" data-col="${c}" data-bound="${defaultBound}" data-dir="1">+</button>` +
      `</div>`;
    if (c === 'close') {
      const maxVal = f.max != null ? f.max : '';
      cell +=
        `<div class="num-filter-group">` +
          `<button class="spin-btn" data-col="${c}" data-bound="max" data-dir="-1">&#x2212;</button>` +
          `<input type="number" class="filter-num" data-col="${c}" data-bound="max" placeholder="max" value="${maxVal}">` +
          `<button class="spin-btn" data-col="${c}" data-bound="max" data-dir="1">+</button>` +
        `</div>`;
    }
    return cell + `</div></td>`;
  }).join('');
}

function attachFilterListeners(def, tabId) {
  document.querySelectorAll('th[data-col]').forEach(th =>
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (state.sortCol === col) state.sortAsc = !state.sortAsc;
      else { state.sortCol = col; state.sortAsc = NUM_COLS.has(col) ? false : true; }
      state.showAllRows = false;
      document.querySelectorAll('th[data-col]').forEach(h => {
        const isSorted = state.sortCol === h.dataset.col;
        h.classList.toggle('sorted', isSorted);
        const arrow = h.querySelector('.sort-arrow');
        if (arrow) arrow.textContent = isSorted ? (state.sortAsc ? '\u25B2' : '\u25BC') : '\u25B4';
      });
      updateRows(tabId);
    })
  );

  const debouncedFilterUpdate = debounce(() => {
    if (state.activeTab !== tabId) return;
    state.showAllRows = false;
    renderTabs();
    updateRows(tabId);
  }, 150);
  document.querySelectorAll('.filter-row input').forEach(input => {
    input.addEventListener('input', () => {
      if (input.type === 'checkbox') return;
      const col = input.dataset.col;
      const bound = input.dataset.bound;
      if (col === 'symbol') {
        if (!state.filters[col]) state.filters[col] = {};
        state.filters[col].text = input.value;
      } else {
        if (!state.filters[col]) state.filters[col] = {};
        state.filters[col][bound] = input.value;
      }
      state.lowSignalActive = false;
      debouncedFilterUpdate();
    });
    if (input.type === 'checkbox') {
      input.addEventListener('change', () => {
        const col = input.dataset.col;
        const bound = input.dataset.bound;
        if (!state.filters[col]) state.filters[col] = { above: true, below: true, ontop: true };
        state.filters[col][bound] = input.checked;
        state.lowSignalActive = false;
        debouncedFilterUpdate();
      });
    }
  });

  document.querySelectorAll('.spin-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const col = btn.dataset.col;
      const bound = btn.dataset.bound;
      const dir = Number(btn.dataset.dir);
      if (!state.filters[col]) state.filters[col] = {};
      const cur = Number(state.filters[col][bound]) || 0;
      state.filters[col][bound] = cur + dir;
      const input = btn.closest('.num-filter-group').querySelector('input');
      if (input) input.value = state.filters[col][bound];
      state.lowSignalActive = false;
      resetTabState();
      renderTabs();
      updateRows(tabId);
    });
  });
}

function attachShowAllListener(tabId) {
  const el = document.querySelector('.show-all-row');
  if (el) el.addEventListener('click', () => { state.showAllRows = true; updateRows(tabId); });
}

function updateRowCount(filteredCount, totalCount) {
  const el = document.querySelector('.row-count');
  if (!el) return false;
  el.innerHTML = `Showing <strong>${filteredCount}</strong> of <strong>${totalCount}</strong>`;
  return true;
}

/* ── Main tab display ── */

function renderSortableTable(tabId, cols) {
  console.log('renderSortableTable: starting', tabId);
  const def = { cols };
  const allRows = state.data[tabId] || [];
  let rows = applyFilters([...allRows], cols);
  sortRows(rows);

  const totalSourced = allRows.filter(row => matchesSources(row.symbol)).length;
  console.log('renderSortableTable: rows filtered', rows.length, 'total', totalSourced);
  renderToolbar(rows.length, totalSourced);

  const ths = renderTableHeader(def);
  const filterCells = renderFilterRow(def);
  const body = renderTbodyHTML(rows, cols, null, tabId);

  console.log('renderSortableTable: updating innerHTML');
  const contentEl = document.getElementById('content');
  contentEl.innerHTML =
    `<div class="table-wrap"><table>` +
    `<thead><tr>${ths}</tr><tr class="filter-row">${filterCells}</tr></thead>` +
    `<tbody>${body}</tbody>` +
    `</table></div>`;
  console.log('renderSortableTable: update complete, current innerHTML length:', contentEl.innerHTML.length);

  attachFilterListeners(def, tabId);
  attachShowAllListener(tabId);
}

function showTab(tabId) {
  console.log('showTab: starting', tabId);
  const def = TAB_DEFS.find(t => t.id === tabId);

  if (def.special) {
    console.log('showTab: rendering special tab', def.special);
    showSpecialTab(def);
    return;
  }

  console.log('showTab: rendering sortable table');
  renderSortableTable(tabId, def.cols);
}

const SPECIAL_TAB_COLS = {
  performance: ['symbol','close','ytdPct','return1Y','return5Y','highPct','lowPct','forwardPE','pctSma200d','pctSma200w'],
  priceBreaks: ['symbol','close','breakoutPrice','breakoutDate','breakoutPct','breakdownPrice','breakdownDate','breakdownPct'],
};

function updateRows(tabId) {
  const def = TAB_DEFS.find(t => t.id === tabId);
  const cols = def.cols || SPECIAL_TAB_COLS[tabId];
  if (!cols) return;
  const allRows = state.data[tabId] || [];
  let rows = applyFilters([...allRows], cols);
  sortRows(rows);

  const totalSourced = allRows.filter(r => matchesSources(r.symbol, r)).length;
  if (!updateRowCount(rows.length, totalSourced)) renderToolbar(rows.length, totalSourced);

  const tbody = document.querySelector('.table-wrap tbody');
  if (!tbody) return;
  tbody.innerHTML = renderTbodyHTML(rows, cols, def.pctField, tabId);
  attachShowAllListener(tabId);
}

/* ── Formatting ── */

function fmtNum(col, v) {
  if (v == null) return '\u2014';
  if (col === 'close' || col === 'ema' || col === 'breakoutPrice' || col === 'breakdownPrice' || col === 'spikeClose' || col === 'high3yr' || col === 'low52wk') return v.toFixed(2);
  if (col === 'pct' || col === 'ytdPct' || col === 'highPct' || col === 'lowPct' || col === 'breakoutPct' || col === 'breakdownPct' || col === 'pctGain' || col === 'pctSma200d' || col === 'pctSma200w') return (v > 0 ? '+' : '') + v.toFixed(2) + '%';
  if (col === 'rsi') return v.toFixed(1);
  if (col === 'forwardPE') return v.toFixed(1);
  return v.toLocaleString();
}

function formatStatus(status) {
  if (!status || status.above == null) return '\u2014';
  const isOnTop = Math.abs(status.pctDiff || 0) <= 1.5;
  if (isOnTop) return `<span style="color:var(--blue)">On-Top</span>`;
  const cls = status.above ? 'pct-positive' : 'pct-negative';
  return `<span class="${cls}">${status.above ? 'Above' : 'Below'}</span>`;
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function populateTabData(byUrl) {
  state.tabCountCache = {};
  TAB_DEFS.forEach(t => {
    if (t.special) return;
    const json = byUrl[t.file];
    if (!json) { state.data[t.id] = []; return; }
    let rows;
    // Backward compat: old format had "below" with "weeksBelow" field
    if (t.key === 'weekBelow' && !json.weekBelow && json.below) {
      rows = json.below.map(row => ({...row, count: row.weeksBelow ?? row.count}));
    } else {
      rows = json[t.key] || [];
    }
    state.data[t.id] = enrichRows(rows, t);
  });
  buildSpecialTabData();
}

function enrichRows(rows, t) {
  return rows.map(r => {
    const s = state.statsMap[r.symbol] || {};
    return {
      ...r,
      name: r.name || s.name || '',
      pct: r[t.pctField] != null ? r[t.pctField] : null,
      ytdPct: s.ytdPct ?? null,
      highPct: s.highPct ?? null,
      lowPct: s.lowPct ?? null,
      rsi: s.rsi ?? null,
      forwardPE: s.forwardPE ?? null,
      marketCap: s.marketCap ?? null,
      emaStatus: s.emaStatus || {},
      wkStatus: formatStatus(s.emaStatus?.weekly),
      moStatus: formatStatus(s.emaStatus?.monthly),
      qtrStatus: formatStatus(s.emaStatus?.quarterly),
    };
  });
}

/* ── Meta ── */

async function renderMeta(json) {
  console.log('renderMeta: starting');
  if (json) state.latestJson = json;
  const currentJson = state.latestJson || {};
  
  const timestamp = currentJson.scanTime ? new Date(currentJson.scanTime).toLocaleString() : '\u2014';
  const updatedText = state.isScanning ? '<strong style="color:var(--accent)">Scan in Progress</strong>' : `<strong>${timestamp}</strong>`;
  
  const scanned = (currentJson.symbolsScanned || 0).toLocaleString();
  const errs = currentJson.errors || 0;

  let total = '';
  try {
    const cb = `_=${Date.now()}`;
    const resp = await fetch(`/symbols/us-equities.txt?${cb}`);
    if (resp.ok) {
      const text = await resp.text();
      const count = text.trim().split('\n').filter(Boolean).length;
      total = ` / ${count.toLocaleString()}`;
    }
  } catch (err) { console.warn('[meta] symbol count fetch failed:', err.message); }

  const errStyle = errs ? ' style="color:var(--red)"' : '';
  const metaEl = document.getElementById('meta');
  metaEl.innerHTML =
    `<span class="meta">Updated ${updatedText}</span>` +
    `<span class="meta-sep">|</span>` +
    `<span class="meta"><a href="/symbols/us-equities.txt" target="_blank" class="meta-link">${scanned}${total} symbols</a></span>` +
    `<span class="meta-sep">|</span>` +
    `<span class="meta"><a href="/results/latest-errors.json" target="_blank" class="meta-link"${errStyle}>${errs} errors</a></span>`;
  console.log('renderMeta: update complete');
}

/* ── Week selector ── */

function fileURLForWeek(latestPath, week) {
  if (week === 'latest') return latestPath;
  return latestPath.replace(/\/latest([-.])/g, `/${week}$1`);
}

function formatWeekLabel(dateStr) {
  const parts = dateStr.split('-');
  if (parts.length !== 3) return dateStr;
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const m = parseInt(parts[1], 10);
  const d = parseInt(parts[2], 10);
  return `${months[m - 1]} ${d}`;
}

function renderWeekSelector() {
  const el = document.getElementById('week-selector');
  if (state.manifest.length < 1) {
    el.style.display = 'none';
    return;
  }
  el.style.display = '';
  const latestCls = state.selectedWeek === 'latest' ? ' active' : '';
  let html = `<button class="source-btn${latestCls}" data-week="latest">Latest</button>`;
  for (const week of state.manifest) {
    const cls = state.selectedWeek === week ? ' active' : '';
    html += `<button class="source-btn${cls}" data-week="${esc(week)}">${formatWeekLabel(week)}</button>`;
  }
  el.innerHTML = html;
  el.querySelectorAll('.source-btn').forEach(btn =>
    btn.addEventListener('click', () => switchWeek(btn.dataset.week))
  );
}

async function switchWeek(week) {
  if (week === state.selectedWeek) return;
  state.selectedWeek = week;
  renderWeekSelector();

  const cb = `_=${Date.now()}`;
  const latestURLs = [...new Set(TAB_DEFS.filter(t => t.file).map(t => t.file))];
  const weekURLs = latestURLs.map(u => fileURLForWeek(u, week));
  const statsURL = fileURLForWeek('/results/latest-stats.json', week);
  const [statsResult, ...results] = await Promise.allSettled([
    fetch(`${statsURL}?${cb}`).then(r => {
      if (!r.ok) throw new Error(r.status);
      return r.json();
    }),
    ...weekURLs.map(u => fetch(`${u}?${cb}`).then(r => {
      if (!r.ok) throw new Error(r.status);
      return r.json();
    })),
  ]);

  if (statsResult.status === 'fulfilled' && statsResult.value.stats) {
    state.statsMap = Object.fromEntries(statsResult.value.stats.map(s => [s.symbol, s]));
    if (statsResult.value.misc) state.statsMap._misc = statsResult.value.misc;
  } else {
    state.statsMap = {};
  }

  const byLatestUrl = {};
  latestURLs.forEach((u, i) => {
    if (results[i].status === 'fulfilled') byLatestUrl[u] = results[i].value;
  });

  const meta = byLatestUrl[latestURLs[0]] || byLatestUrl[latestURLs[1]] || byLatestUrl[latestURLs[2]];
  if (meta) renderMeta(meta);

  populateTabData(byUrl);

  renderTabs();
  showTab(state.activeTab);
}

/* ── Special tab rendering ── */

function showSpecialTab(def) {
  if (def.special === 'miscStats') return showMiscStats();
  if (def.special === 'summary') return showSummaryDashboard(def);
  // All other special tabs use standard sortable/filterable table
  return showSortableSpecialTab(def.id);
}

function showSummaryDashboard(def) {
  console.log('showSummaryDashboard: starting', def.id);
  const stats = Object.values(state.statsMap).filter(s => s.symbol && def.set.has(s.symbol));
  console.log('showSummaryDashboard: found stats', stats.length);
  state.data[def.id] = enrichRows(stats, def);
  console.log('showSummaryDashboard: enriched rows', state.data[def.id].length);
  renderSortableTable(def.id, def.cols);
}

function showSortableSpecialTab(tabId) {
  const cols = SPECIAL_TAB_COLS[tabId];
  if (!cols) return;
  renderSortableTable(tabId, cols);
}

function showMiscStats() {
  const misc = state.miscStats || {};
  document.getElementById('toolbar').innerHTML = '';

  const cards = [
    { label: 'Within 5% of High', value: misc.pctWithin5OfHigh != null ? misc.pctWithin5OfHigh.toFixed(1) + '%' : '\u2014' },
    { label: 'Positive YTD', value: misc.pctPositiveYTD != null ? misc.pctPositiveYTD.toFixed(1) + '%' : '\u2014' },
    { label: 'Avg YTD', value: misc.avgYTD != null ? (misc.avgYTD > 0 ? '+' : '') + misc.avgYTD.toFixed(2) + '%' : '\u2014', color: misc.avgYTD != null ? (misc.avgYTD >= 0 ? 'var(--green)' : 'var(--red)') : null },
    { label: 'Avg Fwd P/E', value: misc.avgForwardPE != null ? misc.avgForwardPE.toFixed(1) : '\u2014' },
    { label: 'Median Fwd P/E', value: misc.medianForwardPE != null ? misc.medianForwardPE.toFixed(1) : '\u2014' },
    { label: 'Above 5wk EMA', value: misc.pctAbove5wkEMA != null ? misc.pctAbove5wkEMA.toFixed(1) + '%' : '\u2014', color: misc.pctAbove5wkEMA != null ? 'var(--green)' : null },
    { label: 'Below 5wk EMA', value: misc.pctBelow5wkEMA != null ? misc.pctBelow5wkEMA.toFixed(1) + '%' : '\u2014', color: misc.pctBelow5wkEMA != null ? 'var(--red)' : null },
    { label: 'Above 200D SMA', value: misc.pctAbove200dSMA != null ? misc.pctAbove200dSMA.toFixed(1) + '%' : '\u2014', color: misc.pctAbove200dSMA != null ? 'var(--green)' : null },
    { label: 'Above 200W SMA', value: misc.pctAbove200wSMA != null ? misc.pctAbove200wSMA.toFixed(1) + '%' : '\u2014', color: misc.pctAbove200wSMA != null ? 'var(--green)' : null },
    { label: 'SPX Since Election', value: misc.spxSinceElection != null ? (misc.spxSinceElection > 0 ? '+' : '') + misc.spxSinceElection.toFixed(2) + '%' : '\u2014', color: misc.spxSinceElection != null ? (misc.spxSinceElection >= 0 ? 'var(--green)' : 'var(--red)') : null },
    { label: 'SPX Since Inauguration', value: misc.spxSinceInauguration != null ? (misc.spxSinceInauguration > 0 ? '+' : '') + misc.spxSinceInauguration.toFixed(2) + '%' : '\u2014', color: misc.spxSinceInauguration != null ? (misc.spxSinceInauguration >= 0 ? 'var(--green)' : 'var(--red)') : null },
    { label: 'SPX Since 2022 Bottom', value: misc.spxSinceBottom2022 != null ? (misc.spxSinceBottom2022 > 0 ? '+' : '') + misc.spxSinceBottom2022.toFixed(2) + '%' : '\u2014', color: misc.spxSinceBottom2022 != null ? (misc.spxSinceBottom2022 >= 0 ? 'var(--green)' : 'var(--red)') : null },
    { label: 'SPX Since ChatGPT', value: misc.spxSinceChatGPT != null ? (misc.spxSinceChatGPT > 0 ? '+' : '') + misc.spxSinceChatGPT.toFixed(2) + '%' : '\u2014', color: misc.spxSinceChatGPT != null ? (misc.spxSinceChatGPT >= 0 ? 'var(--green)' : 'var(--red)') : null },
  ];

  let html = `<div class=\"stat-grid\">` +
    cards.map(c => {
      const colorStyle = c.color ? ` style=\"color:${c.color}\"` : '';
      return `<div class=\"stat-card\">` +
        `<div class=\"stat-label\">${c.label}</div>` +
        `<div class=\"stat-value\"${colorStyle}>${c.value}</div>` +
        `</div>`;
    }).join('') +
    `</div>`;

  if (misc.benchmarkByTicker) {
    html += `<div class=\"stat-section-title\">Index & Sector Benchmarks</div>`;
    html += `<div class=\"table-wrap benchmark-table\"><table><thead><tr><th>Symbol</th><th class=\"num\">YTD</th><th class=\"num\">1 Year</th><th class=\"num\">5 Year</th></tr></thead><tbody>`;
    
    Object.entries(misc.benchmarkByTicker).forEach(([sym, s]) => {
      const ytdCls = s.ytd >= 0 ? 'pct-positive' : 'pct-negative';
      const oneYCls = s.oneY >= 0 ? 'pct-positive' : 'pct-negative';
      const fiveYCls = s.fiveY >= 0 ? 'pct-positive' : 'pct-negative';
      
      html += `<tr>` +
        `<td class=\"symbol\">${sym}</td>` +
        `<td class=\"num ${ytdCls}\">${s.ytd != null ? (s.ytd > 0 ? '+' : '') + s.ytd.toFixed(2) + '%' : '\u2014'}</td>` +
        `<td class=\"num ${oneYCls}\">${s.oneY != null ? (s.oneY > 0 ? '+' : '') + s.oneY.toFixed(2) + '%' : '\u2014'}</td>` +
        `<td class=\"num ${fiveYCls}\">${s.fiveY != null ? (s.fiveY > 0 ? '+' : '') + s.fiveY.toFixed(2) + '%' : '\u2014'}</td>` +
        `</tr>`;
    });
    html += `</tbody></table></div>`;
  }

  document.getElementById('content').innerHTML = html;
}

function buildSpecialTabData() {
  const stats = Object.values(state.statsMap).filter(s => s.symbol);

  // Clear dynamic column registrations
  state.dynPctCols = new Set();
  state.dynPECols = new Set();
  state.dynPEPrevCol = {};

  // Performance
  state.data['performance'] = stats.map(s => ({
    symbol: s.symbol, name: s.name || '', close: s.close ?? null,
    ytdPct: s.ytdPct ?? null, return1Y: s.return1Y ?? null, return5Y: s.return5Y ?? null,
    highPct: s.highPct ?? null, lowPct: s.lowPct ?? null,
    rsi: s.rsi ?? null, forwardPE: s.forwardPE ?? null,
    pctSma200d: s.pctSma200d ?? null, pctSma200w: s.pctSma200w ?? null,
  }));

  // Price Breaks — single combined table
  state.data['priceBreaks'] = stats
    .filter(s => s.breakoutPct != null || s.breakdownPct != null)
    .map(s => ({
      symbol: s.symbol, name: s.name || '', close: s.close ?? null,
      breakoutPrice: s.breakoutPrice ?? null, breakoutDate: s.breakoutDate ?? null, breakoutPct: s.breakoutPct ?? null,
      breakdownPrice: s.breakdownPrice ?? null, breakdownDate: s.breakdownDate ?? null, breakdownPct: s.breakdownPct ?? null,
      rsi: s.rsi ?? null,
    }));

  // Quarterly — Since Quarter
  _buildQuarterlyData(stats, 'sinceQuarter', 'sinceQ');
  _buildQuarterlyData(stats, 'duringQuarter', 'duringQ');

  // Forward P/E
  _buildForwardPEData(stats);

  // VIX Spikes
  _buildVixSpikeData(stats);

  // Misc Stats
  state.data['miscStats'] = [];
  state.miscStats = state.statsMap._misc || {};
}

function _buildQuarterlyData(stats, field, tabId) {
  const filtered = stats.filter(s => s[field]);
  const qKeySet = new Set();
  filtered.forEach(s => Object.keys(s[field]).forEach(k => qKeySet.add(k)));
  const qKeys = sortQuarterKeys(qKeySet);

  state.data[tabId] = filtered.map(s => {
    const row = { symbol: s.symbol, name: s.name || '', close: s.close ?? null, highPct: s.highPct ?? null };
    qKeys.forEach(k => { row[k] = s[field][k] ?? null; });
    return row;
  });

  const cols = ['symbol', 'name', 'close', 'highPct', ...qKeys];
  SPECIAL_TAB_COLS[tabId] = cols;
  qKeys.forEach(k => {
    COL_LABELS[k] = COL_LABELS[k] || k;
    NUM_COLS.add(k);
    state.dynPctCols.add(k);
  });
}

function _buildForwardPEData(stats) {
  const filtered = stats.filter(s => s.forwardPEHistory);
  const qKeySet = new Set();
  filtered.forEach(s => Object.keys(s.forwardPEHistory).forEach(k => qKeySet.add(k)));
  const qKeys = sortQuarterKeys(qKeySet);

  state.data['fwdPE'] = filtered.map(s => {
    const row = { symbol: s.symbol, name: s.name || '', close: s.close ?? null, forwardPE: s.forwardPE ?? null };
    qKeys.forEach(k => { row[k] = s.forwardPEHistory[k] ?? null; });
    return row;
  });

  const cols = ['symbol', 'name', 'close', 'forwardPE', ...qKeys];
  SPECIAL_TAB_COLS['fwdPE'] = cols;
  qKeys.forEach((k, i) => {
    COL_LABELS[k] = COL_LABELS[k] || k;
    NUM_COLS.add(k);
    state.dynPECols.add(k);
    if (i > 0) state.dynPEPrevCol[k] = qKeys[i - 1];
  });
}

function _buildVixSpikeData(stats) {
  const filtered = stats.filter(s => s.vixReturns && s.vixReturns.length);
  const spikeDateSet = new Set();
  filtered.forEach(s => s.vixReturns.forEach(ret => spikeDateSet.add(ret.dateString)));
  const spikeDates = [...spikeDateSet];

  // Collect VIX close for labels; parse M/D/YY dates to sort newest-first
  const spikeVixClose = {};
  filtered.forEach(s => s.vixReturns.forEach(ret => {
    if (!spikeVixClose[ret.dateString]) spikeVixClose[ret.dateString] = ret.vixClose;
  }));
  function parseMDYY(s) {
    const [m, d, y] = s.split('/').map(Number);
    return new Date(2000 + y, m - 1, d).getTime();
  }
  spikeDates.sort((a, b) => parseMDYY(b) - parseMDYY(a));

  state.data['vixSpikes'] = filtered.map(s => {
    const row = { symbol: s.symbol, name: s.name || '', close: s.close ?? null, highPct: s.highPct ?? null };
    const gainByDate = {};
    s.vixReturns.forEach(ret => { gainByDate[ret.dateString] = ret.pctGain; });
    spikeDates.forEach(dateStr => { row['spike_' + dateStr] = gainByDate[dateStr] ?? null; });
    return row;
  });

  const spikeCols = spikeDates.map(dateStr => 'spike_' + dateStr);
  const cols = ['symbol', 'name', 'close', 'highPct', ...spikeCols];
  SPECIAL_TAB_COLS['vixSpikes'] = cols;
  spikeCols.forEach((spikeCol, idx) => {
    const dateStr = spikeDates[idx];
    COL_LABELS[spikeCol] = `${dateStr} (${spikeVixClose[dateStr]?.toFixed(1) || ''})`;
    NUM_COLS.add(spikeCol);
    state.dynPctCols.add(spikeCol);
  });
}

/* ── Fetch & init ── */

async function fetchAll(isSilentRefresh = false) {
  readHash();

  const btn = document.getElementById('run-scan-btn');
  let isChecking = false;

  async function checkScanStatus() {
    if (isChecking) return;
    isChecking = true;
    try {
      const res = await fetch('__ORCHESTRATOR_URL__?dev_key=__DEV_KEY__', { method: 'GET' });
      if (res.ok) {
        const data = await res.json();
        const wasScanning = state.isScanning;
        state.isScanning = data.running;
        
        if (state.isScanning !== wasScanning) {
          renderMeta();
        }

        if (data.running) {
          btn.textContent = 'Scan in Progress';
          btn.disabled = true;
          setTimeout(checkScanStatus, 30000); // Poll every 30s instead of 60s
        } else {
          if (btn.disabled && btn.textContent === 'Scan in Progress') { // just finished
             fetchAll(true);
          }
          btn.textContent = 'Run Scan';
          btn.disabled = false;
        }
      }
    } catch (e) {
      console.error('Status check failed:', e);
    } finally {
      isChecking = false;
    }
  }

  // Only attach the listener once
  if (!btn.hasAttribute('data-listener')) {
    btn.setAttribute('data-listener', 'true');
    btn.addEventListener('click', async () => {
      btn.disabled = true;
      btn.textContent = 'Starting...';
      try {
        const res = await fetch('__ORCHESTRATOR_URL__?dev_key=__DEV_KEY__', { method: 'POST' });
        if (res.status === 429 || res.ok) {
          btn.textContent = 'Scan in Progress';
          setTimeout(checkScanStatus, 5000); 
        } else {
          btn.textContent = 'Failed';
          setTimeout(() => { btn.textContent = 'Run Scan'; btn.disabled = false; }, 3000);
        }
      } catch (e) {
        btn.textContent = 'Error';
        setTimeout(() => { btn.textContent = 'Run Scan'; btn.disabled = false; }, 3000);
      }
    });
    
    // Kick off initial poll immediately
    checkScanStatus();
  }

  const cb = `_=${Date.now()}`;
  const urls = [...new Set(TAB_DEFS.filter(t => t.file).map(t => t.file))];
  
  if (!isSilentRefresh) {
    document.getElementById('content').innerHTML = '<div class="loading">Loading scan data&hellip;</div>';
  }

  const [manifestResult, statsResult, ...results] = await Promise.allSettled([
    fetch(`/results/manifest.json?${cb}`).then(r => {
      if (!r.ok) throw new Error(r.status);
      return r.json();
    }),
    fetch(`/results/latest-stats.json?${cb}`).then(r => {
      if (!r.ok) throw new Error(r.status);
      return r.json();
    }),
    ...urls.map(u => fetch(`${u}?${cb}`).then(r => {
      if (!r.ok) throw new Error(r.status);
      return r.json();
    })),
  ]);

  if (manifestResult.status === 'fulfilled') {
    const mv = manifestResult.value;
    state.manifest = Array.isArray(mv) ? mv : (mv.weeks || []);
  }

  if (statsResult.status === 'fulfilled' && statsResult.value.stats) {
    state.statsMap = Object.fromEntries(statsResult.value.stats.map(s => [s.symbol, s]));
    if (statsResult.value.misc) state.statsMap._misc = statsResult.value.misc;
  }

  const byUrl = {};
  urls.forEach((u, i) => {
    if (results[i].status === 'fulfilled') byUrl[u] = results[i].value;
  });

  const meta = byUrl[urls[0]] || byUrl[urls[1]] || byUrl[urls[2]];
  if (meta) renderMeta(meta);

  populateTabData(byUrl);

  if (!Object.values(byUrl).length) {
    document.getElementById('content').innerHTML = '<div class="error-msg">Failed to load scan data.</div>';
    return;
  }

  initDefaultFilters();
  renderSourceToggles();
  renderWeekSelector();
  renderTabs();
  writeHash();
  showTab(state.activeTab);
}

fetchAll();
