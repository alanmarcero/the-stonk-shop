/* ─────────────────────────────────────────────
   STONK SHOP — Pulse · Crosses · More
   ───────────────────────────────────────────── */

const ORCHESTRATOR_URL = '__ORCHESTRATOR_URL__';
const DEV_KEY = '__DEV_KEY__';

const VIEWS = ['pulse', 'crosses', 'more'];

const DEFAULT_WATCHLIST = ['IBIT', 'TMUS', 'SPY', 'QQQ', 'XLE', 'OIH'];

const SECTOR_ORDER = ['SPY','XLK','XLE','XLF','XLV','XLY','XLI','XLC','XLU','XLP','XLB','XLRE'];
const SECTOR_NAMES = {
  SPY:  'S&P 500',
  QQQ:  'Nasdaq 100',
  DIA:  'Dow 30',
  IWM:  'Russell 2000',
  VTV:  'Large-Cap Value',
  TMUS: 'T-Mobile',
  XLK:  'Technology',
  XLE:  'Energy',
  XLF:  'Financials',
  XLV:  'Healthcare',
  XLY:  'Consumer Discr.',
  XLI:  'Industrials',
  XLC:  'Communications',
  XLU:  'Utilities',
  XLP:  'Consumer Staples',
  XLB:  'Materials',
  XLRE: 'Real Estate',
};

const FILES = {
  weekly:    { up: '/results/latest.json',                       upKey: 'crossovers',      down: '/results/latest-crossdown.json',           downKey: 'crossdowns',      above: '/results/latest-above.json',                aboveKey: 'weekAbove', below: '/results/latest-below.json', belowKey: 'weekBelow' },
  monthly:   { up: '/results/latest-monthly.json',               upKey: 'monthCrossovers', down: '/results/latest-monthly.json',             downKey: 'monthCrossdowns', above: '/results/latest-monthly-below-above.json',  aboveKey: 'monthAbove', below: '/results/latest-monthly-below-above.json', belowKey: 'monthBelow' },
  quarterly: { up: '/results/latest-quarterly.json',             upKey: 'quarterCrossovers', down: '/results/latest-quarterly.json',         downKey: 'quarterCrossdowns', above: '/results/latest-quarterly-below-above.json', aboveKey: 'quarterAbove', below: '/results/latest-quarterly-below-above.json', belowKey: 'quarterBelow' },
};

const COUNT_KEYS = {
  weekly: { up: 'weeksBelow', down: 'weeksAbove' },
  monthly: { up: 'monthsBelow', down: 'monthsAbove' },
  quarterly: { up: 'quartersBelow', down: 'quartersAbove' },
};

const state = {
  view: 'pulse',
  raw: {},
  stats: {},
  misc: {},
  manifest: [],
  selectedWeek: 'latest',
  isScanning: false,
  scanTime: null,
  errorsCount: 0,
  symbolsScanned: 0,
  totalSymbols: 0,

  crossTimeframe: 'weekly',  // weekly | monthly | quarterly
  crossDirection: 'up',       // up | down | above | below
  crossFilter: 'all',         // all | watch | sectors | mega | small
  crossSearch: '',

  watchlist: loadWatchlist(),
  watchlistEditing: false,

  moreOpen: {},
};

/* ── Persistence ── */

function loadWatchlist() {
  try {
    const v = JSON.parse(localStorage.getItem('ss_watchlist'));
    if (Array.isArray(v)) return v.filter(x => typeof x === 'string').slice(0, 30);
  } catch {}
  return [...DEFAULT_WATCHLIST];
}
function saveWatchlist() {
  try { localStorage.setItem('ss_watchlist', JSON.stringify(state.watchlist)); } catch {}
}

/* ── Helpers ── */

const esc = (s) => { const d = document.createElement('div'); d.textContent = s == null ? '' : String(s); return d.innerHTML; };

const fmtPct = (v, opts = {}) => {
  if (v == null || Number.isNaN(v)) return '—';
  const sign = v > 0 ? '+' : '';
  const decimals = opts.decimals != null ? opts.decimals : (Math.abs(v) >= 100 ? 1 : 2);
  return `${sign}${v.toFixed(decimals)}%`;
};

const fmtPrice = (v) => {
  if (v == null || Number.isNaN(v)) return '—';
  if (v >= 1000) return v.toFixed(0);
  if (v >= 100) return v.toFixed(1);
  if (v >= 10) return v.toFixed(2);
  return v.toFixed(2);
};

const fmtTime = (iso) => {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '—';
  return d.toLocaleString(undefined, {
    month: 'short', day: 'numeric',
    hour: 'numeric', minute: '2-digit',
  });
};

const fmtWeekLabel = (dateStr) => {
  if (!dateStr) return '';
  const parts = dateStr.split('-');
  if (parts.length !== 3) return dateStr;
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return `${months[+parts[1]-1]} ${+parts[2]}`;
};

const upDownCls = (v) => v == null ? '' : v >= 0 ? 'up' : 'down';

const tvLink = (sym, interval = 'W') =>
  `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(sym)}&interval=${interval}`;

const isMobile = () => window.innerWidth <= 700;

const debounce = (fn, ms) => {
  let t;
  return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
};

/* ── EMA status helpers ── */

function statusDot(s) {
  if (!s || s.above == null) return '<span class="dot" title="—"></span>';
  const onTop = Math.abs(s.pctDiff || 0) <= 1.5;
  if (onTop) return '<span class="dot flat" title="On-top"></span>';
  return s.above
    ? '<span class="dot up" title="Above"></span>'
    : '<span class="dot down" title="Below"></span>';
}

function tripleDots(emaStatus) {
  if (!emaStatus) return '<span class="watch-dots"><span class="dot"></span><span class="dot"></span><span class="dot"></span></span>';
  return `<span class="watch-dots" title="Wk · Mo · Qtr">
    ${statusDot(emaStatus.weekly)}
    ${statusDot(emaStatus.monthly)}
    ${statusDot(emaStatus.quarterly)}
  </span>`;
}

/* ── Initial render scaffolding ── */

function setView(v) {
  if (!VIEWS.includes(v)) v = 'pulse';
  state.view = v;
  location.hash = v;
  renderNav();
  renderView();
  window.scrollTo({ top: 0, behavior: 'instant' });
}

function renderNav() {
  document.querySelectorAll('.nav-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.view === state.view);
  });
}

function renderView() {
  const main = document.getElementById('main');
  if (state.view === 'pulse') main.innerHTML = renderPulse();
  else if (state.view === 'crosses') main.innerHTML = renderCrosses();
  else main.innerHTML = renderMore();
  attachViewHandlers();
}

function attachViewHandlers() {
  if (state.view === 'pulse') wirePulse();
  if (state.view === 'crosses') wireCrosses();
  if (state.view === 'more') wireMore();
}

/* ─────────────────────────────────────────────
   PULSE VIEW
   ───────────────────────────────────────────── */

function renderPulse() {
  return [
    renderBreadthSection(),
    renderSectorsSection(),
    renderWatchlistSection(),
    renderMilesSection(),
    renderLookbackSection(),
  ].join('');
}

function renderBreadthSection() {
  const m = state.misc || {};
  const bench = m.benchmarkByTicker || {};
  const spy = bench.SPY || {};
  const qqq = bench.QQQ || {};

  const tile = (label, value, valCls, sub) => `
    <div class="tile">
      <div class="tile-label">${esc(label)}</div>
      <div class="tile-value ${valCls}">${value}</div>
      ${sub ? `<div class="tile-sub">${sub}</div>` : ''}
    </div>`;

  const cell = (val) => `<span class="${upDownCls(val)}">${fmtPct(val)}</span>`;

  return `
    <div class="section">
      <div class="section-head">
        <h2 class="section-title"><em>Market</em> Pulse</h2>
        <span class="section-aside">Breadth</span>
      </div>
      <div class="breadth">
        ${tile('SPY · YTD', fmtPct(spy.ytd), upDownCls(spy.ytd), `1Y ${cell(spy.oneY)} · 5Y ${cell(spy.fiveY)}`)}
        ${tile('QQQ · YTD', fmtPct(qqq.ytd), upDownCls(qqq.ytd), `1Y ${cell(qqq.oneY)} · 5Y ${cell(qqq.fiveY)}`)}
        ${tile('Above 5W EMA', m.pctAbove5wkEMA != null ? `${m.pctAbove5wkEMA.toFixed(0)}%` : '—', m.pctAbove5wkEMA >= 50 ? 'up' : 'down', `Below ${m.pctBelow5wkEMA != null ? m.pctBelow5wkEMA.toFixed(0) + '%' : '—'}`)}
        ${tile('Above 200D SMA', m.pctAbove200dSMA != null ? `${m.pctAbove200dSMA.toFixed(0)}%` : '—', m.pctAbove200dSMA >= 50 ? 'up' : 'down', `Above 200W ${m.pctAbove200wSMA != null ? m.pctAbove200wSMA.toFixed(0) + '%' : '—'}`)}
      </div>
    </div>
  `;
}

function renderSectorsSection() {
  const bench = state.misc?.benchmarkByTicker || {};
  const chips = SECTOR_ORDER.map(sym => {
    const b = bench[sym];
    if (!b) return '';
    const st = state.stats[sym]?.emaStatus || {};
    return `
      <button class="chip" data-sym="${sym}">
        <div class="chip-head">
          <span class="chip-sym">${sym}</span>
          ${tripleDots(st).replace('watch-dots', 'chip-dots')}
        </div>
        <div class="chip-name">${esc(SECTOR_NAMES[sym] || sym)}</div>
        <div class="chip-row">
          <span class="chip-ytd ${upDownCls(b.ytd)}">${fmtPct(b.ytd)}</span>
          <span class="chip-1y">1Y ${fmtPct(b.oneY)}</span>
        </div>
      </button>`;
  }).filter(Boolean).join('');

  return `
    <div class="section">
      <div class="section-head">
        <h2 class="section-title"><em>Sectors</em></h2>
        <span class="section-aside">YTD · 1Y · 5EMA</span>
      </div>
      <div class="ribbon-wrap"><div class="ribbon">${chips}</div></div>
    </div>
  `;
}

function renderWatchlistSection() {
  const rows = state.watchlist.map(sym => {
    const s = state.stats[sym];
    const bench = state.misc?.benchmarkByTicker?.[sym];
    const ytd = bench?.ytd ?? s?.ytdPct;
    const oneY = bench?.oneY ?? s?.return1Y;
    const close = s?.close;
    const name = s?.name || SECTOR_NAMES[sym] || '';
    const emaStatus = s?.emaStatus;

    if (!s) {
      return `
        <div class="watch-row" data-sym="${sym}">
          <div class="watch-sym">
            <span class="watch-sym-tic">${esc(sym)}</span>
            <span class="watch-sym-name muted">No data</span>
          </div>
          <span class="watch-price muted">—</span>
          <span class="watch-pct muted">—</span>
          ${tripleDots(null)}
          <button class="remove-btn" data-remove="${esc(sym)}" aria-label="Remove">×</button>
        </div>`;
    }

    return `
      <div class="watch-row" data-sym="${sym}">
        <div class="watch-sym">
          <span class="watch-sym-tic">${esc(sym)}</span>
          <span class="watch-sym-name">${esc(name)}</span>
        </div>
        <span class="watch-price">${fmtPrice(close)}</span>
        <span class="watch-pct ${upDownCls(ytd)}">${fmtPct(ytd)}<div class="watch-pct-sub">1Y ${fmtPct(oneY)}</div></span>
        ${tripleDots(emaStatus)}
        <button class="remove-btn" data-remove="${esc(sym)}" aria-label="Remove">×</button>
      </div>`;
  }).join('');

  const editCls = state.watchlistEditing ? ' editing' : '';
  const editLabel = state.watchlistEditing ? 'Done' : 'Edit';

  return `
    <div class="section">
      <div class="section-head">
        <h2 class="section-title"><em>Watchlist</em></h2>
        <button class="btn-ghost ${state.watchlistEditing ? 'toggled' : ''}" id="watch-edit-btn">${editLabel}</button>
      </div>
      <div class="watch${editCls}" id="watch-list">
        ${rows || '<div class="watch-empty">No tickers pinned. Tap any symbol to add it.</div>'}
      </div>
      ${state.watchlistEditing ? `
        <div class="add-row">
          <input class="add-input" id="watch-add-input" placeholder="Ticker (e.g. NVDA)" maxlength="8" autocomplete="off" spellcheck="false" />
          <button class="add-btn" id="watch-add-btn">Add</button>
        </div>
      ` : ''}
    </div>
  `;
}

function renderMilesSection() {
  const m = state.misc || {};
  const milestones = [
    { key: 'spxSinceElection',     label: 'SPX · Since Election' },
    { key: 'spxSinceInauguration', label: 'SPX · Since Inaug.' },
    { key: 'spxSinceChatGPT',      label: 'SPX · Since ChatGPT' },
    { key: 'spxSinceBottom2022',   label: 'SPX · Since 2022 Low' },
  ];

  const cells = milestones.map(({ key, label }) => {
    const v = m[key];
    return `
      <div class="mile">
        <div class="mile-label">${esc(label)}</div>
        <div class="mile-value ${upDownCls(v)}">${fmtPct(v, { decimals: 1 })}</div>
      </div>`;
  }).join('');

  return `
    <div class="section">
      <div class="section-head">
        <h2 class="section-title">SPX <em>milestones</em></h2>
      </div>
      <div class="miles">${cells}</div>
    </div>
  `;
}

function renderLookbackSection() {
  if (!state.manifest || state.manifest.length === 0) return '';
  const latestCls = state.selectedWeek === 'latest' ? ' active' : '';
  const buttons = [`<button class="lookback-btn${latestCls}" data-week="latest">Latest</button>`]
    .concat(state.manifest.map(w => {
      const cls = state.selectedWeek === w ? ' active' : '';
      return `<button class="lookback-btn${cls}" data-week="${esc(w)}">${fmtWeekLabel(w)}</button>`;
    })).join('');
  return `
    <div class="section">
      <div class="section-head">
        <h2 class="section-title"><em>Lookback</em></h2>
        <span class="section-aside">Saved Mondays</span>
      </div>
      <div class="lookback">${buttons}</div>
    </div>
  `;
}

function wirePulse() {
  // Sector chip → open detail
  document.querySelectorAll('.chip[data-sym]').forEach(el =>
    el.addEventListener('click', () => openSymbolSheet(el.dataset.sym))
  );

  // Watchlist row → open detail (unless tapping remove button)
  document.querySelectorAll('.watch-row[data-sym]').forEach(el => {
    el.addEventListener('click', (e) => {
      if (e.target.closest('.remove-btn')) return;
      openSymbolSheet(el.dataset.sym);
    });
  });

  // Remove from watchlist
  document.querySelectorAll('.remove-btn').forEach(b =>
    b.addEventListener('click', (e) => {
      e.stopPropagation();
      const sym = b.dataset.remove;
      state.watchlist = state.watchlist.filter(s => s !== sym);
      saveWatchlist();
      renderView();
    })
  );

  // Edit toggle
  const editBtn = document.getElementById('watch-edit-btn');
  if (editBtn) editBtn.addEventListener('click', () => {
    state.watchlistEditing = !state.watchlistEditing;
    renderView();
  });

  // Add ticker
  const addInput = document.getElementById('watch-add-input');
  const addBtn = document.getElementById('watch-add-btn');
  if (addInput) {
    addInput.addEventListener('input', () => {
      addInput.value = addInput.value.toUpperCase().replace(/[^A-Z0-9-]/g, '');
    });
    addInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') doAdd();
    });
  }
  if (addBtn) addBtn.addEventListener('click', doAdd);

  function doAdd() {
    const sym = (addInput.value || '').trim().toUpperCase();
    if (!sym) return;
    if (state.watchlist.includes(sym)) { addInput.value = ''; return; }
    state.watchlist.push(sym);
    saveWatchlist();
    addInput.value = '';
    renderView();
  }

  // Lookback buttons
  document.querySelectorAll('.lookback-btn').forEach(b =>
    b.addEventListener('click', () => switchWeek(b.dataset.week))
  );
}

/* ─────────────────────────────────────────────
   CROSSES VIEW
   ───────────────────────────────────────────── */

function getCrossRows() {
  const tf = state.crossTimeframe;
  const dir = state.crossDirection;
  const files = FILES[tf];
  if (!files) return [];

  const file = files[dir];
  const key = files[`${dir}Key`];
  const json = state.raw[file];
  if (!json) return [];

  let rows = json[key] || [];

  // Enrich with stats (sector tag, ytd, etc.)
  rows = rows.map(r => {
    const s = state.stats[r.symbol] || {};
    return {
      ...r,
      name: r.name || s.name || '',
      ytdPct: s.ytdPct ?? null,
      return1Y: s.return1Y ?? null,
      rsi: s.rsi ?? null,
      forwardPE: s.forwardPE ?? null,
      marketCap: s.marketCap ?? null,
      emaStatus: s.emaStatus || {},
    };
  });

  // Filter
  if (state.crossFilter === 'watch') {
    const w = new Set(state.watchlist);
    rows = rows.filter(r => w.has(r.symbol));
  } else if (state.crossFilter === 'sectors') {
    const sectorSet = new Set(SECTOR_ORDER);
    rows = rows.filter(r => sectorSet.has(r.symbol));
  } else if (state.crossFilter === 'mega') {
    rows = rows.filter(r => r.marketCap >= 200_000_000_000);
  } else if (state.crossFilter === 'small') {
    rows = rows.filter(r => r.marketCap > 0 && r.marketCap < 200_000_000_000);
  }

  // Search
  if (state.crossSearch) {
    const q = state.crossSearch.toLowerCase();
    rows = rows.filter(r =>
      r.symbol.toLowerCase().includes(q) ||
      (r.name || '').toLowerCase().includes(q)
    );
  }

  // Default sort: for "up", rank by weeksBelow desc (longer below = stronger reversal); for "down" by weeksAbove desc; for above/below by count desc.
  const sortKey = (dir === 'up' || dir === 'above') ? COUNT_KEYS[tf].up
                : (dir === 'down' || dir === 'below') ? COUNT_KEYS[tf].down
                : null;

  if (dir === 'above' || dir === 'below') {
    rows.sort((a, b) => (b.count || 0) - (a.count || 0));
  } else if (sortKey) {
    rows.sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0));
  }

  return rows;
}

function renderCrosses() {
  const rows = getCrossRows();
  const tf = state.crossTimeframe;
  const dir = state.crossDirection;
  const dirCls = (dir === 'up' || dir === 'above') ? 'up' : (dir === 'down' || dir === 'below') ? 'down' : '';

  const tfSeg = ['weekly','monthly','quarterly'].map(t => {
    const lbl = { weekly: 'Weekly', monthly: 'Monthly', quarterly: 'Quarterly' }[t];
    return `<button class="seg-btn${state.crossTimeframe === t ? ' active' : ''}" data-tf="${t}">${lbl}</button>`;
  }).join('');

  const dirSeg = ['up','down','above','below'].map(d => {
    const lbl = { up: 'Cross Up', down: 'Cross Down', above: 'Above', below: 'Below' }[d];
    const cls = (d === 'up' || d === 'above') ? 'up' : 'down';
    return `<button class="seg-btn ${cls}${state.crossDirection === d ? ' active' : ''}" data-dir="${d}">${lbl}</button>`;
  }).join('');

  const chips = [
    { id: 'all',     label: 'All' },
    { id: 'watch',   label: '★ Watchlist' },
    { id: 'sectors', label: 'Sectors' },
    { id: 'mega',    label: '$200B+' },
    { id: 'small',   label: '$200B−' },
  ].map(c => `<button class="fchip${state.crossFilter === c.id ? ' active' : ''}" data-fchip="${c.id}">${c.label}</button>`).join('');

  const cardHTML = rows.map(r => renderCrossCard(r, tf, dir, dirCls)).join('');
  const empty = rows.length === 0 ? `<div class="empty-state"><div class="glyph">∅</div>No matches.</div>` : '';

  const titleLeft = { up: 'Cross Ups', down: 'Cross Downs', above: 'Above EMA', below: 'Below EMA' }[dir];
  const tfWord = { weekly: 'weekly', monthly: 'monthly', quarterly: 'quarterly' }[tf];

  return `
    <div class="section">
      <div class="section-head">
        <h2 class="section-title">${esc(titleLeft)} · <em>${esc(tfWord)}</em></h2>
        <span class="count-pill">${rows.length}</span>
      </div>

      <div class="seg-row">
        <div class="seg">${tfSeg}</div>
        <div class="seg">${dirSeg}</div>
      </div>

      <div class="chips-row">${chips}</div>

      <div class="cross-search-wrap">
        <span class="cross-search-ic">⌕</span>
        <input class="cross-search-input" id="cross-search" placeholder="Search symbol or name…" value="${esc(state.crossSearch)}" autocomplete="off" />
      </div>

      ${empty}
      <div class="cross-list">${cardHTML}</div>
    </div>
  `;
}

function renderCrossCard(r, tf, dir, dirCls) {
  const tfShort = { weekly: 'wk', monthly: 'mo', quarterly: 'qtr' }[tf];
  const intv = tf === 'monthly' ? 'M' : tf === 'quarterly' ? 'M' : 'W';

  const pctField = (dir === 'up' || dir === 'above') ? 'pctAbove' : 'pctBelow';
  const pct = r[pctField];

  // Signal line
  let signal = '';
  if (dir === 'up') {
    const n = r[COUNT_KEYS[tf].up] ?? 0;
    signal = `<span class="glyph up">▲</span> Crossed up after <strong>${n}</strong> ${tfShort}${n === 1 ? '' : 's'} below`;
  } else if (dir === 'down') {
    const n = r[COUNT_KEYS[tf].down] ?? 0;
    signal = `<span class="glyph down">▼</span> Crossed down after <strong>${n}</strong> ${tfShort}${n === 1 ? '' : 's'} above`;
  } else if (dir === 'above') {
    signal = `<span class="glyph up">▲</span> <strong>${r.count}</strong> ${tfShort}${r.count === 1 ? '' : 's'} above EMA`;
  } else {
    signal = `<span class="glyph down">▼</span> <strong>${r.count}</strong> ${tfShort}${r.count === 1 ? '' : 's'} below EMA`;
  }

  const isWatch = state.watchlist.includes(r.symbol);
  const isSector = SECTOR_ORDER.includes(r.symbol);
  const tag = isWatch ? '<span class="ext" style="color:var(--warn);border-color:rgba(251,191,36,0.4)">★ WATCH</span>'
            : isSector ? `<span class="ext">SECTOR</span>` : '';

  return `
    <button class="cross-card ${dirCls}" data-sym="${esc(r.symbol)}">
      <div class="cross-l">
        <div class="cross-sym">
          <span class="ticker">${esc(r.symbol)}</span>
          ${tag}
        </div>
        ${r.name ? `<div class="cross-name">${esc(r.name)}</div>` : ''}
        <div class="cross-signal">${signal}</div>
      </div>
      <div class="cross-r">
        <span class="cross-price">$${fmtPrice(r.close)}</span>
        <span class="cross-pct ${dirCls}">${pct != null ? (pct > 0 ? '+' : '') + pct.toFixed(2) + '%' : '—'}</span>
        <span class="cross-meta">EMA ${fmtPrice(r.ema)}</span>
      </div>
    </button>
  `;
}

function wireCrosses() {
  document.querySelectorAll('[data-tf]').forEach(b =>
    b.addEventListener('click', () => { state.crossTimeframe = b.dataset.tf; renderView(); })
  );
  document.querySelectorAll('[data-dir]').forEach(b =>
    b.addEventListener('click', () => { state.crossDirection = b.dataset.dir; renderView(); })
  );
  document.querySelectorAll('[data-fchip]').forEach(b =>
    b.addEventListener('click', () => { state.crossFilter = b.dataset.fchip; renderView(); })
  );
  document.querySelectorAll('.cross-card[data-sym]').forEach(b =>
    b.addEventListener('click', () => openSymbolSheet(b.dataset.sym))
  );

  const search = document.getElementById('cross-search');
  if (search) {
    const update = debounce(() => {
      state.crossSearch = search.value.trim();
      // Only re-render the cards, not the whole page (so search input keeps focus)
      const rows = getCrossRows();
      const tf = state.crossTimeframe, dir = state.crossDirection;
      const dirCls = (dir === 'up' || dir === 'above') ? 'up' : 'down';
      const list = document.querySelector('.cross-list');
      const pill = document.querySelector('.count-pill');
      if (list) list.innerHTML = rows.map(r => renderCrossCard(r, tf, dir, dirCls)).join('');
      if (pill) pill.textContent = rows.length;
      // Rewire just the cards
      document.querySelectorAll('.cross-card[data-sym]').forEach(b => {
        b.addEventListener('click', () => openSymbolSheet(b.dataset.sym));
      });
      // Empty state
      const existingEmpty = document.querySelector('.cross-list').previousElementSibling;
      if (existingEmpty && existingEmpty.classList.contains('empty-state')) existingEmpty.remove();
      if (rows.length === 0) {
        const div = document.createElement('div');
        div.className = 'empty-state';
        div.innerHTML = '<div class="glyph">∅</div>No matches.';
        document.querySelector('.cross-list').parentNode.insertBefore(div, document.querySelector('.cross-list'));
      }
    }, 120);
    search.addEventListener('input', update);
  }
}

/* ─────────────────────────────────────────────
   MORE VIEW
   ───────────────────────────────────────────── */

function renderMore() {
  return [
    moreSection('benchmarks', 'Index <em>&amp; Sector</em> Returns', renderBenchTable()),
    moreSection('breadth', 'Breadth <em>statistics</em>', renderBreadthDetail()),
    moreSection('milestones', 'SPX <em>milestones</em>', renderMilestonesDetail()),
    moreSection('breaks', 'Recent <em>price</em> breaks', renderPriceBreaks()),
    moreSection('extremes', 'YTD <em>extremes</em>', renderYTDExtremes()),
  ].join('');
}

function moreSection(id, titleHTML, body) {
  const open = state.moreOpen[id] ? ' open' : '';
  return `
    <div class="more-section${open}" data-more="${id}">
      <div class="more-section-head">
        <h3 class="more-section-title">${titleHTML}</h3>
        <span class="more-section-chev">▾</span>
      </div>
      <div class="more-section-body">${body}</div>
    </div>
  `;
}

function renderBenchTable() {
  const bench = state.misc?.benchmarkByTicker || {};
  if (Object.keys(bench).length === 0) return '<div class="empty-state">No benchmark data.</div>';

  // Order: SPY first, then sectors, then others
  const order = ['SPY','QQQ','DIA','IWM','VTV','TMUS', ...SECTOR_ORDER.filter(s => s !== 'SPY')];
  const seen = new Set();
  const ordered = order.filter(s => bench[s] && !seen.has(s) && seen.add(s));
  Object.keys(bench).forEach(s => { if (!seen.has(s)) { ordered.push(s); seen.add(s); } });

  const rows = [
    `<div class="bench-row hdr"><div>Symbol</div><div style="text-align:right">YTD</div><div style="text-align:right">1Y</div><div style="text-align:right">5Y</div></div>`,
    ...ordered.map(sym => {
      const b = bench[sym];
      return `
        <div class="bench-row" data-sym="${sym}">
          <span class="bench-sym">${sym}</span>
          <span class="bench-cell ${upDownCls(b.ytd)}">${fmtPct(b.ytd)}</span>
          <span class="bench-cell ${upDownCls(b.oneY)}">${fmtPct(b.oneY)}</span>
          <span class="bench-cell ${upDownCls(b.fiveY)}">${fmtPct(b.fiveY, { decimals: 0 })}</span>
        </div>`;
    }),
  ].join('');

  return `<div class="bench">${rows}</div>`;
}

function renderBreadthDetail() {
  const m = state.misc || {};
  const items = [
    { key: 'pctAbove5wkEMA', label: 'Above 5W EMA', fmt: 'pct1' },
    { key: 'pctBelow5wkEMA', label: 'Below 5W EMA', fmt: 'pct1' },
    { key: 'pctAbove200dSMA', label: 'Above 200D SMA', fmt: 'pct1' },
    { key: 'pctAbove200wSMA', label: 'Above 200W SMA', fmt: 'pct1' },
    { key: 'pctWithin5OfHigh', label: 'Within 5% of High', fmt: 'pct1' },
    { key: 'pctPositiveYTD', label: 'Positive YTD', fmt: 'pct1' },
    { key: 'avgYTD', label: 'Avg YTD', fmt: 'pct2' },
    { key: 'avgForwardPE', label: 'Avg Fwd P/E', fmt: 'num' },
    { key: 'medianForwardPE', label: 'Median Fwd P/E', fmt: 'num' },
  ];

  const cells = items.map(it => {
    const v = m[it.key];
    let val = '—', cls = '';
    if (v != null) {
      if (it.fmt === 'pct1') { val = v.toFixed(1) + '%'; cls = v >= 50 ? 'up' : (it.key === 'pctBelow5wkEMA' ? 'down' : ''); }
      else if (it.fmt === 'pct2') { val = (v > 0 ? '+' : '') + v.toFixed(2) + '%'; cls = upDownCls(v); }
      else if (it.fmt === 'num') val = v.toFixed(1);
    }
    return `<div class="kv"><span class="kv-key">${it.label}</span><span class="kv-val ${cls}">${val}</span></div>`;
  }).join('');

  return `<div class="kv-grid">${cells}</div>`;
}

function renderMilestonesDetail() {
  const m = state.misc || {};
  const items = [
    { key: 'spxSinceElection',     label: 'Since Election (Nov 5, 2024)' },
    { key: 'spxSinceInauguration', label: 'Since Inauguration (Jan 20, 2025)' },
    { key: 'spxSinceChatGPT',      label: 'Since ChatGPT (Nov 30, 2022)' },
    { key: 'spxSinceBottom2022',   label: 'Since 2022 Low (Oct 12, 2022)' },
  ];
  const cells = items.map(it => {
    const v = m[it.key];
    const val = v == null ? '—' : (v > 0 ? '+' : '') + v.toFixed(2) + '%';
    return `<div class="kv"><span class="kv-key">${it.label}</span><span class="kv-val ${upDownCls(v)}">${val}</span></div>`;
  }).join('');
  return `<div class="kv-grid">${cells}</div>`;
}

function renderPriceBreaks() {
  const stats = Object.values(state.stats).filter(s =>
    s.symbol && (s.breakoutPct != null || s.breakdownPct != null)
  );

  const recentBreaks = stats
    .filter(s => s.breakoutDate || s.breakdownDate)
    .sort((a, b) => {
      const ad = a.breakoutDate || a.breakdownDate || '';
      const bd = b.breakoutDate || b.breakdownDate || '';
      return bd.localeCompare(ad);
    })
    .slice(0, 25);

  if (recentBreaks.length === 0) return '<div class="empty-state">No recent breaks.</div>';

  const rows = recentBreaks.map(s => {
    const isBreakout = s.breakoutDate && (!s.breakdownDate || s.breakoutDate > s.breakdownDate);
    const date = isBreakout ? s.breakoutDate : s.breakdownDate;
    const price = isBreakout ? s.breakoutPrice : s.breakdownPrice;
    const pct = isBreakout ? s.breakoutPct : s.breakdownPct;
    const cls = isBreakout ? 'up' : 'down';
    const sign = isBreakout ? '▲' : '▼';

    return `
      <button class="cross-card ${cls}" data-sym="${s.symbol}">
        <div class="cross-l">
          <div class="cross-sym"><span class="ticker">${esc(s.symbol)}</span></div>
          ${s.name ? `<div class="cross-name">${esc(s.name)}</div>` : ''}
          <div class="cross-signal"><span class="glyph ${cls}">${sign}</span> ${isBreakout ? 'Breakout' : 'Breakdown'} on <strong>${esc(date)}</strong> @ $${fmtPrice(price)}</div>
        </div>
        <div class="cross-r">
          <span class="cross-price">$${fmtPrice(s.close)}</span>
          <span class="cross-pct ${cls}">${fmtPct(pct)}</span>
        </div>
      </button>`;
  }).join('');

  return `<div class="cross-list">${rows}</div>`;
}

function renderYTDExtremes() {
  const stats = Object.values(state.stats).filter(s => s.symbol && s.ytdPct != null && s.marketCap >= 10_000_000_000);
  const sorted = [...stats].sort((a, b) => b.ytdPct - a.ytdPct);
  const winners = sorted.slice(0, 10);
  const losers = sorted.slice(-10).reverse();

  const block = (title, rows) => `
    <div class="sheet-mini-section">
      <div class="sheet-mini-title">${title}</div>
      <div class="cross-list">
        ${rows.map(s => {
          const cls = upDownCls(s.ytdPct);
          return `
            <button class="cross-card ${cls}" data-sym="${s.symbol}">
              <div class="cross-l">
                <div class="cross-sym"><span class="ticker">${esc(s.symbol)}</span></div>
                ${s.name ? `<div class="cross-name">${esc(s.name)}</div>` : ''}
              </div>
              <div class="cross-r">
                <span class="cross-price">$${fmtPrice(s.close)}</span>
                <span class="cross-pct ${cls}">${fmtPct(s.ytdPct)}</span>
              </div>
            </button>`;
        }).join('')}
      </div>
    </div>`;

  return block('Top 10 (≥ $10B cap)', winners) + block('Bottom 10 (≥ $10B cap)', losers);
}

function wireMore() {
  document.querySelectorAll('.more-section-head').forEach(h => {
    h.addEventListener('click', () => {
      const sec = h.closest('.more-section');
      const id = sec.dataset.more;
      sec.classList.toggle('open');
      state.moreOpen[id] = sec.classList.contains('open');
    });
  });
  document.querySelectorAll('.bench-row[data-sym], .cross-card[data-sym]').forEach(b =>
    b.addEventListener('click', () => openSymbolSheet(b.dataset.sym))
  );
}

/* ─────────────────────────────────────────────
   SYMBOL DETAIL SHEET
   ───────────────────────────────────────────── */

function openSymbolSheet(sym) {
  const s = state.stats[sym] || {};
  const isWatch = state.watchlist.includes(sym);
  const head = document.getElementById('sheet-head');
  const body = document.getElementById('sheet-body');

  const name = s.name || SECTOR_NAMES[sym] || '';
  const close = s.close;

  head.innerHTML = `
    <div class="sheet-title">${esc(sym)} <em>${name ? '· ' + esc(name) : ''}</em></div>
    <div class="sheet-sub">${close != null ? '$' + fmtPrice(close) : '—'}</div>
  `;

  // EMA status
  const e = s.emaStatus || {};
  const emaText = (st) => {
    if (!st || st.above == null) return '—';
    const onTop = Math.abs(st.pctDiff || 0) <= 1.5;
    const tag = onTop ? 'On-top' : (st.above ? 'Above' : 'Below');
    const cls = onTop ? 'warn' : (st.above ? 'up' : 'down');
    return `<span class="${cls}">${tag} (${(st.pctDiff > 0 ? '+' : '') + (st.pctDiff || 0).toFixed(2)}%)</span> · ${st.count} ${st.above ? 'above' : 'below'}`;
  };

  const row = (k, v, cls = '') =>
    `<div class="sheet-row"><span class="sheet-row-key">${esc(k)}</span><span class="sheet-row-val ${cls}">${v}</span></div>`;

  const bench = state.misc?.benchmarkByTicker?.[sym];

  let html = '<div class="sheet-rows">';
  if (s.ytdPct != null) html += row('YTD', fmtPct(s.ytdPct), upDownCls(s.ytdPct));
  if (s.return1Y != null) html += row('1 Year', fmtPct(s.return1Y), upDownCls(s.return1Y));
  if (s.return5Y != null) html += row('5 Year', fmtPct(s.return5Y, { decimals: 0 }), upDownCls(s.return5Y));
  if (bench && bench.fiveY != null && s.return5Y == null) html += row('5 Year', fmtPct(bench.fiveY, { decimals: 0 }), upDownCls(bench.fiveY));
  if (s.highPct != null) html += row('From 3yr High', fmtPct(s.highPct), s.highPct >= -5 ? 'up' : 'down');
  if (s.lowPct != null) html += row('From 52w Low', fmtPct(s.lowPct), s.lowPct > 5 ? 'up' : 'down');
  if (s.rsi != null) html += row('RSI(14)', s.rsi.toFixed(1), s.rsi > 70 ? 'down' : s.rsi < 30 ? 'up' : '');
  if (s.forwardPE != null) html += row('Fwd P/E', s.forwardPE.toFixed(1));
  if (s.pctSma200d != null) html += row('vs 200D SMA', fmtPct(s.pctSma200d), upDownCls(s.pctSma200d));
  if (s.pctSma200w != null) html += row('vs 200W SMA', fmtPct(s.pctSma200w), upDownCls(s.pctSma200w));
  html += '</div>';

  // EMA status section
  html += `
    <div class="sheet-mini-section">
      <div class="sheet-mini-title">5-Period EMA · status</div>
      <div class="sheet-rows">
        ${row('Weekly', emaText(e.weekly))}
        ${row('Monthly', emaText(e.monthly))}
        ${row('Quarterly', emaText(e.quarterly))}
      </div>
    </div>
  `;

  // Breakouts
  if (s.breakoutPct != null || s.breakdownPct != null) {
    html += '<div class="sheet-mini-section"><div class="sheet-mini-title">Swing levels</div><div class="sheet-rows">';
    if (s.breakoutPrice != null) html += row(`Breakout (${esc(s.breakoutDate || '')})`, `$${fmtPrice(s.breakoutPrice)} · ${fmtPct(s.breakoutPct)}`, upDownCls(s.breakoutPct));
    if (s.breakdownPrice != null) html += row(`Breakdown (${esc(s.breakdownDate || '')})`, `$${fmtPrice(s.breakdownPrice)} · ${fmtPct(s.breakdownPct)}`, upDownCls(s.breakdownPct));
    html += '</div></div>';
  }

  // Actions
  html += `
    <div class="sheet-actions">
      <button class="sheet-pin-btn ${isWatch ? 'pinned' : ''}" id="sheet-pin">${isWatch ? '★ Unpin' : '☆ Pin'}</button>
      <a class="sheet-pin-btn" href="${tvLink(sym)}" target="_blank" rel="noopener">↗ TradingView</a>
    </div>
  `;

  body.innerHTML = html;
  showSheet();

  document.getElementById('sheet-pin').addEventListener('click', () => {
    if (state.watchlist.includes(sym)) state.watchlist = state.watchlist.filter(s => s !== sym);
    else state.watchlist.push(sym);
    saveWatchlist();
    hideSheet();
    if (state.view === 'pulse') renderView();
  });
}

function showSheet() {
  document.getElementById('sheet-backdrop').classList.add('open');
  document.getElementById('sheet').classList.add('open');
}
function hideSheet() {
  document.getElementById('sheet-backdrop').classList.remove('open');
  document.getElementById('sheet').classList.remove('open');
}

/* ─────────────────────────────────────────────
   META & SCAN STATUS
   ───────────────────────────────────────────── */

function renderHeaderMeta() {
  const dot = document.getElementById('hdr-dot');
  const meta = document.getElementById('hdr-meta');
  if (dot) dot.classList.toggle('scanning', state.isScanning);
  if (!meta) return;
  if (state.isScanning) {
    meta.innerHTML = `<span class="warn">Scanning…</span>`;
    return;
  }
  const parts = [];
  if (state.scanTime) parts.push(fmtTime(state.scanTime));
  if (state.symbolsScanned) parts.push(`${state.symbolsScanned.toLocaleString()} symbols`);
  if (state.errorsCount) parts.push(`<span class="err">${state.errorsCount} err</span>`);
  meta.innerHTML = parts.join(' · ') || '—';
}

async function checkScanStatus(opts = {}) {
  try {
    const res = await fetch(`${ORCHESTRATOR_URL}?dev_key=${DEV_KEY}`, { method: 'GET' });
    if (!res.ok) return;
    const data = await res.json();
    const wasScanning = state.isScanning;
    state.isScanning = !!data.running;
    renderHeaderMeta();

    const btn = document.getElementById('run-scan-btn');
    if (state.isScanning) {
      btn.classList.add('busy');
      btn.disabled = true;
      setTimeout(checkScanStatus, 30000);
    } else {
      btn.classList.remove('busy');
      btn.disabled = false;
      if (wasScanning) fetchAll(true); // just finished
    }
  } catch (e) {
    console.warn('[scan] status check failed', e);
  }
}

async function triggerScan() {
  const btn = document.getElementById('run-scan-btn');
  btn.disabled = true;
  btn.classList.add('busy');
  try {
    await fetch(`${ORCHESTRATOR_URL}?dev_key=${DEV_KEY}`, { method: 'POST' });
    setTimeout(checkScanStatus, 4000);
  } catch (e) {
    console.warn('[scan] trigger failed', e);
    btn.classList.remove('busy');
    btn.disabled = false;
  }
}

/* ─────────────────────────────────────────────
   DATA LOADING
   ───────────────────────────────────────────── */

function urlsForWeek(week) {
  const files = new Set();
  Object.values(FILES).forEach(g => { files.add(g.up); files.add(g.down); files.add(g.above); files.add(g.below); });
  const list = [...files];
  if (week === 'latest') return { list, statsURL: '/results/latest-stats.json' };
  return {
    list: list.map(u => u.replace(/\/latest([-.])/, `/${week}$1`)),
    statsURL: `/results/${week}-stats.json`,
  };
}

async function fetchAll(silent = false) {
  if (!silent) {
    document.getElementById('main').innerHTML = '<div class="loading">Loading market…</div>';
  }
  const cb = `_=${Date.now()}`;
  const { list, statsURL } = urlsForWeek(state.selectedWeek);

  const [manifestR, statsR, ...results] = await Promise.allSettled([
    fetch(`/results/manifest.json?${cb}`).then(r => r.ok ? r.json() : null).catch(() => null),
    fetch(`${statsURL}?${cb}`).then(r => r.ok ? r.json() : null).catch(() => null),
    ...list.map(u => fetch(`${u}?${cb}`).then(r => r.ok ? r.json() : null).catch(() => null)),
  ]);

  if (manifestR.status === 'fulfilled' && manifestR.value) {
    const m = manifestR.value;
    state.manifest = Array.isArray(m) ? m : (m.weeks || []);
  }

  if (statsR.status === 'fulfilled' && statsR.value) {
    const data = statsR.value;
    state.stats = {};
    (data.stats || []).forEach(s => { state.stats[s.symbol] = s; });
    state.misc = data.misc || {};
    state.scanTime = data.scanTime;
    state.symbolsScanned = data.symbolsScanned || 0;
    state.errorsCount = data.errors || 0;
  }

  // Try to fetch total symbols count
  fetch(`/symbols/us-equities.txt?${cb}`).then(r => r.ok ? r.text() : '').then(txt => {
    if (txt) {
      state.totalSymbols = txt.trim().split('\n').filter(Boolean).length;
      renderHeaderMeta();
    }
  }).catch(() => {});

  state.raw = {};
  list.forEach((u, i) => {
    const result = results[i];
    if (result.status === 'fulfilled' && result.value) {
      // Map back to the latest-* form so getCrossRows can look it up
      const canonical = u.replace(/\/\d{4}-\d{2}-\d{2}([-.])/, '/latest$1');
      state.raw[canonical] = result.value;
    }
  });

  renderHeaderMeta();
  renderView();
}

async function switchWeek(week) {
  if (week === state.selectedWeek) return;
  state.selectedWeek = week;
  await fetchAll();
}

/* ─────────────────────────────────────────────
   INIT
   ───────────────────────────────────────────── */

function readHash() {
  const h = (location.hash || '').replace('#', '');
  if (VIEWS.includes(h)) state.view = h;
}

function attachGlobalHandlers() {
  document.querySelectorAll('.nav-btn').forEach(b => {
    b.addEventListener('click', () => setView(b.dataset.view));
  });
  document.getElementById('run-scan-btn').addEventListener('click', triggerScan);
  document.getElementById('sheet-backdrop').addEventListener('click', hideSheet);
  window.addEventListener('hashchange', () => {
    readHash();
    renderNav();
    renderView();
  });
}

async function init() {
  readHash();
  renderNav();
  attachGlobalHandlers();
  checkScanStatus();
  await fetchAll();
}

init();
