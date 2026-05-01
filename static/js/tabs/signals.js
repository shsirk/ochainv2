import { apiFetch, scalperUrl, strategiesUrl } from '../core/api.js';
import { showSkeleton, hideSkeleton } from '../components/skeleton.js';
import { showError, clearError } from '../components/error-state.js';
import { fmt, fmtInt, fmtPct, chgClass, el } from '../utils.js';

let _ctrl   = null;
let _cref   = null;
let _lastS  = null;

// Columns config: { key, headId, countId, domId, sigsId }
const COLS = [
    { key: 'CE_BUY',  headId: 'colCEBuy',  countId: 'cntCEBuy',  domId: 'domCEBuy',  sigsId: 'sigsCEBuy'  },
    { key: 'PE_BUY',  headId: 'colPEBuy',  countId: 'cntPEBuy',  domId: 'domPEBuy',  sigsId: 'sigsPEBuy'  },
    { key: 'CE_SELL', headId: 'colCESell', countId: 'cntCESell', domId: 'domCESell', sigsId: 'sigsCESell' },
    { key: 'PE_SELL', headId: 'colPESell', countId: 'cntPESell', domId: 'domPESell', sigsId: 'sigsPESell' },
];

export function init(container) {
    _cref = container;
    container.innerHTML = `
<div class="signals-toolbar">
  <label>Strategy</label>
  <select id="signalsStrategy">
    <option value="naked_buyer">Naked Buyer</option>
  </select>
</div>
<div class="signals-board">
  <div class="signals-col" id="colCEBuy">
    <div class="signals-col-head">
      <h3>CE Buy</h3><span class="signals-count" id="cntCEBuy">0</span>
    </div>
    <div class="dom-box" id="domCEBuy">
      <div class="dom-title">Dominance</div>
      <div class="dom-list"></div>
    </div>
    <div class="scalp-signal-list" id="sigsCEBuy"></div>
  </div>
  <div class="signals-col" id="colPEBuy">
    <div class="signals-col-head">
      <h3>PE Buy</h3><span class="signals-count" id="cntPEBuy">0</span>
    </div>
    <div class="dom-box" id="domPEBuy">
      <div class="dom-title">Dominance</div>
      <div class="dom-list"></div>
    </div>
    <div class="scalp-signal-list" id="sigsPEBuy"></div>
  </div>
  <div class="signals-col" id="colCESell">
    <div class="signals-col-head">
      <h3>CE Sell</h3><span class="signals-count" id="cntCESell">0</span>
    </div>
    <div class="dom-box" id="domCESell">
      <div class="dom-title">Dominance</div>
      <div class="dom-list"></div>
    </div>
    <div class="scalp-signal-list" id="sigsCESell"></div>
  </div>
  <div class="signals-col" id="colPESell">
    <div class="signals-col-head">
      <h3>PE Sell</h3><span class="signals-count" id="cntPESell">0</span>
    </div>
    <div class="dom-box" id="domPESell">
      <div class="dom-title">Dominance</div>
      <div class="dom-list"></div>
    </div>
    <div class="scalp-signal-list" id="sigsPESell"></div>
  </div>
</div>`;

    // Fetch strategy list and populate select
    _fetchStrategies(container);

    container.querySelector('#signalsStrategy').addEventListener('change', () => {
        if (_lastS) load(container, _lastS);
    });
}

async function _fetchStrategies(container) {
    try {
        const strategies = await apiFetch(strategiesUrl());
        const sel        = container.querySelector('#signalsStrategy');
        if (!sel || !Array.isArray(strategies) || !strategies.length) return;

        const current = sel.value;
        sel.innerHTML = strategies.map(name =>
            `<option value="${name}"${name === current ? ' selected' : ''}>${_labelStrategy(name)}</option>`
        ).join('');
    } catch {
        // Silently fall back to default option already in DOM
    }
}

function _labelStrategy(name) {
    return name.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

export async function load(container, s) {
    _lastS = s;
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    const sig = _ctrl.signal;

    clearError(container);

    // Show skeletons in all four signal lists
    COLS.forEach(col => {
        const sigsEl = container.querySelector(`#${col.sigsId}`);
        if (sigsEl) showSkeleton(sigsEl, 2);
    });

    const strategy = container.querySelector('#signalsStrategy')?.value || 'naked_buyer';

    try {
        const data = await apiFetch(scalperUrl(s, strategy), sig);
        COLS.forEach(col => {
            hideSkeleton(container.querySelector(`#${col.sigsId}`));
        });
        _distributeData(container, data, strategy);
    } catch (err) {
        if (err.name === 'AbortError') return;
        COLS.forEach(col => hideSkeleton(container.querySelector(`#${col.sigsId}`)));
        showError(container, err.message || 'Failed to load signals', () => load(container, s));
    }
}

function _distributeData(container, data, strategy) {
    const signals    = data.signals     || [];
    const topStrikes = data.top_strikes || [];
    const writerCE   = data.writer_ce   || [];
    const writerPE   = data.writer_pe   || [];

    // Classify signals into 4 columns:
    // CE BUY:  signals with side=CE and action implies buying (buyers, Long Buildup, OI Spike on CE)
    // PE BUY:  signals with side=PE and buying
    // CE SELL: signals from CE writers / short-side
    // PE SELL: signals from PE writers / short-side
    const buckets = {
        CE_BUY:  [],
        PE_BUY:  [],
        CE_SELL: [],
        PE_SELL: [],
    };

    signals.forEach(sig => {
        const side   = (sig.side   || '').toUpperCase();
        const action = (sig.action || '').toUpperCase();

        // Writer signals default to SELL bucket
        const isWriter = (sig.signal_name || '').toLowerCase().includes('writer') ||
                         (sig.signal_name || '').toLowerCase().includes('sell');

        if (side === 'CE') {
            buckets[isWriter ? 'CE_SELL' : 'CE_BUY'].push(sig);
        } else if (side === 'PE') {
            buckets[isWriter ? 'PE_SELL' : 'PE_BUY'].push(sig);
        } else {
            // No side tag — use action or strategy context
            if (strategy === 'naked_seller' || action === 'SELL') {
                buckets['CE_SELL'].push(sig);
            } else {
                buckets['CE_BUY'].push(sig);
            }
        }
    });

    // Supplement CE_SELL / PE_SELL from writer arrays
    writerCE.forEach(w => {
        buckets['CE_SELL'].push({
            signal_name: 'Writer Activity',
            side: 'CE',
            strength: 'medium',
            confidence: null,
            strike: w.strike,
            ltp: w.ltp,
            iv: null,
            detail: `OI: ${fmt(w.oi)}  COI: ${fmt(w.coi_chg)}  Price: ${w.price_chg != null ? Number(w.price_chg).toFixed(2) + '%' : '—'}`,
        });
    });
    writerPE.forEach(w => {
        buckets['PE_SELL'].push({
            signal_name: 'Writer Activity',
            side: 'PE',
            strength: 'medium',
            confidence: null,
            strike: w.strike,
            ltp: w.ltp,
            iv: null,
            detail: `OI: ${fmt(w.oi)}  COI: ${fmt(w.coi_chg)}  Price: ${w.price_chg != null ? Number(w.price_chg).toFixed(2) + '%' : '—'}`,
        });
    });

    COLS.forEach(col => {
        const list = buckets[col.key] || [];
        _renderCount(container, col.countId, list.length);
        _renderDominance(container, col.domId, list, topStrikes, col.key);
        _renderSignalList(container, col.sigsId, list);
    });
}

function _renderCount(container, countId, count) {
    const el = container.querySelector(`#${countId}`);
    if (el) el.textContent = String(count);
}

function _renderDominance(container, domId, signals, topStrikes, colKey) {
    const domBox  = container.querySelector(`#${domId}`);
    if (!domBox) return;
    const listEl  = domBox.querySelector('.dom-list');
    if (!listEl) return;

    // Top 3 by confidence score from signals in this column
    const sorted = [...signals]
        .filter(s => s.confidence != null)
        .sort((a, b) => (b.confidence || 0) - (a.confidence || 0))
        .slice(0, 3);

    if (!sorted.length) {
        listEl.innerHTML = '<div class="dim">—</div>';
        return;
    }

    listEl.innerHTML = sorted.map((sig, i) => {
        const conf = sig.confidence != null ? fmtPct(sig.confidence * 100) : '—';
        return `<div class="dom-item">
  <span class="dom-rank">${i + 1}</span>
  <span class="dom-strike">${fmtInt(sig.strike)}</span>
  <span class="dom-conf">${conf}</span>
</div>`;
    }).join('');
}

function _renderSignalList(container, sigsId, signals) {
    const listEl = container.querySelector(`#${sigsId}`);
    if (!listEl) return;

    if (!signals.length) {
        listEl.innerHTML = '<div class="dim">No signals</div>';
        return;
    }

    listEl.innerHTML = signals.map(sig => {
        const strengthCls = (sig.strength || 'medium').toLowerCase();
        const confPct     = sig.confidence != null ? fmtPct(sig.confidence * 100) : '—';
        return `<div class="scalp-signal-card strength-${strengthCls}">
  <div class="signal-header">
    <span class="signal-name">${sig.signal_name || '—'}</span>
    <span class="signal-strength ${strengthCls}">${sig.strength || ''}</span>
    <span class="signal-strike">${fmtInt(sig.strike)}</span>
  </div>
  <div class="signal-meta">
    <span>Conf: <strong>${confPct}</strong></span>
    <span>LTP: <strong>${fmt(sig.ltp)}</strong></span>
    <span>IV: <strong>${sig.iv != null ? Number(sig.iv).toFixed(1) + '%' : '—'}</strong></span>
  </div>
  <div class="signal-detail">${sig.detail || ''}</div>
</div>`;
    }).join('');
}
