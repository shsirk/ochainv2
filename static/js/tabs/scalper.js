import { apiFetch, scalperUrl } from '../core/api.js';
import { showSkeleton, showSkeletonCharts, hideSkeleton } from '../components/skeleton.js';
import { showError, clearError } from '../components/error-state.js';
import {
    fmt, fmtInt, fmtPct, chgClass, themeColors, chartColors,
    defaultChartOpts, destroyChart, el
} from '../utils.js';

let _ctrl      = null;
let _cref      = null;
let _chart     = null;
let _mode      = 'CE_BUY';
let _lastS     = null;

const MODE_STRATEGY = {
    CE_BUY:  'naked_buyer',
    PE_BUY:  'naked_buyer',
    CE_SELL: 'naked_seller',
    PE_SELL: 'naked_seller',
};

export function init(container) {
    _cref = container;
    container.innerHTML = `
<div class="scalp-header">
  <div class="scalp-mode-switcher">
    <button class="scalp-mode-btn active" data-mode="CE_BUY">CE Buy</button>
    <button class="scalp-mode-btn" data-mode="PE_BUY">PE Buy</button>
    <button class="scalp-mode-btn" data-mode="CE_SELL">CE Sell</button>
    <button class="scalp-mode-btn" data-mode="PE_SELL">PE Sell</button>
  </div>
</div>
<div class="scalp-market-read neutral" id="scalperMarketRead">
  <span class="mr-icon">—</span><span id="mrText">Loading…</span>
  <span class="mr-stats" id="mrStats"></span>
</div>
<div class="scalp-grid">
  <div class="scalp-panel"><h3>Signals</h3><div class="scalp-signal-list" id="scalperSignals"></div></div>
  <div class="scalp-panel">
    <h3>Top Strikes</h3>
    <div class="panel-table-wrap">
      <table class="panel-table" id="scalperTopStrikes">
        <thead>
          <tr>
            <th style="text-align:left">Strike</th>
            <th>Score</th>
            <th>CE LTP</th>
            <th>PE LTP</th>
            <th>CE COI</th>
            <th>PE COI</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
</div>
<div class="scalp-coi-section">
  <h3>COI Flow</h3>
  <div class="chart-box" style="height:200px">
    <div class="chart-wrap"><canvas id="scalperCOICanvas"></canvas></div>
  </div>
</div>
<div class="scalp-grid">
  <div class="scalp-panel"><h3>CE Writers</h3><div id="scalperWriterCE"></div></div>
  <div class="scalp-panel"><h3>PE Writers</h3><div id="scalperWriterPE"></div></div>
</div>`;

    container.querySelector('.scalp-mode-switcher').addEventListener('click', e => {
        const btn = e.target.closest('.scalp-mode-btn');
        if (!btn) return;
        container.querySelectorAll('.scalp-mode-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        _mode = btn.dataset.mode;
        if (_lastS) load(container, _lastS);
    });
}

export async function load(container, s) {
    _lastS = s;
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    const sig = _ctrl.signal;

    clearError(container);

    const signalsEl  = container.querySelector('#scalperSignals');
    const readEl     = container.querySelector('#scalperMarketRead');
    if (signalsEl) showSkeleton(signalsEl, 3);
    if (readEl)   readEl.className = 'scalp-market-read neutral';

    const strategy = MODE_STRATEGY[_mode] || 'naked_buyer';

    try {
        const data = await apiFetch(
            scalperUrl(s, strategy) + `&mode=${_mode}`,
            sig
        );

        hideSkeleton(signalsEl);

        _renderMarketRead(container, data.market_read || {});
        _renderSignals(container, data.signals || []);
        _renderTopStrikes(container, data.top_strikes || []);
        _renderCOIChart(container, data.coi_flow || []);
        _renderWriters(container, data.writer_ce || [], 'CE');
        _renderWriters(container, data.writer_pe || [], 'PE');
    } catch (err) {
        if (err.name === 'AbortError') return;
        hideSkeleton(signalsEl);
        showError(container, err.message || 'Failed to load scalper data', () => load(container, s));
    }
}

function _renderMarketRead(container, mr) {
    const readEl  = container.querySelector('#scalperMarketRead');
    const textEl  = container.querySelector('#mrText');
    const statsEl = container.querySelector('#mrStats');
    if (!readEl || !textEl) return;

    const dir = (mr.direction || 'neutral').toLowerCase();
    const iconMap = { bullish: '↑', bearish: '↓', volatile: '↕', neutral: '—' };

    readEl.className = `scalp-market-read ${dir}`;
    readEl.querySelector('.mr-icon').textContent = iconMap[dir] || '—';
    textEl.textContent = mr.reason || dir;

    if (statsEl) {
        const ceCOI = mr.net_ce_coi != null ? fmt(mr.net_ce_coi) : '—';
        const peCOI = mr.net_pe_coi != null ? fmt(mr.net_pe_coi) : '—';
        statsEl.innerHTML = `CE COI: <span class="${chgClass(mr.net_ce_coi)}">${ceCOI}</span>  PE COI: <span class="${chgClass(mr.net_pe_coi)}">${peCOI}</span>`;
    }
}

function _renderSignals(container, signals) {
    const listEl = container.querySelector('#scalperSignals');
    if (!listEl) return;

    if (!signals.length) {
        listEl.innerHTML = '<div class="dim">No signals for current mode</div>';
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

function _renderTopStrikes(container, strikes) {
    const tbody = container.querySelector('#scalperTopStrikes tbody');
    if (!tbody) return;

    if (!strikes.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="dim" style="text-align:center">—</td></tr>';
        return;
    }

    tbody.innerHTML = strikes.map(r => {
        const scoreW = Math.min(100, r.score || 0);
        return `<tr>
  <td style="text-align:left"><strong>${fmtInt(r.strike)}</strong></td>
  <td>
    <div class="score-bar-wrap" style="display:flex;align-items:center;gap:4px">
      <div class="score-bar" style="width:${scoreW}%;height:6px;background:var(--green);border-radius:3px"></div>
      <span>${r.score != null ? r.score : '—'}</span>
    </div>
  </td>
  <td>${fmt(r.ce_ltp)}</td>
  <td>${fmt(r.pe_ltp)}</td>
  <td class="${chgClass(r.ce_coi)}">${fmt(r.ce_coi)}</td>
  <td class="${chgClass(r.pe_coi)}">${fmt(r.pe_coi)}</td>
</tr>`;
    }).join('');
}

function _renderCOIChart(container, coiFlow) {
    const canvas = container.querySelector('#scalperCOICanvas');
    if (!canvas) return;
    _chart = destroyChart(_chart);

    if (!coiFlow.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No COI flow data</div>';
        return;
    }

    const tc     = themeColors();
    const cc     = chartColors();
    const labels = coiFlow.map(r => String(r.strike));

    _chart = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'CE COI',
                    data: coiFlow.map(r => r.ce_coi),
                    backgroundColor: coiFlow.map(r => r.ce_coi >= 0 ? cc.red : cc.green),
                    borderWidth: 0,
                },
                {
                    label: 'PE COI',
                    data: coiFlow.map(r => r.pe_coi),
                    backgroundColor: coiFlow.map(r => r.pe_coi >= 0 ? cc.green : cc.red),
                    borderWidth: 0,
                },
            ],
        },
        options: {
            ...defaultChartOpts(),
            indexAxis: 'y',
            plugins: { legend: { labels: { color: tc.textDim, font: { size: 10 } } } },
            scales: {
                x: { ticks: { color: tc.textDim, font: { size: 9 }, callback: v => fmt(v) }, grid: { color: tc.grid } },
                y: { ticks: { color: tc.textDim, font: { size: 9 } }, grid: { color: tc.grid } },
            },
        },
    });
}

function _renderWriters(container, writers, side) {
    const hostEl = container.querySelector(`#scalperWriter${side}`);
    if (!hostEl) return;

    if (!writers.length) {
        hostEl.innerHTML = '<div class="dim">No writer data</div>';
        return;
    }

    hostEl.innerHTML = writers.map(w => {
        const chgCls = chgClass(w.price_chg);
        return `<div class="writer-item">
  <span class="writer-strike">${fmtInt(w.strike)}</span>
  <span class="writer-oi">OI: ${fmt(w.oi)}</span>
  <span class="writer-coi ${chgClass(w.coi_chg)}">COI: ${fmt(w.coi_chg)}</span>
  <span class="writer-ltp">₹${fmt(w.ltp)}</span>
  <span class="writer-pchg ${chgCls}">${w.price_chg != null ? (w.price_chg > 0 ? '+' : '') + Number(w.price_chg).toFixed(2) + '%' : '—'}</span>
</div>`;
    }).join('');
}
