import { apiFetch, strikeUrl } from '../core/api.js';
import { showSkeleton, showSkeletonCharts, hideSkeleton } from '../components/skeleton.js';
import { showError, clearError } from '../components/error-state.js';
import {
    fmt, fmtInt, fmtPct, themeColors, chartColors,
    defaultChartOpts, destroyChart, el
} from '../utils.js';

let _ctrl   = null;
let _cref   = null;
let _charts = { oi: null, vol: null, ltp: null, iv: null };

const BUILDUP_COLORS = {
    'Long Buildup':    '#3fb950',
    'Short Buildup':   '#f85149',
    'Long Unwinding':  '#d29922',
    'Short Covering':  '#bc8cff',
};

export function init(container) {
    _cref = container;
    container.innerHTML = `
<div class="strike-header-row">
  <label>Strike</label>
  <select id="strikeSelect"><option value="">— select —</option></select>
</div>
<div class="strike-charts">
  <div class="chart-box strike-chart"><h3>OI</h3><div class="chart-wrap"><canvas id="strikeOICanvas"></canvas></div></div>
  <div class="chart-box strike-chart"><h3>Volume</h3><div class="chart-wrap"><canvas id="strikeVolCanvas"></canvas></div></div>
  <div class="chart-box strike-chart"><h3>LTP</h3><div class="chart-wrap"><canvas id="strikeLTPCanvas"></canvas></div></div>
  <div class="chart-box strike-chart"><h3>IV %</h3><div class="chart-wrap"><canvas id="strikeIVCanvas"></canvas></div></div>
</div>
<div class="buildup-timeline" id="buildupTimeline">
  <div class="bt-label">CE Buildup</div><div class="bt-bar" id="btCE"></div>
  <div class="bt-label">PE Buildup</div><div class="bt-bar" id="btPE"></div>
</div>`;

    container.querySelector('#strikeSelect').addEventListener('change', e => {
        const strike = e.target.value;
        if (strike && _lastS) {
            _loadStrike(container, _lastS, Number(strike));
        }
    });

    // Listen for drill-down events from heatmap / volume tabs
    window.addEventListener('ochain:strikeDrill', e => {
        const { strike } = e.detail || {};
        if (!strike || !_cref) return;
        const sel = _cref.querySelector('#strikeSelect');
        if (sel) {
            const opt = [...sel.options].find(o => Number(o.value) === strike);
            if (opt) {
                sel.value = String(strike);
                if (_lastS) _loadStrike(_cref, _lastS, strike);
            }
        }
    });

    // Listen for chain data to populate the select
    window.addEventListener('ochain:chainData', e => {
        const strikes = e.detail?.strikes || [];
        _populateSelect(container, strikes);
    });
}

let _lastS = null;

export async function load(container, s) {
    _lastS = s;
    clearError(container);

    // Populate strikes from chain data if select is empty
    const sel = container.querySelector('#strikeSelect');
    if (sel && sel.options.length <= 1) {
        // No chain data yet; show placeholder
        return;
    }

    const strike = sel ? Number(sel.value) : null;
    if (!strike) return;

    await _loadStrike(container, s, strike);
}

async function _loadStrike(container, s, strike) {
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    const sig = _ctrl.signal;

    clearError(container);
    showSkeletonCharts(container.querySelector('.strike-charts'), 4);

    try {
        const data = await apiFetch(strikeUrl(s, strike), sig);
        hideSkeleton(container.querySelector('.strike-charts'));
        _renderCharts(container, data);
        _renderBuildup(container, data);
    } catch (err) {
        if (err.name === 'AbortError') return;
        hideSkeleton(container.querySelector('.strike-charts'));
        showError(container, err.message || 'Failed to load strike data', () => _loadStrike(container, s, strike));
    }
}

function _populateSelect(container, strikes) {
    const sel = container.querySelector('#strikeSelect');
    if (!sel) return;
    const current = sel.value;
    sel.innerHTML = '<option value="">— select —</option>' +
        strikes.map(k => `<option value="${k}"${String(k) === current ? ' selected' : ''}>${fmtInt(k)}</option>`).join('');
}

function _renderCharts(container, data) {
    const ce  = data.ce || [];
    const pe  = data.pe || [];
    const tc  = themeColors();
    const cc  = chartColors();
    const tsl = ce.map(r => r.ts);

    const baseOpts = () => ({
        ...defaultChartOpts(),
        plugins: { legend: { labels: { color: tc.textDim, font: { size: 10 } } } },
    });

    // OI chart
    _charts.oi = destroyChart(_charts.oi);
    const oiCanvas = container.querySelector('#strikeOICanvas');
    if (oiCanvas) {
        _charts.oi = new Chart(oiCanvas, {
            type: 'line',
            data: {
                labels: tsl,
                datasets: [
                    { label: 'CE OI', data: ce.map(r => r.oi), borderColor: cc.ce, backgroundColor: 'transparent', tension: 0.2, pointRadius: 0 },
                    { label: 'PE OI', data: pe.map(r => r.oi), borderColor: cc.pe, backgroundColor: 'transparent', tension: 0.2, pointRadius: 0 },
                ],
            },
            options: { ...baseOpts(), scales: { x: { ticks: { color: tc.textDim, font: { size: 8 }, maxRotation: 45 }, grid: { color: tc.grid } }, y: { ticks: { color: tc.textDim, font: { size: 9 } }, grid: { color: tc.grid } } } },
        });
    }

    // Volume chart
    _charts.vol = destroyChart(_charts.vol);
    const volCanvas = container.querySelector('#strikeVolCanvas');
    if (volCanvas) {
        _charts.vol = new Chart(volCanvas, {
            type: 'bar',
            data: {
                labels: tsl,
                datasets: [
                    { label: 'CE Vol', data: ce.map(r => r.vol), backgroundColor: cc.ce },
                    { label: 'PE Vol', data: pe.map(r => r.vol), backgroundColor: cc.pe },
                ],
            },
            options: { ...baseOpts(), scales: { x: { ticks: { color: tc.textDim, font: { size: 8 }, maxRotation: 45 }, grid: { color: tc.grid } }, y: { ticks: { color: tc.textDim, font: { size: 9 } }, grid: { color: tc.grid } } } },
        });
    }

    // LTP chart
    _charts.ltp = destroyChart(_charts.ltp);
    const ltpCanvas = container.querySelector('#strikeLTPCanvas');
    if (ltpCanvas) {
        _charts.ltp = new Chart(ltpCanvas, {
            type: 'line',
            data: {
                labels: tsl,
                datasets: [
                    { label: 'CE LTP', data: ce.map(r => r.ltp), borderColor: cc.ce, backgroundColor: 'transparent', tension: 0.2, pointRadius: 0 },
                    { label: 'PE LTP', data: pe.map(r => r.ltp), borderColor: cc.pe, backgroundColor: 'transparent', tension: 0.2, pointRadius: 0 },
                ],
            },
            options: { ...baseOpts(), scales: { x: { ticks: { color: tc.textDim, font: { size: 8 }, maxRotation: 45 }, grid: { color: tc.grid } }, y: { ticks: { color: tc.textDim, font: { size: 9 } }, grid: { color: tc.grid } } } },
        });
    }

    // IV chart
    _charts.iv = destroyChart(_charts.iv);
    const ivCanvas = container.querySelector('#strikeIVCanvas');
    if (ivCanvas) {
        _charts.iv = new Chart(ivCanvas, {
            type: 'line',
            data: {
                labels: tsl,
                datasets: [
                    { label: 'CE IV', data: ce.map(r => r.iv), borderColor: cc.ce, backgroundColor: 'transparent', tension: 0.2, pointRadius: 0 },
                    { label: 'PE IV', data: pe.map(r => r.iv), borderColor: cc.pe, backgroundColor: 'transparent', tension: 0.2, pointRadius: 0 },
                ],
            },
            options: { ...baseOpts(), scales: { x: { ticks: { color: tc.textDim, font: { size: 8 }, maxRotation: 45 }, grid: { color: tc.grid } }, y: { ticks: { color: tc.textDim, font: { size: 9 } }, grid: { color: tc.grid } } } },
        });
    }
}

function _renderBuildup(container, data) {
    const btCE = container.querySelector('#btCE');
    const btPE = container.querySelector('#btPE');
    if (!btCE || !btPE) return;

    btCE.innerHTML = _buildupBars(data.ce || []);
    btPE.innerHTML = _buildupBars(data.pe || []);
}

function _buildupBars(rows) {
    if (!rows.length) return '<span class="dim">—</span>';
    const total  = rows.length;
    const widthP = (100 / total).toFixed(2);
    return rows.map(r => {
        const buildup = r.buildup || '';
        const color   = BUILDUP_COLORS[buildup] || 'transparent';
        const title   = buildup || 'Unknown';
        return `<span class="bt-segment" style="width:${widthP}%;background:${color};display:inline-block;height:100%;cursor:default;" title="${title}"></span>`;
    }).join('');
}
