import { apiFetch, gexUrl, alertsUrl } from '../core/api.js';
import { showSkeleton, hideSkeleton } from '../components/skeleton.js';
import { showError, clearError } from '../components/error-state.js';
import {
    fmt, fmtInt, chgClass, themeColors, chartColors,
    defaultChartOpts, destroyChart, el
} from '../utils.js';

let _ctrl   = null;
let _charts = { gex: null, dex: null };
let _cref   = null; // container ref

export function init(container) {
    _cref = container;
    container.innerHTML = `
<div class="flow-grid">
  <div class="chart-box flow-chart">
    <h3>Net GEX per Strike</h3>
    <div class="chart-wrap"><canvas id="flowGexCanvas"></canvas></div>
  </div>
  <div class="chart-box flow-chart">
    <h3>CE vs PE GEX</h3>
    <div class="chart-wrap"><canvas id="flowDexCanvas"></canvas></div>
  </div>
</div>
<div class="flow-grid">
  <div class="flow-info-panel" id="flowKeyLevels">
    <h3>Key Levels</h3>
  </div>
  <div class="alerts-section">
    <h3>Unusual Activity</h3>
    <div class="alerts-feed" id="alertsFeed"></div>
  </div>
</div>`;
}

export async function load(container, s) {
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    const sig = _ctrl.signal;

    clearError(container);
    showSkeleton(container.querySelector('#flowKeyLevels'), 6);
    const alertsFeed = container.querySelector('#alertsFeed');
    if (alertsFeed) alertsFeed.innerHTML = '<div class="skeleton"></div>';

    try {
        const [gex, alerts] = await Promise.all([
            apiFetch(gexUrl(s), sig),
            apiFetch(alertsUrl(s), sig),
        ]);

        hideSkeleton(container.querySelector('#flowKeyLevels'));

        _renderGexChart(container, gex);
        _renderDexChart(container, gex);
        _renderKeyLevels(container, gex);
        _renderAlerts(container, alerts);
    } catch (err) {
        if (err.name === 'AbortError') return;
        hideSkeleton(container.querySelector('#flowKeyLevels'));
        showError(container, err.message || 'Failed to load flow data', () => load(container, s));
    }
}

function _renderGexChart(container, gex) {
    const canvas = container.querySelector('#flowGexCanvas');
    if (!canvas) return;
    _charts.gex = destroyChart(_charts.gex);

    const strikes = gex.strikes || [];
    const netGex  = gex.net_gex  || [];
    if (!strikes.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No GEX data</div>';
        return;
    }
    const t  = themeColors();
    const cc = chartColors();
    const labels = strikes.map(String);
    const colors = netGex.map(v => v >= 0 ? cc.green : cc.red);

    _charts.gex = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Net GEX',
                data: netGex,
                backgroundColor: colors,
                borderColor: colors,
                borderWidth: 1,
            }],
        },
        options: {
            ...defaultChartOpts(),
            indexAxis: 'y',
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { label: ctx => `GEX: ${fmt(ctx.raw)}` } },
            },
            scales: {
                x: { ticks: { color: t.textDim, font: { size: 9 } }, grid: { color: t.grid } },
                y: { ticks: { color: t.textDim, font: { size: 9 } }, grid: { color: t.grid } },
            },
        },
    });
}

function _renderDexChart(container, gex) {
    const canvas = container.querySelector('#flowDexCanvas');
    if (!canvas) return;
    _charts.dex = destroyChart(_charts.dex);

    const strikes = gex.strikes || [];
    const ceGex   = gex.ce_gex  || [];
    const peGex   = gex.pe_gex  || [];
    if (!strikes.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No GEX data</div>';
        return;
    }
    const t  = themeColors();
    const cc = chartColors();

    _charts.dex = new Chart(canvas, {
        type: 'bar',
        data: {
            labels: strikes.map(String),
            datasets: [
                { label: 'CE GEX', data: ceGex, backgroundColor: cc.ce, borderWidth: 0 },
                { label: 'PE GEX', data: peGex, backgroundColor: cc.pe, borderWidth: 0 },
            ],
        },
        options: {
            ...defaultChartOpts(),
            indexAxis: 'y',
            plugins: { legend: { labels: { color: t.textDim, font: { size: 10 } } } },
            scales: {
                x: { ticks: { color: t.textDim, font: { size: 9 } }, grid: { color: t.grid } },
                y: { ticks: { color: t.textDim, font: { size: 9 } }, grid: { color: t.grid } },
            },
        },
    });
}

function _renderKeyLevels(container, gex) {
    const panel = container.querySelector('#flowKeyLevels');
    if (!panel) return;

    const rows = [
        ['Total GEX',   fmt(gex.total_gex),               chgClass(gex.total_gex)],
        ['CE GEX',      fmt(gex.total_ce_gex),            ''],
        ['PE GEX',      fmt(gex.total_pe_gex),            ''],
        ['GEX Flip',    gex.flip_point != null ? fmtInt(gex.flip_point) : '—', ''],
        ['Regime',      gex.regime     || '—',            'dim'],
        ['Total DEX',   fmt(gex.dex),                     chgClass(gex.dex)],
        ['Underlying',  fmtInt(gex.underlying_ltp),       ''],
    ];

    panel.innerHTML = '<h3>Key Levels</h3>' +
        rows.map(([label, val, cls]) =>
            `<div class="info-row"><span class="info-label">${label}</span><span class="info-val ${cls}">${val}</span></div>`
        ).join('');
}

function _renderAlerts(container, alerts) {
    const feed = container.querySelector('#alertsFeed');
    if (!feed) return;

    const list = (alerts && alerts.alerts) ? alerts.alerts : (Array.isArray(alerts) ? alerts : []);
    if (!list.length) {
        feed.innerHTML = '<div class="alert-empty dim">No unusual activity detected</div>';
        return;
    }

    feed.innerHTML = list.map(a => {
        const typeClass = 'alert-' + (a.alert_type || '').toLowerCase().replace(/\s+/g, '-');
        const time = a.ts ? new Date(a.ts).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }) : '—';
        return `<div class="alert-item ${typeClass}">
  <span class="alert-badge">${a.side || ''}</span>
  <span class="alert-strike">${fmtInt(a.strike)}</span>
  <span class="alert-type">${a.alert_type || '—'}</span>
  <span class="alert-detail">${a.detail || ''}</span>
  <span class="alert-time">${time}</span>
</div>`;
    }).join('');
}
