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
    <h3>Gamma Exposure (GEX)</h3>
    <div class="chart-wrap"><canvas id="flowGexCanvas"></canvas></div>
  </div>
  <div class="chart-box flow-chart">
    <h3>Delta Exposure (DEX)</h3>
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

    const rows = (gex.gex_by_strike || []).slice().reverse();
    if (!rows.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No GEX data</div>';
        return;
    }
    const t  = themeColors();
    const cc = chartColors();
    const labels = rows.map(r => String(r.strike));
    const data   = rows.map(r => r.net_gex);
    const colors = data.map(v => v >= 0 ? cc.green : cc.red);

    _charts.gex = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Net GEX',
                data,
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
                tooltip: {
                    callbacks: {
                        label: ctx => `GEX: ${fmt(ctx.raw)}`,
                    },
                },
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

    const rows = (gex.dex_by_strike || []).slice().reverse();
    if (!rows.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No DEX data</div>';
        return;
    }
    const t  = themeColors();
    const cc = chartColors();
    const labels = rows.map(r => String(r.strike));
    const data   = rows.map(r => r.net_dex);
    const colors = data.map(v => v >= 0 ? cc.green : cc.red);

    _charts.dex = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Net DEX',
                data,
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
                tooltip: {
                    callbacks: {
                        label: ctx => `DEX: ${fmtInt(ctx.raw)}`,
                    },
                },
            },
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

    const em = gex.expected_move || {};
    const walls = gex.gex_walls || {};

    const rows = [
        ['Total GEX',     fmt(gex.total_gex),            chgClass(gex.total_gex)],
        ['GEX Flip',      fmtInt(gex.gex_flip),          ''],
        ['GEX Regime',    gex.gex_regime || '—',         'dim'],
        ['Call Wall',     fmtInt(walls.call_wall),        'negative'],
        ['Put Wall',      fmtInt(walls.put_wall),         'positive'],
        ['1σ Up',         fmtInt(em.one_sigma_up),        'positive'],
        ['1σ Down',       fmtInt(em.one_sigma_down),      'negative'],
        ['2σ Up',         fmtInt(em.two_sigma_up),        'positive'],
        ['2σ Down',       fmtInt(em.two_sigma_down),      'negative'],
    ];

    panel.innerHTML = '<h3>Key Levels</h3>' +
        rows.map(([label, val, cls]) =>
            `<div class="info-row"><span class="info-label">${label}</span><span class="info-val ${cls}">${val}</span></div>`
        ).join('');
}

function _renderAlerts(container, alerts) {
    const feed = container.querySelector('#alertsFeed');
    if (!feed) return;

    if (!alerts || !alerts.length) {
        feed.innerHTML = '<div class="alert-empty dim">No unusual activity detected</div>';
        return;
    }

    feed.innerHTML = alerts.map(a => {
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
