import { apiFetch, ivUrl } from '../core/api.js';
import { showError, clearError } from '../components/error-state.js';
import {
    fmt, fmtPct, themeColors, chartColors,
    defaultChartOpts, destroyChart, el
} from '../utils.js';

let _ctrl   = null;
let _cref   = null;
let _charts = { smile: null, intraday: null };

export function init(container) {
    _cref = container;
    container.innerHTML = `
<div class="iv-grid">
  <div class="chart-box iv-chart">
    <h3>IV Smile</h3>
    <div class="chart-wrap"><canvas id="ivSmileCanvas"></canvas></div>
  </div>
  <div class="chart-box iv-chart">
    <h3>ATM IV Intraday</h3>
    <div class="chart-wrap"><canvas id="ivIntradayCanvas"></canvas></div>
  </div>
</div>
<div class="iv-surface-wrap">
  <h3>IV Surface (3D)</h3>
  <div id="ivSurface3D" style="height:380px"></div>
</div>`;
}

export async function load(container, s) {
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    const sig = _ctrl.signal;

    clearError(container);

    try {
        const data = await apiFetch(ivUrl(s), sig);

        _renderSmile(container, data.iv_smile || {});
        _renderIntraday(container, data.atm_iv_intraday || []);
        _renderSurface3D(container, data.iv_surface || null);
    } catch (err) {
        if (err.name === 'AbortError') return;
        showError(container, err.message || 'Failed to load IV data', () => load(container, s));
    }
}

function _renderSmile(container, smileData) {
    const canvas = container.querySelector('#ivSmileCanvas');
    if (!canvas) return;
    _charts.smile = destroyChart(_charts.smile);

    // smileData = {strikes, ce_iv, pe_iv, expiry, dte, ...}
    const strikes = smileData.strikes || [];
    if (!strikes.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No IV smile data</div>';
        return;
    }

    const tc      = themeColors();
    const expLabel = smileData.expiry || '';
    const ceIV    = smileData.ce_iv || [];
    const peIV    = smileData.pe_iv || [];

    _charts.smile = new Chart(canvas, {
        type: 'line',
        data: {
            labels: strikes.map(String),
            datasets: [
                {
                    label: `CE IV${expLabel ? ' (' + expLabel + ')' : ''}`,
                    data: ceIV,
                    borderColor: tc.ce,
                    backgroundColor: 'transparent',
                    tension: 0.3,
                    pointRadius: 2,
                    spanGaps: true,
                },
                {
                    label: `PE IV${expLabel ? ' (' + expLabel + ')' : ''}`,
                    data: peIV,
                    borderColor: tc.pe,
                    backgroundColor: 'transparent',
                    tension: 0.3,
                    pointRadius: 2,
                    spanGaps: true,
                },
            ],
        },
        options: {
            ...defaultChartOpts(),
            plugins: {
                legend: { labels: { color: tc.textDim, font: { size: 10 } } },
                tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${fmtPct(ctx.raw)}` } },
            },
            scales: {
                x: { ticks: { color: tc.textDim, font: { size: 8 }, maxRotation: 45 }, grid: { color: tc.grid } },
                y: { ticks: { color: tc.textDim, font: { size: 9 } }, grid: { color: tc.grid }, title: { display: true, text: 'IV %', color: tc.textDim } },
            },
        },
    });
}

function _renderIntraday(container, rows) {
    const canvas = container.querySelector('#ivIntradayCanvas');
    if (!canvas) return;
    _charts.intraday = destroyChart(_charts.intraday);

    if (!rows.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No intraday IV data</div>';
        return;
    }

    const tc     = themeColors();
    const labels = rows.map(r => r.ts);

    _charts.intraday = new Chart(canvas, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'CE IV',
                    data: rows.map(r => r.ce_iv),
                    borderColor: tc.ce || tc.blue,
                    backgroundColor: 'transparent',
                    tension: 0.2,
                    pointRadius: 0,
                },
                {
                    label: 'PE IV',
                    data: rows.map(r => r.pe_iv),
                    borderColor: tc.pe || '#f0883e',
                    backgroundColor: 'transparent',
                    tension: 0.2,
                    pointRadius: 0,
                },
                {
                    label: 'Avg IV',
                    data: rows.map(r => r.avg_iv),
                    borderColor: tc.textDim,
                    backgroundColor: 'transparent',
                    tension: 0.2,
                    pointRadius: 0,
                    borderDash: [4, 4],
                },
            ],
        },
        options: {
            ...defaultChartOpts(),
            plugins: {
                legend: { labels: { color: tc.textDim, font: { size: 10 } } },
                tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${fmtPct(ctx.raw)}` } },
            },
            scales: {
                x: { ticks: { color: tc.textDim, font: { size: 8 }, maxRotation: 45 }, grid: { color: tc.grid } },
                y: { ticks: { color: tc.textDim, font: { size: 9 } }, grid: { color: tc.grid }, title: { display: true, text: 'IV %', color: tc.textDim } },
            },
        },
    });
}

function _renderSurface3D(container, surfaceData) {
    const surfaceEl = container.querySelector('#ivSurface3D');
    if (!surfaceEl) return;

    // Backend returns: {strikes, dte, ce_surface, pe_surface, ...}
    const dtes   = surfaceData?.dte   || surfaceData?.dtes   || [];
    const matrix = surfaceData?.ce_surface || surfaceData?.matrix || [];
    const hasData = surfaceData &&
        Array.isArray(surfaceData.strikes) && surfaceData.strikes.length > 1 &&
        dtes.length > 1 && matrix.length;

    if (!hasData) {
        surfaceEl.innerHTML = '<div class="dim" style="padding:2rem;text-align:center">3D surface requires multiple expiries</div>';
        return;
    }

    surfaceEl.innerHTML = '';

    const _doPlot = () => {
        const tc = themeColors();
        window.Plotly.newPlot(
            surfaceEl,
            [{
                type: 'surface',
                z: matrix,
                x: dtes,
                y: surfaceData.strikes,
                colorscale: 'Viridis',
            }],
            {
                paper_bgcolor: 'transparent',
                plot_bgcolor:  'transparent',
                font: { color: tc.text, size: 10 },
                margin: { t: 20, b: 40, l: 40, r: 20 },
                scene: {
                    xaxis: { title: 'DTE', gridcolor: tc.grid },
                    yaxis: { title: 'Strike', gridcolor: tc.grid },
                    zaxis: { title: 'IV %', gridcolor: tc.grid },
                    bgcolor: 'transparent',
                },
            },
            { responsive: true, displayModeBar: false }
        );
    };

    if (window.Plotly) {
        _doPlot();
        return;
    }

    // Lazy-load Plotly from CDN
    const script   = document.createElement('script');
    script.src     = 'https://cdn.plot.ly/plotly-2.35.2.min.js';
    script.onload  = _doPlot;
    script.onerror = () => {
        surfaceEl.innerHTML = '<div class="dim" style="padding:2rem;text-align:center">Failed to load Plotly — 3D surface unavailable</div>';
    };
    document.head.appendChild(script);
}
