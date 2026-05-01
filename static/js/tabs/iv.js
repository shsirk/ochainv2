import { apiFetch, ivUrl } from '../core/api.js';
import { showSkeleton, showSkeletonCharts, hideSkeleton } from '../components/skeleton.js';
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
    showSkeletonCharts(container.querySelector('.iv-grid'), 2);
    const surfaceEl = container.querySelector('#ivSurface3D');
    if (surfaceEl) surfaceEl.innerHTML = '<div class="skeleton-chart" style="height:100%"></div>';

    try {
        const data = await apiFetch(ivUrl(s), sig);
        hideSkeleton(container.querySelector('.iv-grid'));

        _renderSmile(container, data.iv_smile || {});
        _renderIntraday(container, data.atm_iv_intraday || []);
        _renderSurface3D(container, data.iv_surface || null);
    } catch (err) {
        if (err.name === 'AbortError') return;
        hideSkeleton(container.querySelector('.iv-grid'));
        showError(container, err.message || 'Failed to load IV data', () => load(container, s));
    }
}

function _renderSmile(container, smileMap) {
    const canvas = container.querySelector('#ivSmileCanvas');
    if (!canvas) return;
    _charts.smile = destroyChart(_charts.smile);

    const expiries = Object.keys(smileMap);
    if (!expiries.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No IV smile data</div>';
        return;
    }

    const tc     = themeColors();
    const palette = [tc.blue, tc.green, tc.pe, tc.yellow, tc.purple, tc.red];

    // Collect union of all strikes
    const allStrikes = [...new Set(
        expiries.flatMap(exp => (smileMap[exp] || []).map(r => r.strike))
    )].sort((a, b) => a - b);

    const datasets = expiries.map((exp, i) => {
        const rows    = smileMap[exp] || [];
        const byStrike = Object.fromEntries(rows.map(r => [r.strike, r.iv]));
        return {
            label: exp,
            data: allStrikes.map(k => byStrike[k] ?? null),
            borderColor: palette[i % palette.length],
            backgroundColor: 'transparent',
            tension: 0.3,
            pointRadius: 2,
            spanGaps: true,
        };
    });

    _charts.smile = new Chart(canvas, {
        type: 'line',
        data: { labels: allStrikes.map(String), datasets },
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

    const hasData = surfaceData &&
        Array.isArray(surfaceData.strikes) && surfaceData.strikes.length > 1 &&
        Array.isArray(surfaceData.dtes)    && surfaceData.dtes.length    > 1 &&
        Array.isArray(surfaceData.matrix)  && surfaceData.matrix.length;

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
                z: surfaceData.matrix,
                x: surfaceData.dtes,
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
