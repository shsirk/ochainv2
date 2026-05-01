import { apiFetch, heatmapUrl } from '../core/api.js';
import { showSkeleton, hideSkeleton } from '../components/skeleton.js';
import { showError, clearError } from '../components/error-state.js';
import { fmt, fmtInt, themeColors, el } from '../utils.js';

let _ctrl    = null;
let _cref    = null;
let _tooltip = null;

export function init(container) {
    _cref = container;
    container.innerHTML = `
<div class="volume-insights">
  <div class="volume-panel">
    <h3>Volume Insights</h3>
    <div class="volume-kpis">
      <div class="volume-kpi"><span>CE Vol</span><strong id="volCeTotal">—</strong></div>
      <div class="volume-kpi"><span>PE Vol</span><strong id="volPeTotal">—</strong></div>
      <div class="volume-kpi"><span>PE/CE</span><strong id="volRatio">—</strong></div>
    </div>
    <div class="volume-spikes"><h4>Top Volume Spikes</h4><ul id="volSpikes"></ul></div>
  </div>
</div>
<div id="volHmHost"></div>`;

    _tooltip = el('div', 'hm-tooltip');
    _tooltip.style.cssText = 'position:fixed;display:none;pointer-events:none;z-index:9999;';
    document.body.appendChild(_tooltip);
}

export async function load(container, s) {
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    const sig = _ctrl.signal;

    clearError(container);
    const host = container.querySelector('#volHmHost');
    showSkeleton(host, 4);

    try {
        const [volChgData, volData] = await Promise.all([
            apiFetch(heatmapUrl(s, 'volume_change'), sig),
            apiFetch(heatmapUrl(s, 'volume'), sig),
        ]);

        hideSkeleton(host);

        _renderKPIs(container, volData, volChgData);
        _renderSpikes(container, volChgData);
        _renderHeatmap(host, volChgData, container);
    } catch (err) {
        if (err.name === 'AbortError') return;
        hideSkeleton(host);
        showError(container, err.message || 'Failed to load volume data', () => load(container, s));
    }
}

function _renderKPIs(container, volData, volChgData) {
    const matrix  = volData.matrix  || [];
    const strikes = volData.strikes || [];

    if (!matrix.length) return;

    const mid = Math.floor(strikes.length / 2);
    let ceVol = 0, peVol = 0;

    // Use strike index parity: below midpoint = PE range, above = CE range (proxy split)
    matrix.forEach((row, ri) => {
        const rowSum = row.reduce((acc, v) => acc + (v ?? 0), 0);
        if (ri >= mid) {
            ceVol += rowSum;
        } else {
            peVol += rowSum;
        }
    });

    const ratio = ceVol ? (peVol / ceVol).toFixed(2) : '—';

    const ceEl = container.querySelector('#volCeTotal');
    const peEl = container.querySelector('#volPeTotal');
    const rtEl = container.querySelector('#volRatio');
    if (ceEl) ceEl.textContent = fmtInt(Math.round(ceVol));
    if (peEl) peEl.textContent = fmtInt(Math.round(peVol));
    if (rtEl) rtEl.textContent = String(ratio);
}

function _renderSpikes(container, volChgData) {
    const ul      = container.querySelector('#volSpikes');
    if (!ul) return;

    const matrix     = volChgData.matrix     || [];
    const strikes    = volChgData.strikes    || [];
    const timestamps = volChgData.timestamps || [];

    const cells = [];
    matrix.forEach((row, ri) => {
        row.forEach((val, ci) => {
            if (val != null) cells.push({ val, ri, ci });
        });
    });

    cells.sort((a, b) => Math.abs(b.val) - Math.abs(a.val));
    const top5 = cells.slice(0, 5);

    if (!top5.length) {
        ul.innerHTML = '<li class="dim">No spike data</li>';
        return;
    }

    ul.innerHTML = top5.map(({ val, ri, ci }) => {
        const strike = strikes[ri] ?? '?';
        const ts     = timestamps[ci] ?? '?';
        const cls    = val >= 0 ? 'positive' : 'negative';
        return `<li class="spike-item"><span class="spike-strike">${fmtInt(strike)}</span> <span class="spike-ts">${ts}</span> <span class="spike-val ${cls}">${fmt(val)}</span></li>`;
    }).join('');
}

function _lerp(a, b, t) {
    return Math.round(a + (b - a) * t);
}

function _colorDiverging(t, tc) {
    const redRgb      = _hexParse(tc.red);
    const surface2Rgb = _hexParse(tc.surface2);
    const greenRgb    = _hexParse(tc.green);

    if (t < 0.5) {
        const u = t * 2;
        return `rgb(${_lerp(redRgb[0], surface2Rgb[0], u)},${_lerp(redRgb[1], surface2Rgb[1], u)},${_lerp(redRgb[2], surface2Rgb[2], u)})`;
    } else {
        const u = (t - 0.5) * 2;
        return `rgb(${_lerp(surface2Rgb[0], greenRgb[0], u)},${_lerp(surface2Rgb[1], greenRgb[1], u)},${_lerp(surface2Rgb[2], greenRgb[2], u)})`;
    }
}

function _hexParse(hex) {
    if (!hex || !hex.startsWith('#')) return [28, 35, 51];
    const h = hex.replace('#', '');
    return [parseInt(h.slice(0, 2), 16) || 0, parseInt(h.slice(2, 4), 16) || 0, parseInt(h.slice(4, 6), 16) || 0];
}

function _renderHeatmap(host, data, container) {
    const strikes    = data.strikes    || [];
    const timestamps = data.timestamps || [];
    const matrix     = data.matrix     || [];

    if (!strikes.length || !timestamps.length || !matrix.length) {
        host.innerHTML = '<div class="dim">No volume heatmap data available</div>';
        return;
    }

    const CELL_H  = 20;
    const LABEL_W = 60;
    const tc      = themeColors();

    const flat  = matrix.flatMap(row => row.map(v => v ?? 0));
    const minV  = Math.min(...flat);
    const maxV  = Math.max(...flat);
    const range = maxV - minV || 1;

    const canvasW = Math.max(600, timestamps.length * 14 + LABEL_W);
    const canvasH = strikes.length * CELL_H + 24;

    const canvas  = document.createElement('canvas');
    canvas.width  = canvasW;
    canvas.height = canvasH;
    canvas.style.cssText = 'display:block;width:100%;cursor:crosshair;';

    host.innerHTML = '';
    host.appendChild(canvas);

    const ctx   = canvas.getContext('2d');
    const cellW = (canvasW - LABEL_W) / timestamps.length;

    matrix.forEach((row, ri) => {
        const y = ri * CELL_H;
        row.forEach((val, ci) => {
            const raw = val ?? 0;
            const t   = (raw - minV) / range;
            ctx.fillStyle = _colorDiverging(t, tc);
            ctx.fillRect(LABEL_W + ci * cellW, y, cellW - 1, CELL_H - 1);
        });
        ctx.fillStyle = tc.textDim;
        ctx.font      = '9px monospace';
        ctx.textAlign = 'right';
        ctx.fillText(String(strikes[ri]), LABEL_W - 4, y + CELL_H - 6);
    });

    const labelY = strikes.length * CELL_H + 14;
    ctx.fillStyle = tc.textDim;
    ctx.font      = '8px monospace';
    ctx.textAlign = 'center';
    const step    = Math.max(1, Math.floor(timestamps.length / 20));
    timestamps.forEach((ts, ci) => {
        if (ci % step === 0) {
            ctx.fillText(ts, LABEL_W + ci * cellW + cellW / 2, labelY);
        }
    });

    canvas.addEventListener('mousemove', e => {
        const rect  = canvas.getBoundingClientRect();
        const scaleX = canvas.width  / rect.width;
        const scaleY = canvas.height / rect.height;
        const cx    = (e.clientX - rect.left) * scaleX;
        const cy    = (e.clientY - rect.top)  * scaleY;
        const ci    = Math.floor((cx - LABEL_W) / cellW);
        const ri    = Math.floor(cy / CELL_H);
        if (ci >= 0 && ci < timestamps.length && ri >= 0 && ri < strikes.length) {
            const val = matrix[ri]?.[ci] ?? null;
            if (_tooltip) {
                _tooltip.style.display = 'block';
                _tooltip.style.left    = (e.clientX + 12) + 'px';
                _tooltip.style.top     = (e.clientY - 20) + 'px';
                _tooltip.textContent   = `${strikes[ri]} @ ${timestamps[ci]}: ${fmt(val)}`;
            }
        } else {
            if (_tooltip) _tooltip.style.display = 'none';
        }
    });
    canvas.addEventListener('mouseleave', () => { if (_tooltip) _tooltip.style.display = 'none'; });

    canvas.addEventListener('click', e => {
        const rect   = canvas.getBoundingClientRect();
        const scaleX = canvas.width  / rect.width;
        const scaleY = canvas.height / rect.height;
        const cx     = (e.clientX - rect.left) * scaleX;
        const cy     = (e.clientY - rect.top)  * scaleY;
        const ci     = Math.floor((cx - LABEL_W) / cellW);
        const ri     = Math.floor(cy / CELL_H);
        if (ci >= 0 && ci < timestamps.length && ri >= 0 && ri < strikes.length) {
            window.dispatchEvent(new CustomEvent('ochain:strikeDrill', {
                detail: { strike: strikes[ri], ts: timestamps[ci] },
            }));
        }
    });
}
