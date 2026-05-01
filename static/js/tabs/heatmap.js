import { apiFetch, heatmapUrl } from '../core/api.js';
import { showSkeleton, hideSkeleton } from '../components/skeleton.js';
import { showError, clearError } from '../components/error-state.js';
import { fmt, themeColors, el } from '../utils.js';

let _ctrl    = null;
let _cref    = null;
let _tooltip = null;
let _lastS   = null;

export function init(container) {
    _cref = container;
    container.innerHTML = `
<div class="heatmap-controls">
  <label>Metric</label>
  <select id="hmMetric">
    <option value="ce_oi" selected>CE OI</option>
    <option value="pe_oi">PE OI</option>
    <option value="ce_iv">CE IV</option>
    <option value="pe_iv">PE IV</option>
    <option value="ce_volume">CE Volume</option>
    <option value="pe_volume">PE Volume</option>
    <option value="ce_ltp">CE LTP</option>
    <option value="pe_ltp">PE LTP</option>
  </select>
</div>
<div id="hmHost"></div>
<div class="hm-legend"><span>Low</span><div class="hm-bar"></div><span>High</span></div>`;

    _tooltip = el('div', 'hm-tooltip');
    _tooltip.style.cssText = 'position:fixed;display:none;pointer-events:none;z-index:9999;';
    document.body.appendChild(_tooltip);

    container.querySelector('#hmMetric').addEventListener('change', () => {
        if (_lastS) load(container, _lastS);
    });
}

export async function load(container, s) {
    _lastS = s;
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    const sig = _ctrl.signal;

    const host = container.querySelector('#hmHost');
    clearError(container);
    showSkeleton(host, 4);

    const metric = container.querySelector('#hmMetric')?.value || 'ce_oi';

    try {
        const data = await apiFetch(heatmapUrl(s, metric), sig);
        hideSkeleton(host);
        _renderHeatmap(host, data, metric);
    } catch (err) {
        if (err.name === 'AbortError') return;
        hideSkeleton(host);
        showError(container, err.message || 'Failed to load heatmap', () => load(container, s));
    }
}

function _lerp(a, b, t) {
    return Math.round(a + (b - a) * t);
}

function _colorDiverging(t, tc) {
    // t in [0,1], 0.5=neutral
    if (t < 0.5) {
        const u = t * 2;
        const r = _lerp(parseInt(tc.red.slice(1, 3) || 'f8', 16), parseInt(tc.surface2.slice(1, 3) || '1c', 16), u);
        const g = _lerp(parseInt(tc.red.slice(3, 5) || '51', 16), parseInt(tc.surface2.slice(3, 5) || '23', 16), u);
        const b = _lerp(parseInt(tc.red.slice(5, 7) || '49', 16), parseInt(tc.surface2.slice(5, 7) || '33', 16), u);
        return `rgb(${r},${g},${b})`;
    } else {
        const u = (t - 0.5) * 2;
        const surface2Hex = tc.surface2.replace('#', '');
        const greenHex    = tc.green.replace('#', '');
        const r = _lerp(parseInt(surface2Hex.slice(0, 2), 16), parseInt(greenHex.slice(0, 2), 16), u);
        const g = _lerp(parseInt(surface2Hex.slice(2, 4), 16), parseInt(greenHex.slice(2, 4), 16), u);
        const b = _lerp(parseInt(surface2Hex.slice(4, 6), 16), parseInt(greenHex.slice(4, 6), 16), u);
        return `rgb(${r},${g},${b})`;
    }
}

function _colorSequential(t, tc) {
    // surface2 → blue
    const surface2Hex = tc.surface2.replace('#', '');
    const blueHex     = tc.blue.replace('#', '');
    const r = _lerp(parseInt(surface2Hex.slice(0, 2), 16), parseInt(blueHex.slice(0, 2), 16), t);
    const g = _lerp(parseInt(surface2Hex.slice(2, 4), 16), parseInt(blueHex.slice(2, 4), 16), t);
    const b = _lerp(parseInt(surface2Hex.slice(4, 6), 16), parseInt(blueHex.slice(4, 6), 16), t);
    return `rgb(${r},${g},${b})`;
}

function _hexToRgbSafe(hex) {
    if (!hex || !hex.startsWith('#')) return [0, 0, 0];
    const h = hex.replace('#', '');
    return [
        parseInt(h.slice(0, 2), 16) || 0,
        parseInt(h.slice(2, 4), 16) || 0,
        parseInt(h.slice(4, 6), 16) || 0,
    ];
}

function _renderHeatmap(host, data, metric) {
    const strikes    = data.strikes    || [];
    const timestamps = data.timestamps || [];
    const matrix     = data.matrix     || [];

    if (!strikes.length || !timestamps.length || !matrix.length) {
        host.innerHTML = '<div class="dim">No heatmap data available</div>';
        return;
    }

    const CELL_H   = 20;
    const LABEL_W  = 60;
    const tc       = themeColors();
    const diverging = false; // all current metrics are non-negative; use sequential scale

    // flatten for normalization
    const flat = matrix.flatMap(row => row.map(v => v ?? 0));
    const minV = Math.min(...flat);
    const maxV = Math.max(...flat);
    const range = maxV - minV || 1;

    const canvasW = Math.max(600, timestamps.length * 14 + LABEL_W);
    const canvasH = strikes.length * CELL_H + 24; // extra for time labels

    const canvas  = document.createElement('canvas');
    canvas.width  = canvasW;
    canvas.height = canvasH;
    canvas.style.cssText = 'display:block;width:100%;cursor:crosshair;';

    host.innerHTML = '';
    host.appendChild(canvas);

    const ctx     = canvas.getContext('2d');
    const cellW   = (canvasW - LABEL_W) / timestamps.length;

    // draw cells
    matrix.forEach((row, ri) => {
        const y = ri * CELL_H;
        row.forEach((val, ci) => {
            const raw = val ?? 0;
            const t   = diverging
                ? (raw - minV) / range            // 0→1 over full range
                : (raw - minV) / range;

            const color = diverging
                ? _colorDiverging(t, tc)
                : _colorSequential(t, tc);

            ctx.fillStyle = color;
            ctx.fillRect(LABEL_W + ci * cellW, y, cellW - 1, CELL_H - 1);
        });

        // strike label
        ctx.fillStyle = tc.textDim;
        ctx.font       = '9px monospace';
        ctx.textAlign  = 'right';
        ctx.fillText(String(strikes[ri]), LABEL_W - 4, y + CELL_H - 6);
    });

    // time labels row at bottom
    const labelY = strikes.length * CELL_H + 14;
    ctx.fillStyle = tc.textDim;
    ctx.font      = '8px monospace';
    ctx.textAlign = 'center';
    const step = Math.max(1, Math.floor(timestamps.length / 20));
    timestamps.forEach((ts, ci) => {
        if (ci % step === 0) {
            ctx.fillText(ts, LABEL_W + ci * cellW + cellW / 2, labelY);
        }
    });

    // tooltip
    canvas.addEventListener('mousemove', e => {
        const rect = canvas.getBoundingClientRect();
        const scaleX = canvas.width  / rect.width;
        const scaleY = canvas.height / rect.height;
        const cx = (e.clientX - rect.left) * scaleX;
        const cy = (e.clientY - rect.top)  * scaleY;
        const ci = Math.floor((cx - LABEL_W) / cellW);
        const ri = Math.floor(cy / CELL_H);
        if (ci >= 0 && ci < timestamps.length && ri >= 0 && ri < strikes.length) {
            const val = matrix[ri]?.[ci] ?? null;
            _tooltip.style.display = 'block';
            _tooltip.style.left    = (e.clientX + 12) + 'px';
            _tooltip.style.top     = (e.clientY - 20) + 'px';
            _tooltip.textContent   = `${strikes[ri]} @ ${timestamps[ci]}: ${fmt(val)}`;
        } else {
            _tooltip.style.display = 'none';
        }
    });
    canvas.addEventListener('mouseleave', () => { _tooltip.style.display = 'none'; });

    canvas.addEventListener('click', e => {
        const rect = canvas.getBoundingClientRect();
        const scaleX = canvas.width  / rect.width;
        const scaleY = canvas.height / rect.height;
        const cx = (e.clientX - rect.left) * scaleX;
        const cy = (e.clientY - rect.top)  * scaleY;
        const ci = Math.floor((cx - LABEL_W) / cellW);
        const ri = Math.floor(cy / CELL_H);
        if (ci >= 0 && ci < timestamps.length && ri >= 0 && ri < strikes.length) {
            window.dispatchEvent(new CustomEvent('ochain:strikeDrill', {
                detail: { strike: strikes[ri], ts: timestamps[ci] },
            }));
        }
    });
}
