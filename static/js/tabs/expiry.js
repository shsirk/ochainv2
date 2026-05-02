import { apiFetch, expiriesUrl, analyzeUrl } from '../core/api.js';
import { showSkeleton, hideSkeleton } from '../components/skeleton.js';
import { showError, clearError } from '../components/error-state.js';
import {
    fmt, fmtInt, fmtPct, chgClass, themeColors, chartColors,
    defaultChartOpts, destroyChart, el
} from '../utils.js';

let _ctrl  = null;
let _cref  = null;
let _chart = null;

export function init(container) {
    _cref = container;
    container.innerHTML = `
<div class="chart-box full-width" style="height:240px">
  <h3>OI by Expiry</h3>
  <div class="chart-wrap"><canvas id="expiryOICanvas"></canvas></div>
</div>
<div class="expiry-tables">
  <div class="expiry-panel">
    <h3>Per-Expiry Summary</h3>
    <div class="panel-table-wrap">
      <table class="panel-table" id="expiryTable">
        <thead>
          <tr>
            <th style="text-align:left">Expiry</th>
            <th>PCR OI</th>
            <th>PCR Vol</th>
            <th>Max Pain</th>
            <th>Support</th>
            <th>Resistance</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
  <div class="expiry-panel">
    <h3>Rollover Activity</h3>
    <div class="panel-table-wrap">
      <table class="panel-table" id="rolloverTable">
        <thead>
          <tr>
            <th style="text-align:left">Expiry</th>
            <th>CE Chg</th>
            <th>PE Chg</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
</div>`;
}

export async function load(container, s) {
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    const sig = _ctrl.signal;

    clearError(container);
    showSkeleton(container.querySelector('#expiryTable tbody'), 3);
    showSkeleton(container.querySelector('#rolloverTable tbody'), 3);

    try {
        const listData = await apiFetch(expiriesUrl(s), sig);
        const expiryDates = (listData.expiries || []).slice(0, 4);

        if (!expiryDates.length) {
            hideSkeleton(container.querySelector('#expiryTable tbody'));
            hideSkeleton(container.querySelector('#rolloverTable tbody'));
            _renderOIChart(container, []);
            _renderExpiryTable(container, []);
            _renderRolloverTable(container, []);
            return;
        }

        // Fetch analyze data for each expiry in parallel
        const analyzeResults = await Promise.all(
            expiryDates.map(exp => apiFetch(analyzeUrl({ ...s, expiry: exp }), sig))
        );

        hideSkeleton(container.querySelector('#expiryTable tbody'));
        hideSkeleton(container.querySelector('#rolloverTable tbody'));

        const _topStrike = (arr, oiKey) => Array.isArray(arr)
            ? (arr.reduce((m, r) => r[oiKey] > (m?.[oiKey] ?? -1) ? r : m, null)?.strike ?? null)
            : null;

        const expiries = expiryDates.map((exp, i) => {
            const d  = analyzeResults[i];
            const st = d.strikes || [];
            const sr = d.summary?.support_resistance || {};
            return {
                expiry:     exp,
                ce_oi:      st.reduce((acc, r) => acc + (r.ce_oi || 0), 0),
                pe_oi:      st.reduce((acc, r) => acc + (r.pe_oi || 0), 0),
                pcr_oi:     d.summary?.pcr?.pcr_oi,
                pcr_vol:    d.summary?.pcr?.pcr_volume ?? d.summary?.pcr?.pcr_vol,
                max_pain:   d.summary?.atm?.max_pain,
                support:    _topStrike(sr.support,    'pe_oi'),
                resistance: _topStrike(sr.resistance, 'ce_oi'),
            };
        });

        const rollover = [];
        for (let i = 0; i < expiries.length - 1; i++) {
            rollover.push({
                near_expiry: expiries[i].expiry,
                next_expiry: expiries[i + 1].expiry,
                ce_near: expiries[i].ce_oi,
                ce_next: expiries[i + 1].ce_oi,
                pe_near: expiries[i].pe_oi,
                pe_next: expiries[i + 1].pe_oi,
            });
        }

        _renderOIChart(container, expiries);
        _renderExpiryTable(container, expiries);
        _renderRolloverTable(container, rollover);
    } catch (err) {
        if (err.name === 'AbortError') return;
        hideSkeleton(container.querySelector('#expiryTable tbody'));
        hideSkeleton(container.querySelector('#rolloverTable tbody'));
        showError(container, err.message || 'Failed to load expiry data', () => load(container, s));
    }
}

function _renderOIChart(container, expiries) {
    const canvas = container.querySelector('#expiryOICanvas');
    if (!canvas) return;
    _chart = destroyChart(_chart);

    if (!expiries.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No expiry data</div>';
        return;
    }

    const tc     = themeColors();
    const cc     = chartColors();
    const labels = expiries.map(e => e.expiry);

    _chart = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'CE OI',
                    data: expiries.map(e => e.ce_oi),
                    backgroundColor: cc.ce,
                    borderColor: cc.ce,
                    borderWidth: 1,
                },
                {
                    label: 'PE OI',
                    data: expiries.map(e => e.pe_oi),
                    backgroundColor: cc.pe,
                    borderColor: cc.pe,
                    borderWidth: 1,
                },
            ],
        },
        options: {
            ...defaultChartOpts(),
            plugins: { legend: { labels: { color: tc.textDim, font: { size: 10 } } } },
            scales: {
                x: { ticks: { color: tc.textDim, font: { size: 9 }, maxRotation: 45 }, grid: { color: tc.grid } },
                y: {
                    ticks: { color: tc.textDim, font: { size: 9 }, callback: v => fmt(v) },
                    grid: { color: tc.grid },
                },
            },
        },
    });
}

function _renderExpiryTable(container, expiries) {
    const tbody = container.querySelector('#expiryTable tbody');
    if (!tbody) return;

    if (!expiries.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="dim" style="text-align:center">No data</td></tr>';
        return;
    }

    tbody.innerHTML = expiries.map(e => {
        const pcrClass = e.pcr_oi >= 1 ? 'positive' : 'negative';
        return `<tr>
  <td style="text-align:left">${e.expiry}</td>
  <td class="${pcrClass}">${e.pcr_oi != null ? Number(e.pcr_oi).toFixed(2) : '—'}</td>
  <td>${e.pcr_vol != null ? Number(e.pcr_vol).toFixed(2) : '—'}</td>
  <td>${fmtInt(e.max_pain)}</td>
  <td class="positive">${fmtInt(e.support)}</td>
  <td class="negative">${fmtInt(e.resistance)}</td>
</tr>`;
    }).join('');
}

function _renderRolloverTable(container, rollover) {
    const tbody = container.querySelector('#rolloverTable tbody');
    if (!tbody) return;

    if (!rollover.length) {
        tbody.innerHTML = '<tr><td colspan="3" class="dim" style="text-align:center">No rollover data</td></tr>';
        return;
    }

    tbody.innerHTML = rollover.map(r => {
        const ceChg = (r.ce_next ?? 0) - (r.ce_near ?? 0);
        const peChg = (r.pe_next ?? 0) - (r.pe_near ?? 0);
        return `<tr>
  <td style="text-align:left">${r.near_expiry || '—'} → ${r.next_expiry || '—'}</td>
  <td class="${chgClass(ceChg)}">${fmt(ceChg)}</td>
  <td class="${chgClass(peChg)}">${fmt(peChg)}</td>
</tr>`;
    }).join('');
}
