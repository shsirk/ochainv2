import { apiFetch, analyzeUrl } from '../core/api.js';
import { showSkeletonCharts, hideSkeleton } from '../components/skeleton.js';
import { showError, clearError } from '../components/error-state.js';
import { fmt, fmtInt, fmtPct, chgClass, buildupClass, themeColors, chartColors, defaultChartOpts, destroyChart } from '../utils.js';

let _container = null;
let _ctrl = null;
let _chartOI = null, _chartChange = null, _chartIV = null, _chartStraddle = null;

export function init(container) {
    _container = container;
    container.innerHTML = `
<div class="charts-row" id="chainCharts">
  <div class="chart-box"><h3>OI Distribution</h3><div class="chart-wrap"><canvas id="chainOICanvas"></canvas></div></div>
  <div class="chart-box"><h3>OI Change</h3><div class="chart-wrap"><canvas id="chainChangeCanvas"></canvas></div></div>
  <div class="chart-box"><h3>IV Skew</h3><div class="chart-wrap"><canvas id="chainIVCanvas"></canvas></div></div>
</div>
<div class="chain-section">
  <div class="chain-section-header">
    <h2>Option Chain</h2>
    <a class="export-btn" id="chainExportBtn" href="#" download>&#8615; CSV</a>
  </div>
  <div class="chain-table-wrap">
    <table id="chainTable">
      <thead>
        <tr>
          <th colspan="7" class="ce-header">CALLS</th>
          <th class="strike-header">Strike</th>
          <th colspan="7" class="pe-header">PUTS</th>
        </tr>
        <tr>
          <th class="ce-header">Buildup</th><th class="ce-header">OI</th><th class="ce-header">&#916;OI</th>
          <th class="ce-header">Vol</th><th class="ce-header">IV</th><th class="ce-header">LTP</th><th class="ce-header">&#916;LTP</th>
          <th class="strike-header">&#9733;</th>
          <th class="pe-header">&#916;LTP</th><th class="pe-header">LTP</th><th class="pe-header">IV</th>
          <th class="pe-header">Vol</th><th class="pe-header">&#916;OI</th><th class="pe-header">OI</th><th class="pe-header">Buildup</th>
        </tr>
      </thead>
      <tbody id="chainBody"></tbody>
    </table>
  </div>
</div>
<div class="straddle-section">
  <h2>Straddle Premium</h2>
  <div class="chart-box full-width"><div class="chart-wrap"><canvas id="straddleCanvas"></canvas></div></div>
</div>`;

    container.querySelector('#chainExportBtn')?.addEventListener('click', e => {
        e.preventDefault();
        _downloadCSV();
    });
}

export async function load(container, s) {
    if (!container.querySelector('#chainBody')) init(container);
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    clearError(container);

    const charts = container.querySelector('#chainCharts');
    showSkeletonCharts(charts, 3);

    try {
        const data = await apiFetch(analyzeUrl(s), _ctrl.signal);
        hideSkeleton(charts);

        window.dispatchEvent(new CustomEvent('ochain:chainData', { detail: data }));

        _renderCharts(container, data);
        _renderTable(container, data);
    } catch (err) {
        if (err.name === 'AbortError') return;
        hideSkeleton(charts);
        showError(container, err.message, () => load(container, s));
    }
}

function _renderCharts(container, data) {
    const strikes  = (data.strikes || []);
    const atm      = data.summary?.atm?.atm_strike;
    const t        = themeColors();
    const c        = chartColors(0.75);
    const baseOpts = defaultChartOpts();

    const labels   = strikes.map(r => r.strike);
    const ceOI     = strikes.map(r => r.CE_openInterest || r.ce_oi || 0);
    const peOI     = strikes.map(r => r.PE_openInterest || r.pe_oi || 0);
    const ceChg    = strikes.map(r => r.CE_changeinOpenInterest || r.ce_oi_chg || 0);
    const peChg    = strikes.map(r => r.PE_changeinOpenInterest || r.pe_oi_chg || 0);
    const ceIV     = strikes.map(r => r.CE_impliedVolatility || r.ce_iv || null);
    const peIV     = strikes.map(r => r.PE_impliedVolatility || r.pe_iv || null);
    const cePrem   = strikes.map(r => r.CE_lastPrice || r.ce_ltp || 0);
    const pePrem   = strikes.map(r => r.PE_lastPrice || r.pe_ltp || 0);
    const straddle = strikes.map((r, i) => (cePrem[i] || 0) + (pePrem[i] || 0));

    _chartOI = destroyChart(_chartOI);
    _chartChange = destroyChart(_chartChange);
    _chartIV = destroyChart(_chartIV);
    _chartStraddle = destroyChart(_chartStraddle);

    const mkBar = (id, ds) => {
        const cv = container.querySelector('#' + id);
        if (!cv) return null;
        return new Chart(cv, { type: 'bar', data: { labels, datasets: ds }, options: { ...baseOpts, plugins: { ...baseOpts.plugins, legend: { ...baseOpts.plugins.legend, position: 'top' } }, scales: { x: { ...baseOpts.scales.x, stacked: false }, y: { ...baseOpts.scales.y } } } });
    };

    _chartOI = mkBar('chainOICanvas', [
        { label: 'CE OI', data: ceOI, backgroundColor: c.ce },
        { label: 'PE OI', data: peOI, backgroundColor: c.pe },
    ]);
    _chartChange = mkBar('chainChangeCanvas', [
        { label: 'CE ΔOI', data: ceChg, backgroundColor: ceChg.map(v => v >= 0 ? c.green : c.red) },
        { label: 'PE ΔOI', data: peChg, backgroundColor: peChg.map(v => v >= 0 ? c.green : c.red) },
    ]);

    const ivCv = container.querySelector('#chainIVCanvas');
    if (ivCv) {
        _chartIV = new Chart(ivCv, {
            type: 'line',
            data: { labels, datasets: [
                { label: 'CE IV', data: ceIV, borderColor: c.ce, backgroundColor: 'transparent', tension: 0.3, pointRadius: 2 },
                { label: 'PE IV', data: peIV, borderColor: c.pe, backgroundColor: 'transparent', tension: 0.3, pointRadius: 2 },
            ]},
            options: baseOpts,
        });
    }

    const sCv = container.querySelector('#straddleCanvas');
    if (sCv) {
        _chartStraddle = new Chart(sCv, {
            type: 'bar',
            data: { labels, datasets: [{ label: 'Straddle Premium', data: straddle, backgroundColor: c.purple }] },
            options: { ...baseOpts, plugins: { ...baseOpts.plugins, legend: { display: false } } },
        });
    }
}

function _renderTable(container, data) {
    const tbody = container.querySelector('#chainBody');
    if (!tbody) return;
    const strikes   = data.strikes || [];
    const deltas    = data.deltas  || null;
    const atm       = data.summary?.atm?.atm_strike;
    const maxPain   = data.summary?.atm?.max_pain;
    const deltaMap  = {};
    if (deltas) deltas.forEach(r => { deltaMap[r.strike] = r; });

    const maxOI = Math.max(...strikes.map(r => Math.max(r.CE_openInterest || r.ce_oi || 0, r.PE_openInterest || r.pe_oi || 0)), 1);

    tbody.innerHTML = '';
    strikes.forEach(row => {
        const strike  = row.strike;
        const prev    = deltaMap[strike];
        const ceOI    = row.CE_openInterest   || row.ce_oi   || 0;
        const peOI    = row.PE_openInterest   || row.pe_oi   || 0;
        const ceChg   = row.CE_changeinOpenInterest || row.ce_oi_chg || (prev ? (row.CE_openInterest - prev.CE_openInterest) : null);
        const peChg   = row.PE_changeinOpenInterest || row.pe_oi_chg || (prev ? (row.PE_openInterest - prev.PE_openInterest) : null);
        const ceVol   = row.CE_totalTradedVolume || row.ce_vol || 0;
        const peVol   = row.PE_totalTradedVolume || row.pe_vol || 0;
        const ceIV    = row.CE_impliedVolatility || row.ce_iv;
        const peIV    = row.PE_impliedVolatility || row.pe_iv;
        const ceLTP   = row.CE_lastPrice || row.ce_ltp;
        const peLTP   = row.PE_lastPrice || row.pe_ltp;
        const ceLTPChg = row.CE_change   || row.ce_ltp_chg;
        const peLTPChg = row.PE_change   || row.pe_ltp_chg;
        const ceBU    = row.ce_buildup   || row.CE_buildup   || '';
        const peBU    = row.pe_buildup   || row.PE_buildup   || '';
        const ceBar   = Math.round((ceOI / maxOI) * 100);
        const peBar   = Math.round((peOI / maxOI) * 100);
        const isATM   = atm && Math.abs(strike - atm) < 1;
        const isMaxP  = maxPain && Math.abs(strike - maxPain) < 1;

        const tr = document.createElement('tr');
        tr.dataset.strike = strike;
        if (isATM)  tr.classList.add('atm-row');
        if (isMaxP) tr.classList.add('max-pain-row');

        tr.innerHTML = `
<td class="buildup-cell ${buildupClass(ceBU)}">${ceBU || '—'}</td>
<td class="oi-bar-cell"><div class="bar bar-ce" style="width:${ceBar}%"></div><span class="val">${fmt(ceOI)}</span></td>
<td class="${chgClass(ceChg)}">${ceChg != null ? fmt(ceChg) : '—'}</td>
<td>${fmt(ceVol)}</td>
<td>${ceIV != null ? ceIV.toFixed(1) + '%' : '—'}</td>
<td>${ceLTP != null ? fmtInt(ceLTP) : '—'}</td>
<td class="${chgClass(ceLTPChg)}">${ceLTPChg != null ? fmt(ceLTPChg) : '—'}</td>
<td class="strike-col">${fmtInt(strike)}</td>
<td class="${chgClass(peLTPChg)}">${peLTPChg != null ? fmt(peLTPChg) : '—'}</td>
<td>${peLTP != null ? fmtInt(peLTP) : '—'}</td>
<td>${peIV != null ? peIV.toFixed(1) + '%' : '—'}</td>
<td>${fmt(peVol)}</td>
<td class="${chgClass(peChg)}">${peChg != null ? fmt(peChg) : '—'}</td>
<td class="oi-bar-cell"><div class="bar bar-pe" style="width:${peBar}%"></div><span class="val">${fmt(peOI)}</span></td>
<td class="buildup-cell ${buildupClass(peBU)}">${peBU || '—'}</td>`;

        tr.addEventListener('click', () => {
            window.dispatchEvent(new CustomEvent('ochain:strikeDrill', { detail: { strike } }));
        });
        tbody.appendChild(tr);
    });
}

let _lastData = null;
function _downloadCSV() {
    window.dispatchEvent(new CustomEvent('ochain:requestChainData', {}));
    const data = window._lastChainDataForExport;
    if (!data?.strikes?.length) return;
    const rows = data.strikes;
    const header = ['Strike','CE_OI','CE_OI_Chg','CE_Vol','CE_IV','CE_LTP','PE_LTP','PE_IV','PE_Vol','PE_OI_Chg','PE_OI'];
    const lines = [header.join(',')];
    rows.forEach(r => {
        lines.push([
            r.strike,
            r.CE_openInterest || r.ce_oi || 0,
            r.CE_changeinOpenInterest || r.ce_oi_chg || 0,
            r.CE_totalTradedVolume || r.ce_vol || 0,
            r.CE_impliedVolatility || r.ce_iv || '',
            r.CE_lastPrice || r.ce_ltp || '',
            r.PE_lastPrice || r.pe_ltp || '',
            r.PE_impliedVolatility || r.pe_iv || '',
            r.PE_totalTradedVolume || r.pe_vol || 0,
            r.PE_changeinOpenInterest || r.pe_oi_chg || 0,
            r.PE_openInterest || r.pe_oi || 0,
        ].join(','));
    });
    const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `ochain_${data.symbol}_${data.trade_date}.csv`;
    a.click();
    URL.revokeObjectURL(url);
}
