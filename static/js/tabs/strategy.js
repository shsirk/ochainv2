import { apiPost } from '../core/api.js';
import { showError, clearError } from '../components/error-state.js';
import {
    fmt, fmtInt, fmtPct, chgClass, themeColors, chartColors,
    defaultChartOpts, destroyChart, el
} from '../utils.js';

let _ctrl   = null;
let _cref   = null;
let _chart  = null;
let _legs   = [];
let _lastS  = null;

const TEMPLATES = {
    straddle: (atm) => [
        { type: 'CE', action: 'BUY', strike: atm, qty: 1, premium: 0 },
        { type: 'PE', action: 'BUY', strike: atm, qty: 1, premium: 0 },
    ],
    strangle: (atm) => [
        { type: 'CE', action: 'BUY', strike: atm + 200, qty: 1, premium: 0 },
        { type: 'PE', action: 'BUY', strike: atm - 200, qty: 1, premium: 0 },
    ],
    bull_call: (atm) => [
        { type: 'CE', action: 'BUY',  strike: atm,       qty: 1, premium: 0 },
        { type: 'CE', action: 'SELL', strike: atm + 200, qty: 1, premium: 0 },
    ],
    bear_put: (atm) => [
        { type: 'PE', action: 'BUY',  strike: atm,       qty: 1, premium: 0 },
        { type: 'PE', action: 'SELL', strike: atm - 200, qty: 1, premium: 0 },
    ],
    iron_condor: (atm) => [
        { type: 'PE', action: 'SELL', strike: atm - 200, qty: 1, premium: 0 },
        { type: 'PE', action: 'BUY',  strike: atm - 400, qty: 1, premium: 0 },
        { type: 'CE', action: 'SELL', strike: atm + 200, qty: 1, premium: 0 },
        { type: 'CE', action: 'BUY',  strike: atm + 400, qty: 1, premium: 0 },
    ],
};

export function init(container) {
    _cref = container;
    container.innerHTML = `
<div class="strategy-grid">
  <div class="strat-builder">
    <h3>Strategy Builder</h3>
    <div class="strat-templates">
      <button data-tpl="straddle">Straddle</button>
      <button data-tpl="strangle">Strangle</button>
      <button data-tpl="bull_call">Bull Call</button>
      <button data-tpl="bear_put">Bear Put</button>
      <button data-tpl="iron_condor">Iron Condor</button>
    </div>
    <div id="legRows"></div>
    <div class="strat-add-row">
      <select id="legType">
        <option value="CE">CE</option>
        <option value="PE">PE</option>
      </select>
      <select id="legAction">
        <option value="BUY">BUY</option>
        <option value="SELL">SELL</option>
      </select>
      <input id="legStrike" type="number" placeholder="Strike" style="width:80px">
      <input id="legQty"    type="number" placeholder="Qty" value="1" style="width:50px">
      <input id="legPrem"   type="number" placeholder="Premium" style="width:75px">
      <button id="addLegBtn">+ Add</button>
    </div>
    <div class="strat-whatif">
      <label>Spot <input id="stratSpot" type="number" placeholder="Spot"></label>
      <label>IV% <input id="stratIV"   type="number" value="15" placeholder="IV%"></label>
      <label>DTE <input id="stratDTE"  type="number" value="7"  placeholder="Days"></label>
      <button id="calcBtn">Calculate</button>
    </div>
  </div>
  <div>
    <div class="chart-box" style="height:280px">
      <h3>Payoff</h3>
      <div class="chart-wrap"><canvas id="stratPayoffCanvas"></canvas></div>
    </div>
    <div class="strat-summary" id="stratSummary" style="display:none"></div>
  </div>
</div>`;

    // Template buttons
    container.querySelector('.strat-templates').addEventListener('click', e => {
        const tpl = e.target.dataset?.tpl;
        if (!tpl || !TEMPLATES[tpl]) return;
        const atm = _getATM();
        _legs = TEMPLATES[tpl](atm);
        _renderLegs(container);
        if (_lastS) {
            const spotEl = container.querySelector('#stratSpot');
            if (spotEl && !spotEl.value) spotEl.value = atm;
        }
    });

    // Add leg
    container.querySelector('#addLegBtn').addEventListener('click', () => {
        const type    = container.querySelector('#legType').value;
        const action  = container.querySelector('#legAction').value;
        const strike  = Number(container.querySelector('#legStrike').value) || _getATM();
        const qty     = Number(container.querySelector('#legQty').value)    || 1;
        const premium = Number(container.querySelector('#legPrem').value)   || 0;
        _legs.push({ type, action, strike, qty, premium });
        _renderLegs(container);
    });

    // Calculate
    container.querySelector('#calcBtn').addEventListener('click', () => {
        _calculate(container);
    });
}

// load() is manual-only — no auto-trigger from slider
export async function load(container, s) {
    _lastS = s;
    const spotEl = container.querySelector('#stratSpot');
    if (spotEl && !spotEl.value && s.symbol) {
        // Pre-fill spot with a reasonable default if state has it
        // (will be overridden by user)
    }
}

function _getATM() {
    if (_lastS) {
        // Try to infer ATM from any available state — use a round number
        return 22000;
    }
    return 22000;
}

function _renderLegs(container) {
    const legRows = container.querySelector('#legRows');
    if (!legRows) return;

    if (!_legs.length) {
        legRows.innerHTML = '<div class="dim strat-empty">No legs — add via form or use a template</div>';
        return;
    }

    legRows.innerHTML = _legs.map((leg, i) => {
        const actionCls = leg.action === 'BUY' ? 'positive' : 'negative';
        return `<div class="strat-leg-row">
  <span class="leg-type ${leg.type === 'CE' ? 'ce-badge' : 'pe-badge'}">${leg.type}</span>
  <span class="leg-action ${actionCls}">${leg.action}</span>
  <span class="leg-strike">${fmtInt(leg.strike)}</span>
  <span class="leg-qty">x${leg.qty}</span>
  <span class="leg-prem">₹${fmt(leg.premium)}</span>
  <button class="leg-remove" data-idx="${i}">✕</button>
</div>`;
    }).join('');

    legRows.querySelectorAll('.leg-remove').forEach(btn => {
        btn.addEventListener('click', () => {
            const idx = Number(btn.dataset.idx);
            _legs.splice(idx, 1);
            _renderLegs(container);
        });
    });
}

async function _calculate(container) {
    if (!_legs.length) {
        showError(container.querySelector('.strat-builder'), 'Add at least one leg before calculating');
        return;
    }

    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    const sig = _ctrl.signal;

    const spot = Number(container.querySelector('#stratSpot').value) || _getATM();
    const iv   = Number(container.querySelector('#stratIV').value)   || 15;
    const dte  = Number(container.querySelector('#stratDTE').value)  || 7;

    clearError(container.querySelector('.strat-builder'));

    const calcBtn = container.querySelector('#calcBtn');
    if (calcBtn) { calcBtn.disabled = true; calcBtn.textContent = 'Calculating…'; }

    try {
        const result = await apiPost('/api/strategy/payoff', {
            legs: _legs,
            spot,
            iv_pct: iv,
            dte,
        }, sig);

        _renderPayoffChart(container, result);
        _renderSummary(container, result);
    } catch (err) {
        if (err.name === 'AbortError') return;
        showError(container.querySelector('.strat-builder'), err.message || 'Calculation failed', () => _calculate(container));
    } finally {
        if (calcBtn) { calcBtn.disabled = false; calcBtn.textContent = 'Calculate'; }
    }
}

function _renderPayoffChart(container, result) {
    const canvas = container.querySelector('#stratPayoffCanvas');
    if (!canvas) return;
    _chart = destroyChart(_chart);

    const payoff = result.payoff || [];
    if (!payoff.length) {
        canvas.parentElement.innerHTML = '<div class="dim">No payoff data</div>';
        return;
    }

    const tc     = themeColors();
    const labels = payoff.map(p => fmtInt(p.spot));
    const values = payoff.map(p => p.pnl);

    // Split into green (profit) / red (loss) segment colors
    const pointColors = values.map(v => v >= 0 ? tc.green : tc.red);

    _chart = new Chart(canvas, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'P&L',
                data: values,
                borderColor: tc.green,
                backgroundColor: (ctx) => {
                    // gradient fill
                    const chart = ctx.chart;
                    const { ctx: c2d, chartArea } = chart;
                    if (!chartArea) return 'transparent';
                    const grad = c2d.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
                    grad.addColorStop(0, tc.green + '55');
                    grad.addColorStop(0.5, 'transparent');
                    grad.addColorStop(1, tc.red + '55');
                    return grad;
                },
                fill: true,
                tension: 0.1,
                pointRadius: 0,
                segment: {
                    borderColor: ctx => ctx.p0.parsed.y >= 0 && ctx.p1.parsed.y >= 0 ? tc.green
                                      : ctx.p0.parsed.y < 0  && ctx.p1.parsed.y < 0  ? tc.red
                                      : tc.textDim,
                },
            }],
        },
        options: {
            ...defaultChartOpts(),
            plugins: {
                legend: { display: false },
                annotation: undefined,
                tooltip: { callbacks: { label: ctx => `P&L: ${fmt(ctx.raw)}` } },
            },
            scales: {
                x: { ticks: { color: tc.textDim, font: { size: 8 }, maxRotation: 45 }, grid: { color: tc.grid } },
                y: {
                    ticks: { color: tc.textDim, font: { size: 9 }, callback: v => fmt(v) },
                    grid: { color: tc.grid },
                    afterDataLimits: axis => {
                        // draw zero line emphasis handled by grid
                    },
                },
            },
        },
    });
}

function _renderSummary(container, result) {
    const summaryEl = container.querySelector('#stratSummary');
    if (!summaryEl) return;

    summaryEl.style.display = '';
    const g = result.greeks || {};
    const be = (result.breakevens || []).map(v => fmtInt(v)).join(', ') || '—';

    const mpCls = chgClass(result.max_profit ?? 0);
    const mlCls = chgClass(-(result.max_loss  ?? 0));

    summaryEl.innerHTML = `
<div class="strat-metrics">
  <div class="strat-metric"><span>Max Profit</span><strong class="positive">${result.max_profit === Infinity ? '∞' : fmt(result.max_profit)}</strong></div>
  <div class="strat-metric"><span>Max Loss</span><strong class="negative">${result.max_loss === Infinity ? '∞' : fmt(result.max_loss)}</strong></div>
  <div class="strat-metric"><span>Net Premium</span><strong class="${chgClass(result.net_premium)}">${fmt(result.net_premium)}</strong></div>
  <div class="strat-metric"><span>Risk/Reward</span><strong>${result.risk_reward != null ? Number(result.risk_reward).toFixed(2) : '—'}</strong></div>
  <div class="strat-metric"><span>Breakevens</span><strong>${be}</strong></div>
  <div class="strat-metric"><span>POP</span><strong>${result.pop != null ? fmtPct(result.pop * 100) : '—'}</strong></div>
</div>
<div class="strat-greeks">
  <span class="greek-item">Δ <strong>${g.delta != null ? Number(g.delta).toFixed(3) : '—'}</strong></span>
  <span class="greek-item">Γ <strong>${g.gamma != null ? Number(g.gamma).toFixed(4) : '—'}</strong></span>
  <span class="greek-item">Θ <strong class="negative">${g.theta != null ? Number(g.theta).toFixed(3) : '—'}</strong></span>
  <span class="greek-item">V <strong>${g.vega != null ? Number(g.vega).toFixed(3) : '—'}</strong></span>
</div>`;
}
