import * as state from '../core/state.js';
import { apiFetch } from '../core/api.js';

export async function init() {
    _initTheme();
    await _loadSymbols();
    _wireExpiry();
    _wireDate();
    _wireTf();
    _wireLiveBtn();
}

async function _loadSymbols() {
    let symbols = [];
    try { symbols = await apiFetch('/api/symbols'); } catch {}
    const container = document.getElementById('symbolPills');
    if (!container) return;
    container.innerHTML = '';

    const current = state.get('symbol');
    const first = current && symbols.includes(current) ? current : (symbols[0] || '');
    if (!state.get('symbol') && first) state.set('symbol', first);

    symbols.forEach(sym => {
        const btn = document.createElement('button');
        btn.className = 'sym-pill' + (sym === state.get('symbol') ? ' active' : '');
        btn.textContent = sym;
        btn.addEventListener('click', () => _selectSymbol(sym));
        container.appendChild(btn);
    });

    if (first) await _loadDates(first);
}

async function _selectSymbol(sym) {
    state.set('symbol', sym);
    document.querySelectorAll('.sym-pill').forEach(b => b.classList.toggle('active', b.textContent === sym));
    await _loadDates(sym);
}

async function _loadDates(sym) {
    const sel = document.getElementById('dateSelect');
    if (!sel) return;
    sel.innerHTML = '<option>Loading…</option>';
    let dates = [];
    try { dates = await apiFetch(`/api/dates/${sym}`); } catch {}
    sel.innerHTML = '';
    dates.forEach(d => {
        const o = document.createElement('option');
        o.value = d; o.textContent = d;
        sel.appendChild(o);
    });
    const saved = state.get('date');
    if (saved && dates.includes(saved)) sel.value = saved;
    else if (dates.length) { sel.value = dates[0]; state.set('date', dates[0]); }
    await _loadExpiries(sym, state.get('date'));
}

async function _loadExpiries(sym, date) {
    const sel = document.getElementById('expirySelect');
    if (!sel) return;
    sel.innerHTML = '<option>Loading…</option>';
    let expiries = [];
    try { expiries = await apiFetch(`/api/expiry_list/${sym}?date=${date}`); } catch {}
    sel.innerHTML = '';
    expiries.forEach(e => {
        const o = document.createElement('option');
        o.value = e; o.textContent = e;
        sel.appendChild(o);
    });
    const saved = state.get('expiry');
    if (saved && expiries.includes(saved)) sel.value = saved;
    else if (expiries.length) { sel.value = expiries[0]; state.set('expiry', expiries[0]); }
    state.set('_headersReady', true);
}

function _wireExpiry() {
    document.getElementById('expirySelect')?.addEventListener('change', e => {
        state.set('expiry', e.target.value);
        state.set('_controlChanged', 'expiry');
    });
}

function _wireDate() {
    document.getElementById('dateSelect')?.addEventListener('change', async e => {
        state.set('date', e.target.value);
        await _loadExpiries(state.get('symbol'), e.target.value);
        state.set('_controlChanged', 'date');
    });
}

function _wireTf() {
    const sel = document.getElementById('tfSelect');
    if (!sel) return;
    const saved = state.get('timeframe');
    if (saved) sel.value = saved;
    sel.addEventListener('change', e => {
        state.set('timeframe', e.target.value);
        state.set('_controlChanged', 'timeframe');
    });
}

function _wireLiveBtn() {
    const btn = document.getElementById('liveBtn');
    if (!btn) return;
    btn.addEventListener('click', () => {
        const next = !state.get('liveMode');
        state.set('liveMode', next);
        btn.classList.toggle('active', next);
    });
}

function _initTheme() {
    const btn = document.getElementById('themeBtn');
    if (!btn) return;
    btn.addEventListener('click', () => {
        const isLight = document.documentElement.getAttribute('data-theme') === 'light';
        if (isLight) {
            document.documentElement.removeAttribute('data-theme');
            try { localStorage.setItem('ochain.theme', '"dark"'); } catch {}
        } else {
            document.documentElement.setAttribute('data-theme', 'light');
            try { localStorage.setItem('ochain.theme', '"light"'); } catch {}
        }
        state.set('_themeChanged', Date.now());
    });
}
