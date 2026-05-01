import * as state    from './core/state.js';
import { apiFetch, snapshotsUrl } from './core/api.js';
import { ws }         from './core/ws.js';
import * as header    from './components/header.js';
import * as slider    from './components/slider.js';
import { updateSummary, clearSummary } from './components/summary.js';
import { push as pushToast } from './components/alert-toast.js';
import { apiFetch as _af, analyzeUrl } from './core/api.js';

import * as tabChain    from './tabs/chain.js';
import * as tabFlow     from './tabs/flow.js';
import * as tabHeatmap  from './tabs/heatmap.js';
import * as tabVolume   from './tabs/volume.js';
import * as tabStrike   from './tabs/strike.js';
import * as tabIV       from './tabs/iv.js';
import * as tabExpiry   from './tabs/expiry.js';
import * as tabStrategy from './tabs/strategy.js';
import * as tabScalper  from './tabs/scalper.js';
import * as tabSignals  from './tabs/signals.js';

const TABS = {
    chain:    tabChain,
    flow:     tabFlow,
    heatmap:  tabHeatmap,
    volume:   tabVolume,
    strike:   tabStrike,
    iv:       tabIV,
    expiry:   tabExpiry,
    strategy: tabStrategy,
    scalper:  tabScalper,
    signals:  tabSignals,
};

const _inited = new Set();
let _chainAbort = null;

/* ── Boot ──────────────────────────────────────────────────────────────── */
async function boot() {
    state.restore();
    slider.init();
    await header.init();

    // Tab nav wiring
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });

    // Cascade: header ready → load snapshots
    state.subscribe('_headersReady', async () => {
        await loadSnapshots();
    });

    // Cascade: control changed → reload snapshots
    state.subscribe('_controlChanged', async () => {
        state.set('chainData', null);
        await loadSnapshots();
    });

    // Slider moved → reload active tab
    state.subscribe('_sliderChanged', () => {
        state.set('chainData', null);
        loadActiveTab();
        _refreshSummary();
    });

    // Theme changed → reload active tab (charts need recolor)
    state.subscribe('_themeChanged', () => loadActiveTab());

    // Live mode toggle
    state.subscribe('liveMode', live => {
        const sym = state.get('symbol');
        if (live && sym) {
            ws.connect(sym);
        } else {
            ws.disconnect();
        }
    });

    // WS new snapshot → append + advance if live mode
    ws.onSnapshot(evt => {
        const snaps = state.get('snapshots') || [];
        if (!snaps.find(s => s.id === evt.snapshot_id)) {
            snaps.push({ id: evt.snapshot_id, ts: evt.ts });
            state.set('snapshots', snaps);
            document.getElementById('snapCount').textContent = snaps.length + ' snapshots';
            slider.update();
        }
        if (state.get('liveMode')) {
            state.set('toIdx', snaps.length - 1);
            slider.update();
            state.set('chainData', null);
            loadActiveTab();
            _refreshSummary();
        }
    });

    // Strike drill cross-tab navigation
    window.addEventListener('ochain:strikeDrill', e => {
        state.set('strikeDrillTarget', e.detail.strike || e.detail);
        switchTab('strike');
    });

    // Activate the stored tab
    const savedTab = state.get('activeTab') || 'chain';
    switchTab(savedTab);
}

/* ── Tab management ──────────────────────────────────────────────────────── */
function switchTab(name) {
    state.set('activeTab', name);
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === name));
    document.querySelectorAll('.tab-content').forEach(s => s.classList.toggle('active', s.id === 'tab-' + name));

    const mod = TABS[name];
    const container = document.getElementById('tab-' + name);
    if (!mod || !container) return;

    if (!_inited.has(name)) {
        mod.init(container);
        _inited.add(name);
    }

    if (name !== 'strategy') {
        mod.load(container, _stateSnap());
    }
}

function loadActiveTab() {
    const name = state.get('activeTab') || 'chain';
    const mod  = TABS[name];
    const container = document.getElementById('tab-' + name);
    if (!mod || !container || name === 'strategy') return;
    if (!_inited.has(name)) { mod.init(container); _inited.add(name); }
    mod.load(container, _stateSnap());
}

/* ── Snapshot loading ───────────────────────────────────────────────────── */
async function loadSnapshots() {
    const s = state.get('symbol');
    const d = state.get('date');
    const e = state.get('expiry');
    if (!s || !d || !e) return;

    slider.stopPlay?.();
    let snaps = [];
    try { snaps = await apiFetch(snapshotsUrl({ symbol: s, date: d, expiry: e, timeframe: state.get('timeframe') || '1m' })); }
    catch {}

    state.set('fromIdx', 0);
    state.set('toIdx', snaps.length > 0 ? snaps.length - 1 : 0);
    state.set('snapshots', snaps);
    slider.build();

    if (snaps.length) {
        await _refreshSummary();
        loadActiveTab();
    } else {
        clearSummary();
    }
}

/* ── Summary refresh ────────────────────────────────────────────────────── */
async function _refreshSummary() {
    const cached = state.get('chainData');
    if (cached) { updateSummary(cached); return; }

    if (_chainAbort) _chainAbort.abort();
    _chainAbort = new AbortController();
    try {
        const data = await _af(analyzeUrl(_stateSnap()), _chainAbort.signal);
        state.set('chainData', data);
        window._lastChainDataForExport = data;
        updateSummary(data);
    } catch (err) {
        if (err.name !== 'AbortError') clearSummary();
    }
}

/* ── Helpers ────────────────────────────────────────────────────────────── */
function _stateSnap() {
    return {
        symbol:    state.get('symbol'),
        expiry:    state.get('expiry'),
        date:      state.get('date'),
        fromIdx:   state.get('fromIdx') ?? 0,
        toIdx:     state.get('toIdx')   ?? -1,
        timeframe: state.get('timeframe') || '1m',
    };
}

boot();
