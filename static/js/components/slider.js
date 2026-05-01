import * as state from '../core/state.js';

let _playTimer = null;
let _pendingLoad = null;

export function build() {
    const snaps = state.get('snapshots') || [];
    const n = snaps.length;
    document.getElementById('snapCount').textContent = n + ' snapshots';

    if (!n) { _clearTimeline(); _setSelection(0, 0); return; }

    state.set('fromIdx', 0);
    state.set('toIdx', n - 1);
    _drawTimeline(snaps);
    _updateThumbs();
    _updateLabels();
}

export function update() {
    _updateThumbs();
    _updateLabels();
    _drawTimeline(state.get('snapshots') || []);
}

export function init() {
    _initDrag('thumbFrom', 'fromIdx');
    _initDrag('thumbTo',   'toIdx');
    document.getElementById('playBtn')?.addEventListener('click', _togglePlay);
    document.getElementById('resetBaseBtn')?.addEventListener('click', () => {
        state.set('fromIdx', 0);
        update();
        _scheduleLoad();
    });
}

function _initDrag(id, key) {
    const thumb = document.getElementById(id);
    const slider = document.getElementById('rangeSlider');
    if (!thumb || !slider) return;
    let dragging = false;

    const getIdx = clientX => {
        const rect = slider.getBoundingClientRect();
        const pct = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
        const snaps = state.get('snapshots') || [];
        return Math.round(pct * Math.max(0, snaps.length - 1));
    };

    const onMove = e => {
        if (!dragging) return;
        const x = e.touches ? e.touches[0].clientX : e.clientX;
        const idx = getIdx(x);
        const snaps = state.get('snapshots') || [];
        const n = snaps.length;
        if (key === 'fromIdx') {
            state.set('fromIdx', Math.min(idx, state.get('toIdx')));
        } else {
            state.set('toIdx', Math.max(idx, state.get('fromIdx')));
        }
        update();
    };

    const onUp = () => {
        if (!dragging) return;
        dragging = false;
        thumb.classList.remove('active');
        _scheduleLoad();
    };

    thumb.addEventListener('mousedown', e => { e.preventDefault(); dragging = true; thumb.classList.add('active'); });
    thumb.addEventListener('touchstart', e => { dragging = true; thumb.classList.add('active'); }, { passive: true });
    document.addEventListener('mousemove', onMove);
    document.addEventListener('touchmove', onMove, { passive: true });
    document.addEventListener('mouseup', onUp);
    document.addEventListener('touchend', onUp);

    slider.addEventListener('click', e => {
        if (e.target === thumb) return;
        const idx = getIdx(e.clientX);
        const snaps = state.get('snapshots') || [];
        const mid = Math.round((state.get('fromIdx') + state.get('toIdx')) / 2);
        if (key === 'fromIdx' && idx < state.get('toIdx')) {
            state.set('fromIdx', idx);
        } else if (key === 'toIdx' && idx > state.get('fromIdx')) {
            state.set('toIdx', idx);
        }
        update();
        _scheduleLoad();
    });
}

function _scheduleLoad() {
    clearTimeout(_pendingLoad);
    _pendingLoad = setTimeout(() => state.set('_sliderChanged', Date.now()), 80);
}

function _togglePlay() {
    const btn = document.getElementById('playBtn');
    if (_playTimer) {
        clearInterval(_playTimer);
        _playTimer = null;
        if (btn) { btn.textContent = '▶ Play'; btn.classList.remove('playing'); }
        return;
    }
    if (btn) { btn.textContent = '⏸ Pause'; btn.classList.add('playing'); }
    _playTimer = setInterval(() => {
        const snaps = state.get('snapshots') || [];
        const toIdx = state.get('toIdx');
        if (toIdx >= snaps.length - 1) {
            clearInterval(_playTimer); _playTimer = null;
            if (btn) { btn.textContent = '▶ Play'; btn.classList.remove('playing'); }
            return;
        }
        state.set('toIdx', toIdx + 1);
        update();
        state.set('_sliderChanged', Date.now());
    }, 1500);
}

export function stopPlay() {
    if (_playTimer) { clearInterval(_playTimer); _playTimer = null; }
    const btn = document.getElementById('playBtn');
    if (btn) { btn.textContent = '▶ Play'; btn.classList.remove('playing'); }
}

function _updateThumbs() {
    const snaps = state.get('snapshots') || [];
    const n = snaps.length;
    const from = state.get('fromIdx') || 0;
    const to   = state.get('toIdx') ?? (n - 1);
    const pctFrom = n > 1 ? (from / (n - 1)) * 100 : 0;
    const pctTo   = n > 1 ? (to   / (n - 1)) * 100 : 100;
    const tF = document.getElementById('thumbFrom');
    const tT = document.getElementById('thumbTo');
    const sel = document.getElementById('rangeSelected');
    if (tF)  tF.style.left  = pctFrom + '%';
    if (tT)  tT.style.left  = pctTo   + '%';
    if (sel) { sel.style.left = pctFrom + '%'; sel.style.width = (pctTo - pctFrom) + '%'; }
}

function _updateLabels() {
    const snaps = state.get('snapshots') || [];
    const from  = state.get('fromIdx') || 0;
    const to    = state.get('toIdx') ?? (snaps.length - 1);
    const fmtTs = ts => ts ? ts.replace('T', ' ').slice(0, 16) : '—';
    const fEl = document.getElementById('fromLabel');
    const tEl = document.getElementById('toLabel');
    const bi  = document.getElementById('baseIndicator');
    if (fEl) fEl.textContent = fmtTs(snaps[from]?.ts);
    if (tEl) tEl.textContent = fmtTs(snaps[to]?.ts);
    if (bi)  bi.classList.toggle('hidden', from === 0);
}

function _drawTimeline(snaps) {
    const container = document.getElementById('rangeTimeline');
    if (!container) return;
    container.innerHTML = '';
    const n = snaps.length;
    if (!n) return;
    const from = state.get('fromIdx') || 0;
    const to   = state.get('toIdx') ?? (n - 1);
    const step = Math.max(1, Math.floor(n / 40));
    for (let i = 0; i < n; i += step) {
        const snap = snaps[i];
        const pct = n > 1 ? (i / (n - 1)) * 100 : 0;
        const tick = document.createElement('div');
        tick.className = 'tl-tick'
            + (i === 0 ? ' base-tick' : '')
            + (i >= from && i <= to ? ' in-range' : '');
        tick.style.left = pct + '%';
        const ts = snap?.ts || '';
        const timeStr = ts ? ts.replace('T', ' ').slice(11, 16) : '';
        tick.innerHTML = `<span class="dot"></span><span class="time">${timeStr}</span>`;
        container.appendChild(tick);
    }
}

function _clearTimeline() {
    const c = document.getElementById('rangeTimeline');
    if (c) c.innerHTML = '';
    const sel = document.getElementById('rangeSelected');
    if (sel) { sel.style.left = '0'; sel.style.width = '100%'; }
}

function _setSelection(from, to) {
    const tF = document.getElementById('thumbFrom');
    const tT = document.getElementById('thumbTo');
    if (tF) tF.style.left = '0%';
    if (tT) tT.style.left = '100%';
}
