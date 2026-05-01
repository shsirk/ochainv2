const _data = {};
const _subs = {};
const PERSIST = new Set(['symbol','expiry','date','activeTab','timeframe']);

export function get(key) { return _data[key]; }

export function set(key, value) {
    const old = _data[key];
    _data[key] = value;
    if (_subs[key]) _subs[key].forEach(cb => { try { cb(value, old); } catch {} });
    if (PERSIST.has(key)) {
        try { localStorage.setItem('ochain.' + key, JSON.stringify(value)); } catch {}
    }
}

export function subscribe(key, cb) {
    if (!_subs[key]) _subs[key] = [];
    _subs[key].push(cb);
    return () => { if (_subs[key]) _subs[key] = _subs[key].filter(f => f !== cb); };
}

export function restore() {
    PERSIST.forEach(key => {
        try {
            const raw = localStorage.getItem('ochain.' + key);
            if (raw !== null) _data[key] = JSON.parse(raw);
        } catch {}
    });
    if (!_data.timeframe) _data.timeframe = '1m';
    if (!_data.activeTab) _data.activeTab = 'chain';
    _data.fromIdx = 0;
    _data.toIdx = -1;
    _data.snapshots = [];
    _data.chainData = null;
    _data.strikeDrillTarget = null;
    _data.liveMode = false;
}
