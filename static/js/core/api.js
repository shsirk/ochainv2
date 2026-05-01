export class ApiError extends Error {
    constructor(status, message) {
        super(message);
        this.name = 'ApiError';
        this.status = status;
    }
}

async function _doFetch(url, opts) {
    let resp;
    try {
        resp = await fetch(url, opts);
    } catch (err) {
        if (err.name === 'AbortError') throw err;
        throw new ApiError(0, 'Network error');
    }
    if (!resp.ok) {
        let msg = `HTTP ${resp.status}`;
        try { const j = await resp.json(); msg = j.detail || j.message || msg; } catch {}
        throw new ApiError(resp.status, msg);
    }
    try { return await resp.json(); }
    catch { throw new ApiError(resp.status, 'Invalid JSON'); }
}

export function apiFetch(url, signal) {
    return _doFetch(url, signal ? { signal } : {});
}

export function apiPost(url, body, signal) {
    return _doFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        ...(signal ? { signal } : {}),
    });
}

function qs(s) {
    const p = new URLSearchParams();
    p.set('date', s.date || '');
    p.set('expiry', s.expiry || '');
    p.set('from_idx', s.fromIdx ?? 0);
    p.set('to_idx', s.toIdx ?? -1);
    p.set('tf', s.timeframe || '1m');
    return p.toString();
}

export const analyzeUrl    = s => `/api/analyze/${s.symbol}?${qs(s)}`;
export const snapshotsUrl  = s => `/api/snapshots/${s.symbol}?date=${s.date}&expiry=${s.expiry}&tf=${s.timeframe || '1m'}`;
export const heatmapUrl    = (s, metric) => `/api/heatmap/${s.symbol}?${qs(s)}&metric=${metric}`;
export const gexUrl        = s => `/api/gex/${s.symbol}?${qs(s)}`;
export const scalperUrl    = (s, strat) => `/api/scalper/${s.symbol}?${qs(s)}&strategy=${strat || 'naked_buyer'}`;
export const strikeUrl     = (s, strike) => `/api/strike/${s.symbol}/${strike}?date=${s.date}&expiry=${s.expiry}&tf=${s.timeframe || '1m'}`;
export const ivUrl         = s => `/api/iv_surface/${s.symbol}?${qs(s)}`;
export const alertsUrl     = s => `/api/alerts/${s.symbol}?date=${s.date}&expiry=${s.expiry}&limit=50`;
export const expiriesUrl   = s => `/api/expiries/${s.symbol}?${qs(s)}`;
export const strategiesUrl = () => `/api/v2/strategies`;
