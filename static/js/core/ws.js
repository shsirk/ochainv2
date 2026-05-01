class WsManager {
    constructor() {
        this._url = null;
        this._ws = null;
        this._snapshotCbs = [];
        this._alertCbs = [];
        this._delay = 1000;
        this._timer = null;
        this.isConnected = false;
    }

    connect(symbol) {
        if (this._url && this._url.endsWith('/' + symbol) && this.isConnected) return;
        this.disconnect();
        const proto = location.protocol === 'https:' ? 'wss' : 'ws';
        this._url = `${proto}://${location.host}/ws/live/${symbol}`;
        this._delay = 1000;
        this._open();
    }

    _open() {
        try { this._ws = new WebSocket(this._url); } catch { return; }
        this._ws.onopen = () => { this.isConnected = true; this._delay = 1000; };
        this._ws.onmessage = evt => {
            try {
                const d = JSON.parse(evt.data);
                this._snapshotCbs.forEach(cb => { try { cb(d); } catch {} });
            } catch {}
        };
        this._ws.onclose = () => {
            this.isConnected = false;
            if (this._url) {
                this._timer = setTimeout(() => {
                    this._delay = Math.min(this._delay * 2, 30000);
                    this._open();
                }, this._delay);
            }
        };
        this._ws.onerror = () => { try { this._ws.close(); } catch {} };
    }

    disconnect() {
        this._url = null;
        this.isConnected = false;
        clearTimeout(this._timer);
        if (this._ws) {
            this._ws.onclose = null;
            try { this._ws.close(); } catch {}
            this._ws = null;
        }
    }

    onSnapshot(cb) {
        this._snapshotCbs.push(cb);
        return () => { this._snapshotCbs = this._snapshotCbs.filter(f => f !== cb); };
    }

    onAlert(cb) {
        this._alertCbs.push(cb);
        return () => { this._alertCbs = this._alertCbs.filter(f => f !== cb); };
    }
}

export const ws = new WsManager();
