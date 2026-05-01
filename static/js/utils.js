export function fmt(n) {
    if (n == null || isNaN(n)) return '—';
    const a = Math.abs(n);
    if (a >= 1e7) return (n / 1e7).toFixed(2) + 'Cr';
    if (a >= 1e5) return (n / 1e5).toFixed(2) + 'L';
    if (a >= 1000) return (n / 1000).toFixed(1) + 'K';
    return typeof n === 'number' ? n.toFixed(2) : String(n);
}

export function fmtInt(n) {
    if (n == null || isNaN(n)) return '—';
    return Number(n).toLocaleString('en-IN');
}

export function fmtPct(n) {
    if (n == null || isNaN(n)) return '—';
    return Number(n).toFixed(2) + '%';
}

export function chgClass(v) {
    return v > 0 ? 'positive' : v < 0 ? 'negative' : '';
}

export function buildupClass(b) {
    if (!b || b === '-') return '';
    return 'buildup-' + String(b).toLowerCase().replace(/\s+/g, '-');
}

export function themeColors() {
    const s = getComputedStyle(document.documentElement);
    const g = name => s.getPropertyValue(name).trim();
    return {
        text:    g('--text')     || '#e6edf3',
        textDim: g('--text-dim') || '#8b949e',
        grid:    g('--grid-line')|| 'rgba(48,54,61,0.4)',
        surface: g('--surface')  || '#161b22',
        surface2:g('--surface2') || '#1c2333',
        border:  g('--border')   || '#30363d',
        ce:      g('--ce-accent')|| '#58a6ff',
        pe:      g('--pe-accent')|| '#f0883e',
        green:   g('--green')    || '#3fb950',
        red:     g('--red')      || '#f85149',
        blue:    g('--blue')     || '#58a6ff',
        yellow:  g('--yellow')   || '#d29922',
        purple:  g('--purple')   || '#bc8cff',
    };
}

function _rgba(hex, a) {
    const m = /^#?([0-9a-fA-F]{3,6})$/.exec(hex.trim());
    if (!m) return hex;
    let h = m[1];
    if (h.length === 3) h = h.split('').map(c => c + c).join('');
    const r = parseInt(h.slice(0, 2), 16);
    const g = parseInt(h.slice(2, 4), 16);
    const b = parseInt(h.slice(4, 6), 16);
    return `rgba(${r},${g},${b},${a})`;
}

export function chartColors(alpha = 0.8) {
    const t = themeColors();
    return {
        ce:     _rgba(t.ce, alpha),
        pe:     _rgba(t.pe, alpha),
        green:  _rgba(t.green, alpha),
        red:    _rgba(t.red, alpha),
        purple: _rgba(t.purple, alpha),
        yellow: _rgba(t.yellow, alpha),
        blue:   _rgba(t.blue, alpha),
    };
}

export function defaultChartOpts() {
    const t = themeColors();
    return {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 180 },
        plugins: { legend: { labels: { color: t.textDim, font: { size: 11 } } } },
        scales: {
            x: { ticks: { color: t.textDim, font: { size: 9 }, maxRotation: 45 }, grid: { color: t.grid } },
            y: { ticks: { color: t.textDim, font: { size: 9 } }, grid: { color: t.grid } },
        },
    };
}

export function destroyChart(c) { if (c) c.destroy(); return null; }

export function el(tag, cls, html) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (html != null) e.innerHTML = html;
    return e;
}
