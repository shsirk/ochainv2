import * as state from '../core/state.js';

const MAX_TOASTS = 5;
let _queue = [];

export function push(alert) {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    if (_queue.length >= MAX_TOASTS) {
        _dismiss(_queue[0], container);
    }

    const type = (alert.alert_type || '').toLowerCase().replace(/\s+/g,'-');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <span class="toast-badge">${alert.alert_type || 'ALERT'}</span>
        <span class="toast-body">
            <span class="toast-strike">${alert.strike ?? ''} ${alert.side ?? ''}</span>
            <span class="toast-detail">${alert.detail || ''}</span>
        </span>
        <span class="toast-dismiss">&#10005;</span>`;

    toast.addEventListener('click', () => {
        if (alert.strike) state.set('strikeDrillTarget', alert.strike);
        _dismiss(toast, container);
    });
    toast.querySelector('.toast-dismiss').addEventListener('click', e => {
        e.stopPropagation();
        _dismiss(toast, container);
    });

    container.appendChild(toast);
    _queue.push(toast);

    setTimeout(() => _dismiss(toast, container), 6000);
}

function _dismiss(toast, container) {
    if (!toast.parentNode) return;
    toast.classList.add('removing');
    setTimeout(() => { toast.remove(); _queue = _queue.filter(t => t !== toast); }, 220);
}
