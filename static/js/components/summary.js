import { fmt, fmtInt } from '../utils.js';

export function updateSummary(data) {
    const s = data?.summary || {};
    const pcr = s.pcr || {};
    const atm = s.atm || {};
    const sr  = s.support_resistance || {};
    const em  = s.expected_move || {};
    const gex = s.gex || {};

    _set('scCeOI',      fmtInt(pcr.total_ce_oi));
    _set('scPeOI',      fmtInt(pcr.total_pe_oi));
    _set('scPcrOI',     pcr.pcr_oi != null ? Number(pcr.pcr_oi).toFixed(2) : '—');
    _set('scPcrVol',    pcr.pcr_vol != null ? Number(pcr.pcr_vol).toFixed(2) : '—');
    _set('scMaxPain',   fmtInt(atm.max_pain));
    _set('scAtmIV',     atm.atm_iv != null ? Number(atm.atm_iv).toFixed(1) + '%' : '—');
    _set('scExpMove',   em.expected_move_abs != null ? '±' + fmt(em.expected_move_abs) : '—');
    _set('scSupport',   fmtInt(sr.support));
    _set('scResistance',fmtInt(sr.resistance));
    _set('scGex',       gex.gex_regime || '—');

    const card = document.getElementById('cardPCR');
    if (card && pcr.pcr_oi != null) {
        card.classList.remove('pcr-bull','pcr-bear');
        if (pcr.pcr_oi > 1.2) card.classList.add('pcr-bull');
        else if (pcr.pcr_oi < 0.8) card.classList.add('pcr-bear');
    }

    const bias = data?.bias || s.bias;
    const banner = document.getElementById('biasBanner');
    if (banner && bias?.direction) {
        banner.className = 'bias-banner ' + bias.direction.toLowerCase();
        banner.textContent = bias.direction + ' — ' + (bias.reason || '');
    } else if (banner) {
        banner.className = 'bias-banner hidden';
    }
}

export function clearSummary() {
    ['scCeOI','scPeOI','scPcrOI','scPcrVol','scMaxPain','scAtmIV','scExpMove','scSupport','scResistance','scGex']
        .forEach(id => _set(id, '—'));
    document.getElementById('cardPCR')?.classList.remove('pcr-bull','pcr-bear');
    const b = document.getElementById('biasBanner');
    if (b) b.className = 'bias-banner hidden';
}

function _set(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val ?? '—';
}
