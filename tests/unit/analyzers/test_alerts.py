"""Unit tests for analyzers/alerts.py"""

import pytest
import pandas as pd

from ochain_v2.analyzers.alerts import (
    AlertThresholds, IV_JUMP, OI_INITIATION, OI_SPIKE,
    VOLUME_SURGE, VOL_INITIATION, detect_alerts,
)

STRIKES = [22450.0, 22500.0, 22550.0]
SYM, EXP = "NIFTY", "2026-03-27"


def _df(ce_oi=100_000, pe_oi=80_000, ce_vol=5_000, pe_vol=4_000,
        ce_iv=15.0, pe_iv=14.0) -> pd.DataFrame:
    return pd.DataFrame({
        "strike":    STRIKES,
        "ce_oi":     [ce_oi]  * len(STRIKES),
        "pe_oi":     [pe_oi]  * len(STRIKES),
        "ce_volume": [ce_vol] * len(STRIKES),
        "pe_volume": [pe_vol] * len(STRIKES),
        "ce_iv":     [ce_iv]  * len(STRIKES),
        "pe_iv":     [pe_iv]  * len(STRIKES),
    })


class TestDetectAlerts:
    def test_no_prev_returns_empty(self) -> None:
        assert detect_alerts(_df(), None) == []

    def test_empty_curr_returns_empty(self) -> None:
        assert detect_alerts(pd.DataFrame(), _df()) == []

    def test_oi_spike_detected(self) -> None:
        curr = _df(ce_oi=150_000)   # +50%
        prev = _df(ce_oi=100_000)
        alerts = detect_alerts(curr, prev, symbol=SYM, expiry=EXP)
        types = [a["alert_type"] for a in alerts]
        assert OI_SPIKE in types
        spikes = [a for a in alerts if a["alert_type"] == OI_SPIKE and a["side"] == "CE"]
        assert len(spikes) == len(STRIKES)
        assert spikes[0]["magnitude"] == pytest.approx(50.0, rel=1e-2)

    def test_oi_spike_not_triggered_below_threshold(self) -> None:
        curr = _df(ce_oi=115_000)   # +15% < 20% threshold
        prev = _df(ce_oi=100_000)
        alerts = detect_alerts(curr, prev, symbol=SYM, expiry=EXP)
        assert not any(a["alert_type"] == OI_SPIKE for a in alerts)

    def test_oi_initiation_detected(self) -> None:
        curr = _df(ce_oi=100_000)
        prev = _df(ce_oi=0)
        alerts = detect_alerts(curr, prev, symbol=SYM, expiry=EXP)
        assert any(a["alert_type"] == OI_INITIATION for a in alerts)

    def test_volume_surge_detected(self) -> None:
        curr = _df(ce_vol=20_000)   # 4× previous
        prev = _df(ce_vol=5_000)
        alerts = detect_alerts(curr, prev, symbol=SYM, expiry=EXP)
        assert any(a["alert_type"] == VOLUME_SURGE and a["side"] == "CE" for a in alerts)

    def test_iv_jump_detected(self) -> None:
        curr = _df(ce_iv=18.0)   # +3pp
        prev = _df(ce_iv=15.0)
        alerts = detect_alerts(curr, prev, symbol=SYM, expiry=EXP)
        assert any(a["alert_type"] == IV_JUMP for a in alerts)

    def test_custom_thresholds(self) -> None:
        thr = AlertThresholds(oi_spike_pct=10.0)
        curr = _df(ce_oi=115_000)   # +15% — below default 20%, above custom 10%
        prev = _df(ce_oi=100_000)
        alerts = detect_alerts(curr, prev, thresholds=thr)
        assert any(a["alert_type"] == OI_SPIKE for a in alerts)

    def test_alert_fields_present(self) -> None:
        curr = _df(ce_oi=150_000)
        prev = _df(ce_oi=100_000)
        alerts = detect_alerts(curr, prev, symbol=SYM, expiry=EXP)
        required = {"alert_type", "symbol", "expiry", "strike", "side",
                    "detail", "magnitude", "ts"}
        for alert in alerts:
            assert required.issubset(alert.keys())

    def test_min_oi_filter_suppresses_noise(self) -> None:
        # Tiny OI should not generate OI spike alerts
        thr = AlertThresholds(min_oi_for_alerts=200_000)
        curr = _df(ce_oi=150_000)
        prev = _df(ce_oi=100_000)
        alerts = detect_alerts(curr, prev, thresholds=thr)
        assert not any(a["alert_type"] == OI_SPIKE for a in alerts)
