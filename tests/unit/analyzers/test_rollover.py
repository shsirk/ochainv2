"""Unit tests for analyzers/rollover.py"""

import pandas as pd
import pytest

from ochain_v2.analyzers.rollover import detect_rollover

STRIKES = [22450.0, 22500.0, 22550.0]


def _df(ce_oi=100_000, pe_oi=80_000) -> pd.DataFrame:
    return pd.DataFrame({
        "strike": STRIKES,
        "ce_oi":  [ce_oi] * 3,
        "pe_oi":  [pe_oi] * 3,
    })


class TestDetectRollover:
    def test_returns_all_keys(self) -> None:
        result = detect_rollover(_df(), _df())
        required = {
            "is_rolling", "near_total_oi", "far_total_oi",
            "near_oi_change", "far_oi_change", "rollover_ratio",
            "rollover_pct", "dominant_side", "interpretation",
        }
        assert required.issubset(result.keys())

    def test_no_prev_gives_none_changes(self) -> None:
        result = detect_rollover(_df(), _df())
        assert result["near_oi_change"] is None
        assert result["far_oi_change"]  is None
        assert result["is_rolling"]     is False

    def test_active_rollover_detected(self) -> None:
        # Near OI drops, far OI rises → rollover
        near_curr = _df(ce_oi=80_000, pe_oi=60_000)     # decreased
        near_prev = _df(ce_oi=100_000, pe_oi=80_000)
        far_curr  = _df(ce_oi=50_000, pe_oi=40_000)     # increased
        far_prev  = _df(ce_oi=30_000, pe_oi=25_000)
        result = detect_rollover(near_curr, far_curr, near_prev, far_prev)
        assert result["is_rolling"] is True

    def test_no_rollover_when_near_oi_growing(self) -> None:
        near_curr = _df(ce_oi=120_000)
        near_prev = _df(ce_oi=100_000)
        far_curr  = _df(ce_oi=50_000)
        far_prev  = _df(ce_oi=40_000)
        result = detect_rollover(near_curr, far_curr, near_prev, far_prev)
        assert result["is_rolling"] is False

    def test_rollover_ratio_computed(self) -> None:
        # far_total / near_total
        near = _df(ce_oi=100_000, pe_oi=100_000)     # total = 600_000
        far  = _df(ce_oi=50_000,  pe_oi=50_000)      # total = 300_000
        result = detect_rollover(near, far)
        assert result["rollover_ratio"] == pytest.approx(0.5, abs=0.01)

    def test_rollover_ratio_none_when_near_total_zero(self) -> None:
        near = _df(ce_oi=0, pe_oi=0)
        far  = _df()
        result = detect_rollover(near, far)
        assert result["rollover_ratio"] is None

    def test_rollover_pct_computed_when_rolling(self) -> None:
        near_curr = _df(ce_oi=80_000, pe_oi=60_000)
        near_prev = _df(ce_oi=100_000, pe_oi=80_000)
        far_curr  = _df(ce_oi=50_000,  pe_oi=40_000)
        far_prev  = _df(ce_oi=30_000,  pe_oi=25_000)
        result = detect_rollover(near_curr, far_curr, near_prev, far_prev)
        assert result["rollover_pct"] is not None
        assert 0 < result["rollover_pct"] <= 100

    def test_rollover_pct_none_when_not_rolling(self) -> None:
        near_curr = _df(ce_oi=120_000)
        near_prev = _df(ce_oi=100_000)
        far_curr  = _df(ce_oi=50_000)
        far_prev  = _df(ce_oi=40_000)
        result = detect_rollover(near_curr, far_curr, near_prev, far_prev)
        assert result["rollover_pct"] is None

    def test_interpretation_insufficient_data(self) -> None:
        result = detect_rollover(_df(), _df())
        assert "Insufficient" in result["interpretation"]

    def test_interpretation_active_rollover(self) -> None:
        near_curr = _df(ce_oi=80_000, pe_oi=60_000)
        near_prev = _df(ce_oi=100_000, pe_oi=80_000)
        far_curr  = _df(ce_oi=50_000,  pe_oi=40_000)
        far_prev  = _df(ce_oi=30_000,  pe_oi=25_000)
        result = detect_rollover(near_curr, far_curr, near_prev, far_prev)
        assert "rollover" in result["interpretation"].lower()

    def test_dominant_side_ce(self) -> None:
        # CE heavy near, CE heavy far
        near_curr = _df(ce_oi=50_000,  pe_oi=10_000)
        near_prev = _df(ce_oi=100_000, pe_oi=10_000)
        far_curr  = _df(ce_oi=70_000,  pe_oi=20_000)
        far_prev  = _df(ce_oi=20_000,  pe_oi=10_000)
        result = detect_rollover(near_curr, far_curr, near_prev, far_prev)
        assert result["dominant_side"] in ("CE", "both", None)

    def test_total_oi_values(self) -> None:
        near = _df(ce_oi=100_000, pe_oi=80_000)
        far  = _df(ce_oi=50_000,  pe_oi=40_000)
        result = detect_rollover(near, far)
        assert result["near_total_oi"] == (100_000 + 80_000) * 3
        assert result["far_total_oi"]  == (50_000  + 40_000) * 3

    def test_nulls_in_oi_handled(self) -> None:
        near = pd.DataFrame({
            "strike": STRIKES, "ce_oi": [None, 100_000, None], "pe_oi": [80_000, None, 80_000]
        })
        far = _df()
        result = detect_rollover(near, far)
        assert isinstance(result["near_total_oi"], int)
