"""Unit tests for analyzers/heatmap.py"""

import numpy as np
import pandas as pd
import pytest
from datetime import datetime, timezone

from ochain_v2.analyzers.heatmap import VALID_METRICS, build_heatmap_matrix


def _make_df(n_strikes=3, n_snapshots=4, metric="ce_oi") -> pd.DataFrame:
    strikes = [22450.0, 22500.0, 22550.0][:n_strikes]
    rows = []
    base_ts = datetime(2025, 3, 27, 9, 15, tzinfo=timezone.utc)
    for snap_i in range(n_snapshots):
        ts = base_ts.replace(minute=15 + snap_i)
        for s_i, strike in enumerate(strikes):
            rows.append({
                "strike":   strike,
                "ts":       ts,
                "bucket_1m": snap_i,
                metric:     100_000 + snap_i * 1_000 + s_i * 500,
            })
    return pd.DataFrame(rows)


class TestBuildHeatmapMatrix:
    def test_invalid_metric_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid metric"):
            build_heatmap_matrix(pd.DataFrame(), "invalid_col")

    def test_empty_df_returns_empty(self) -> None:
        result = build_heatmap_matrix(pd.DataFrame(columns=["strike", "ts", "ce_oi"]),
                                      "ce_oi")
        assert result["strikes"] == []
        assert result["timestamps"] == []
        assert result["matrix"] == []
        assert result["min_val"] is None
        assert result["max_val"] is None

    def test_matrix_shape(self) -> None:
        df = _make_df(n_strikes=3, n_snapshots=4)
        result = build_heatmap_matrix(df, "ce_oi")
        assert len(result["strikes"])    == 3
        assert len(result["timestamps"]) == 4
        assert len(result["matrix"])     == 3
        assert all(len(row) == 4 for row in result["matrix"])

    def test_strikes_sorted_ascending(self) -> None:
        df = _make_df()
        result = build_heatmap_matrix(df, "ce_oi")
        assert result["strikes"] == sorted(result["strikes"])

    def test_min_max_vals(self) -> None:
        df = _make_df(n_strikes=3, n_snapshots=4)
        result = build_heatmap_matrix(df, "ce_oi")
        flat = [v for row in result["matrix"] for v in row if v is not None]
        assert result["min_val"] == pytest.approx(min(flat))
        assert result["max_val"] == pytest.approx(max(flat))

    def test_bucket_filter_from(self) -> None:
        df = _make_df(n_snapshots=4)    # buckets 0..3
        result = build_heatmap_matrix(df, "ce_oi", from_bucket=2)
        assert len(result["timestamps"]) == 2   # buckets 2 and 3

    def test_bucket_filter_to(self) -> None:
        df = _make_df(n_snapshots=4)
        result = build_heatmap_matrix(df, "ce_oi", to_bucket=1)
        assert len(result["timestamps"]) == 2   # buckets 0 and 1

    def test_bucket_filter_range(self) -> None:
        df = _make_df(n_snapshots=4)
        result = build_heatmap_matrix(df, "ce_oi", from_bucket=1, to_bucket=2)
        assert len(result["timestamps"]) == 2

    def test_no_none_in_fully_populated_matrix(self) -> None:
        df = _make_df(n_strikes=3, n_snapshots=3)
        result = build_heatmap_matrix(df, "ce_oi")
        assert all(v is not None for row in result["matrix"] for v in row)

    def test_nan_becomes_none_in_matrix(self) -> None:
        df = _make_df(n_strikes=2, n_snapshots=2)
        df.loc[0, "ce_oi"] = float("nan")
        result = build_heatmap_matrix(df, "ce_oi")
        # At least one None should exist
        all_vals = [v for row in result["matrix"] for v in row]
        assert None in all_vals

    def test_metric_in_result(self) -> None:
        df = _make_df()
        result = build_heatmap_matrix(df, "ce_oi")
        assert result["metric"] == "ce_oi"

    def test_valid_metrics_includes_expected(self) -> None:
        for m in ["ce_oi", "pe_oi", "ce_iv", "pe_iv", "ce_volume", "pe_volume"]:
            assert m in VALID_METRICS

    def test_pe_oi_metric(self) -> None:
        df = _make_df(metric="pe_oi")
        result = build_heatmap_matrix(df, "pe_oi")
        assert result["min_val"] is not None
        assert result["max_val"] is not None
        assert result["max_val"] >= result["min_val"]

    def test_duplicate_ts_strike_uses_last(self) -> None:
        # Two rows with same (ts, strike) — pivot should use aggfunc="last"
        ts = datetime(2025, 3, 27, 9, 15, tzinfo=timezone.utc)
        df = pd.DataFrame([
            {"strike": 22500.0, "ts": ts, "bucket_1m": 0, "ce_oi": 100_000},
            {"strike": 22500.0, "ts": ts, "bucket_1m": 0, "ce_oi": 200_000},
        ])
        result = build_heatmap_matrix(df, "ce_oi")
        assert result["matrix"][0][0] == 200_000.0
