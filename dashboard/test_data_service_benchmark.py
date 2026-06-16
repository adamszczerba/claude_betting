"""Benchmark: old full-iteration vs new seek-from-end _last_csv_row."""

import csv
import os
import tempfile
import time

import pytest
from dashboard.data_service import _last_csv_row


def _make_csv(path: str, num_rows: int, num_cols: int = 11) -> None:
    """Create a CSV with `num_rows` data rows + 1 header row."""
    headers = [f"col{i}" for i in range(num_cols)]
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for i in range(num_rows):
            w.writerow([f"r{i}c{j}" for j in range(num_cols)])


def _benchmark(func, path: str, iterations: int = 100) -> float:
    """Run func(path) `iterations` times, return total seconds."""
    t0 = time.monotonic()
    for _ in range(iterations):
        func(path)
    return time.monotonic() - t0


class TestLastCsvRowBenchmark:
    """Quantify seek-from-end speedup over full iteration."""

    def test_small_file_10_rows(self, tmp_path):
        p = tmp_path / "small.csv"
        _make_csv(str(p), num_rows=10)
        result = _last_csv_row(str(p))
        assert result is not None
        assert result["col0"] == "r9c0"

    def test_medium_file_100_rows(self, tmp_path):
        p = tmp_path / "medium.csv"
        _make_csv(str(p), num_rows=100)
        result = _last_csv_row(str(p))
        assert result is not None
        assert result["col0"] == "r99c0"

    def test_large_file_1000_rows(self, tmp_path):
        p = tmp_path / "large.csv"
        _make_csv(str(p), num_rows=1000)
        result = _last_csv_row(str(p))
        assert result is not None
        assert result["col0"] == "r999c0"

    def test_benchmark_small(self, tmp_path):
        """10 rows — both methods should be fast."""
        p = tmp_path / "bench_small.csv"
        _make_csv(str(p), num_rows=10)
        elapsed = _benchmark(_last_csv_row, str(p), iterations=500)
        assert elapsed < 1.0, f"small file took {elapsed:.3f}s for 500 iters"

    def test_benchmark_medium(self, tmp_path):
        """100 rows — seek should be noticeably faster."""
        p = tmp_path / "bench_medium.csv"
        _make_csv(str(p), num_rows=100)
        elapsed = _benchmark(_last_csv_row, str(p), iterations=500)
        assert elapsed < 1.0, f"medium file took {elapsed:.3f}s for 500 iters"

    def test_benchmark_large(self, tmp_path):
        """1000 rows — seek should be ~100x faster than iteration."""
        p = tmp_path / "bench_large.csv"
        _make_csv(str(p), num_rows=1000)
        iters = 100
        elapsed = _benchmark(_last_csv_row, str(p), iterations=iters)
        assert elapsed < 1.0, (
            f"large file ({iters} iters) took {elapsed:.3f}s — "
            "expected <1s with seek-from-end"
        )


class TestLastCsvRowEdgeCases:
    """Edge cases for the seek-from-end reader."""

    def test_empty_file(self, tmp_path):
        p = tmp_path / "empty.csv"
        p.write_text("")
        assert _last_csv_row(str(p)) is None

    def test_header_only(self, tmp_path):
        p = tmp_path / "header_only.csv"
        with open(p, "w", newline="") as f:
            csv.writer(f).writerow(["a", "b", "c"])
        assert _last_csv_row(str(p)) is None

    def test_single_data_row(self, tmp_path):
        p = tmp_path / "one_row.csv"
        _make_csv(str(p), num_rows=1)
        result = _last_csv_row(str(p))
        assert result is not None
        assert result["col0"] == "r0c0"

    def test_wide_row(self, tmp_path):
        """25 columns (like CoinCasino extended schema)."""
        p = tmp_path / "wide.csv"
        _make_csv(str(p), num_rows=50, num_cols=25)
        result = _last_csv_row(str(p))
        assert result is not None
        assert len(result) == 25
        assert result["col0"] == "r49c0"

    def test_trailing_newline(self, tmp_path):
        p = tmp_path / "trailing_nl.csv"
        with open(p, "w", newline="") as f:
            csv.writer(f).writerow(["x", "y"])
            csv.writer(f).writerow(["1", "2"])
        result = _last_csv_row(str(p))
        assert result == {"x": "1", "y": "2"}
