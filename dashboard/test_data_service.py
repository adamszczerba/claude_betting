"""Tests for dashboard.data_service — filename parsing and CSV reading."""

import csv
import os
import tempfile

import pytest
from dashboard.data_service import _parse_filename, _last_csv_row, scan_today


class TestParseFilename:
    def test_coincasino(self):
        info = _parse_filename(
            "Liverpool_vs_Arsenal_Premier League_cc_2026-04-07.csv"
        )
        assert info is not None
        assert info["team1"] == "Liverpool"
        assert info["team2"] == "Arsenal"
        assert info["tournament"] == "Premier League"
        assert info["bookmaker"] == "coincasino"
        assert info["date"] == "2026-04-07"

    def test_betfair(self):
        info = _parse_filename(
            "Rot-Weiss Essen_vs_Schweinfurt_3. Liga_bf_2026-04-07.csv"
        )
        assert info is not None
        assert info["bookmaker"] == "betfair"

    def test_bet365(self):
        info = _parse_filename(
            "TeamA_vs_TeamB_Cup_b365_2026-04-07.csv"
        )
        assert info is not None
        assert info["bookmaker"] == "bet365"

    def test_betfair_exchange(self):
        info = _parse_filename(
            "TeamA_vs_TeamB_League_bfx_2026-04-07.csv"
        )
        assert info is not None
        assert info["bookmaker"] == "betfair_exchange"

    def test_unknown_tag(self):
        assert _parse_filename("A_vs_B_L_xx_2026-01-01.csv") is None

    def test_non_csv(self):
        assert _parse_filename("A_vs_B_L_cc_2026-01-01.json") is None


class TestLastCsvRow:
    def test_single_row(self, tmp_path):
        p = tmp_path / "test.csv"
        with open(p, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["a", "b"])
            w.writerow(["1", "2"])
        result = _last_csv_row(str(p))
        assert result == {"a": "1", "b": "2"}

    def test_multiple_rows(self, tmp_path):
        p = tmp_path / "test.csv"
        with open(p, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["x"])
            w.writerow(["old"])
            w.writerow(["latest"])
        result = _last_csv_row(str(p))
        assert result == {"x": "latest"}

    def test_header_only(self, tmp_path):
        p = tmp_path / "test.csv"
        with open(p, "w", newline="") as f:
            csv.writer(f).writerow(["col"])
        assert _last_csv_row(str(p)) is None

    def test_missing_file(self):
        assert _last_csv_row("/nonexistent/path.csv") is None


class TestScanToday:
    def test_reads_real_db(self):
        """Smoke test: scan_today should not crash on the real match_database."""
        db = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "match_database",
        )
        if not os.path.isdir(db):
            pytest.skip("match_database not available")
        rows = scan_today(db_root=db)
        # Just verify it returns a list of dicts
        assert isinstance(rows, list)
        for r in rows:
            assert "bookmaker" in r
            assert "team1" in r
