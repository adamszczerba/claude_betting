"""
Tests for the STS v2 scraper helpers.

Run:
    python -m pytest v2_sts/test_sts_scraper.py -v
"""
import unittest
from v2_sts.sts_scraper import (
    _format_odd,
    _extract_match_status,
    parse_soccer_event,
    parse_sts_response,
)


class TestFormatOdd(unittest.TestCase):
    def test_float(self):
        self.assertEqual(_format_odd(2.5), "2.50")

    def test_int(self):
        self.assertEqual(_format_odd(3), "3.00")

    def test_string(self):
        self.assertEqual(_format_odd("1.91"), "1.91")

    def test_none(self):
        self.assertEqual(_format_odd(None), "")

    def test_empty(self):
        self.assertEqual(_format_odd(""), "")

    def test_invalid(self):
        self.assertEqual(_format_odd("xyz"), "")


class TestExtractMatchStatus(unittest.TestCase):
    def test_halftime(self):
        self.assertEqual(_extract_match_status({"status": "HALFTIME"}), "HT")

    def test_finished(self):
        self.assertEqual(_extract_match_status({"status": "FINISHED"}), "FT")

    def test_penalties(self):
        self.assertEqual(_extract_match_status({"status": "PENALTIES"}), "PEN")

    def test_running(self):
        self.assertEqual(_extract_match_status({"status": "RUNNING"}), "")

    def test_empty(self):
        self.assertEqual(_extract_match_status({}), "")


class TestParseSoccerEvent(unittest.TestCase):
    def _make_event(self, **overrides):
        base = {
            "id": 99001,
            "participants": [
                {"name": "Legia Warszawa"},
                {"name": "Cracovia"},
            ],
            "tournament": {"name": "Ekstraklasa"},
            "score": {"home": 1, "away": 0},
            "matchTime": "55",
            "status": "RUNNING",
            "markets": [
                {
                    "type": "1X2",
                    "outcomes": [
                        {"type": "1", "odds": 1.80},
                        {"type": "X", "odds": 3.60},
                        {"type": "2", "odds": 4.50},
                    ],
                },
                {
                    "type": "OVER_UNDER",
                    "line": 2.5,
                    "outcomes": [
                        {"type": "OVER", "odds": 1.90},
                        {"type": "UNDER", "odds": 1.90},
                    ],
                },
            ],
            "startDate": "2026-04-15T18:00:00Z",
        }
        base.update(overrides)
        return base

    def test_basic_parse(self):
        ev = parse_soccer_event(self._make_event())
        self.assertEqual(ev["team1"], "Legia Warszawa")
        self.assertEqual(ev["team2"], "Cracovia")
        self.assertEqual(ev["tournament"], "Ekstraklasa")
        self.assertEqual(ev["home_score"], "1")
        self.assertEqual(ev["away_score"], "0")
        self.assertEqual(ev["match_time"], "55")
        self.assertEqual(ev["odd_1"], "1.80")
        self.assertEqual(ev["odd_X"], "3.60")
        self.assertEqual(ev["odd_2"], "4.50")
        self.assertEqual(ev["total_line"], "2.5")
        self.assertEqual(ev["odd_over"], "1.90")
        self.assertEqual(ev["odd_under"], "1.90")

    def test_insufficient_participants(self):
        self.assertIsNone(parse_soccer_event({"id": 1, "participants": [{"name": "A"}]}))

    def test_home_away_fields(self):
        ev = parse_soccer_event({
            "id": 2,
            "home": {"name": "Team A"},
            "away": {"name": "Team B"},
            "tournament": {"name": "Liga"},
            "score": {"home": 0, "away": 0},
            "matchTime": "10",
            "status": "RUNNING",
            "markets": [],
        })
        self.assertIsNotNone(ev)
        self.assertEqual(ev["team1"], "Team A")
        self.assertEqual(ev["team2"], "Team B")


class TestParseSTSResponse(unittest.TestCase):
    def test_events_list(self):
        data = {
            "events": [
                {
                    "id": 1,
                    "participants": [{"name": "A"}, {"name": "B"}],
                    "tournament": {"name": "T"},
                    "score": {"home": 0, "away": 0},
                    "status": "RUNNING",
                    "markets": [],
                }
            ]
        }
        events = parse_sts_response(data)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["team1"], "A")

    def test_empty_response(self):
        self.assertEqual(parse_sts_response({}), [])


if __name__ == "__main__":
    unittest.main()

