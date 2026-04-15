"""
Tests for the Pinnacle v2 scraper helpers.

Run:
    python -m pytest v2_pinnacle/test_pinnacle_scraper.py -v
"""
import unittest
from v2_pinnacle.pinnacle_scraper import (
    _format_price,
    parse_matchup_data,
    merge_odds_into_events,
    _extract_match_status,
)


class TestFormatPrice(unittest.TestCase):
    def test_float_value(self):
        self.assertEqual(_format_price(2.5), "2.50")

    def test_int_value(self):
        self.assertEqual(_format_price(3), "3.00")

    def test_string_value(self):
        self.assertEqual(_format_price("1.91"), "1.91")

    def test_none(self):
        self.assertEqual(_format_price(None), "")

    def test_empty_string(self):
        self.assertEqual(_format_price(""), "")

    def test_invalid(self):
        self.assertEqual(_format_price("abc"), "")


class TestExtractMatchStatus(unittest.TestCase):
    def test_halftime(self):
        self.assertEqual(_extract_match_status({"liveStatus": {"status": "halftime"}}), "HT")

    def test_ended(self):
        self.assertEqual(_extract_match_status({"liveStatus": {"status": "ended"}}), "FT")

    def test_penalties(self):
        self.assertEqual(_extract_match_status({"liveStatus": {"status": "penalties"}}), "PEN")

    def test_running(self):
        self.assertEqual(_extract_match_status({"liveStatus": {"status": "started", "period": 1}}), "")

    def test_empty(self):
        self.assertEqual(_extract_match_status({}), "")


class TestParseMatchupData(unittest.TestCase):
    def _make_matchup(self, **overrides):
        base = {
            "id": 12345,
            "isLive": True,
            "participants": [
                {"name": "Liverpool", "alignment": "home"},
                {"name": "Arsenal", "alignment": "away"},
            ],
            "league": {"name": "Premier League"},
            "liveStatus": {
                "status": "started",
                "period": 1,
                "elapsed": 1845,
                "score": {"home": 1, "away": 0},
            },
            "startTime": "2026-04-15T15:00:00Z",
        }
        base.update(overrides)
        return base

    def test_basic_parse(self):
        ev = parse_matchup_data(self._make_matchup())
        self.assertEqual(ev["team1"], "Liverpool")
        self.assertEqual(ev["team2"], "Arsenal")
        self.assertEqual(ev["tournament"], "Premier League")
        self.assertEqual(ev["matchup_id"], "12345")
        self.assertEqual(ev["home_score"], "1")
        self.assertEqual(ev["away_score"], "0")
        self.assertEqual(ev["match_time"], "30:45")

    def test_not_live_returns_none(self):
        self.assertIsNone(parse_matchup_data(self._make_matchup(isLive=False)))

    def test_insufficient_participants(self):
        self.assertIsNone(parse_matchup_data(self._make_matchup(
            participants=[{"name": "Only One"}]
        )))


class TestMergeOdds(unittest.TestCase):
    def test_merge_moneyline_and_total(self):
        events = {
            "123": {
                "matchup_id": "123",
                "odd_1": "", "odd_X": "", "odd_2": "",
                "total_line": "", "odd_over": "", "odd_under": "",
            }
        }
        odds_data = [
            {
                "matchupId": 123,
                "prices": [
                    {"period": 0, "designation": "home", "type": "moneyline", "price": 2.10},
                    {"period": 0, "designation": "draw", "type": "moneyline", "price": 3.40},
                    {"period": 0, "designation": "away", "type": "moneyline", "price": 3.50},
                    {"period": 0, "designation": "over", "type": "total", "price": 1.85, "points": 2.5},
                    {"period": 0, "designation": "under", "type": "total", "price": 1.95, "points": 2.5},
                ],
            }
        ]
        merge_odds_into_events(events, odds_data)
        ev = events["123"]
        self.assertEqual(ev["odd_1"], "2.10")
        self.assertEqual(ev["odd_X"], "3.40")
        self.assertEqual(ev["odd_2"], "3.50")
        self.assertEqual(ev["total_line"], "2.5")
        self.assertEqual(ev["odd_over"], "1.85")
        self.assertEqual(ev["odd_under"], "1.95")

    def test_skip_non_match_period(self):
        events = {
            "456": {
                "matchup_id": "456",
                "odd_1": "", "odd_X": "", "odd_2": "",
                "total_line": "", "odd_over": "", "odd_under": "",
            }
        }
        odds_data = [
            {
                "matchupId": 456,
                "prices": [
                    {"period": 1, "designation": "home", "type": "moneyline", "price": 1.50},
                ],
            }
        ]
        merge_odds_into_events(events, odds_data)
        # Period 1 (first half) odds should NOT be merged
        self.assertEqual(events["456"]["odd_1"], "")


if __name__ == "__main__":
    unittest.main()

