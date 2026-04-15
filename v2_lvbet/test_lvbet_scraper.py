"""
Tests for the LVBet v2 scraper helpers.

Run:
    python -m pytest v2_lvbet/test_lvbet_scraper.py -v
"""
import unittest
from v2_lvbet.lvbet_scraper import compute_match_time, parse_soccer_events


class TestComputeMatchTime(unittest.TestCase):
    def test_frozen_clock_at_halftime(self):
        clock = {"match_time": "45:00", "stopped": True, "timestamp": 1000000}
        result = compute_match_time(clock, match_status_code=31)  # HT
        self.assertEqual(result, "45:00")

    def test_empty_match_time(self):
        clock = {"match_time": "", "stopped": False}
        self.assertEqual(compute_match_time(clock), "")

    def test_no_timestamp(self):
        clock = {"match_time": "30:00", "stopped": False}
        self.assertEqual(compute_match_time(clock), "30:00")


class TestParseSoccerEvents(unittest.TestCase):
    def _make_snapshot(self, **event_overrides):
        ev = {
            "desc": {
                "sport": "1",
                "competitors": [
                    {"name": "Legia Warszawa"},
                    {"name": "Lech Poznan"},
                ],
                "category": "cat1",
                "tournament": "tourn1",
                "scheduled": 1713189600,
            },
            "score": {"home_score": "2", "away_score": "1"},
            "state": {
                "clock": {"match_time": "65:00", "stopped": True},
                "match_status": 7,
            },
            "markets": {
                "1": {
                    "": {
                        "1": {"k": "1.50"},
                        "2": {"k": "4.00"},
                        "3": {"k": "6.50"},
                    }
                },
                "18": {
                    "total=2.5": {
                        "12": {"k": "1.80"},
                        "13": {"k": "2.00"},
                    }
                },
            },
        }
        ev.update(event_overrides)
        return {
            "events": {"ev1": ev},
            "categories": {"cat1": {"name": "Poland"}},
            "tournaments": {"tourn1": {"name": "Ekstraklasa"}},
        }

    def test_basic_parse(self):
        events = parse_soccer_events(self._make_snapshot())
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev["team1"], "Legia Warszawa")
        self.assertEqual(ev["team2"], "Lech Poznan")
        self.assertEqual(ev["tournament"], "Ekstraklasa")
        self.assertEqual(ev["home_score"], "2")
        self.assertEqual(ev["away_score"], "1")
        self.assertEqual(ev["odd_1"], "1.50")
        self.assertEqual(ev["odd_X"], "4.00")
        self.assertEqual(ev["odd_2"], "6.50")
        self.assertEqual(ev["total_line"], "2.5")
        self.assertEqual(ev["odd_over"], "1.80")
        self.assertEqual(ev["odd_under"], "2.00")

    def test_skip_non_soccer(self):
        events = parse_soccer_events(self._make_snapshot(
            desc={"sport": "5", "competitors": [{"name": "A"}, {"name": "B"}]}
        ))
        self.assertEqual(len(events), 0)

    def test_skip_virtual(self):
        snapshot = self._make_snapshot()
        snapshot["events"]["ev1"]["desc"]["virtual"] = True
        events = parse_soccer_events(snapshot)
        self.assertEqual(len(events), 0)


if __name__ == "__main__":
    unittest.main()

