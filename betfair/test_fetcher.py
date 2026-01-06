from unittest import TestCase


import unittest
from unittest.mock import MagicMock, patch
from datetime import date

class TestBetfairScraper(unittest.TestCase):

    def parse_league_removes_consecutive_Live(self):
        scraper = BetfairScraper()
        league_elements = ["League Name", "Other", "Data", "Here", "Live", "Live", "Team1 1 - 0 Team2 45′", "1.5", "2.0", "3.0"]
        result = scraper.parse_league(league_elements)
        self.assertEqual(len(result), 1)
        self.assertIn("League Name", list(result.keys())[0].league)

