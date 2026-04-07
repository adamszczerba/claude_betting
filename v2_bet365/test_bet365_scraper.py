"""
Tests for the Bet365 v2 scraper helpers.

Run:
    python -m pytest v2_bet365/test_bet365_scraper.py -v
"""
import unittest
from v2_bet365.bet365_scraper import parse_decimal_odds


class TestParseDecimalOdds(unittest.TestCase):

    def test_standard_decimal(self):
        self.assertEqual(parse_decimal_odds("1.50"), "1.50")

    def test_large_odds(self):
        self.assertEqual(parse_decimal_odds("12.00"), "12.00")

    def test_long_odds(self):
        self.assertEqual(parse_decimal_odds("501.00"), "501.00")

    def test_short_odds(self):
        self.assertEqual(parse_decimal_odds("1.01"), "1.01")

    def test_evens(self):
        self.assertEqual(parse_decimal_odds("2.00"), "2.00")

    def test_suspended_dash(self):
        self.assertEqual(parse_decimal_odds("-"), "")

    def test_suspended_off(self):
        self.assertEqual(parse_decimal_odds("OFF"), "")

    def test_suspended_susp(self):
        self.assertEqual(parse_decimal_odds("SUSP"), "")

    def test_sp(self):
        self.assertEqual(parse_decimal_odds("SP"), "")

    def test_empty_string(self):
        self.assertEqual(parse_decimal_odds(""), "")

    def test_whitespace(self):
        self.assertEqual(parse_decimal_odds("  "), "")

    def test_integer_odds(self):
        self.assertEqual(parse_decimal_odds("5"), "5.00")

    def test_three_decimal_places(self):
        self.assertEqual(parse_decimal_odds("3.125"), "3.12")

    def test_with_whitespace(self):
        self.assertEqual(parse_decimal_odds("  2.50  "), "2.50")

    def test_zero(self):
        self.assertEqual(parse_decimal_odds("0"), "")

    def test_negative(self):
        self.assertEqual(parse_decimal_odds("-1.5"), "")

    def test_garbage(self):
        self.assertEqual(parse_decimal_odds("abc"), "")


if __name__ == "__main__":
    unittest.main()

