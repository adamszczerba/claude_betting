"""
Tests for the Betfair v2 scraper helpers.

Run:
    python -m pytest v2_betfair/test_betfair_scraper.py -v
"""
import unittest
from v2_betfair.betfair_scraper import fractional_to_decimal


class TestFractionalToDecimal(unittest.TestCase):

    def test_standard_fraction(self):
        self.assertEqual(fractional_to_decimal("2/7"), "1.29")

    def test_evens(self):
        self.assertEqual(fractional_to_decimal("1/1"), "2.00")

    def test_long_odds(self):
        self.assertEqual(fractional_to_decimal("12/1"), "13.00")

    def test_short_odds(self):
        self.assertEqual(fractional_to_decimal("1/200"), "1.00")

    def test_suspended_dash(self):
        self.assertEqual(fractional_to_decimal("-"), "")

    def test_empty_string(self):
        self.assertEqual(fractional_to_decimal(""), "")

    def test_whitespace(self):
        self.assertEqual(fractional_to_decimal("  "), "")

    def test_already_decimal(self):
        self.assertEqual(fractional_to_decimal("2.5"), "2.5")

    def test_fraction_16_5(self):
        self.assertEqual(fractional_to_decimal("16/5"), "4.20")

    def test_fraction_500_1(self):
        self.assertEqual(fractional_to_decimal("500/1"), "501.00")

    def test_fraction_10_11(self):
        self.assertEqual(fractional_to_decimal("10/11"), "1.91")

    def test_fraction_9_4(self):
        self.assertEqual(fractional_to_decimal("9/4"), "3.25")

    def test_fraction_with_whitespace(self):
        self.assertEqual(fractional_to_decimal("  2/7  "), "1.29")


if __name__ == "__main__":
    unittest.main()

