"""
Tests for the Betfair Exchange scraper helpers.

Run:
    python -m pytest v2_betfair_exchange/test_betfair_exchange_scraper.py -v
"""
import unittest
from v2_betfair_exchange.betfair_exchange_scraper import average_back_lay, _parse_decimal


class TestAverageBackLay(unittest.TestCase):

    def test_both_present(self):
        """Average of back=1.40 and lay=1.50 → 1.45"""
        self.assertEqual(average_back_lay("1.40", "1.50"), "1.45")

    def test_exact_values(self):
        """Average of 3.00 and 3.20 → 3.10"""
        self.assertEqual(average_back_lay("3.00", "3.20"), "3.10")

    def test_only_back(self):
        """Only back available → use back"""
        self.assertEqual(average_back_lay("2.50", ""), "2.50")

    def test_only_lay(self):
        """Only lay available → use lay"""
        self.assertEqual(average_back_lay("", "4.00"), "4.00")

    def test_both_empty(self):
        """Neither available → empty string"""
        self.assertEqual(average_back_lay("", ""), "")

    def test_dash_values(self):
        """Dashes (suspended) → empty string"""
        self.assertEqual(average_back_lay("-", "-"), "")

    def test_back_dash_lay_present(self):
        """Back suspended, lay present → use lay"""
        self.assertEqual(average_back_lay("-", "2.00"), "2.00")

    def test_whitespace(self):
        """Whitespace-padded values"""
        self.assertEqual(average_back_lay("  1.80  ", "  2.20  "), "2.00")

    def test_large_odds(self):
        """Large exchange odds"""
        self.assertEqual(average_back_lay("100.0", "110.0"), "105.00")

    def test_small_odds(self):
        """Very small odds (heavy favourite)"""
        self.assertEqual(average_back_lay("1.01", "1.02"), "1.02")  # 1.015 → 1.02

    def test_integer_strings(self):
        """Integer-like strings"""
        self.assertEqual(average_back_lay("5", "6"), "5.50")

    def test_none_values(self):
        """None inputs (edge case)"""
        self.assertEqual(average_back_lay(None, None), "")

    def test_none_and_valid(self):
        self.assertEqual(average_back_lay(None, "3.50"), "3.50")


class TestParseDecimal(unittest.TestCase):

    def test_valid_float(self):
        self.assertEqual(_parse_decimal("2.50"), 2.50)

    def test_valid_int(self):
        self.assertEqual(_parse_decimal("3"), 3.0)

    def test_zero(self):
        self.assertIsNone(_parse_decimal("0"))

    def test_negative(self):
        self.assertIsNone(_parse_decimal("-1"))

    def test_empty(self):
        self.assertIsNone(_parse_decimal(""))

    def test_dash(self):
        self.assertIsNone(_parse_decimal("-"))

    def test_garbage(self):
        self.assertIsNone(_parse_decimal("abc"))

    def test_none(self):
        self.assertIsNone(_parse_decimal(""))


if __name__ == "__main__":
    unittest.main()

