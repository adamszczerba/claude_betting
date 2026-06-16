"""Tests for dashboard.matcher — fuzzy alignment and normalisation."""

import pytest
from dashboard.matcher import normalize, match_score, align, build_grouped_table


class TestNormalize:
    def test_strip_accents(self):
        assert normalize("São Paulo") == "sao paulo"

    def test_strip_fc(self):
        assert normalize("Liverpool FC") == "liverpool"

    def test_strip_utd(self):
        assert normalize("Manchester Utd") == "manchester"

    def test_punctuation(self):
        assert normalize("Rot-Weiss Essen") == "rot weiss essen"

    def test_case_fold(self):
        assert normalize("BAYERN MÜNCHEN") == "bayern munchen"

    def test_multiple_suffixes(self):
        # "FC United" → both FC and United stripped
        result = normalize("FC United")
        assert result.strip() == ""

    def test_u20_stripped(self):
        assert normalize("Chapecoense U20") == "chapecoense"


class TestMatchScore:
    def test_identical(self):
        score = match_score("Liverpool", "Arsenal", "Premier League",
                            "Liverpool", "Arsenal", "Premier League")
        assert score > 105  # 100 base + tournament bonus

    def test_with_suffix_diff(self):
        """Same teams with FC/without FC should still score high."""
        score = match_score("Liverpool FC", "Arsenal FC", "Premier League",
                            "Liverpool", "Arsenal", "Premier League")
        assert score > 100

    def test_different_teams(self):
        score = match_score("Liverpool", "Arsenal", "Premier League",
                            "Real Madrid", "Barcelona", "La Liga")
        assert score < 55  # well below 85 threshold

    def test_near_miss(self):
        """Slightly different spelling should be ok above threshold."""
        score = match_score("Aalesunds FK", "Fredrikstad FK", "Eliteserien",
                            "Aalesund", "Fredrikstad", "Eliteserien")
        assert score > 85

    def test_accented_vs_plain(self):
        score = match_score("São Paulo", "Palmeiras", "Brasileirão",
                            "Sao Paulo", "Palmeiras", "Brasileirao")
        assert score > 100


class TestAlign:
    def test_basic_alignment(self):
        anchors = [
            {"team1": "Liverpool", "team2": "Arsenal", "tournament": "PL"},
            {"team1": "Chelsea", "team2": "Spurs", "tournament": "PL"},
        ]
        others = [
            {"team1": "Liverpool FC", "team2": "Arsenal FC", "tournament": "Premier League"},
        ]
        links = align(anchors, others)
        assert 0 in links
        assert links[0][0] == 0  # maps to first anchor
        assert links[0][1] > 85

    def test_no_match_below_threshold(self):
        anchors = [
            {"team1": "Liverpool", "team2": "Arsenal", "tournament": "PL"},
        ]
        others = [
            {"team1": "Real Madrid", "team2": "Barcelona", "tournament": "La Liga"},
        ]
        links = align(anchors, others)
        assert len(links) == 0

    def test_ambiguous_rejected(self):
        """When two anchors are equally similar, the gap check should reject."""
        anchors = [
            {"team1": "Team A", "team2": "Team B", "tournament": "League"},
            {"team1": "Team A", "team2": "Team B", "tournament": "Cup"},
        ]
        others = [
            {"team1": "Team A", "team2": "Team B", "tournament": "League"},
        ]
        links = align(anchors, others)
        # Should link to first anchor (tournament match) or reject if gap < 5
        if links:
            assert links[0][0] == 0


class TestBuildGroupedTable:
    def test_cc_only(self):
        rows = [
            {"bookmaker": "coincasino", "team1": "Liverpool",
             "team2": "Arsenal", "tournament": "PL", "odd_1": "2.5",
             "odd_X": "3.0", "odd_2": "2.8"},
        ]
        grouped = build_grouped_table(rows)
        assert len(grouped) == 1
        assert "coincasino" in grouped[0]["odds"]

    def test_cross_bookmaker_link(self):
        rows = [
            {"bookmaker": "coincasino", "team1": "Liverpool",
             "team2": "Arsenal", "tournament": "Premier League",
             "odd_1": "2.5", "odd_X": "3.0", "odd_2": "2.8"},
            {"bookmaker": "betfair", "team1": "Liverpool FC",
             "team2": "Arsenal FC", "tournament": "Premier League",
             "odd_1": "2.6", "odd_X": "3.1", "odd_2": "2.7"},
        ]
        grouped = build_grouped_table(rows)
        assert len(grouped) == 1
        assert "betfair" in grouped[0]["odds"]
