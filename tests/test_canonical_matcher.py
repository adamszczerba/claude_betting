"""Tests for Canonical Match Matcher."""

import pytest
from matching.canonical import CanonicalMatcher, normalize_team, _make_canonical_id


class TestNormalizeTeam:
    def test_basic(self):
        assert normalize_team("Liverpool FC") == "liverpool"

    def test_accents(self):
        assert normalize_team("Atlético Madrid") == "atletico madrid"

    def test_case_insensitive(self):
        assert normalize_team("ARSENAL") == "arsenal"

    def test_punctuation(self):
        assert normalize_team("A.S. Roma") == "a s roma"

    def test_suffix_stripping(self):
        assert normalize_team("Manchester United") == "manchester"


class TestMakeCanonicalId:
    def test_deterministic(self):
        id1 = _make_canonical_id("Liverpool", "Arsenal", "Premier League")
        id2 = _make_canonical_id("Liverpool", "Arsenal", "Premier League")
        assert id1 == id2

    def test_order_independent(self):
        id1 = _make_canonical_id("Liverpool", "Arsenal", "Premier League")
        id2 = _make_canonical_id("Arsenal", "Liverpool", "Premier League")
        assert id1 == id2

    def test_different_matches_different_ids(self):
        id1 = _make_canonical_id("Liverpool", "Arsenal", "Premier League")
        id2 = _make_canonical_id("Chelsea", "Man City", "Premier League")
        assert id1 != id2


class TestCanonicalMatcher:
    def test_create_new(self):
        m = CanonicalMatcher()
        cid, conf = m.get_or_create("Liverpool", "Arsenal", "Premier League")
        assert cid.startswith("match_")
        assert conf == 1.0

    def test_same_match_returns_same_id(self):
        m = CanonicalMatcher()
        cid1, _ = m.get_or_create("Liverpool", "Arsenal", "Premier League", "cc", "cc_123")
        cid2, conf2 = m.get_or_create("Liverpool FC", "Arsenal FC", "EPL", "bf", "bf_456")
        assert cid1 == cid2
        assert conf2 > 0.7

    def test_fuzzy_matching(self):
        m = CanonicalMatcher()
        m.get_or_create("Liverpool", "Arsenal", "Premier League", "cc", "cc_1")
        cid, conf = m.get_or_create("Liverpool FC", "Arsenal", "Premier League", "bf", "bf_1")
        assert conf >= 0.75

    def test_different_matches_different_ids(self):
        m = CanonicalMatcher()
        cid1, _ = m.get_or_create("Liverpool", "Arsenal", "Premier League")
        cid2, _ = m.get_or_create("Chelsea", "Man City", "Premier League")
        assert cid1 != cid2

    def test_fast_lookup_by_bookmaker_id(self):
        m = CanonicalMatcher()
        cid1, _ = m.get_or_create("Liverpool", "Arsenal", "PL", "cc", "cc_123")
        cid2, conf2 = m.get_or_create("", "", "", "cc", "cc_123")
        assert cid1 == cid2
        assert conf2 == 1.0  # exact lookup

    def test_get_canonical_id_no_create(self):
        m = CanonicalMatcher()
        m.get_or_create("Liverpool", "Arsenal", "PL", "cc", "cc_1")
        cid, conf = m.get_canonical_id("Liverpool FC", "Arsenal FC", "Premier League")
        assert conf >= 0.75
