"""
Canonical match identity & matching.

Produces stable canonical_match_id values from per-bookmaker match data.
Uses normalized team names + tournament aliases + kickoff window heuristics.

Usage
-----
>>> matcher = CanonicalMatcher()
>>> cid, confidence = matcher.get_canonical_id("Liverpool", "Arsenal", "Premier League")
>>> print(cid, confidence)
"""

from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from rapidfuzz import fuzz

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
_SPACES_RE = re.compile(r"\s+")
_STRIP_SUFFIXES = re.compile(
    r"\b(fc|sc|afc|ac|cf|fk|bk|if|sk|ssc|us|as|cd|ud|rcd|rc|"
    r"sporting|club|united|city|town|athletic|rovers|wanderers|"
    r"utd|ii|iii|iv|u17|u18|u19|u20|u21|u23)\b"
)

# Known tournament aliases → canonical name
_TOURNAMENT_ALIASES: Dict[str, str] = {
    "england premier league": "Premier League",
    "premier league": "Premier League",
    "epl": "Premier League",
    "spain la liga": "La Liga",
    "la liga": "La Liga",
    "laliga": "La Liga",
    "germany bundesliga": "Bundesliga",
    "bundesliga": "Bundesliga",
    "italy serie a": "Serie A",
    "serie a": "Serie A",
    "france ligue 1": "Ligue 1",
    "ligue 1": "Ligue 1",
    "uefa champions league": "Champions League",
    "champions league": "Champions League",
    "ucl": "Champions League",
    "uefa europa league": "Europa League",
    "europa league": "Europa League",
}


def normalize_team(name: str) -> str:
    """Return a canonical, accent-free, lower-case version of a team name."""
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    s = stripped.casefold()
    s = _PUNCT_RE.sub(" ", s)
    s = _STRIP_SUFFIXES.sub(" ", s)
    s = _SPACES_RE.sub(" ", s).strip()
    return s


def _normalize_tournament(name: str) -> str:
    key = normalize_team(name)
    return _TOURNAMENT_ALIASES.get(key, name.strip())


# ---------------------------------------------------------------------------
# Canonical ID generation
# ---------------------------------------------------------------------------

def _make_canonical_id(team1: str, team2: str, tournament: str) -> str:
    """Deterministic canonical ID from normalized match data."""
    t1 = normalize_team(team1)
    t2 = normalize_team(team2)
    t = normalize_team(tournament)
    # Sort team names for order-independence
    teams = sorted([t1, t2])
    raw = f"{teams[0]}|{teams[1]}|{t}"
    h = hashlib.md5(raw.encode()).hexdigest()[:12]
    return f"match_{h}"


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------

@dataclass
class _MatchEntry:
    canonical_id: str
    team1: str
    team2: str
    tournament: str
    bookmaker_ids: Dict[str, str] = field(default_factory=dict)  # bk → match_id
    confidence: float = 1.0


class CanonicalMatcher:
    """
    Maintains a mapping from per-bookmaker match data to canonical match IDs.

    Usage
    -----
    >>> matcher = CanonicalMatcher()
    >>> cid, conf = matcher.get_or_create("Liverpool", "Arsenal", "Premier League", "coincasino", "cc_123")
    >>> cid2, conf2 = matcher.get_or_create("Liverpool FC", "Arsenal FC", "EPL", "betfair", "bf_456")
    >>> assert cid == cid2  # same canonical match
    """

    def __init__(self):
        self._entries: Dict[str, _MatchEntry] = {}  # canonical_id → entry
        self._lookup: Dict[Tuple[str, str], str] = {}  # (bookmaker, match_id) → canonical_id

    def get_or_create(
        self,
        team1: str,
        team2: str,
        tournament: str,
        bookmaker: str = "",
        match_id: str = "",
    ) -> Tuple[str, float]:
        """Find existing canonical match or create new one.

        Returns (canonical_id, confidence).
        """
        # Fast path: exact lookup by bookmaker + match_id
        if bookmaker and match_id:
            key = (bookmaker, match_id)
            if key in self._lookup:
                cid = self._lookup[key]
                return cid, self._entries[cid].confidence

        # Try fuzzy match against existing entries
        best_cid: Optional[str] = None
        best_score: float = 0.0

        for cid, entry in self._entries.items():
            score = self._similarity(team1, team2, tournament,
                                     entry.team1, entry.team2, entry.tournament)
            if score > best_score:
                best_score = score
                best_cid = cid

        if best_cid is not None and best_score >= 75.0:
            # Link to existing
            entry = self._entries[best_cid]
            if bookmaker and match_id:
                entry.bookmaker_ids[bookmaker] = match_id
                self._lookup[(bookmaker, match_id)] = best_cid
            confidence = min(1.0, best_score / 100.0)
            entry.confidence = max(entry.confidence, confidence)
            return best_cid, confidence

        # Create new canonical entry
        cid = _make_canonical_id(team1, team2, tournament)
        canon_tournament = _normalize_tournament(tournament)
        entry = _MatchEntry(
            canonical_id=cid,
            team1=team1,
            team2=team2,
            tournament=canon_tournament,
        )
        if bookmaker and match_id:
            entry.bookmaker_ids[bookmaker] = match_id
            self._lookup[(bookmaker, match_id)] = cid
        self._entries[cid] = entry
        return cid, 1.0

    def get_canonical_id(self, team1: str, team2: str, tournament: str) -> Tuple[str, float]:
        """Get canonical ID without creating a new entry."""
        for cid, entry in self._entries.items():
            score = self._similarity(team1, team2, tournament,
                                     entry.team1, entry.team2, entry.tournament)
            if score >= 75.0:
                return cid, min(1.0, score / 100.0)
        return _make_canonical_id(team1, team2, tournament), 0.0

    @staticmethod
    def _similarity(t1a: str, t2a: str, toura: str,
                    t1b: str, t2b: str, tourb: str) -> float:
        s1 = fuzz.token_sort_ratio(normalize_team(t1a), normalize_team(t1b))
        s2 = fuzz.token_sort_ratio(normalize_team(t2a), normalize_team(t2b))
        st = fuzz.token_sort_ratio(normalize_team(toura), normalize_team(tourb))
        return (s1 + s2) / 2.0 + st / 10.0  # 0–110 scale
