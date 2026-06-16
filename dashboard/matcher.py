"""
Cross-bookmaker fuzzy match alignment.

CoinCasino fixtures are the anchor set.  For each other bookmaker's fixture
we find the best CoinCasino match (if any) based on team-name similarity.

Normalisation pipeline:
  1. Unicode NFKD → strip accents / diacritics
  2. case-fold
  3. strip punctuation, collapse whitespace
  4. expand common abbreviations (FC, SC, AFC, …)

Similarity is computed pairwise (home↔home, away↔away) using
rapidfuzz.fuzz.token_sort_ratio and averaged.  A tournament bonus
(0–10 pts) is added when tournament names are also similar.

Threshold: 85 (balanced).  We also require the gap between best and
second-best candidate ≥ 5 to reduce ambiguous links.
"""

import re
import unicodedata
from typing import Dict, List, Optional, Tuple

from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
_SPACES_RE = re.compile(r"\s+")

# Common suffixes that different bookmakers include or omit
_STRIP_SUFFIXES = re.compile(
    r"\b(fc|sc|afc|ac|cf|fk|bk|if|sk|ssc|us|as|cd|ud|rcd|rc|"
    r"sporting|club|united|city|town|athletic|rovers|wanderers|"
    r"utd|ii|iii|iv|u17|u18|u19|u20|u21|u23)\b"
)


def normalize(name: str) -> str:
    """Return a canonical, accent-free, lower-case version of *name*."""
    # NFKD decomposition → strip combining marks
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    s = stripped.casefold()
    s = _PUNCT_RE.sub(" ", s)
    s = _STRIP_SUFFIXES.sub(" ", s)
    s = _SPACES_RE.sub(" ", s).strip()
    return s


def _team_similarity(a: str, b: str) -> float:
    """Similarity score (0-100) between two team names."""
    return fuzz.token_sort_ratio(normalize(a), normalize(b))


def _tournament_bonus(t1: str, t2: str) -> float:
    """0-10 bonus when tournament names are similar."""
    score = fuzz.token_sort_ratio(normalize(t1), normalize(t2))
    return score / 10.0  # 0..10


def match_score(
    home_a: str, away_a: str, tourn_a: str,
    home_b: str, away_b: str, tourn_b: str,
) -> float:
    """Combined similarity score (0-110) for two fixtures."""
    home_sim = _team_similarity(home_a, home_b)
    away_sim = _team_similarity(away_a, away_b)
    avg = (home_sim + away_sim) / 2.0
    bonus = _tournament_bonus(tourn_a, tourn_b)
    return avg + bonus


# ---------------------------------------------------------------------------
# Alignment
# ---------------------------------------------------------------------------

THRESHOLD = 85.0
MIN_GAP = 5.0  # gap between best and 2nd-best to accept


def align(
    anchor_fixtures: List[dict],
    other_fixtures: List[dict],
) -> Dict[int, Tuple[int, float]]:
    """Link *other_fixtures* → *anchor_fixtures* by similarity.

    Parameters
    ----------
    anchor_fixtures : list of dicts with keys team1, team2, tournament
    other_fixtures  : list of dicts with same keys

    Returns
    -------
    dict  {other_index: (anchor_index, score)}
          Only includes links that pass THRESHOLD and gap checks.
    """
    links: Dict[int, Tuple[int, float]] = {}
    # Pre-compute to avoid N² re-normalisation
    for oi, ofix in enumerate(other_fixtures):
        best_idx: Optional[int] = None
        best_score: float = 0.0
        second_score: float = 0.0

        for ai, afix in enumerate(anchor_fixtures):
            s = match_score(
                afix["team1"], afix["team2"], afix["tournament"],
                ofix["team1"], ofix["team2"], ofix["tournament"],
            )
            if s > best_score:
                second_score = best_score
                best_score = s
                best_idx = ai
            elif s > second_score:
                second_score = s

        if best_idx is not None and best_score >= THRESHOLD:
            gap = best_score - second_score
            if gap >= MIN_GAP or second_score < THRESHOLD:
                links[oi] = (best_idx, best_score)
    return links


def build_grouped_table(all_rows: List[dict]) -> List[dict]:
    """Group rows by CoinCasino anchor match, attaching other bookmakers.

    Parameters
    ----------
    all_rows : output of data_service.scan_today()

    Returns
    -------
    list of dicts, one per CoinCasino match, each with structure::

        {
          "team1": str, "team2": str, "tournament": str,
          "odds": {
            "coincasino": {col: val, ...},
            "betfair":    {col: val, ...} | None,
            "bet365":     {col: val, ...} | None,
            "betfair_exchange": {col: val, ...} | None,
          },
          "match_scores": {bookmaker: float, ...},  # similarity scores
        }
    """
    cc_rows = [r for r in all_rows if r["bookmaker"] == "coincasino"]
    other_by_bk: Dict[str, List[dict]] = {}
    for r in all_rows:
        if r["bookmaker"] != "coincasino":
            other_by_bk.setdefault(r["bookmaker"], []).append(r)

    # Build anchor index for alignment
    anchor_fixtures = [
        {"team1": r["team1"], "team2": r["team2"], "tournament": r["tournament"]}
        for r in cc_rows
    ]

    # Align each bookmaker
    bk_links: Dict[str, Dict[int, Tuple[int, float]]] = {}
    for bk, rows in other_by_bk.items():
        other_fixtures = [
            {"team1": r["team1"], "team2": r["team2"], "tournament": r["tournament"]}
            for r in rows
        ]
        bk_links[bk] = align(anchor_fixtures, other_fixtures)

    # Assemble grouped output
    grouped: List[dict] = []
    for ai, cc in enumerate(cc_rows):
        entry: dict = {
            "team1": cc["team1"],
            "team2": cc["team2"],
            "tournament": cc["tournament"],
            "odds": {"coincasino": cc},
            "match_scores": {},
        }
        for bk, links in bk_links.items():
            # Check if any other-fixture maps to this anchor
            for oi, (linked_ai, score) in links.items():
                if linked_ai == ai:
                    entry["odds"][bk] = other_by_bk[bk][oi]
                    entry["match_scores"][bk] = score
                    break
        grouped.append(entry)
    return grouped
