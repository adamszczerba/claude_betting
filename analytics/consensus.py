"""
Cross-bookmaker consensus fair odds.

Weights reflect market sharpness (Pinnacle > BetfairExchange > ... > soft books).
For each match group (output of dashboard.matcher.build_grouped_table) this
module:
  1. Strips overround from each bookmaker's odds (normalize method).
  2. Computes a weighted median of implied probabilities for each outcome.
  3. Returns fair decimal odds from those consensus probabilities.

Usage
-----
>>> from dashboard.data_service import scan_today
>>> from dashboard.matcher import build_grouped_table
>>> from analytics.consensus import weighted_consensus
>>> rows = scan_today()
>>> grouped = build_grouped_table(rows)
>>> fair_map = weighted_consensus(grouped)
>>> fair_map["Liverpool_vs_Arsenal_Premier_League"]
FairOdds(odd_1=2.45, odd_X=3.61, odd_2=2.97, odd_over=1.91, odd_under=1.97)
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from analytics.overround import remove_overround

__all__ = ["weighted_consensus", "FairOdds", "BOOKMAKER_WEIGHTS"]

# ---------------------------------------------------------------------------
# Bookmaker weights (higher = sharper / more trusted)
# ---------------------------------------------------------------------------

BOOKMAKER_WEIGHTS: Dict[str, float] = {
    "pinnacle":         1.00,
    "betfair_exchange": 0.85,
    "bet365":           0.50,
    "betfair":          0.50,
    "coincasino":       0.40,
    "lvbet":            0.35,
    "sts":              0.35,
}

_MARKET_COLS = ["odd_1", "odd_X", "odd_2", "odd_over", "odd_under"]
_MAIN_3WAY   = ["odd_1", "odd_X", "odd_2"]
_OVERUNDER    = ["odd_over", "odd_under"]


@dataclass
class FairOdds:
    odd_1:     Optional[float] = None
    odd_X:     Optional[float] = None
    odd_2:     Optional[float] = None
    odd_over:  Optional[float] = None
    odd_under: Optional[float] = None

    def as_dict(self) -> Dict[str, Optional[float]]:
        return {
            "odd_1":     self.odd_1,
            "odd_X":     self.odd_X,
            "odd_2":     self.odd_2,
            "odd_over":  self.odd_over,
            "odd_under": self.odd_under,
        }


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 1.0 else None
    except (ValueError, TypeError):
        return None


def _weighted_median(values: List[float], weights: List[float]) -> float:
    """Weighted median: sort by value, find point where cumulative weight ≥ 0.5."""
    if not values:
        raise ValueError("Empty values list")
    pairs = sorted(zip(values, weights), key=lambda x: x[0])
    total_w = sum(w for _, w in pairs)
    cumulative = 0.0
    for v, w in pairs:
        cumulative += w
        if cumulative >= total_w / 2.0:
            return v
    return pairs[-1][0]


def _fair_from_group(group: dict, cols: List[str]) -> Dict[str, Optional[float]]:
    """Compute fair probabilities for *cols* using all bookmakers in *group*.

    Steps
    -----
    1. For each bookmaker that has ALL columns in *cols*, strip overround and
       get fair implied probabilities.
    2. Weighted-median across bookmakers per outcome.
    3. Convert back to fair decimal odds.
    """
    # Collect per-bookmaker de-juiced probs
    bk_probs: Dict[str, Dict[str, float]] = {}   # bk → {col: fair_prob}

    for bk, weight in BOOKMAKER_WEIGHTS.items():
        bk_data = group.get("odds", {}).get(bk)
        if not bk_data:
            continue
        raw = [_safe_float(bk_data.get(c)) for c in cols]
        if any(r is None for r in raw):
            continue
        raw_odds: List[float] = raw  # type: ignore[assignment]
        try:
            fair_odds = remove_overround(raw_odds, method="normalize")
            bk_probs[bk] = {c: 1.0 / fo for c, fo in zip(cols, fair_odds)}
        except Exception:
            continue

    if not bk_probs:
        return {c: None for c in cols}

    result: Dict[str, Optional[float]] = {}
    for col in cols:
        probs   = [bk_probs[bk][col] for bk in bk_probs if col in bk_probs[bk]]
        weights = [BOOKMAKER_WEIGHTS[bk] for bk in bk_probs if col in bk_probs[bk]]
        if not probs:
            result[col] = None
            continue
        if len(probs) == 1:
            consensus_prob = probs[0]
        else:
            consensus_prob = _weighted_median(probs, weights)
        result[col] = 1.0 / consensus_prob if consensus_prob > 0 else None

    return result


def _match_key(group: dict) -> str:
    return f"{group['team1']}_vs_{group['team2']}_{group['tournament']}"


def weighted_consensus(grouped: List[dict]) -> Dict[str, FairOdds]:
    """Compute consensus fair odds for every matched group.

    Parameters
    ----------
    grouped : output of dashboard.matcher.build_grouped_table()

    Returns
    -------
    dict mapping match_key → FairOdds
    """
    result: Dict[str, FairOdds] = {}

    for group in grouped:
        key = _match_key(group)

        main_fair  = _fair_from_group(group, _MAIN_3WAY)
        ou_fair    = _fair_from_group(group, _OVERUNDER)

        result[key] = FairOdds(
            odd_1     = main_fair.get("odd_1"),
            odd_X     = main_fair.get("odd_X"),
            odd_2     = main_fair.get("odd_2"),
            odd_over  = ou_fair.get("odd_over"),
            odd_under = ou_fair.get("odd_under"),
        )

    return result

