"""
Value bet detection.

A ValueSignal is emitted when a bookmaker's offered odds exceed the consensus
fair odds by at least the configured threshold.

  edge_pct = (bookmaker_odds / fair_odds - 1) * 100

Usage
-----
>>> from analytics.value import scan_value
>>> signals = scan_value(grouped, fair_map)
>>> for s in signals:
...     print(s.bookmaker, s.market, f"{s.edge_pct:+.1f}%")
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from analytics.consensus import FairOdds

__all__ = ["ValueSignal", "scan_value"]

# Default thresholds — overridden by decisions/config.yaml at runtime
DEFAULT_MAIN_THRESHOLD_PCT  = 3.0   # odd_1, odd_X, odd_2
DEFAULT_NICHE_THRESHOLD_PCT = 5.0   # odd_over, odd_under

_MAIN_COLS  = {"odd_1", "odd_X", "odd_2"}
_NICHE_COLS = {"odd_over", "odd_under"}
_ALL_COLS   = _MAIN_COLS | _NICHE_COLS


@dataclass
class ValueSignal:
    match_key:      str
    team1:          str
    team2:          str
    tournament:     str
    bookmaker:      str
    market:         str   # "odd_1" | "odd_X" | "odd_2" | "odd_over" | "odd_under"
    bookmaker_odds: float
    fair_odds:      float
    edge_pct:       float # (bookmaker_odds / fair_odds - 1) * 100
    timestamp:      datetime.datetime = field(default_factory=datetime.datetime.now)
    odds_snapshot:  dict = field(default_factory=dict)

    @property
    def is_main(self) -> bool:
        return self.market in _MAIN_COLS


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 1.0 else None
    except (ValueError, TypeError):
        return None


def _match_key(group: dict) -> str:
    return f"{group['team1']}_vs_{group['team2']}_{group['tournament']}"


def scan_value(
    grouped: List[dict],
    fair_map: Dict[str, FairOdds],
    main_threshold_pct: float  = DEFAULT_MAIN_THRESHOLD_PCT,
    niche_threshold_pct: float = DEFAULT_NICHE_THRESHOLD_PCT,
) -> List[ValueSignal]:
    """Scan all bookmaker odds in *grouped* for value bets vs *fair_map*.

    Parameters
    ----------
    grouped              : output of build_grouped_table()
    fair_map             : output of weighted_consensus()
    main_threshold_pct   : minimum edge % for 1X2 markets
    niche_threshold_pct  : minimum edge % for over/under markets

    Returns
    -------
    Sorted list of ValueSignal (highest edge first).
    """
    signals: List[ValueSignal] = []
    now = datetime.datetime.now()

    for group in grouped:
        key = _match_key(group)
        fair = fair_map.get(key)
        if fair is None:
            continue

        fair_dict = fair.as_dict()

        for bk, bk_data in group.get("odds", {}).items():
            if not bk_data:
                continue

            for col in _ALL_COLS:
                fair_odds = fair_dict.get(col)
                if fair_odds is None or fair_odds <= 1.0:
                    continue

                bk_odds = _safe_float(bk_data.get(col))
                if bk_odds is None:
                    continue

                edge_pct = (bk_odds / fair_odds - 1.0) * 100.0
                threshold = main_threshold_pct if col in _MAIN_COLS else niche_threshold_pct

                if edge_pct >= threshold:
                    signals.append(ValueSignal(
                        match_key      = key,
                        team1          = group["team1"],
                        team2          = group["team2"],
                        tournament     = group["tournament"],
                        bookmaker      = bk,
                        market         = col,
                        bookmaker_odds = bk_odds,
                        fair_odds      = fair_odds,
                        edge_pct       = edge_pct,
                        timestamp      = now,
                        odds_snapshot  = {b: d for b, d in group.get("odds", {}).items()
                                          if d is not None},
                    ))

    signals.sort(key=lambda s: s.edge_pct, reverse=True)
    return signals

