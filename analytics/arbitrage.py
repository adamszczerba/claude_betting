"""
Arbitrage detection across aligned bookmakers.

An arbitrage exists when sum(1 / best_odds_i) < 1 for a complete market.
The guaranteed profit percentage is (1 - sum(1/best_odds_i)) * 100.

Markets scanned:
  - 3-way (home / draw / away)
  - 2-way over/under

Usage
-----
>>> from analytics.arbitrage import scan_arb
>>> arb_signals = scan_arb(grouped)
>>> for a in arb_signals:
...     print(f"{a.match_key}  profit={a.guaranteed_profit_pct:.2f}%")
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

__all__ = ["ArbSignal", "ArbLeg", "scan_arb"]

MIN_PROFIT_PCT = 0.5  # ignore arbs below this (likely stale data)


@dataclass
class ArbLeg:
    bookmaker: str
    market:    str    # "odd_1" | "odd_X" | "odd_2" | "odd_over" | "odd_under"
    odds:      float


@dataclass
class ArbSignal:
    match_key:             str
    team1:                 str
    team2:                 str
    tournament:            str
    legs:                  List[ArbLeg]
    guaranteed_profit_pct: float
    timestamp:             datetime.datetime = field(default_factory=datetime.datetime.now)
    odds_snapshot:         dict              = field(default_factory=dict)


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


def _best_odds(group: dict, col: str) -> Optional[Tuple[str, float]]:
    """Return (bookmaker, best_odds) across all bookmakers for *col*."""
    best_bk: Optional[str] = None
    best_val: float = 0.0
    for bk, bk_data in group.get("odds", {}).items():
        if not bk_data:
            continue
        v = _safe_float(bk_data.get(col))
        if v is not None and v > best_val:
            best_val = v
            best_bk = bk
    if best_bk is None:
        return None
    return best_bk, best_val


def _check_arb(
    group: dict,
    cols: List[str],
    now: datetime.datetime,
) -> Optional[ArbSignal]:
    """Check if an arbitrage exists across *cols* in *group*."""
    legs: List[ArbLeg] = []
    for col in cols:
        result = _best_odds(group, col)
        if result is None:
            return None
        bk, odds = result
        legs.append(ArbLeg(bookmaker=bk, market=col, odds=odds))

    arb_sum = sum(1.0 / leg.odds for leg in legs)
    profit_pct = (1.0 - arb_sum) * 100.0

    if profit_pct < MIN_PROFIT_PCT:
        return None

    return ArbSignal(
        match_key             = _match_key(group),
        team1                 = group["team1"],
        team2                 = group["team2"],
        tournament            = group["tournament"],
        legs                  = legs,
        guaranteed_profit_pct = profit_pct,
        timestamp             = now,
        odds_snapshot         = {b: d for b, d in group.get("odds", {}).items()
                                 if d is not None},
    )


def scan_arb(grouped: List[dict]) -> List[ArbSignal]:
    """Scan all grouped match entries for arbitrage opportunities.

    Parameters
    ----------
    grouped : output of build_grouped_table()

    Returns
    -------
    List of ArbSignal sorted by guaranteed_profit_pct descending.
    """
    signals: List[ArbSignal] = []
    now = datetime.datetime.now()

    _3WAY = ["odd_1", "odd_X", "odd_2"]
    _2WAY = ["odd_over", "odd_under"]

    for group in grouped:
        # 3-way arb
        if sig := _check_arb(group, _3WAY, now):
            signals.append(sig)
        # Over/Under arb
        if sig := _check_arb(group, _2WAY, now):
            signals.append(sig)

    signals.sort(key=lambda s: s.guaranteed_profit_pct, reverse=True)
    return signals

