"""
Kelly criterion stake sizing (fractional Kelly).

Formula (standard Kelly):
    b = decimal_odds - 1
    p = fair probability (from consensus)
    q = 1 - p
    kelly_full = (b*p - q) / b  = p - q/b

The fractional variant multiplies by *fraction* (default ¼) to reduce variance
while still exploiting positive-expectation bets.

Usage
-----
>>> from decisions.kelly import kelly_stake
>>> stake = kelly_stake(edge_pct=4.5, decimal_odds=2.30, bankroll=1000.0)
>>> print(f"Stake: €{stake:.2f}")
"""

from __future__ import annotations

__all__ = ["kelly_stake", "implied_probability"]


def implied_probability(decimal_odds: float) -> float:
    """Fair implied probability from decimal odds (no margin assumed)."""
    if decimal_odds <= 1.0:
        raise ValueError(f"Odds must be > 1.0, got {decimal_odds}")
    return 1.0 / decimal_odds


def kelly_stake(
    edge_pct:     float,
    decimal_odds: float,
    bankroll:     float,
    fraction:     float = 0.25,
    min_stake:    float = 1.0,
    max_stake:    float = 50.0,
) -> float:
    """Compute a fractional Kelly stake in EUR.

    Parameters
    ----------
    edge_pct     : expected edge expressed as a percentage (e.g. 4.5 for 4.5%)
    decimal_odds : bookmaker offered odds (e.g. 2.30)
    bankroll     : current bankroll in EUR
    fraction     : Kelly fraction (default 0.25 = ¼-Kelly)
    min_stake    : minimum stake floor in EUR
    max_stake    : maximum stake cap in EUR

    Returns
    -------
    Stake in EUR, clamped to [min_stake, max_stake].
    Returns min_stake if Kelly formula yields ≤ 0 (no positive expectation).
    """
    if decimal_odds <= 1.0:
        raise ValueError(f"Odds must be > 1.0, got {decimal_odds}")
    if bankroll <= 0:
        raise ValueError("Bankroll must be positive")

    b = decimal_odds - 1.0          # net profit per unit staked (if win)
    # Derive fair probability from the edge:
    # edge = (bk_odds / fair_odds - 1)  →  fair_odds = bk_odds / (1 + edge/100)
    fair_odds = decimal_odds / (1.0 + edge_pct / 100.0)
    p = 1.0 / fair_odds             # fair win probability
    q = 1.0 - p                     # fair loss probability

    kelly_full = (b * p - q) / b    # full-Kelly fraction of bankroll

    if kelly_full <= 0:
        return min_stake

    stake = bankroll * kelly_full * fraction
    return max(min_stake, min(stake, max_stake))

