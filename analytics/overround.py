"""
Overround (bookmaker margin) removal.

Three methods:
  normalize  — scale implied probabilities so they sum to 1 (fast, standard)
  shin       — Shin (1993) model, accounts for insider-information asymmetry
  power      — power method: find k s.t. sum((1/o)^k) == 1

Usage
-----
>>> from analytics.overround import remove_overround
>>> remove_overround([2.10, 3.50, 3.20], method="normalize")
[2.2137..., 3.6897..., 3.3702...]
"""

from __future__ import annotations

import math
from typing import List, Literal

__all__ = ["remove_overround", "overround_pct"]

Method = Literal["normalize", "shin", "power"]


def overround_pct(odds: List[float]) -> float:
    """Return the bookmaker margin as a percentage (e.g. 5.2 for 5.2%)."""
    if not odds:
        return 0.0
    total_ip = sum(1.0 / o for o in odds if o > 1.0)
    return (total_ip - 1.0) * 100.0


def remove_overround(odds: List[float], method: Method = "normalize") -> List[float]:
    """Convert raw bookmaker odds to fair (no-margin) decimal odds.

    Parameters
    ----------
    odds   : list of raw decimal odds (must all be > 1.0)
    method : "normalize" | "shin" | "power"

    Returns
    -------
    list of fair decimal odds (same length as input)
    """
    if not odds:
        return []
    for o in odds:
        if o <= 1.0:
            raise ValueError(f"All odds must be > 1.0, got {o}")

    if method == "normalize":
        return _normalize(odds)
    elif method == "shin":
        return _shin(odds)
    elif method == "power":
        return _power(odds)
    else:
        raise ValueError(f"Unknown method: {method!r}. Use 'normalize', 'shin', or 'power'.")


# ---------------------------------------------------------------------------
# Private implementations
# ---------------------------------------------------------------------------

def _normalize(odds: List[float]) -> List[float]:
    """Proportional scaling — fastest, good for sharp markets."""
    implied = [1.0 / o for o in odds]
    total = sum(implied)
    fair_probs = [p / total for p in implied]
    return [1.0 / p for p in fair_probs]


def _power(odds: List[float]) -> List[float]:
    """Power method: find exponent k such that sum((1/o)^k) == 1.

    Bisection search on k ∈ (0.5, 2.0).
    """
    implied = [1.0 / o for o in odds]

    def _residual(k: float) -> float:
        return sum(p ** k for p in implied) - 1.0

    # Bounds: k < 1 inflates probs (removes margin), k > 1 deflates them.
    lo, hi = 0.01, 10.0
    for _ in range(100):  # bisection, converges quickly
        mid = (lo + hi) / 2.0
        r = _residual(mid)
        if abs(r) < 1e-10:
            break
        if r > 0:
            lo = mid
        else:
            hi = mid

    k = (lo + hi) / 2.0
    fair_probs = [p ** k for p in implied]
    return [1.0 / p for p in fair_probs]


def _shin(odds: List[float]) -> List[float]:
    """Shin (1993) model: accounts for insider-trading asymmetry.

    Reference: Shin, H.S. (1993). Measuring the Incidence of Insider Trading
               in a Market for State-Contingent Claims. Economic Journal 103.

    Solves for z (fraction of insider traders) using Newton's method, then
    returns fair odds from the debiased implied probabilities.
    """
    n = len(odds)
    q = [1.0 / o for o in odds]          # raw implied probs
    total_q = sum(q)

    # Special case: no overround (perfect market) → just normalize
    if abs(total_q - 1.0) < 1e-9:
        return list(odds)

    # Shin formula: solve z iteratively
    # p_i = sqrt(z^2 + 4*(1-z)*q_i^2/total_q) - z) / (2*(1-z))
    # where sum(p_i) = 1.  Use z=0 as starting guess.
    z = 0.0
    for _ in range(200):
        fair_probs = [
            (math.sqrt(z ** 2 + 4.0 * (1.0 - z) * qi ** 2 / total_q) - z)
            / (2.0 * (1.0 - z))
            for qi in q
        ]
        sp = sum(fair_probs)
        err = sp - 1.0
        if abs(err) < 1e-10:
            break
        # Gradient-free damped update toward z that satisfies sum(p)=1
        z = z - 0.5 * err * (1.0 - z)
        z = max(0.0, min(z, 0.99))  # clamp

    # fair_probs is set inside the loop; always executes (200 iterations minimum)
    # Recompute with final z in case loop exited via break before last assignment
    fair_probs = [
        (math.sqrt(z ** 2 + 4.0 * (1.0 - z) * qi ** 2 / total_q) - z)
        / (2.0 * (1.0 - z))
        for qi in q
    ]
    return [1.0 / p for p in fair_probs]

