"""
Signal router: converts filtered analytics signals into BetOrder objects
and writes them to the ledger.

Usage
-----
>>> from decisions.signal_router import SignalRouter
>>> router = SignalRouter(config)
>>> for signal in signals:
...     order = router.to_order(signal)
...     if order:
...         ledger.record_order(order)
"""

from __future__ import annotations

import datetime
import os
import random
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from analytics.value import ValueSignal
from analytics.arbitrage import ArbSignal
from decisions.kelly import kelly_stake

__all__ = ["BetOrder", "SignalRouter"]


@dataclass
class BetOrder:
    id:            str           = field(default_factory=lambda: str(uuid.uuid4()))
    created_at:    datetime.datetime = field(default_factory=datetime.datetime.now)
    bookmaker:     str           = "coincasino"
    match_key:     str           = ""
    team1:         str           = ""
    team2:         str           = ""
    tournament:    str           = ""
    market:        str           = ""   # "odd_1" | "odd_X" | ...
    outcome:       str           = ""   # human label, e.g. "1" | "X" | "2" | "over" | "under"
    stake_eur:     float         = 1.0
    min_price:     float         = 1.01  # abort if accepted_price < min_price
    edge_pct:      float         = 0.0
    signal_type:   str           = "VALUE"  # "VALUE" | "ARB"
    status:        str           = "PENDING"
    expiry_sec:    int           = 30        # order expires if not placed within N sec

    def as_dict(self) -> dict:
        return {
            "id":          self.id,
            "created_at":  self.created_at.isoformat(),
            "bookmaker":   self.bookmaker,
            "match_key":   self.match_key,
            "team1":       self.team1,
            "team2":       self.team2,
            "tournament":  self.tournament,
            "market":      self.market,
            "outcome":     self.outcome,
            "stake_eur":   self.stake_eur,
            "min_price":   self.min_price,
            "edge_pct":    self.edge_pct,
            "signal_type": self.signal_type,
            "status":      self.status,
            "expiry_sec":  self.expiry_sec,
        }


_MARKET_OUTCOME = {
    "odd_1":     "1",
    "odd_X":     "X",
    "odd_2":     "2",
    "odd_over":  "over",
    "odd_under": "under",
}


class SignalRouter:
    """Convert analytics signals to BetOrder objects."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        from decisions.risk_manager import _load_config
        cfg = config or _load_config()
        r = cfg.get("risk", {})
        self.bankroll:     float = float(cfg.get("bankroll_eur", 1000.0))
        self.fraction:     float = float(cfg.get("kelly_fraction", 0.25))
        self.min_stake:    float = float(r.get("min_stake_eur", 1.0))
        self.max_stake:    float = float(r.get("max_stake_eur", 50.0))
        self.jitter:       float = float(
            cfg.get("execution", {}).get("stake_jitter_fraction", 0.02))
        self.drift_thresh: float = float(
            cfg.get("execution", {}).get("price_drift_threshold", 0.02))

    def to_order(self, signal: Union[ValueSignal, ArbSignal]) -> Optional[BetOrder]:
        """Convert a signal to a BetOrder. Returns None if signal type not supported."""
        if isinstance(signal, ValueSignal):
            return self._value_order(signal)
        if isinstance(signal, ArbSignal):
            return self._arb_order(signal)
        return None

    # ------------------------------------------------------------------

    def _value_order(self, sig: ValueSignal) -> BetOrder:
        stake = kelly_stake(
            edge_pct     = sig.edge_pct,
            decimal_odds = sig.bookmaker_odds,
            bankroll     = self.bankroll,
            fraction     = self.fraction,
            min_stake    = self.min_stake,
            max_stake    = self.max_stake,
        )
        stake = self._jitter(stake)
        min_price = sig.bookmaker_odds * (1.0 - self.drift_thresh)

        return BetOrder(
            bookmaker   = sig.bookmaker,
            match_key   = sig.match_key,
            team1       = sig.team1,
            team2       = sig.team2,
            tournament  = sig.tournament,
            market      = sig.market,
            outcome     = _MARKET_OUTCOME.get(sig.market, sig.market),
            stake_eur   = round(stake, 2),
            min_price   = round(min_price, 4),
            edge_pct    = round(sig.edge_pct, 2),
            signal_type = "VALUE",
        )

    def _arb_order(self, sig: ArbSignal) -> Optional[BetOrder]:
        """Generate order for the CC leg of an arb (only leg we can execute)."""
        cc_leg = next((l for l in sig.legs if l.bookmaker == "coincasino"), None)
        if cc_leg is None:
            return None

        # Proportional stake for the CC leg: stake_i = total / (odds_i * n_legs)
        # Use min_stake as total for now — operator can adjust
        total_stake = self.min_stake * 5
        leg_stake = total_stake / (cc_leg.odds * len(sig.legs))
        leg_stake = self._jitter(max(self.min_stake, min(leg_stake, self.max_stake)))
        min_price = cc_leg.odds * (1.0 - self.drift_thresh)

        return BetOrder(
            bookmaker   = "coincasino",
            match_key   = sig.match_key,
            team1       = sig.team1,
            team2       = sig.team2,
            tournament  = sig.tournament,
            market      = cc_leg.market,
            outcome     = _MARKET_OUTCOME.get(cc_leg.market, cc_leg.market),
            stake_eur   = round(leg_stake, 2),
            min_price   = round(min_price, 4),
            edge_pct    = round(sig.guaranteed_profit_pct, 2),
            signal_type = "ARB",
        )

    def _jitter(self, stake: float) -> float:
        """Apply small random variation to stake for stealth, then re-clamp."""
        factor = 1.0 + random.uniform(-self.jitter, self.jitter)
        return max(self.min_stake, min(stake * factor, self.max_stake))

