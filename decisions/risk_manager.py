"""
Risk manager: filter analytics signals using configurable thresholds.

Reads thresholds from decisions/config.yaml (or a supplied dict).

Usage
-----
>>> from decisions.risk_manager import RiskManager
>>> rm = RiskManager()
>>> rm.allow_signal(signal)   # True / False
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Union

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

from analytics.value import ValueSignal
from analytics.arbitrage import ArbSignal

__all__ = ["RiskManager"]

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")

# Regex to extract minutes from match_time strings like "67:23", "90+4", "HT"
_MIN_RE = re.compile(r"^(\d+)")


def _load_config(path: str = _CONFIG_PATH) -> Dict[str, Any]:
    if _HAS_YAML:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    # Fallback defaults — no PyYAML installed
    return {
        "value_thresholds": {"main_markets": 3.0, "niche_markets": 5.0},
        "risk": {
            "max_stake_eur": 50.0,
            "min_stake_eur": 1.0,
            "max_concurrent_bets": 3,
            "max_exposure_per_match_eur": 100.0,
            "suspend_on_status": ["HT", "FT", "AET", "PEN"],
            "min_minutes_remaining": 5,
        },
        "leagues": {"whitelist": [], "blacklist": ["Esoccer", "Virtual"]},
    }


class RiskManager:
    """Stateless signal filter (plus optional concurrent-bet tracking)."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._cfg = config or _load_config()
        r = self._cfg.get("risk", {})
        vt = self._cfg.get("value_thresholds", {})
        lg = self._cfg.get("leagues", {})

        self.main_threshold:    float      = vt.get("main_markets", 3.0)
        self.niche_threshold:   float      = vt.get("niche_markets", 5.0)
        self.max_stake:         float      = r.get("max_stake_eur", 50.0)
        self.min_stake:         float      = r.get("min_stake_eur", 1.0)
        self.max_concurrent:    int        = r.get("max_concurrent_bets", 3)
        self.max_exposure:      float      = r.get("max_exposure_per_match_eur", 100.0)
        self.suspend_statuses:  List[str]  = r.get("suspend_on_status",
                                                    ["HT", "FT", "AET", "PEN"])
        self.min_minutes_remaining: int    = r.get("min_minutes_remaining", 5)
        self.whitelist:         List[str]  = lg.get("whitelist", [])
        self.blacklist:         List[str]  = lg.get("blacklist", ["Esoccer", "Virtual"])

        # Runtime state: match_key → current exposure in EUR
        self._exposure: Dict[str, float] = {}
        self._active_bets: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def allow_signal(self, signal: Union[ValueSignal, "ArbSignal"],
                     cc_odds_snapshot: Optional[dict] = None) -> bool:
        """Return True if this signal should generate a BetOrder."""
        if isinstance(signal, ValueSignal):
            return self._allow_value(signal, cc_odds_snapshot)
        return self._allow_arb(signal)

    def record_bet(self, match_key: str, stake: float) -> None:
        """Track a placed bet for exposure/concurrency accounting."""
        self._exposure[match_key] = self._exposure.get(match_key, 0.0) + stake
        self._active_bets += 1

    def settle_bet(self, match_key: str, stake: float) -> None:
        """Reduce exposure when a bet settles (win or loss)."""
        self._exposure[match_key] = max(0.0, self._exposure.get(match_key, 0.0) - stake)
        self._active_bets = max(0, self._active_bets - 1)

    # ------------------------------------------------------------------
    # Internal checks
    # ------------------------------------------------------------------

    def _allow_value(self, sig: ValueSignal,
                     cc_data: Optional[dict]) -> bool:
        # Edge threshold
        threshold = (self.main_threshold if sig.is_main else self.niche_threshold)
        if sig.edge_pct < threshold:
            return False

        # Only execute on CoinCasino (only supported executor)
        if sig.bookmaker != "coincasino":
            return False

        # League filters
        if not self._league_ok(sig.tournament):
            return False

        # Match status filter (use CC data when available)
        if cc_data is not None:
            status = cc_data.get("match_status", "")
            if status in self.suspend_statuses:
                return False
            # Minutes remaining check
            match_time = cc_data.get("match_time", "")
            if not self._enough_time(match_time):
                return False

        # Concurrency / exposure
        if self._active_bets >= self.max_concurrent:
            return False
        if self._exposure.get(sig.match_key, 0.0) >= self.max_exposure:
            return False

        return True

    def _allow_arb(self, sig: "ArbSignal") -> bool:
        # Arb legs must include CoinCasino
        bks = {leg.bookmaker for leg in sig.legs}
        if "coincasino" not in bks:
            return False
        if not self._league_ok(sig.tournament):
            return False
        if self._active_bets >= self.max_concurrent:
            return False
        if self._exposure.get(sig.match_key, 0.0) >= self.max_exposure:
            return False
        return True

    def _league_ok(self, tournament: str) -> bool:
        t_lower = tournament.lower()
        if self.whitelist:
            if not any(w.lower() in t_lower for w in self.whitelist):
                return False
        for bl in self.blacklist:
            if bl.lower() in t_lower:
                return False
        return True

    def _enough_time(self, match_time: str) -> bool:
        """Return True if enough time remains (based on match_time string)."""
        if self.min_minutes_remaining <= 0:
            return True
        if not match_time:
            return True   # unknown → allow, let execution verify
        m = _MIN_RE.match(match_time)
        if not m:
            return True
        minute = int(m.group(1))
        # 90+ minute → treat as < 5 minutes remaining
        remaining = max(0, 90 - minute)
        return remaining >= self.min_minutes_remaining

