"""
Risk Manager v2 — extended with new safety gates from NEW_ARCHITECTURE.md §4.3.3.

Adds:
  - trading_enabled() global safety gate (kill switch, daily loss limit, staleness)
  - Sport-level exposure tracking
  - Prematch vs live risk profiles
  - Signal agreement requirements
  - is_suspended market check
  - ML confidence gate

This module extends the existing RiskManager. Import from here for the new engine.
"""

from __future__ import annotations

import datetime
import logging
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
from decisions.engine import CompositeSignal

log = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
_MIN_RE = re.compile(r"^(\d+)")


def _load_config(path: str = _CONFIG_PATH) -> Dict[str, Any]:
    if _HAS_YAML:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    return {
        "value_thresholds": {"main_markets": 3.0, "niche_markets": 5.0},
        "risk": {
            "max_stake_eur": 50.0,
            "min_stake_eur": 1.0,
            "max_concurrent_bets": 3,
            "max_exposure_per_match_eur": 100.0,
            "max_exposure_per_sport_eur": 300.0,
            "suspend_on_status": ["HT", "FT", "AET", "PEN"],
            "min_minutes_remaining": 5,
            "kill_switch": False,
            "max_daily_loss_eur": 200.0,
            "staleness_sec": 10,
        },
        "leagues": {"whitelist": [], "blacklist": ["Esoccer", "Virtual"]},
        "signal_weights": {"comparator": 0.40, "ml": 0.30, "web_search": 0.15, "live_data": 0.15},
        "min_signal_sources": 2,
        "ml": {"enabled": False, "min_confidence": 0.7},
    }


class RiskManager:
    """Extended risk manager with v2 safety gates."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._cfg = config or _load_config()
        r = self._cfg.get("risk", {})
        vt = self._cfg.get("value_thresholds", {})
        lg = self._cfg.get("leagues", {})
        ml_cfg = self._cfg.get("ml", {})

        # Thresholds
        self.main_threshold: float = float(vt.get("main_markets", 3.0))
        self.niche_threshold: float = float(vt.get("niche_markets", 5.0))
        self.prematch_threshold: float = float(vt.get("prematch_markets", 2.0))

        # Stake limits
        self.max_stake: float = float(r.get("max_stake_eur", 50.0))
        self.min_stake: float = float(r.get("min_stake_eur", 1.0))

        # Concurrency / exposure
        self.max_concurrent: int = int(r.get("max_concurrent_bets", 3))
        self.max_exposure_per_match: float = float(r.get("max_exposure_per_match_eur", 100.0))
        self.max_exposure_per_sport: float = float(r.get("max_exposure_per_sport_eur", 300.0))

        # Status / time filters
        self.suspend_statuses: List[str] = r.get("suspend_on_status", ["HT", "FT", "AET", "PEN"])
        self.min_minutes_remaining: int = int(r.get("min_minutes_remaining", 5))

        # League filters
        self.whitelist: List[str] = lg.get("whitelist", [])
        self.blacklist: List[str] = lg.get("blacklist", ["Esoccer", "Virtual"])

        # Kill switch & daily loss limit
        self.kill_switch: bool = r.get("kill_switch", False)
        self.max_daily_loss_eur: float = float(r.get("max_daily_loss_eur", 200.0))
        self.staleness_sec: int = int(r.get("staleness_sec", 10))

        # Signal agreement
        self.min_signal_sources: int = self._cfg.get("min_signal_sources", 2)
        self.min_ml_confidence: float = float(ml_cfg.get("min_confidence", 0.7))

        # Runtime state
        self._exposure: Dict[str, float] = {}          # match_key → EUR
        self._sport_exposure: Dict[str, float] = {}    # sport → EUR
        self._active_bets: int = 0
        self._realized_loss_today: float = 0.0
        self._last_data_ts: Optional[datetime.datetime] = None

    # ------------------------------------------------------------------
    # Global safety gate (NEW in v2)
    # ------------------------------------------------------------------

    def trading_enabled(self) -> bool:
        """Global safety gate. Returns False to halt ALL betting."""
        if self.kill_switch:
            log.warning("KILL SWITCH ACTIVE — all betting halted")
            return False
        if self._realized_loss_today >= self.max_daily_loss_eur:
            log.warning("Daily loss limit reached (%.2f / %.2f EUR) — betting halted",
                        self._realized_loss_today, self.max_daily_loss_eur)
            return False
        if self._data_is_stale():
            log.warning("Data is stale (>%ds old) — betting halted", self.staleness_sec)
            return False
        return True

    def _data_is_stale(self) -> bool:
        if self.staleness_sec <= 0:
            return False
        if self._last_data_ts is None:
            return True
        age = (datetime.datetime.now() - self._last_data_ts).total_seconds()
        return age > self.staleness_sec

    def update_data_timestamp(self) -> None:
        """Call this when fresh data is received."""
        self._last_data_ts = datetime.datetime.now()

    # ------------------------------------------------------------------
    # Per-bet filter (extended for v2)
    # ------------------------------------------------------------------

    def filter(self, composite: CompositeSignal) -> bool:
        """Risk filter for CompositeSignal (v2 engine)."""
        # Match status
        if composite.match_status in self.suspend_statuses:
            return False

        # Market suspended
        if composite.is_suspended:
            return False

        # Concurrency
        if self._active_bets >= self.max_concurrent:
            return False

        # Per-match exposure
        if self._exposure.get(composite.match_key, 0.0) >= self.max_exposure_per_match:
            return False

        # Per-sport exposure
        if self._sport_exposure.get(composite.sport, 0.0) >= self.max_exposure_per_sport:
            return False

        # Signal agreement
        if composite.signal_count < self.min_signal_sources:
            return False

        # ML confidence gate (only if ML signal present)
        ml_breakdown = composite.signal_breakdown.get("ML", {})
        if ml_breakdown and ml_breakdown.get("avg_confidence", 1.0) < self.min_ml_confidence:
            log.debug("ML confidence %.2f below minimum %.2f",
                      ml_breakdown.get("avg_confidence", 0), self.min_ml_confidence)
            return False

        # Prematch risk profile
        if composite.is_prematch:
            return self._prematch_risk_check(composite)

        return True

    def allow_signal(self, signal: Union[ValueSignal, ArbSignal],
                     cc_odds_snapshot: Optional[dict] = None) -> bool:
        """Legacy filter for ValueSignal / ArbSignal (v1 compatibility)."""
        if isinstance(signal, ValueSignal):
            return self._allow_value(signal, cc_odds_snapshot)
        return self._allow_arb(signal)

    # ------------------------------------------------------------------
    # Legacy internal checks (v1 compat)
    # ------------------------------------------------------------------

    def _allow_value(self, sig: ValueSignal, cc_data: Optional[dict]) -> bool:
        threshold = self.main_threshold if sig.is_main else self.niche_threshold
        if sig.edge_pct < threshold:
            return False
        if sig.bookmaker != "coincasino":
            return False
        if not self._league_ok(sig.tournament):
            return False
        if cc_data is not None:
            status = cc_data.get("match_status", "")
            if status in self.suspend_statuses:
                return False
            if not self._enough_time(cc_data.get("match_time", "")):
                return False
        if self._active_bets >= self.max_concurrent:
            return False
        if self._exposure.get(sig.match_key, 0.0) >= self.max_exposure_per_match:
            return False
        return True

    def _allow_arb(self, sig: ArbSignal) -> bool:
        bks = {leg.bookmaker for leg in sig.legs}
        if "coincasino" not in bks:
            return False
        if not self._league_ok(sig.tournament):
            return False
        if self._active_bets >= self.max_concurrent:
            return False
        if self._exposure.get(sig.match_key, 0.0) >= self.max_exposure_per_match:
            return False
        return True

    def _prematch_risk_check(self, composite: CompositeSignal) -> bool:
        """Additional checks for prematch bets."""
        # Prematch bets are inherently riskier (more time for odds to move)
        # Require higher confidence
        if composite.confidence < 0.7:
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
        if self.min_minutes_remaining <= 0:
            return True
        if not match_time:
            return True
        m = _MIN_RE.match(match_time)
        if not m:
            return True
        minute = int(m.group(1))
        remaining = max(0, 90 - minute)
        return remaining >= self.min_minutes_remaining

    # ------------------------------------------------------------------
    # Bet tracking
    # ------------------------------------------------------------------

    def record_bet(self, match_key: str, stake: float, sport: str = "football") -> None:
        self._exposure[match_key] = self._exposure.get(match_key, 0.0) + stake
        self._sport_exposure[sport] = self._sport_exposure.get(sport, 0.0) + stake
        self._active_bets += 1

    def settle_bet(self, match_key: str, stake: float, pnl: float = 0.0,
                   sport: str = "football") -> None:
        self._exposure[match_key] = max(0.0, self._exposure.get(match_key, 0.0) - stake)
        self._sport_exposure[sport] = max(0.0, self._sport_exposure.get(sport, 0.0) - stake)
        self._active_bets = max(0, self._active_bets - 1)
        if pnl < 0:
            self._realized_loss_today += abs(pnl)
