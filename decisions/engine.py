"""
Decision Engine — signal fusion, risk filtering, stake sizing, order creation.

Pipeline:
  1. Aggregate signals per match (from Signal Bus)
  2. Fuse signals into CompositeSignal (weighted average)
  3. Check edge vs threshold
  4. Risk filter (kill switch, exposure, staleness, signal agreement)
  5. Kelly stake sizing
  6. Human approval gate (optional)
  7. Emit BetOrder to ledger

Usage
-----
>>> engine = DecisionEngine(config, bus, risk_manager, ledger)
>>> orders = engine.evaluate()
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from decisions.kelly import kelly_stake
from decisions.signal_router import BetOrder, SignalRouter
from signals.bus import Signal, SignalBus

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Composite signal (fused from multiple providers)
# ---------------------------------------------------------------------------

@dataclass
class CompositeSignal:
    match_id: str = ""
    canonical_match_id: str = ""
    match_key: str = ""
    sport: str = "football"
    team1: str = ""
    team2: str = ""
    tournament: str = ""
    market: str = ""
    direction: str = "BACK"
    best_odds: float = 0.0
    best_bookmaker: str = ""
    best_odds_snapshot_ts: Optional[datetime.datetime] = None
    edge_pct: float = 0.0
    confidence: float = 0.0
    match_status: str = ""
    is_prematch: bool = False
    is_suspended: bool = False
    signal_count: int = 0
    signal_breakdown: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)
    _signals: List[Signal] = field(default_factory=list, repr=False)


# ---------------------------------------------------------------------------
# Decision Engine
# ---------------------------------------------------------------------------

class DecisionEngine:
    """Fuses signals from all providers into betting decisions."""

    # Default provider weights (overridden by config)
    DEFAULT_WEIGHTS = {
        "COMPARATOR": 0.40,
        "ML": 0.30,
        "WEB_SEARCH": 0.15,
        "LIVE_DATA": 0.15,
    }

    def __init__(
        self,
        config: Dict[str, Any],
        signal_bus: SignalBus,
        risk_manager,
        ledger,
        approval_gate=None,
    ):
        self._config = config
        self._bus = signal_bus
        self._risk = risk_manager
        self._ledger = ledger
        self._approval = approval_gate
        self._router = SignalRouter(config)

        sw = config.get("signal_weights", {})
        self._weights = {**self.DEFAULT_WEIGHTS, **sw}
        self._min_sources: int = config.get("min_signal_sources", 2)
        self._min_confidence: float = config.get("min_composite_confidence", 0.6)
        self._bankroll: float = float(config.get("bankroll_eur", 1000.0))
        self._kelly_fraction: float = float(config.get("kelly_fraction", 0.25))
        self._max_stake: float = float(config.get("risk", {}).get("max_stake_eur", 50.0))
        self._min_stake: float = float(config.get("risk", {}).get("min_stake_eur", 1.0))

    def evaluate(self) -> List[BetOrder]:
        """Main evaluation loop. Returns list of BetOrders created."""
        # Global safety gate
        if hasattr(self._risk, 'trading_enabled') and not self._risk.trading_enabled():
            log.warning("Trading disabled by risk manager (kill switch / loss limit / staleness)")
            return []

        # 1. Collect all active signals
        signals = self._bus.get_recent(last_n_sec=60)
        if not signals:
            log.debug("No active signals on bus.")
            return []

        # 2. Group by (canonical_match_id, market, direction)
        grouped = self._group_signals(signals)

        orders: List[BetOrder] = []
        for key, match_signals in grouped.items():
            # 3. Fuse signals
            composite = self._fuse_signals(match_signals)
            if composite is None:
                continue

            # 4. Check edge threshold
            min_edge = self._threshold_for(composite.market, composite.is_prematch)
            if composite.edge_pct < min_edge:
                log.debug("Edge %.2f%% below threshold %.2f%% for %s",
                          composite.edge_pct, min_edge, composite.match_key)
                continue

            # 5. Check composite confidence
            if composite.confidence < self._min_confidence:
                log.debug("Confidence %.2f below minimum %.2f for %s",
                          composite.confidence, self._min_confidence, composite.match_key)
                continue

            # 6. Risk filter
            if not self._risk.filter(composite):
                log.debug("Risk filter rejected: %s", composite.match_key)
                continue

            # 7. Size the bet
            stake = kelly_stake(
                edge_pct=composite.edge_pct,
                decimal_odds=composite.best_odds,
                bankroll=self._bankroll,
                fraction=self._kelly_fraction,
                min_stake=self._min_stake,
                max_stake=self._max_stake,
            )
            stake = min(stake, self._max_stake)

            # 8. Create order
            order = self._create_order(composite, stake)

            # 9. Approval gate
            if self._approval and self._approval.requires_approval(order):
                self._approval.submit_for_approval(order)
                log.info("Order %s submitted for approval: %s %s %.2f EUR",
                         order.id[:8], order.match_key, order.market, order.stake_eur)
                continue

            # 10. Record in ledger
            self._ledger.record_order(order, odds_snapshot={"composite": composite.signal_breakdown})
            if hasattr(self._risk, 'record_bet'):
                self._risk.record_bet(order.match_key, order.stake_eur)
            orders.append(order)
            log.info("BetOrder created: %s %s %.2f EUR edge=%.1f%% conf=%.2f signals=%d",
                     order.match_key, order.market, order.stake_eur,
                     composite.edge_pct, composite.confidence, composite.signal_count)

        return orders

    def _group_signals(self, signals: List[Signal]) -> Dict[str, List[Signal]]:
        """Group signals by (match_key, market, direction)."""
        groups: Dict[str, List[Signal]] = {}
        for s in signals:
            if s.is_expired():
                continue
            match_ref = s.canonical_match_id or s.match_key or s.match_id
            key = f"{match_ref}|{s.market}|{s.direction}"
            groups.setdefault(key, []).append(s)
        return groups

    def _fuse_signals(self, signals: List[Signal]) -> Optional[CompositeSignal]:
        """Fuse multiple signals into a single CompositeSignal.

        Returns None if signals conflict or insufficient agreement.
        """
        if not signals:
            return None

        # Check direction agreement
        directions = {s.direction for s in signals if s.direction != "NEUTRAL"}
        if len(directions) > 1:
            log.debug("Conflicting directions: %s — skipping", directions)
            return None

        # Check signal source diversity
        source_types = {s.signal_type for s in signals}
        if len(source_types) < self._min_sources:
            log.debug("Only %d signal type(s) — need %d", len(source_types), self._min_sources)
            return None

        # Weighted fusion
        total_weight = 0.0
        weighted_edge = 0.0
        weighted_confidence = 0.0
        best_odds = 0.0
        best_bookmaker = ""
        best_ts: Optional[datetime.datetime] = None
        breakdown: Dict[str, Dict[str, Any]] = {}

        for s in signals:
            w = self._weights.get(s.signal_type, 0.1)
            total_weight += w
            weighted_edge += s.edge_pct * w * s.confidence
            weighted_confidence += s.confidence * w

            # Track best odds from payload
            bk_odds = s.payload.get("bookmaker_odds", 0)
            if bk_odds and bk_odds > best_odds:
                best_odds = bk_odds
                best_bookmaker = s.payload.get("bookmaker", s.payload.get("best_bookmaker", ""))
                best_ts = s.timestamp

            # Breakdown by type
            st = s.signal_type
            if st not in breakdown:
                breakdown[st] = {"count": 0, "edges": [], "confidences": []}
            breakdown[st]["count"] += 1
            breakdown[st]["edges"].append(s.edge_pct)
            breakdown[st]["confidences"].append(s.confidence)

        if total_weight == 0:
            return None

        # Finalize breakdown stats
        for st in breakdown:
            edges = breakdown[st]["edges"]
            confs = breakdown[st]["confidences"]
            breakdown[st]["avg_edge"] = sum(edges) / len(edges)
            breakdown[st]["avg_confidence"] = sum(confs) / len(confs)
            del breakdown[st]["edges"]
            del breakdown[st]["confidences"]

        # Use the first signal for metadata (they should all be for the same match)
        primary = signals[0]
        return CompositeSignal(
            match_id=primary.match_id,
            canonical_match_id=primary.canonical_match_id,
            match_key=primary.match_key,
            sport=primary.sport,
            team1=primary.team1,
            team2=primary.team2,
            tournament=primary.tournament,
            market=primary.market,
            direction=primary.direction,
            best_odds=best_odds,
            best_bookmaker=best_bookmaker,
            best_odds_snapshot_ts=best_ts,
            edge_pct=round(weighted_edge / total_weight, 2),
            confidence=round(weighted_confidence / total_weight, 3),
            signal_count=len(signals),
            signal_breakdown=breakdown,
            timestamp=datetime.datetime.now(),
            _signals=signals,
        )

    def _threshold_for(self, market: str, is_prematch: bool) -> float:
        """Get the edge threshold for a given market type."""
        vt = self._config.get("value_thresholds", {})
        if is_prematch:
            return float(vt.get("prematch_markets", 2.0))
        if market in ("odd_1", "odd_X", "odd_2"):
            return float(vt.get("main_markets", 3.0))
        return float(vt.get("niche_markets", 5.0))

    def _create_order(self, composite: CompositeSignal, stake: float) -> BetOrder:
        """Create a BetOrder from a CompositeSignal."""
        from decisions.signal_router import _MARKET_OUTCOME
        return BetOrder(
            bookmaker=composite.best_bookmaker or "coincasino",
            match_key=composite.match_key,
            team1=composite.team1,
            team2=composite.team2,
            tournament=composite.tournament,
            market=composite.market,
            outcome=_MARKET_OUTCOME.get(composite.market, composite.market),
            stake_eur=round(stake, 2),
            min_price=round(composite.best_odds * 0.98, 4),
            edge_pct=composite.edge_pct,
            signal_type="FUSED",
        )
