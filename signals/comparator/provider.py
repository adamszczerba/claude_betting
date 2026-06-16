"""
Comparator Signal Provider — refactored from analytics/.

Reads latest CSV rows, computes fair odds via consensus,
detects value bets and arbitrage, publishes Signal objects to the Signal Bus.

Usage
-----
>>> provider = ComparatorSignalProvider(bus, db_root="match_database")
>>> signals = provider.scan()
"""

from __future__ import annotations

import datetime
import logging
import os
import sys
from typing import List

from signals.bus import Signal

log = logging.getLogger(__name__)

# Ensure project root is importable
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


class ComparatorSignalProvider:
    """Cross-bookmaker value & arb detection → Signal Bus."""

    PROVIDER_NAME = "comparator"
    PROVIDER_VERSION = "2.0.0"

    def __init__(self, bus, db_root: str = "match_database"):
        self._bus = bus
        self._db_root = db_root

    def scan(self) -> List[Signal]:
        """Run full scan: read odds (Redis or CSV) → consensus → value/arb → publish signals."""
        try:
            from dashboard.matcher import build_grouped_table
            from analytics.consensus import weighted_consensus
            from analytics.value import scan_value
            from analytics.arbitrage import scan_arb
        except ImportError as exc:
            log.error("Import error in comparator provider: %s", exc)
            return []

        # Try Redis streams first, fall back to CSV scan
        rows = self._fetch_rows()
        if not rows:
            log.debug("No odds rows found (Redis or CSV).")
            return []

        grouped = build_grouped_table(rows)
        if not grouped:
            log.debug("No grouped matches.")
            return []

        fair_map = weighted_consensus(grouped)

        signals: List[Signal] = []
        for sig in scan_value(grouped, fair_map):
            signals.append(self._value_to_signal(sig))
        for sig in scan_arb(grouped):
            signals.append(self._arb_to_signal(sig))

        # Publish all signals to the bus
        for sig in signals:
            self._bus.publish(sig)

        log.info("Comparator: published %d signals (%d value, %d arb)",
                 len(signals),
                 len([s for s in signals if "ARB" not in s.dedupe_key]),
                 len([s for s in signals if "ARB" in s.dedupe_key]))
        return signals

    def _fetch_rows(self) -> list:
        """Fetch latest odds rows from Redis streams, falling back to CSV scan."""
        # Try Redis first
        try:
            from scrapers.shared.stream_consumer import read_latest_from_streams, is_redis_available
            if is_redis_available():
                rows = read_latest_from_streams()
                if rows:
                    log.info("Comparator: read %d rows from Redis streams", len(rows))
                    return rows
                log.debug("Redis available but no stream data yet — falling back to CSV")
            else:
                log.debug("Redis unavailable — falling back to CSV scan")
        except Exception as exc:
            log.warning("Redis stream read failed (%s) — falling back to CSV", exc)

        # Fallback: scan CSV files
        try:
            from dashboard.data_service import scan_today
            rows = scan_today(db_root=self._db_root)
            log.info("Comparator: read %d rows from CSV files", len(rows))
            return rows
        except Exception as exc:
            log.error("CSV scan failed: %s", exc)
            return []

    def _value_to_signal(self, val) -> Signal:
        """Convert analytics ValueSignal → bus Signal."""
        direction = "BACK"  # value bets are always BACK
        edge = round(val.edge_pct, 2)
        dedupe_key = (
            f"{self.PROVIDER_NAME}|{val.match_key}|{val.market}|"
            f"{direction}|{edge:.1f}|{val.timestamp.strftime('%Y%m%d%H%M')}"
        )
        return Signal(
            signal_type="COMPARATOR",
            sport="football",
            match_id=val.match_key,
            canonical_match_id=val.match_key,  # will be resolved by matcher
            match_key=val.match_key,
            team1=val.team1,
            team2=val.team2,
            tournament=val.tournament,
            market=val.market,
            direction=direction,
            confidence=min(1.0, val.edge_pct / 10.0),  # rough confidence from edge
            edge_pct=edge,
            payload={
                "bookmaker": val.bookmaker,
                "bookmaker_odds": val.bookmaker_odds,
                "fair_odds": val.fair_odds,
                "odds_snapshot": val.odds_snapshot,
            },
            ttl_sec=120,
            dedupe_key=dedupe_key,
            provider_name=self.PROVIDER_NAME,
            provider_version=self.PROVIDER_VERSION,
        )

    def _arb_to_signal(self, arb) -> Signal:
        """Convert analytics ArbSignal → bus Signal."""
        dedupe_key = (
            f"{self.PROVIDER_NAME}|{arb.match_key}|ARB|"
            f"{arb.guaranteed_profit_pct:.1f}|{arb.timestamp.strftime('%Y%m%d%H%M')}"
        )
        return Signal(
            signal_type="COMPARATOR",
            sport="football",
            match_id=arb.match_key,
            canonical_match_id=arb.match_key,
            match_key=arb.match_key,
            team1=arb.team1,
            team2=arb.team2,
            tournament=arb.tournament,
            market="arb",
            direction="BACK",
            confidence=min(1.0, arb.guaranteed_profit_pct / 5.0),
            edge_pct=round(arb.guaranteed_profit_pct, 2),
            payload={
                "legs": [{"bookmaker": l.bookmaker, "market": l.market, "odds": l.odds}
                         for l in arb.legs],
                "guaranteed_profit_pct": arb.guaranteed_profit_pct,
                "odds_snapshot": arb.odds_snapshot,
            },
            ttl_sec=60,  # arbs expire fast
            dedupe_key=dedupe_key,
            provider_name=self.PROVIDER_NAME,
            provider_version=self.PROVIDER_VERSION,
        )
