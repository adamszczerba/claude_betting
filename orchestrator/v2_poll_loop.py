"""
Orchestrator v2 — signal bus integrated poll loop.

Instead of running analytics inline, this version:
  1. Runs the Comparator Signal Provider (publishes to bus)
  2. Runs the Decision Engine (consumes from bus → creates BetOrders)
  3. Keeps existing REST API + SSE from orchestrator/main.py

This module is designed to be imported and used alongside the existing
orchestrator, or as a drop-in replacement for _run_cycle().

Usage
-----
>>> from orchestrator.v2_poll_loop import OrchestratorV2
>>> orch = OrchestratorV2(db_root="match_database", ledger_path="ledger/bets.db")
>>> orch.run_cycle()   # single cycle
>>> orch.start()        # start background thread
"""

from __future__ import annotations

import datetime
import logging
import os
import sys
import threading
from typing import List

log = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


class OrchestratorV2:
    """Signal-bus-driven orchestrator."""

    def __init__(
        self,
        db_root: str = "match_database",
        ledger_path: str = "ledger/bets.db",
        config_path: str = "",
        signal_bus_backend: str = "memory",
    ):
        from decisions.risk_manager_v2 import _load_config, RiskManager
        from ledger.ledger import Ledger
        from signals.bus import SignalBus
        from decisions.engine import DecisionEngine
        from decisions.approval import ApprovalGate
        from signals.comparator.provider import ComparatorSignalProvider

        self._config = _load_config(config_path or os.path.join(_ROOT, "decisions", "config.yaml"))
        self._db_root = db_root

        # Initialize components
        self._bus = SignalBus(backend=signal_bus_backend)
        self._ledger = Ledger(ledger_path)
        self._risk = RiskManager(self._config)
        self._approval = ApprovalGate(self._config)
        self._engine = DecisionEngine(
            config=self._config,
            signal_bus=self._bus,
            risk_manager=self._risk,
            ledger=self._ledger,
            approval_gate=self._approval,
        )
        self._comparator = ComparatorSignalProvider(self._bus, db_root=db_root)

    def run_cycle(self) -> List:
        """Execute one full cycle: comparator scan → signal bus → decision engine."""
        now = datetime.datetime.now()
        log.info("=== Cycle start %s ===", now.isoformat())

        # 1. Run comparator signal provider (publishes to bus)
        comparator_signals = self._comparator.scan()
        log.info("Comparator published %d signals", len(comparator_signals))

        # 2. Update risk manager data timestamp (fresh data received)
        self._risk.update_data_timestamp()

        # 3. Expire stale orders
        expired = self._ledger.expire_stale_orders()
        if expired:
            log.info("Expired %d stale orders", expired)

        # 4. Run decision engine (consumes from bus → creates orders)
        orders = self._engine.evaluate()
        log.info("Decision engine created %d orders", len(orders))

        return orders

    def start(self, interval: float = 2.0) -> None:
        """Start background poll loop."""
        from scrapers.v2_coincasino import sleep_until_next_tick

        def _loop():
            while True:
                try:
                    self.run_cycle()
                except Exception:
                    log.exception("Cycle error")
                sleep_until_next_tick(interval)

        t = threading.Thread(target=_loop, daemon=True, name="orch-v2-poll")
        t.start()
        log.info("Orchestrator v2 poll loop started (interval=%.1fs)", interval)

    @property
    def bus(self):
        return self._bus

    @property
    def ledger(self):
        return self._ledger

    @property
    def risk(self):
        return self._risk

    @property
    def approval(self):
        return self._approval
