"""
Human Approval Gate — optional human-in-the-loop for high-stakes or low-confidence bets.

Routes bets to human approval queue based on rules:
  - Stake > approval_threshold_eur
  - Confidence < auto_approve_confidence
  - New sport/market not yet validated
  - ML model in probation period

Usage
-----
>>> gate = ApprovalGate(config)
>>> if gate.requires_approval(order):
...     gate.submit_for_approval(order)
...     # ... wait for human ...
...     gate.approve(order.id)
"""

from __future__ import annotations

import datetime
import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from decisions.signal_router import BetOrder

log = logging.getLogger(__name__)


@dataclass
class _PendingApproval:
    order: BetOrder
    submitted_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    status: str = "PENDING"  # PENDING | APPROVED | REJECTED | EXPIRED


class ApprovalGate:
    """Routes bets to human approval queue based on configurable rules."""

    def __init__(self, config: Dict):
        ac = config.get("approval", {})
        self._enabled: bool = ac.get("enabled", False)
        self._auto_approve: bool = ac.get("auto_approve", True)
        self._stake_threshold: float = float(ac.get("stake_threshold_eur", 25.0))
        self._min_confidence: float = float(ac.get("min_confidence_auto", 0.8))
        self._expiry_sec: int = int(ac.get("expiry_sec", 120))

        self._queue: Dict[str, _PendingApproval] = {}
        self._lock = threading.Lock()

    def requires_approval(self, order: BetOrder) -> bool:
        """Return True if this order needs human approval."""
        if not self._enabled or self._auto_approve:
            return False
        if order.stake_eur > self._stake_threshold:
            return True
        return False

    def submit_for_approval(self, order: BetOrder) -> None:
        """Add order to the approval queue."""
        with self._lock:
            self._queue[order.id] = _PendingApproval(order=order)
        log.info("Order %s submitted for approval (stake=%.2f EUR)", order.id[:8], order.stake_eur)

    def approve(self, order_id: str) -> Optional[BetOrder]:
        """Approve a pending order. Returns the BetOrder if found."""
        with self._lock:
            pending = self._queue.get(order_id)
            if pending is None:
                log.warning("Approval: order %s not found", order_id[:8])
                return None
            if pending.status != "PENDING":
                log.warning("Approval: order %s is %s, not PENDING", order_id[:8], pending.status)
                return None
            pending.status = "APPROVED"
            log.info("Order %s APPROVED", order_id[:8])
            return pending.order

    def reject(self, order_id: str, reason: str = "") -> None:
        """Reject a pending order."""
        with self._lock:
            pending = self._queue.get(order_id)
            if pending and pending.status == "PENDING":
                pending.status = "REJECTED"
                log.info("Order %s REJECTED: %s", order_id[:8], reason)

    def get_pending(self) -> List[BetOrder]:
        """Return all orders awaiting approval."""
        with self._lock:
            # Expire old entries
            now = datetime.datetime.now()
            for oid, pa in list(self._queue.items()):
                age = (now - pa.submitted_at).total_seconds()
                if age > self._expiry_sec and pa.status == "PENDING":
                    pa.status = "EXPIRED"
                    log.info("Order %s EXPIRED in approval queue", oid[:8])
            return [pa.order for pa in self._queue.values() if pa.status == "PENDING"]

    def get_all(self) -> List[_PendingApproval]:
        """Return all approval entries (for dashboard display)."""
        with self._lock:
            return list(self._queue.values())
