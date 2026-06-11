"""
Dry-run executor — logs orders, never places real bets.

This is the DEFAULT executor. Set EXECUTION_MODE=live in the environment
(or decisions/config.yaml) to enable the real CoinCasino executor.

Usage
-----
>>> from execution.dry_run import DryRunExecutor
>>> executor = DryRunExecutor()
>>> receipt = executor.place_bet(order)
"""

from __future__ import annotations

import datetime
import logging
import uuid

from decisions.signal_router import BetOrder
from execution.base import BetReceipt, Executor

log = logging.getLogger(__name__)

__all__ = ["DryRunExecutor"]


class DryRunExecutor(Executor):
    """Paper-trading executor. Records every order as if placed, no HTTP calls."""

    def __init__(self):
        log.info("[DRY-RUN] Executor active — no real bets will be placed.")

    def place_bet(self, order: BetOrder) -> BetReceipt:
        receipt = BetReceipt(
            order_id       = order.id,
            bookmaker      = order.bookmaker,
            receipt_id     = f"DRY-{uuid.uuid4().hex[:8].upper()}",
            accepted_price = order.min_price,   # simulate best-case acceptance
            stake_eur      = order.stake_eur,
            placed_at      = datetime.datetime.now(),
            notes          = "dry_run",
        )
        log.info(
            "[DRY-RUN] Would place %.2f EUR on %s / %s @ %.4f  "
            "(match: %s | edge: %+.1f%%)",
            order.stake_eur, order.market, order.outcome,
            order.min_price, order.match_key, order.edge_pct,
        )
        return receipt

    def close(self) -> None:
        pass

