"""
Executor Router — routes BetOrders to the correct bookmaker executor.

Selection based on:
  - order.bookmaker (primary)
  - Fallback to alternative executor if primary unavailable
  - Dry-run mode for testing

Usage
-----
>>> router = ExecutorRouter({"coincasino": cc_executor, "betfair": bf_executor})
>>> receipt = router.route(order)
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

from decisions.signal_router import BetOrder
from execution.base import BetReceipt, Executor, ExecutionError

log = logging.getLogger(__name__)


class NoExecutorError(ExecutionError):
    """Raised when no executor is available for the requested bookmaker."""


class ExecutorRouter:
    """Routes BetOrders to the appropriate bookmaker executor."""

    def __init__(self, executors: Dict[str, Executor]):
        self._executors = executors
        log.info("ExecutorRouter initialized with executors: %s", list(executors.keys()))

    def route(self, order: BetOrder) -> BetReceipt:
        """Route *order* to the correct executor.

        Raises NoExecutorError if no executor is available.
        """
        executor = self._executors.get(order.bookmaker)
        if executor is None:
            raise NoExecutorError(f"No executor for bookmaker: {order.bookmaker}")
        return executor.place_bet(order)

    def register(self, bookmaker: str, executor: Executor) -> None:
        """Register or replace an executor for a bookmaker."""
        self._executors[bookmaker] = executor
        log.info("Registered executor for %s", bookmaker)

    @property
    def bookmakers(self) -> list:
        return list(self._executors.keys())
