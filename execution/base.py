"""
Abstract Executor interface.

All execution backends must implement place_bet() and return a BetReceipt.
"""

from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from decisions.signal_router import BetOrder

__all__ = ["Executor", "BetReceipt", "PriceDriftError", "ExecutionError"]


class PriceDriftError(Exception):
    """Raised when the live price has slipped beyond the drift threshold."""


class ExecutionError(Exception):
    """General execution failure (network, DOM, etc.)."""


@dataclass
class BetReceipt:
    order_id:       str
    bookmaker:      str
    receipt_id:     str             # bookmaker's own bet ID
    accepted_price: float
    stake_eur:      float
    placed_at:      datetime.datetime = field(default_factory=datetime.datetime.now)
    notes:          str              = ""

    def as_dict(self) -> dict:
        return {
            "order_id":       self.order_id,
            "bookmaker":      self.bookmaker,
            "receipt_id":     self.receipt_id,
            "accepted_price": self.accepted_price,
            "stake_eur":      self.stake_eur,
            "placed_at":      self.placed_at.isoformat(),
            "notes":          self.notes,
        }


class Executor(ABC):
    """Abstract executor — one implementation per supported bookmaker."""

    @abstractmethod
    def place_bet(self, order: BetOrder) -> BetReceipt:
        """Place *order* and return a BetReceipt on success.

        Raise PriceDriftError if live price slips below order.min_price.
        Raise ExecutionError for any other failure.
        """

    def close(self) -> None:
        """Release resources (browser sessions, connections, etc.)."""

