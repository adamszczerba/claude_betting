"""Tests for Executor Router."""

import pytest
from unittest.mock import MagicMock
from execution.router import ExecutorRouter, NoExecutorError
from execution.base import BetReceipt
from decisions.signal_router import BetOrder


def _make_order(bookmaker="coincasino"):
    return BetOrder(
        bookmaker=bookmaker,
        match_key="Liverpool_vs_Arsenal_PL",
        team1="Liverpool",
        team2="Arsenal",
        tournament="Premier League",
        market="odd_1",
        outcome="1",
        stake_eur=10.0,
        min_price=2.0,
    )


class TestExecutorRouter:
    def test_route_to_correct_executor(self):
        cc_executor = MagicMock()
        cc_executor.place_bet.return_value = BetReceipt(
            order_id="o1", bookmaker="coincasino", receipt_id="r1",
            accepted_price=2.0, stake_eur=10.0,
        )
        bf_executor = MagicMock()

        router = ExecutorRouter({"coincasino": cc_executor, "betfair": bf_executor})
        order = _make_order("coincasino")
        receipt = router.route(order)

        cc_executor.place_bet.assert_called_once_with(order)
        bf_executor.place_bet.assert_not_called()
        assert receipt.receipt_id == "r1"

    def test_no_executor_raises(self):
        router = ExecutorRouter({})
        order = _make_order("coincasino")
        with pytest.raises(NoExecutorError):
            router.route(order)

    def test_register_new_executor(self):
        router = ExecutorRouter({})
        new_exec = MagicMock()
        router.register("pinnacle", new_exec)
        assert "pinnacle" in router.bookmakers

    def test_bookmakers_list(self):
        router = ExecutorRouter({"cc": MagicMock(), "bf": MagicMock()})
        assert set(router.bookmakers) == {"cc", "bf"}
