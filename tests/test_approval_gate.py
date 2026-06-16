"""Tests for Approval Gate."""

import pytest
from decisions.approval import ApprovalGate
from decisions.signal_router import BetOrder


def _make_order(stake_eur=10.0):
    return BetOrder(
        match_key="Liverpool_vs_Arsenal_PL",
        team1="Liverpool",
        team2="Arsenal",
        tournament="Premier League",
        market="odd_1",
        outcome="1",
        stake_eur=stake_eur,
        min_price=2.0,
    )


class TestApprovalGate:
    def test_disabled_gate_never_requires(self):
        gate = ApprovalGate({"approval": {"enabled": False}})
        assert gate.requires_approval(_make_order(stake_eur=100.0)) is False

    def test_auto_approve_bypasses(self):
        gate = ApprovalGate({"approval": {"enabled": True, "auto_approve": True}})
        assert gate.requires_approval(_make_order(stake_eur=100.0)) is False

    def test_stake_below_threshold(self):
        gate = ApprovalGate({"approval": {"enabled": True, "auto_approve": False, "stake_threshold_eur": 25.0}})
        assert gate.requires_approval(_make_order(stake_eur=10.0)) is False

    def test_stake_above_threshold(self):
        gate = ApprovalGate({"approval": {"enabled": True, "auto_approve": False, "stake_threshold_eur": 25.0}})
        assert gate.requires_approval(_make_order(stake_eur=30.0)) is True

    def test_submit_and_approve(self):
        gate = ApprovalGate({"approval": {"enabled": True, "auto_approve": False}})
        order = _make_order(stake_eur=30.0)
        gate.submit_for_approval(order)
        pending = gate.get_pending()
        assert len(pending) == 1

        approved = gate.approve(order.id)
        assert approved is not None
        assert approved.id == order.id
        assert len(gate.get_pending()) == 0

    def test_reject(self):
        gate = ApprovalGate({"approval": {"enabled": True, "auto_approve": False}})
        order = _make_order(stake_eur=30.0)
        gate.submit_for_approval(order)
        gate.reject(order.id, "too risky")
        assert len(gate.get_pending()) == 0

    def test_approve_nonexistent(self):
        gate = ApprovalGate({"approval": {"enabled": True, "auto_approve": False}})
        result = gate.approve("nonexistent")
        assert result is None

    def test_double_approve_returns_none(self):
        gate = ApprovalGate({"approval": {"enabled": True, "auto_approve": False}})
        order = _make_order(stake_eur=30.0)
        gate.submit_for_approval(order)
        gate.approve(order.id)
        result = gate.approve(order.id)  # already approved
        assert result is None
