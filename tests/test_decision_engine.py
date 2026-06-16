"""Tests for Decision Engine — signal fusion, risk filtering, order creation."""

import datetime
import pytest
from unittest.mock import MagicMock, patch
from signals.bus import Signal, SignalBus
from decisions.engine import DecisionEngine, CompositeSignal


def _make_config(overrides=None):
    cfg = {
        "bankroll_eur": 1000.0,
        "kelly_fraction": 0.25,
        "value_thresholds": {
            "main_markets": 3.0,
            "niche_markets": 5.0,
            "prematch_markets": 2.0,
        },
        "signal_weights": {
            "COMPARATOR": 0.40,
            "ML": 0.30,
            "WEB_SEARCH": 0.15,
            "LIVE_DATA": 0.15,
        },
        "min_signal_sources": 2,
        "min_composite_confidence": 0.6,
        "risk": {
            "max_stake_eur": 50.0,
            "min_stake_eur": 1.0,
            "max_concurrent_bets": 3,
            "max_exposure_per_match_eur": 100.0,
            "max_exposure_per_sport_eur": 300.0,
            "suspend_on_status": ["HT", "FT", "AET", "PEN"],
            "kill_switch": False,
            "max_daily_loss_eur": 200.0,
            "staleness_sec": 10,
        },
        "leagues": {"whitelist": [], "blacklist": ["Esoccer", "Virtual"]},
        "ml": {"enabled": False, "min_confidence": 0.7},
        "approval": {"enabled": False, "auto_approve": True},
    }
    if overrides:
        cfg.update(overrides)
    return cfg


def _make_signal(signal_type="COMPARATOR", edge_pct=4.0, confidence=0.8,
                 market="odd_1", direction="BACK", match_key="Liverpool_vs_Arsenal_PL",
                 bookmaker="coincasino", bookmaker_odds=2.50, **kwargs):
    return Signal(
        signal_type=signal_type,
        match_key=match_key,
        team1="Liverpool",
        team2="Arsenal",
        tournament="Premier League",
        market=market,
        direction=direction,
        edge_pct=edge_pct,
        confidence=confidence,
        payload={
            "bookmaker": bookmaker,
            "bookmaker_odds": bookmaker_odds,
            "fair_odds": 2.30,
        },
        provider_name=signal_type.lower(),
        provider_version="1.0.0",
        **kwargs,
    )


class TestSignalFusion:
    def test_fuse_two_signals_same_direction(self):
        bus = SignalBus(backend="memory")
        risk = MagicMock()
        risk.trading_enabled.return_value = True
        risk.filter.return_value = True
        ledger = MagicMock()

        engine = DecisionEngine(_make_config(), bus, risk, ledger)

        s1 = _make_signal("COMPARATOR", edge_pct=6.0, confidence=0.9)
        s2 = _make_signal("ML", edge_pct=5.0, confidence=0.8)
        bus.publish(s1)
        bus.publish(s2)

        orders = engine.evaluate()
        assert len(orders) == 1
        assert orders[0].edge_pct > 0
        assert orders[0].market == "odd_1"

    def test_conflicting_directions_skipped(self):
        bus = SignalBus(backend="memory")
        risk = MagicMock()
        risk.trading_enabled.return_value = True
        risk.filter.return_value = True
        ledger = MagicMock()

        engine = DecisionEngine(_make_config(), bus, risk, ledger)

        s1 = _make_signal("COMPARATOR", direction="BACK")
        s2 = _make_signal("ML", direction="LAY")
        bus.publish(s1)
        bus.publish(s2)

        orders = engine.evaluate()
        assert len(orders) == 0

    def test_single_signal_type_rejected(self):
        """Need at least 2 independent signal types."""
        bus = SignalBus(backend="memory")
        risk = MagicMock()
        risk.trading_enabled.return_value = True
        risk.filter.return_value = True
        ledger = MagicMock()

        engine = DecisionEngine(_make_config(), bus, risk, ledger)

        s1 = _make_signal("COMPARATOR")
        bus.publish(s1)

        orders = engine.evaluate()
        assert len(orders) == 0

    def test_edge_below_threshold_rejected(self):
        bus = SignalBus(backend="memory")
        risk = MagicMock()
        risk.trading_enabled.return_value = True
        risk.filter.return_value = True
        ledger = MagicMock()

        engine = DecisionEngine(_make_config(), bus, risk, ledger)

        s1 = _make_signal("COMPARATOR", edge_pct=1.0)  # below 3% main threshold
        s2 = _make_signal("ML", edge_pct=1.0)
        bus.publish(s1)
        bus.publish(s2)

        orders = engine.evaluate()
        assert len(orders) == 0

    def test_kill_switch_blocks_all(self):
        bus = SignalBus(backend="memory")
        risk = MagicMock()
        risk.trading_enabled.return_value = False
        ledger = MagicMock()

        engine = DecisionEngine(_make_config(), bus, risk, ledger)

        s1 = _make_signal("COMPARATOR")
        s2 = _make_signal("ML")
        bus.publish(s1)
        bus.publish(s2)

        orders = engine.evaluate()
        assert len(orders) == 0

    def test_risk_filter_rejects(self):
        bus = SignalBus(backend="memory")
        risk = MagicMock()
        risk.trading_enabled.return_value = True
        risk.filter.return_value = False  # risk rejects everything
        ledger = MagicMock()

        engine = DecisionEngine(_make_config(), bus, risk, ledger)

        s1 = _make_signal("COMPARATOR")
        s2 = _make_signal("ML")
        bus.publish(s1)
        bus.publish(s2)

        orders = engine.evaluate()
        assert len(orders) == 0

    def test_composite_signal_properties(self):
        bus = SignalBus(backend="memory")
        risk = MagicMock()
        risk.trading_enabled.return_value = True
        risk.filter.return_value = True
        ledger = MagicMock()

        engine = DecisionEngine(_make_config(), bus, risk, ledger)

        s1 = _make_signal("COMPARATOR", edge_pct=7.0, confidence=0.9, bookmaker_odds=2.50)
        s2 = _make_signal("ML", edge_pct=6.0, confidence=0.8, bookmaker_odds=2.50)
        bus.publish(s1)
        bus.publish(s2)

        orders = engine.evaluate()
        assert len(orders) == 1
        order = orders[0]
        assert order.stake_eur >= 1.0
        assert order.stake_eur <= 50.0
        assert order.match_key == "Liverpool_vs_Arsenal_PL"
        assert order.signal_type == "FUSED"

    def test_no_signals_no_orders(self):
        bus = SignalBus(backend="memory")
        risk = MagicMock()
        risk.trading_enabled.return_value = True
        ledger = MagicMock()

        engine = DecisionEngine(_make_config(), bus, risk, ledger)
        orders = engine.evaluate()
        assert len(orders) == 0

    def test_multiple_markets_independent(self):
        """Different markets on same match should produce separate orders."""
        bus = SignalBus(backend="memory")
        risk = MagicMock()
        risk.trading_enabled.return_value = True
        risk.filter.return_value = True
        ledger = MagicMock()

        engine = DecisionEngine(_make_config(), bus, risk, ledger)

        # odd_1 signals
        bus.publish(_make_signal("COMPARATOR", market="odd_1", edge_pct=4.0))
        bus.publish(_make_signal("ML", market="odd_1", edge_pct=3.5))

        # odd_2 signals
        bus.publish(_make_signal("COMPARATOR", market="odd_2", edge_pct=5.0))
        bus.publish(_make_signal("ML", market="odd_2", edge_pct=4.0))

        orders = engine.evaluate()
        assert len(orders) == 2
        markets = {o.market for o in orders}
        assert "odd_1" in markets
        assert "odd_2" in markets


class TestThresholdFor:
    def test_main_market_threshold(self):
        engine = DecisionEngine(_make_config(), MagicMock(), MagicMock(), MagicMock())
        assert engine._threshold_for("odd_1", False) == 3.0
        assert engine._threshold_for("odd_X", False) == 3.0

    def test_niche_market_threshold(self):
        engine = DecisionEngine(_make_config(), MagicMock(), MagicMock(), MagicMock())
        assert engine._threshold_for("odd_over", False) == 5.0
        assert engine._threshold_for("odd_under", False) == 5.0

    def test_prematch_threshold(self):
        engine = DecisionEngine(_make_config(), MagicMock(), MagicMock(), MagicMock())
        assert engine._threshold_for("odd_1", True) == 2.0
