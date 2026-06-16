"""Tests for Risk Manager v2."""

import datetime
import pytest
from decisions.risk_manager_v2 import RiskManager
from decisions.engine import CompositeSignal


def _make_config(overrides=None):
    cfg = {
        "value_thresholds": {"main_markets": 3.0, "niche_markets": 5.0},
        "risk": {
            "max_stake_eur": 50.0,
            "min_stake_eur": 1.0,
            "max_concurrent_bets": 3,
            "max_exposure_per_match_eur": 100.0,
            "max_exposure_per_sport_eur": 300.0,
            "suspend_on_status": ["HT", "FT", "AET", "PEN"],
            "min_minutes_remaining": 5,
            "kill_switch": False,
            "max_daily_loss_eur": 200.0,
            "staleness_sec": 10,
        },
        "leagues": {"whitelist": [], "blacklist": ["Esoccer", "Virtual"]},
        "signal_weights": {"comparator": 0.40, "ml": 0.30},
        "min_signal_sources": 2,
        "ml": {"enabled": False, "min_confidence": 0.7},
    }
    if overrides:
        cfg.update(overrides)
    return cfg


def _make_composite(signal_count=2, match_status="", is_suspended=False,
                    is_prematch=False, sport="football", confidence=0.8,
                    ml_confidence=0.9):
    breakdown = {"COMPARATOR": {"count": 1, "avg_edge": 4.0, "avg_confidence": 0.8}}
    if signal_count >= 2:
        breakdown["ML"] = {"count": 1, "avg_edge": 3.0, "avg_confidence": ml_confidence}
    return CompositeSignal(
        match_key="Liverpool_vs_Arsenal_PL",
        sport=sport,
        match_status=match_status,
        is_suspended=is_suspended,
        is_prematch=is_prematch,
        signal_count=signal_count,
        signal_breakdown=breakdown,
        confidence=confidence,
    )


class TestTradingEnabled:
    def test_trading_enabled_by_default(self):
        rm = RiskManager(_make_config())
        rm.update_data_timestamp()
        assert rm.trading_enabled() is True

    def test_kill_switch_blocks(self):
        cfg = _make_config({"risk": {"kill_switch": True, "max_daily_loss_eur": 200.0, "staleness_sec": 10}})
        rm = RiskManager(cfg)
        assert rm.trading_enabled() is False

    def test_daily_loss_limit_blocks(self):
        rm = RiskManager(_make_config())
        rm._realized_loss_today = 200.0
        assert rm.trading_enabled() is False

    def test_staleness_blocks(self):
        rm = RiskManager(_make_config())
        rm._last_data_ts = datetime.datetime.now() - datetime.timedelta(seconds=20)
        rm.staleness_sec = 10
        assert rm.trading_enabled() is False

    def test_fresh_data_passes(self):
        rm = RiskManager(_make_config())
        rm.update_data_timestamp()
        assert rm.trading_enabled() is True


class TestFilter:
    def test_normal_signal_passes(self):
        rm = RiskManager(_make_config())
        comp = _make_composite()
        assert rm.filter(comp) is True

    def test_suspended_match_blocked(self):
        rm = RiskManager(_make_config())
        comp = _make_composite(match_status="HT")
        assert rm.filter(comp) is False

    def test_market_suspended_blocked(self):
        rm = RiskManager(_make_config())
        comp = _make_composite(is_suspended=True)
        assert rm.filter(comp) is False

    def test_insufficient_signal_sources(self):
        rm = RiskManager(_make_config())
        comp = _make_composite(signal_count=1)
        assert rm.filter(comp) is False

    def test_max_concurrent_blocked(self):
        rm = RiskManager(_make_config())
        rm._active_bets = 3
        comp = _make_composite()
        assert rm.filter(comp) is False

    def test_exposure_limit_blocked(self):
        rm = RiskManager(_make_config())
        rm._exposure["Liverpool_vs_Arsenal_PL"] = 100.0
        comp = _make_composite()
        assert rm.filter(comp) is False

    def test_sport_exposure_limit_blocked(self):
        rm = RiskManager(_make_config())
        rm._sport_exposure["football"] = 300.0
        comp = _make_composite()
        assert rm.filter(comp) is False

    def test_ml_confidence_gate(self):
        rm = RiskManager(_make_config())
        comp = _make_composite(ml_confidence=0.5)  # below 0.7 minimum
        assert rm.filter(comp) is False

    def test_prematch_low_confidence_blocked(self):
        rm = RiskManager(_make_config())
        comp = _make_composite(is_prematch=True, confidence=0.5)  # below 0.7
        assert rm.filter(comp) is False

    def test_prematch_high_confidence_passes(self):
        rm = RiskManager(_make_config())
        comp = _make_composite(is_prematch=True, confidence=0.8)
        assert rm.filter(comp) is True


class TestBetTracking:
    def test_record_bet(self):
        rm = RiskManager(_make_config())
        rm.record_bet("match1", 10.0)
        assert rm._exposure["match1"] == 10.0
        assert rm._active_bets == 1

    def test_settle_bet(self):
        rm = RiskManager(_make_config())
        rm.record_bet("match1", 10.0)
        rm.settle_bet("match1", 10.0, pnl=-5.0)
        assert rm._active_bets == 0
        assert rm._realized_loss_today == 5.0

    def test_sport_exposure_tracking(self):
        rm = RiskManager(_make_config())
        rm.record_bet("m1", 20.0, sport="football")
        rm.record_bet("m2", 30.0, sport="football")
        assert rm._sport_exposure["football"] == 50.0

    def test_multiple_sports(self):
        rm = RiskManager(_make_config())
        rm.record_bet("m1", 50.0, sport="football")
        rm.record_bet("m2", 40.0, sport="tennis")
        assert rm._sport_exposure["football"] == 50.0
        assert rm._sport_exposure["tennis"] == 40.0
