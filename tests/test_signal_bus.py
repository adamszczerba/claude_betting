"""Tests for Signal Bus."""

import datetime
import time
import pytest
from signals.bus import Signal, SignalBus, _InMemoryBus


class TestSignal:
    def test_create_signal(self):
        s = Signal(
            signal_type="COMPARATOR",
            match_key="Liverpool_vs_Arsenal_PL",
            team1="Liverpool",
            team2="Arsenal",
            market="odd_1",
            edge_pct=3.5,
            confidence=0.8,
        )
        assert s.signal_type == "COMPARATOR"
        assert s.edge_pct == 3.5
        assert s.direction == "BACK"
        assert s.ttl_sec == 300

    def test_signal_expiry(self):
        s = Signal(ttl_sec=0)
        time.sleep(0.01)
        assert s.is_expired()

    def test_signal_not_expired(self):
        s = Signal(ttl_sec=60)
        assert not s.is_expired()

    def test_signal_json_roundtrip(self):
        s = Signal(
            signal_type="ML",
            match_key="TeamA_vs_TeamB_League",
            market="odd_2",
            edge_pct=5.0,
            confidence=0.9,
            payload={"model_version": "v3"},
        )
        raw = s.to_json()
        s2 = Signal.from_json(raw)
        assert s2.signal_type == "ML"
        assert s2.match_key == "TeamA_vs_TeamB_League"
        assert s2.edge_pct == 5.0
        assert s2.payload["model_version"] == "v3"


class TestInMemoryBus:
    def test_publish_and_get(self):
        bus = _InMemoryBus()
        s = Signal(match_id="m1", market="odd_1", edge_pct=3.0)
        bus.publish(s)
        results = bus.get_recent(match_id="m1", last_n_sec=60)
        assert len(results) == 1
        assert results[0].edge_pct == 3.0

    def test_get_recent_by_canonical_id(self):
        bus = _InMemoryBus()
        s = Signal(canonical_match_id="c1", market="odd_1")
        bus.publish(s)
        results = bus.get_recent(canonical_match_id="c1", last_n_sec=60)
        assert len(results) == 1

    def test_expired_signals_excluded(self):
        bus = _InMemoryBus()
        s = Signal(match_id="m1", ttl_sec=0)
        bus.publish(s)
        time.sleep(0.01)
        results = bus.get_recent(match_id="m1", last_n_sec=60)
        assert len(results) == 0

    def test_subscribe_receives_signals(self):
        bus = _InMemoryBus()
        received = []

        def callback(sig):
            received.append(sig)

        bus.subscribe(["COMPARATOR"], callback)
        s = Signal(signal_type="COMPARATOR", match_id="m1")
        bus.publish(s)
        assert len(received) == 1
        assert received[0].match_id == "m1"

    def test_subscribe_filters_by_type(self):
        bus = _InMemoryBus()
        received = []

        def callback(sig):
            received.append(sig)

        bus.subscribe(["ML"], callback)
        bus.publish(Signal(signal_type="COMPARATOR", match_id="m1"))
        bus.publish(Signal(signal_type="ML", match_id="m2"))
        assert len(received) == 1
        assert received[0].signal_type == "ML"


class TestSignalBus:
    def test_auto_selects_memory(self):
        bus = SignalBus(backend="memory")
        assert isinstance(bus._impl, _InMemoryBus)

    def test_explicit_memory(self):
        bus = SignalBus(backend="memory")
        s = Signal(match_id="m1", edge_pct=3.0)
        bus.publish(s)
        results = bus.get_recent(match_id="m1")
        assert len(results) == 1
