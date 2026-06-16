"""
Signal Bus — typed pub/sub message broker.

Transport: Redis (pub/sub for real-time delivery + sorted sets for recent-signal cache).
Fallback: in-memory implementation when Redis is unavailable (testing / local dev).

Message schema:
    Signal dataclass with all fields from NEW_ARCHITECTURE.md §4.2.1

Usage
-----
>>> bus = SignalBus()
>>> bus.publish(signal)
>>> signals = bus.get_recent(match_id="abc", last_n_sec=60)
>>> bus.subscribe(["COMPARATOR", "ML"], callback_fn)
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Signal dataclass
# ---------------------------------------------------------------------------

@dataclass
class Signal:
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    signal_type: str = "COMPARATOR"          # "COMPARATOR" | "ML" | "WEB_SEARCH" | "LIVE_DATA"
    timestamp: datetime = field(default_factory=datetime.now)
    sport: str = "football"
    match_id: str = ""                       # per-bookmaker raw match identifier
    canonical_match_id: str = ""             # cross-bookmaker id from matcher
    match_key: str = ""                      # human-readable "TeamA_vs_TeamB_Tournament"
    team1: str = ""
    team2: str = ""
    tournament: str = ""
    market: str = ""                         # "odd_1", "odd_X", "odd_2", "odd_over", etc.
    direction: str = "BACK"                  # "BACK" | "LAY" | "NEUTRAL"
    confidence: float = 0.0                  # 0.0–1.0
    edge_pct: float = 0.0                    # estimated edge percentage
    payload: dict = field(default_factory=dict)
    ttl_sec: int = 300                       # signal expires after N seconds
    dedupe_key: str = ""                     # deterministic key for dedup
    provider_name: str = ""                  # which provider emitted this
    provider_version: str = ""               # provider git SHA / tag

    def is_expired(self) -> bool:
        age = (datetime.now() - self.timestamp).total_seconds()
        return age > self.ttl_sec

    def to_json(self) -> str:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return json.dumps(d, default=str)

    @classmethod
    def from_json(cls, raw: str) -> "Signal":
        d = json.loads(raw)
        d["timestamp"] = datetime.fromisoformat(d["timestamp"])
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# In-memory fallback (no Redis)
# ---------------------------------------------------------------------------

class _InMemoryBus:
    """Thread-safe in-memory signal bus for testing and local dev."""

    def __init__(self):
        self._signals: List[Signal] = []
        self._subscribers: Dict[str, List[Callable]] = {}

    def publish(self, signal: Signal) -> None:
        self._signals.append(signal)
        # Prune expired
        self._signals = [s for s in self._signals if not s.is_expired()]
        # Notify subscribers
        for cb in self._subscribers.get(signal.signal_type, []):
            try:
                cb(signal)
            except Exception:
                log.exception("Subscriber callback error")

    def subscribe(self, signal_types: List[str], callback: Callable) -> None:
        for st in signal_types:
            self._subscribers.setdefault(st, []).append(callback)

    def get_recent(self, match_id: str = "", last_n_sec: int = 60,
                   canonical_match_id: str = "") -> List[Signal]:
        cutoff = datetime.now() - timedelta(seconds=last_n_sec)
        results = [s for s in self._signals if s.timestamp >= cutoff and not s.is_expired()]
        if match_id:
            results = [s for s in results if s.match_id == match_id]
        if canonical_match_id:
            results = [s for s in results if s.canonical_match_id == canonical_match_id]
        return results


# ---------------------------------------------------------------------------
# Redis-backed bus
# ---------------------------------------------------------------------------

class _RedisBus:
    """Redis-backed signal bus with pub/sub + recent-signal cache."""

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0,
                 signal_ttl_sec: int = 300):
        import redis
        self._redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self._pubsub = self._redis.pubsub()
        self._ttl = signal_ttl_sec
        self._channel = "signals:all"

    def publish(self, signal: Signal) -> None:
        raw = signal.to_json()
        # Publish for real-time subscribers
        self._redis.publish(self._channel, raw)
        # Also store in sorted set for recent queries (score = unix timestamp)
        key = f"signals:recent:{signal.canonical_match_id or signal.match_id}"
        score = signal.timestamp.timestamp()
        self._redis.zadd(key, {raw: score})
        self._redis.expire(key, self._ttl)
        # Global recent set
        self._redis.zadd("signals:global", {raw: score})
        self._redis.expire("signals:global", self._ttl)

    def subscribe(self, signal_types: List[str], callback: Callable) -> None:
        import threading
        self._pubsub.subscribe(self._channel)

        def _listen():
            for msg in self._pubsub.listen():
                if msg["type"] != "message":
                    continue
                try:
                    signal = Signal.from_json(msg["data"])
                    if signal.signal_type in signal_types:
                        callback(signal)
                except Exception:
                    log.exception("Error processing bus message")

        t = threading.Thread(target=_listen, daemon=True)
        t.start()

    def get_recent(self, match_id: str = "", last_n_sec: int = 60,
                   canonical_match_id: str = "") -> List[Signal]:
        cutoff = datetime.now() - timedelta(seconds=last_n_sec)
        min_score = cutoff.timestamp()
        if canonical_match_id:
            key = f"signals:recent:{canonical_match_id}"
        elif match_id:
            key = f"signals:recent:{match_id}"
        else:
            key = "signals:global"
        raw_list = self._redis.zrangebyscore(key, min_score, "+inf")
        signals = []
        for raw in raw_list:
            try:
                s = Signal.from_json(raw)
                if not s.is_expired():
                    signals.append(s)
            except Exception:
                pass
        return signals


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

class SignalBus:
    """
    Unified interface — auto-selects Redis or in-memory backend.

    Parameters
    ----------
    backend : "redis" | "memory"
    host, port, db : Redis connection params
    signal_ttl_sec : how long signals persist in the recent cache
    """

    def __init__(self, backend: str = "auto", host: str = "localhost",
                 port: int = 6379, db: int = 0, signal_ttl_sec: int = 300):
        if backend == "auto":
            backend = "redis" if self._redis_available(host, port) else "memory"

        if backend == "redis":
            try:
                self._impl = _RedisBus(host, port, db, signal_ttl_sec)
                log.info("SignalBus using Redis backend (%s:%d)", host, port)
            except Exception as exc:
                log.warning("Redis unavailable (%s), falling back to in-memory bus", exc)
                self._impl = _InMemoryBus()
        else:
            self._impl = _InMemoryBus()
            log.info("SignalBus using in-memory backend")

    @staticmethod
    def _redis_available(host: str, port: int) -> bool:
        try:
            import redis
            r = redis.Redis(host=host, port=port, socket_timeout=1)
            r.ping()
            return True
        except Exception:
            return False

    def publish(self, signal: Signal) -> None:
        self._impl.publish(signal)

    def subscribe(self, signal_types: List[str], callback: Callable) -> None:
        self._impl.subscribe(signal_types, callback)

    def get_recent(self, match_id: str = "", last_n_sec: int = 60,
                   canonical_match_id: str = "") -> List[Signal]:
        return self._impl.get_recent(match_id, last_n_sec, canonical_match_id)
