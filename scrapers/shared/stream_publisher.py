"""
Redis Stream publisher for scraper odds snapshots.

Usage
-----
    from scrapers.shared.stream_publisher import publish_snapshot

    # After CSV write, in the poll loop:
    publish_snapshot(bookmaker="bf", events=events)

The publisher is best-effort: failures are logged but never raise.
If Redis is unavailable, the call returns silently.

Environment
-----------
REDIS_HOST — Redis hostname (default: localhost)
REDIS_PORT — Redis port (default: 6379)
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# Approx 3h of data: ~50 matches × 7 bookmakers × (1800s / 2s interval) ≈ 9450
STREAM_MAXLEN = 10000

# Lazy singleton client
_client: Optional["redis.Redis"] = None


def _get_client() -> Optional["redis.Redis"]:
    """Create or return cached Redis client. Returns None on failure."""
    global _client
    if _client is not None:
        return _client

    try:
        import redis
    except ImportError:
        log.warning("redis package not installed — stream publishing disabled")
        return None

    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))

    try:
        _client = redis.Redis(host=host, port=port, decode_responses=True, socket_timeout=2)
        _client.ping()
        log.info("Stream publisher connected to Redis at %s:%d", host, port)
        return _client
    except Exception as exc:
        log.warning("Redis unavailable at %s:%d (%s) — stream publishing disabled", host, port, exc)
        _client = None  # type: ignore[assignment]
        return None


def _ev_to_json(ev: Dict[str, Any]) -> str:
    """Serialize a single event dict to JSON for stream publishing."""
    # Build a clean dict with only the fields consumers need
    # Include all standard fields plus any extra market fields the scraper provides
    d: Dict[str, Any] = {}
    for k, v in ev.items():
        # Skip internal/sentinel keys
        if k.startswith("_"):
            continue
        d[k] = v
    return json.dumps(d, default=str)


def publish_snapshot(bookmaker: str, events: List[Dict[str, Any]]) -> None:
    """
    Publish a list of match events to the Redis stream for this bookmaker.

    Each event is published as a separate XADD message. Uses MAXLEN to bound
    stream size. Failures are logged but never raise — CSV write path is unaffected.

    Parameters
    ----------
    bookmaker : str
        Bookmaker tag (e.g., "bf", "b365", "cc", "bfx", "pin", "lv", "sts").
    events : list[dict]
        List of event dicts (the same dicts passed to MatchCSVWriter.write()).
    """
    client = _get_client()
    if client is None:
        return

    if not events:
        return

    stream_key = f"odds:{bookmaker}"
    try:
        pipe = client.pipeline(transaction=False)
        for ev in events:
            pipe.xadd(stream_key, {"data": _ev_to_json(ev)}, maxlen=STREAM_MAXLEN, approximate=True)
        pipe.execute()
    except Exception as exc:
        log.warning("Failed to publish snapshot to %s: %s", stream_key, exc)
        # Reset client so next call retries connection
        global _client
        _client = None
