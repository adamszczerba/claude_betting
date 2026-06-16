"""
Redis Stream consumer for reading latest odds snapshots.

Provides a drop-in replacement for dashboard.scan_today() that reads
from Redis Streams instead of scanning CSV files.

Usage
-----
    from scrapers.shared.stream_consumer import read_latest_from_streams

    rows = read_latest_from_streams(redis_host="localhost")
    # Returns list of dicts with same shape as scan_today()

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

# Bookmaker tags used by scrapers as BOOKMAKER_TAG (and stream key suffix)
# These match the actual BOOKMAKER_TAG constants in each scraper
_BOOKMAKER_TAGS = ["cc", "bf", "b365", "bfx", "pin", "lv", "sts"]

# All bookmaker stream keys (derived from tags)
BOOKMAKER_STREAMS = [f"odds:{tag}" for tag in _BOOKMAKER_TAGS]

# Map bookmaker tag → bookmaker directory name (match_database layout)
_TAG_TO_BOOKMAKER = {
    "cc": "coincasino",
    "bf": "betfair",
    "b365": "bet365",
    "bfx": "betfair_exchange",
    "pin": "pinnacle",
    "lv": "lvbet",
    "sts": "sts",
}


def _get_client() -> Optional["redis.Redis"]:
    """Create Redis client. Returns None on failure."""
    try:
        import redis
    except ImportError:
        return None

    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))

    try:
        client = redis.Redis(host=host, port=port, decode_responses=True, socket_timeout=2)
        client.ping()
        return client
    except Exception:
        return None


def read_latest_from_streams(
    redis_host: str = "",
    redis_port: int = 0,
    streams: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Read the latest message from each bookmaker stream and return
    a list of row dicts matching the scan_today() format.

    Each returned dict has keys:
        bookmaker, team1, team2, tournament, date,
        + all CSV column values (timestamp, match_time, odd_1, etc.)

    Parameters
    ----------
    redis_host : str
        Overrides REDIS_HOST env var.
    redis_port : int
        Overrides REDIS_PORT env var.
    streams : list[str], optional
        Specific stream keys to read. Defaults to all BOOKMAKER_STREAMS.

    Returns
    -------
    list[dict]
        One dict per match per bookmaker (same shape as scan_today()).
    """
    import redis

    host = redis_host or os.environ.get("REDIS_HOST", "localhost")
    port = redis_port or int(os.environ.get("REDIS_PORT", "6379"))

    try:
        client = redis.Redis(host=host, port=port, decode_responses=True, socket_timeout=2)
        client.ping()
    except Exception as exc:
        log.warning("Redis unavailable (%s) — cannot read streams", exc)
        return []

    target_streams = streams or BOOKMAKER_STREAMS
    results: List[Dict[str, Any]] = []

    for stream_key in target_streams:
        bk_suffix = stream_key.split(":", 1)[1] if ":" in stream_key else stream_key
        bookmaker = _TAG_TO_BOOKMAKER.get(bk_suffix, bk_suffix)

        try:
            # Read last message from stream
            msgs = client.xrevrange(stream_key, count=1)
            if not msgs:
                continue

            # msgs is list of (message_id, {field: value})
            # Each message contains one match event in "data" field
            # We need ALL unique matches, so read more
            # Read last N messages to get all matches from latest poll cycle
            all_msgs = client.xrevrange(stream_key, count=500)

            # Deduplicate by match (team1+team2+tournament), keep latest per match
            seen: Dict[str, Dict[str, Any]] = {}
            for msg_id, fields in reversed(all_msgs):  # oldest first
                raw = fields.get("data", "")
                if not raw:
                    continue
                try:
                    ev = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                match_key = f"{ev.get('team1', '')}|{ev.get('team2', '')}|{ev.get('tournament', '')}"
                if match_key and match_key not in seen:
                    seen[match_key] = ev

            for ev in seen.values():
                row = dict(ev)
                row["bookmaker"] = bookmaker
                if "team1" not in row:
                    row["team1"] = ""
                if "team2" not in row:
                    row["team2"] = ""
                if "tournament" not in row:
                    row["tournament"] = ""
                results.append(row)

        except Exception as exc:
            log.warning("Error reading stream %s: %s", stream_key, exc)
            continue

    return results


def is_redis_available(redis_host: str = "", redis_port: int = 0) -> bool:
    """Check if Redis is reachable."""
    host = redis_host or os.environ.get("REDIS_HOST", "localhost")
    port = redis_port or int(os.environ.get("REDIS_PORT", "6379"))
    try:
        import redis
        client = redis.Redis(host=host, port=port, decode_responses=True, socket_timeout=1)
        client.ping()
        return True
    except Exception:
        return False
