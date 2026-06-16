"""
LVBet Live Soccer Odds Scraper
================================
Fetches live soccer match data (scores + odds) from the Betby/sptpub API
that powers lvbet.pl/pl/zaklady-na-zywo.

LVBet uses the same Betby backend as CoinCasino but with a different brand ID.
No VPN required (Polish bookmaker, publicly accessible).

Each live match gets its own CSV file:
    {team1}_vs_{team2}_{tournament}_lv_{date}.csv

Usage:
    python v2_lvbet/lvbet_scraper.py
    python v2_lvbet/lvbet_scraper.py --interval 5
    python v2_lvbet/lvbet_scraper.py -o my_data_dir
"""

import argparse
import csv
import datetime
import logging
import os
import re
import sys
import time
from typing import Dict, List

# sync_clock lives in v2_coincasino/ locally, or same dir in Docker — make it importable
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _here)                                               # Docker: /app
sys.path.insert(0, os.path.join(_here, "..", "v2_coincasino"))          # local dev
from sync_clock import sleep_until_next_tick  # noqa: E402

import requests


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BETBY_API_BASE = "https://api-h-c7818b61-608.sptpub.com"
BRAND_ID = "2432254656609652736"  # LVBet brand ID on Betby — same infra as CoinCasino
# NOTE: If LVBet uses a distinct brand ID, update this value. Discover it by
#       inspecting XHR requests on lvbet.pl live page (Network → filter sptpub).
LANGUAGE = "en"
BOOKMAKER_TAG = "lv"

SOCCER_SPORT_ID = "1"

DEFAULT_POLL_INTERVAL = 2.0

# Betradar UOF market IDs
MARKET_1X2 = "1"
MARKET_TOTAL = "18"

# Match-status codes (same Betby backend as CoinCasino)
STATUS_LABELS = {
    6: "1H",
    7: "2H",
    22: "PRE",
    31: "HT",
    32: "AET",
    33: "ETHT",
    41: "ET1",
    42: "ET2",
    50: "PEN",
    100: "FT",
}

_PLAYING_STATUSES = {6, 7, 41, 42}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://lvbet.pl",
    "Referer": "https://lvbet.pl/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lvbet")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_filename(s: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '_', s).strip()


def _csv_path(output_dir: str, team1: str, team2: str,
              tournament: str, date: datetime.date) -> str:
    fname = (
        f"{_safe_filename(team1)}_vs_{_safe_filename(team2)}_"
        f"{_safe_filename(tournament)}_"
        f"{BOOKMAKER_TAG}_{date}.csv"
    )
    day_dir = os.path.join(output_dir, str(date))
    os.makedirs(day_dir, exist_ok=True)
    return os.path.join(day_dir, fname)


CSV_COLUMNS = [
    "timestamp",
    "match_time",
    "match_status",
    "home_score",
    "away_score",
    "odd_1",
    "odd_X",
    "odd_2",
    "total_line",
    "odd_over",
    "odd_under",
]


def _write_header_if_needed(path: str) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(CSV_COLUMNS)


def _append_row(path: str, row: list) -> None:
    with open(path, "a", newline="") as f:
        csv.writer(f).writerow(row)


# ---------------------------------------------------------------------------
# Clock computation (same logic as CoinCasino — shared Betby backend)
# ---------------------------------------------------------------------------

def compute_match_time(clock: dict, match_status_code: int | None = None) -> str:
    """Return a human-readable match minute string like '67:23'.

    Extrapolates from the Betby clock snapshot using wall-clock elapsed
    time, same algorithm as the CoinCasino scraper.
    """
    raw = clock.get("match_time", "")
    if not raw:
        return ""

    ts = clock.get("timestamp")
    stopped = clock.get("stopped", True)
    is_playing = match_status_code in _PLAYING_STATUSES

    if ts is None or (stopped and not is_playing):
        return raw

    if isinstance(ts, (int, float)) and ts > 1_000_000_000_000:
        ts_sec = ts / 1000.0
    else:
        ts_sec = float(ts)

    parts = raw.split(":")
    try:
        base_min = int(parts[0])
        base_sec = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        return raw

    base_total_sec = base_min * 60 + base_sec
    elapsed = max(0.0, time.time() - ts_sec)
    current_total_sec = int(base_total_sec + elapsed)
    mins = current_total_sec // 60
    secs = current_total_sec % 60
    return f"{mins}:{secs:02d}"


# ---------------------------------------------------------------------------
# Betby API client (reused pattern from CoinCasino)
# ---------------------------------------------------------------------------

class BetbyClient:
    """Thin wrapper around the Betby sptpub REST API used by LVBet."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def fetch_live_snapshot(self) -> dict:
        meta = self._get_json(
            f"/api/v4/live/brand/{BRAND_ID}/{LANGUAGE}/0"
        )
        top_versions = meta.get("top_events_versions", [])
        rest_versions = meta.get("rest_events_versions", [])

        merged: dict = {}
        for ver in top_versions + rest_versions:
            chunk = self._get_json(
                f"/api/v4/live/brand/{BRAND_ID}/{LANGUAGE}/{ver}"
            )
            if not merged:
                merged = chunk
            else:
                for key in ("events", "sports", "categories", "tournaments"):
                    if key in chunk:
                        merged.setdefault(key, {}).update(chunk[key])
                if "status" in chunk:
                    merged.setdefault("status", {}).update(chunk["status"])
        return merged

    def _get_json(self, path: str) -> dict:
        url = BETBY_API_BASE + path
        resp = self.session.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Event parser (soccer only)
# ---------------------------------------------------------------------------

def parse_soccer_events(snapshot: dict) -> List[dict]:
    """Extract real soccer events from the Betby snapshot."""
    events_raw = snapshot.get("events", {})
    categories = snapshot.get("categories", {})
    tournaments = snapshot.get("tournaments", {})

    results: list[dict] = []
    for eid, ev in events_raw.items():
        desc = ev.get("desc", {})

        if desc.get("sport") != SOCCER_SPORT_ID:
            continue
        if desc.get("virtual", False):
            continue

        competitors = desc.get("competitors", [])
        if len(competitors) < 2:
            continue

        team1 = competitors[0].get("name", "?")
        team2 = competitors[1].get("name", "?")

        cat_id = desc.get("category", "")
        tourn_id = desc.get("tournament", "")
        tourn_name = tournaments.get(tourn_id, {}).get("name", "")

        # Score
        score = ev.get("score", {})
        home_score = score.get("home_score", "")
        away_score = score.get("away_score", "")

        # Match time
        state = ev.get("state", {})
        clock = state.get("clock", {})
        match_status_code = state.get("match_status")
        match_time = compute_match_time(clock, match_status_code)
        match_status = STATUS_LABELS.get(
            match_status_code,
            str(match_status_code) if match_status_code else ""
        )

        # 1X2 odds
        markets = ev.get("markets", {})
        odd_1, odd_x, odd_2 = "", "", ""
        m1x2 = markets.get(MARKET_1X2, {}).get("", {})
        if m1x2:
            odd_1 = m1x2.get("1", {}).get("k", "")
            odd_x = m1x2.get("2", {}).get("k", "")
            odd_2 = m1x2.get("3", {}).get("k", "")

        # Total (over/under)
        total_line, odd_over, odd_under = "", "", ""
        m_total = markets.get(MARKET_TOTAL, {})
        if m_total:
            for spec_key, outcomes in m_total.items():
                m = re.search(r"total=([\d.]+)", spec_key)
                if m:
                    total_line = m.group(1)
                    odd_over = outcomes.get("12", {}).get("k", "")
                    odd_under = outcomes.get("13", {}).get("k", "")
                    break

        scheduled_ts = desc.get("scheduled", 0)
        scheduled_dt = (datetime.datetime.fromtimestamp(scheduled_ts)
                        if scheduled_ts else None)

        results.append({
            "event_id": eid,
            "team1": team1,
            "team2": team2,
            "tournament": tourn_name,
            "home_score": home_score,
            "away_score": away_score,
            "match_time": match_time,
            "match_status": match_status,
            "odd_1": odd_1,
            "odd_X": odd_x,
            "odd_2": odd_2,
            "total_line": total_line,
            "odd_over": odd_over,
            "odd_under": odd_under,
            "scheduled": scheduled_dt,
        })

    return results


# ---------------------------------------------------------------------------
# Writer - one CSV per match
# ---------------------------------------------------------------------------

class MatchCSVWriter:
    """Manages one CSV file per live match."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self._paths: Dict[str, str] = {}

    def write(self, ev: dict) -> None:
        eid = ev["event_id"]
        if eid not in self._paths:
            date = (ev["scheduled"].date()
                    if ev["scheduled"] else datetime.date.today())
            path = _csv_path(
                self.output_dir,
                ev["team1"], ev["team2"],
                ev["tournament"],
                date,
            )
            self._paths[eid] = path
            _write_header_if_needed(path)

        now = datetime.datetime.now().isoformat(timespec="milliseconds")
        row = [
            now,
            ev["match_time"],
            ev["match_status"],
            ev["home_score"],
            ev["away_score"],
            ev["odd_1"],
            ev["odd_X"],
            ev["odd_2"],
            ev["total_line"],
            ev["odd_over"],
            ev["odd_under"],
        ]
        _append_row(self._paths[eid], row)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(
    output_dir: str = "match_database/lvbet",
    interval: float = DEFAULT_POLL_INTERVAL,
) -> None:
    client = BetbyClient()
    writer = MatchCSVWriter(output_dir)

    log.info("LVBet live SOCCER odds scraper started")
    log.info("  output dir : %s", os.path.abspath(output_dir))
    log.info("  interval   : %.1fs", interval)
    log.info("Press Ctrl+C to stop.\n")

    cycle = 0
    while True:
        t0 = time.monotonic()
        cycle += 1
        try:
            snapshot = client.fetch_live_snapshot()
            events = parse_soccer_events(snapshot)

            for ev in events:
                writer.write(ev)

            n_total = len(events)
            n_with_odds = sum(1 for e in events if e["odd_1"])
            elapsed = time.monotonic() - t0
            log.info(
                "cycle %4d  |  %3d soccer matches  |  %3d with odds  |  %.2fs",
                cycle, n_total, n_with_odds, elapsed,
            )

        except requests.RequestException as exc:
            log.warning("Network error: %s", exc)
        except Exception:
            log.exception("Unexpected error in cycle %d", cycle)

        sleep_until_next_tick(interval)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="LVBet live soccer odds scraper (Betby API)"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="match_database/lvbet",
        help="Directory for CSV files (default: match_database/lvbet)",
    )
    parser.add_argument(
        "-i", "--interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL,
        help=f"Poll interval in seconds (default: {DEFAULT_POLL_INTERVAL})",
    )
    args = parser.parse_args()

    try:
        run(output_dir=args.output_dir, interval=args.interval)
    except KeyboardInterrupt:
        log.info("\nStopped by user.")


if __name__ == "__main__":
    main()

