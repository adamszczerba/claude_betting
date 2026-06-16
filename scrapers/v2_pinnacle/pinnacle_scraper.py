"""
Pinnacle Live Soccer Odds Scraper
====================================
Fetches live soccer match data (scores + odds) from Pinnacle's guest API
(guest.api.arcadia.pinnacle.com).

Each live match gets its own CSV file:
    {team1}_vs_{team2}_{tournament}_pin_{date}.csv

Usage:
    python v2_pinnacle/pinnacle_scraper.py
    python v2_pinnacle/pinnacle_scraper.py --interval 5
    python v2_pinnacle/pinnacle_scraper.py -o my_data_dir

Requires UK VPN (Pinnacle is geo-restricted in some regions).
"""

import argparse
import csv
import datetime
import logging
import os
import re
import sys
import time
from typing import Dict

# sync_clock lives in v2_coincasino/ locally, or same dir in Docker — make it importable
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _here)                                               # Docker: /app
sys.path.insert(0, os.path.join(_here, "..", "v2_coincasino"))          # local dev
from sync_clock import sleep_until_next_tick  # noqa: E402

import requests


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PINNACLE_API_BASE = "https://guest.api.arcadia.pinnacle.com/0.1"
BOOKMAKER_TAG = "pin"

SOCCER_SPORT_ID = 29  # Pinnacle sport ID for soccer

DEFAULT_POLL_INTERVAL = 5.0  # seconds (be conservative with rate limits)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://www.pinnacle.com",
    "Referer": "https://www.pinnacle.com/",
    "X-API-Key": "CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0R",
    "X-Device-UUID": "66a3b4c2-a0ce-4a20-85c8-926eab0f2900",
    "Content-Type": "application/json",
}

# Period numbers used by Pinnacle for soccer
PERIOD_MATCH = 0   # full match (1X2, totals)
PERIOD_1H = 1      # first half
PERIOD_2H = 2      # second half

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pinnacle")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_filename(s: str) -> str:
    """Replace chars that are problematic in filenames."""
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
    "odd_1",       # home win
    "odd_X",       # draw
    "odd_2",       # away win
    "total_line",  # e.g. 2.5
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
# Pinnacle API client
# ---------------------------------------------------------------------------

class PinnacleClient:
    """Thin wrapper around the Pinnacle guest API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def fetch_live_matchups(self) -> list:
        """Fetch all live soccer matchups with embedded odds."""
        url = f"{PINNACLE_API_BASE}/matchups"
        params = {
            "sportId": SOCCER_SPORT_ID,
            "isLive": "true",
            "withSpecials": "false",
        }
        resp = self.session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def fetch_live_odds(self) -> list:
        """Fetch live odds (prices) for soccer matchups."""
        url = f"{PINNACLE_API_BASE}/matchups/odds"
        params = {
            "sportId": SOCCER_SPORT_ID,
            "isLive": "true",
        }
        resp = self.session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _format_price(price) -> str:
    """Format a decimal price value to string."""
    if price is None or price == "":
        return ""
    try:
        return f"{float(price):.2f}"
    except (ValueError, TypeError):
        return ""


def _extract_match_time(matchup: dict) -> str:
    """Extract match time from Pinnacle matchup data."""
    live_status = matchup.get("liveStatus") or {}
    elapsed = live_status.get("elapsed")
    if elapsed is not None:
        mins = int(elapsed) // 60
        secs = int(elapsed) % 60
        return f"{mins}:{secs:02d}"
    return ""


def _extract_match_status(matchup: dict) -> str:
    """Extract match status from Pinnacle matchup data."""
    live_status = matchup.get("liveStatus") or {}
    status = live_status.get("status", "")
    period = live_status.get("period")

    # Map Pinnacle status codes
    if status == "halftime":
        return "HT"
    if status == "ended":
        return "FT"
    if status == "penalties":
        return "PEN"
    if status == "overtime":
        return "AET"
    if period == 1:
        return ""  # 1st half running
    if period == 2:
        return ""  # 2nd half running
    return ""


def parse_matchup_data(matchup: dict) -> dict | None:
    """Parse a single Pinnacle matchup into our standard format.

    Returns None if the matchup is not a valid soccer event.
    """
    # Must be a live event
    if not matchup.get("isLive"):
        return None

    participants = matchup.get("participants", [])
    if len(participants) < 2:
        return None

    # Find home and away teams
    home = None
    away = None
    for p in participants:
        alignment = p.get("alignment", "")
        if alignment == "home":
            home = p
        elif alignment == "away":
            away = p

    if not home or not away:
        # Fallback: use first two participants
        home = participants[0]
        away = participants[1]

    team1 = home.get("name", "?")
    team2 = away.get("name", "?")

    # League / tournament
    league = matchup.get("league", {})
    tournament = league.get("name", "")

    # Scores
    scores = matchup.get("liveStatus", {}).get("score") or {}
    home_score = ""
    away_score = ""

    # Pinnacle provides scores in different formats
    if isinstance(scores, dict):
        home_score = str(scores.get("home", ""))
        away_score = str(scores.get("away", ""))
    elif isinstance(scores, list):
        # Sometimes scores come as period scores list
        total_home = 0
        total_away = 0
        for s in scores:
            total_home += s.get("home", 0)
            total_away += s.get("away", 0)
        home_score = str(total_home)
        away_score = str(total_away)

    match_time = _extract_match_time(matchup)
    match_status = _extract_match_status(matchup)

    return {
        "matchup_id": str(matchup.get("id", "")),
        "team1": team1,
        "team2": team2,
        "tournament": tournament,
        "home_score": home_score,
        "away_score": away_score,
        "match_time": match_time,
        "match_status": match_status,
        "odd_1": "",
        "odd_X": "",
        "odd_2": "",
        "total_line": "",
        "odd_over": "",
        "odd_under": "",
        "scheduled": matchup.get("startTime"),
    }


def merge_odds_into_events(events: Dict[str, dict], odds_data: list) -> None:
    """Merge odds from the odds endpoint into the parsed events dict.

    Modifies events in-place.
    """
    for odds_entry in odds_data:
        matchup_id = str(odds_entry.get("matchupId", ""))
        ev = events.get(matchup_id)
        if ev is None:
            continue

        prices = odds_entry.get("prices", [])
        for price in prices:
            period = price.get("period", 0)
            if period != PERIOD_MATCH:
                continue

            designation = price.get("designation", "")
            bet_type = price.get("type", "")
            odds_val = price.get("price")

            # Moneyline (1X2)
            if bet_type == "moneyline":
                if designation == "home":
                    ev["odd_1"] = _format_price(odds_val)
                elif designation == "draw":
                    ev["odd_X"] = _format_price(odds_val)
                elif designation == "away":
                    ev["odd_2"] = _format_price(odds_val)

            # Total (over/under)
            elif bet_type == "total":
                points = price.get("points")
                if points is not None:
                    ev["total_line"] = str(points)
                if designation == "over":
                    ev["odd_over"] = _format_price(odds_val)
                elif designation == "under":
                    ev["odd_under"] = _format_price(odds_val)


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
        eid = ev["matchup_id"]
        if eid not in self._paths:
            scheduled = ev.get("scheduled")
            if scheduled and isinstance(scheduled, str):
                try:
                    date = datetime.datetime.fromisoformat(
                        scheduled.replace("Z", "+00:00")
                    ).date()
                except ValueError:
                    date = datetime.date.today()
            else:
                date = datetime.date.today()
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
    output_dir: str = "match_database/pinnacle",
    interval: float = DEFAULT_POLL_INTERVAL,
) -> None:
    client = PinnacleClient()
    writer = MatchCSVWriter(output_dir)

    log.info("Pinnacle live SOCCER odds scraper started")
    log.info("  output dir : %s", os.path.abspath(output_dir))
    log.info("  interval   : %.1fs", interval)
    log.info("Press Ctrl+C to stop.\n")

    cycle = 0
    while True:
        t0 = time.monotonic()
        cycle += 1
        try:
            # Fetch matchups and odds in parallel-ish (two sequential calls)
            matchups_data = client.fetch_live_matchups()
            odds_data = client.fetch_live_odds()

            # Parse matchups
            events: Dict[str, dict] = {}
            raw_list = matchups_data if isinstance(matchups_data, list) else []
            for m in raw_list:
                ev = parse_matchup_data(m)
                if ev:
                    events[ev["matchup_id"]] = ev

            # Merge odds into events
            odds_list = odds_data if isinstance(odds_data, list) else []
            merge_odds_into_events(events, odds_list)

            # Write to CSV
            for ev in events.values():
                writer.write(ev)

            n_total = len(events)
            n_with_odds = sum(1 for e in events.values() if e["odd_1"])
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
        description="Pinnacle live soccer odds scraper (REST API)"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="match_database/pinnacle",
        help="Directory for CSV files (default: match_database/pinnacle)",
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

