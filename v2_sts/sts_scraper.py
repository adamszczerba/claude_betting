"""
STS Live Soccer Odds Scraper
===============================
Fetches live soccer match data (scores + odds) from the STS public API
at offer.sts.pl.

STS (sts.pl) is Poland's largest bookmaker. No VPN required.

Each live match gets its own CSV file:
    {team1}_vs_{team2}_{tournament}_sts_{date}.csv

Usage:
    python v2_sts/sts_scraper.py
    python v2_sts/sts_scraper.py --interval 5
    python v2_sts/sts_scraper.py -o my_data_dir
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
STS_API_BASE = "https://offer.sts.pl/api/web/v2"
BOOKMAKER_TAG = "sts"

SOCCER_SPORT_ID = 1  # STS sport ID for soccer/football

DEFAULT_POLL_INTERVAL = 2.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://www.sts.pl",
    "Referer": "https://www.sts.pl/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sts")

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
# STS API client
# ---------------------------------------------------------------------------

class STSClient:
    """Thin wrapper around the STS offer API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def fetch_live_events(self) -> dict:
        """Fetch all live soccer events from STS."""
        url = f"{STS_API_BASE}/live/sports/{SOCCER_SPORT_ID}"
        resp = self.session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _format_odd(value) -> str:
    """Format odds value to string."""
    if value is None or value == "":
        return ""
    try:
        return f"{float(value):.2f}"
    except (ValueError, TypeError):
        return ""


def _extract_match_time(event: dict) -> str:
    """Extract match minute from STS event data."""
    # STS provides matchTime or clock info
    match_time = event.get("matchTime") or event.get("clock", {}).get("minute")
    if match_time is not None:
        return str(match_time)

    # Try liveData
    live = event.get("liveData") or {}
    minute = live.get("minute") or live.get("matchTime")
    if minute is not None:
        return str(minute)

    return ""


def _extract_match_status(event: dict) -> str:
    """Extract match status from STS event data."""
    status = event.get("status", "")
    live = event.get("liveData") or {}
    period = live.get("period") or event.get("period", "")

    status_str = str(status).upper()

    if status_str in ("HALFTIME", "HT") or str(period).upper() == "HALFTIME":
        return "HT"
    if status_str in ("FINISHED", "ENDED", "FT"):
        return "FT"
    if status_str in ("PENALTIES", "PEN"):
        return "PEN"
    if status_str in ("EXTRA_TIME", "AET", "OVERTIME"):
        return "AET"

    return ""


def parse_soccer_event(event: dict) -> dict | None:
    """Parse a single STS event into our standard format.

    Returns None if the event is not a valid live soccer match.
    """
    # Must have participants / competitors
    participants = event.get("participants") or event.get("competitors") or []
    if len(participants) < 2:
        # STS may use a flat structure with home/away fields
        home_name = event.get("home", {}).get("name") or event.get("homeName")
        away_name = event.get("away", {}).get("name") or event.get("awayName")
        if not home_name or not away_name:
            return None
    else:
        home_name = participants[0].get("name", "?")
        away_name = participants[1].get("name", "?")

    tournament = (
        event.get("tournament", {}).get("name")
        or event.get("league", {}).get("name")
        or event.get("competitionName")
        or ""
    )

    # Scores
    score = event.get("score") or event.get("liveData", {}).get("score") or {}
    if isinstance(score, dict):
        home_score = str(score.get("home", ""))
        away_score = str(score.get("away", ""))
    elif isinstance(score, str) and ":" in score:
        parts = score.split(":")
        home_score = parts[0].strip()
        away_score = parts[1].strip()
    else:
        home_score = ""
        away_score = ""

    match_time = _extract_match_time(event)
    match_status = _extract_match_status(event)

    # Extract odds from markets
    odd_1, odd_x, odd_2 = "", "", ""
    total_line, odd_over, odd_under = "", "", ""

    markets = event.get("markets") or event.get("odds") or []
    if isinstance(markets, list):
        for market in markets:
            market_type = (
                market.get("type", "")
                or market.get("name", "")
                or market.get("marketType", "")
            ).upper()

            outcomes = market.get("outcomes") or market.get("selections") or []

            # 1X2 market
            if market_type in ("1X2", "MATCH_RESULT", "WINNER", "RESULT"):
                for outcome in outcomes:
                    otype = (
                        outcome.get("type", "")
                        or outcome.get("name", "")
                        or outcome.get("label", "")
                    ).upper()
                    odds_val = outcome.get("odds") or outcome.get("price")

                    if otype in ("1", "HOME", "W1"):
                        odd_1 = _format_odd(odds_val)
                    elif otype in ("X", "DRAW"):
                        odd_x = _format_odd(odds_val)
                    elif otype in ("2", "AWAY", "W2"):
                        odd_2 = _format_odd(odds_val)

            # Over/Under market
            elif "OVER" in market_type or "TOTAL" in market_type:
                line = market.get("line") or market.get("handicap") or market.get("points")
                if line is not None:
                    total_line = str(line)

                for outcome in outcomes:
                    otype = (
                        outcome.get("type", "")
                        or outcome.get("name", "")
                        or outcome.get("label", "")
                    ).upper()
                    odds_val = outcome.get("odds") or outcome.get("price")

                    if "OVER" in otype:
                        odd_over = _format_odd(odds_val)
                    elif "UNDER" in otype:
                        odd_under = _format_odd(odds_val)

    elif isinstance(markets, dict):
        # Some STS responses may use dict keyed by market type
        m1x2 = markets.get("1X2") or markets.get("match_result") or {}
        if isinstance(m1x2, dict):
            odd_1 = _format_odd(m1x2.get("1") or m1x2.get("home"))
            odd_x = _format_odd(m1x2.get("X") or m1x2.get("draw"))
            odd_2 = _format_odd(m1x2.get("2") or m1x2.get("away"))

        m_total = markets.get("over_under") or markets.get("total") or {}
        if isinstance(m_total, dict):
            total_line = str(m_total.get("line", ""))
            odd_over = _format_odd(m_total.get("over"))
            odd_under = _format_odd(m_total.get("under"))

    scheduled = event.get("startDate") or event.get("startTime") or event.get("scheduledStart")

    return {
        "event_id": str(event.get("id", event.get("eventId", ""))),
        "team1": home_name,
        "team2": away_name,
        "tournament": tournament,
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
        "scheduled": scheduled,
    }


def parse_sts_response(data: dict) -> List[dict]:
    """Parse the top-level STS API response into a list of events.

    STS responses can be structured as:
      - {"events": [...]}
      - {"categories": [{"events": [...]}]}
      - direct list [...]
    """
    events: List[dict] = []

    # Try top-level events list
    raw_events = data.get("events") or data.get("matches") or []
    if isinstance(data, list):
        raw_events = data

    # If empty, try nested categories/leagues
    if not raw_events:
        for cat in data.get("categories", []):
            for league in cat.get("leagues", cat.get("tournaments", [])):
                raw_events.extend(league.get("events", league.get("matches", [])))

    for raw in raw_events:
        ev = parse_soccer_event(raw)
        if ev:
            events.append(ev)

    return events


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
            scheduled = ev.get("scheduled")
            if scheduled and isinstance(scheduled, str):
                try:
                    date = datetime.datetime.fromisoformat(
                        scheduled.replace("Z", "+00:00")
                    ).date()
                except ValueError:
                    date = datetime.date.today()
            elif scheduled and isinstance(scheduled, datetime.datetime):
                date = scheduled.date()
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
    output_dir: str = "match_database/sts",
    interval: float = DEFAULT_POLL_INTERVAL,
) -> None:
    client = STSClient()
    writer = MatchCSVWriter(output_dir)

    log.info("STS live SOCCER odds scraper started")
    log.info("  output dir : %s", os.path.abspath(output_dir))
    log.info("  interval   : %.1fs", interval)
    log.info("Press Ctrl+C to stop.\n")

    cycle = 0
    while True:
        t0 = time.monotonic()
        cycle += 1
        try:
            data = client.fetch_live_events()
            events = parse_sts_response(data)

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
        description="STS live soccer odds scraper (REST API)"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="match_database/sts",
        help="Directory for CSV files (default: match_database/sts)",
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

