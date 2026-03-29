"""
CoinCasino Live Sports Odds Scraper
====================================
Fetches live match data (scores + odds) every 2 seconds from the Betby API
that powers coincasino.com/en/sports?bt-path=%2Flive

Each live match gets its own CSV file:
    {team1}_vs_{team2}_{sport}_{tournament}_cc_{date}.csv

Usage:
    python v2/coincasino_scraper.py                  # scrape all live sports
    python v2/coincasino_scraper.py --sport soccer    # only soccer
    python v2/coincasino_scraper.py --interval 5      # poll every 5s
"""

import argparse
import csv
import datetime
import json
import logging
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BETBY_API_BASE = "https://api-h-c7818b61-608.sptpub.com"
BRAND_ID = "2432254656609652736"
LANGUAGE = "en"
BOOKMAKER_TAG = "cc"  # short tag used in filenames

DEFAULT_POLL_INTERVAL = 2.0  # seconds

# Key market IDs we track
MARKET_1X2 = "1"          # 1x2 (soccer, etc.)  outcomes: 1=home, 2=draw, 3=away
MARKET_WINNER = "186"     # Winner (tennis, etc.) outcomes: 4=home, 5=away
MARKET_TOTAL = "18"       # Total (over/under)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://www.coincasino.com",
    "Referer": "https://www.coincasino.com/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("coincasino")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_filename(s: str) -> str:
    """Replace chars that are problematic in filenames."""
    return re.sub(r'[\\/*?:"<>|]', '_', s).strip()


def _csv_path(output_dir: str, team1: str, team2: str, sport: str,
              tournament: str, date: datetime.date) -> str:
    fname = (
        f"{_safe_filename(team1)}_vs_{_safe_filename(team2)}_"
        f"{_safe_filename(sport)}_{_safe_filename(tournament)}_"
        f"{BOOKMAKER_TAG}_{date}.csv"
    )
    return os.path.join(output_dir, fname)


CSV_COLUMNS = [
    "timestamp",
    "match_time",
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
            writer = csv.writer(f)
            writer.writerow(CSV_COLUMNS)


def _append_row(path: str, row: list) -> None:
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)


# ---------------------------------------------------------------------------
# Betby API client
# ---------------------------------------------------------------------------

class BetbyClient:
    """Thin wrapper around the Betby sptpub REST API used by CoinCasino."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._market_descs: Optional[dict] = None

    # -- public --------------------------------------------------------

    def fetch_live_snapshot(self) -> dict:
        """
        Return the full live-events snapshot.

        The API works in two phases:
          1. GET .../en/0  → gives version numbers for top/rest chunks
          2. GET .../en/{version}  → gives actual event data

        We merge top + rest into one dict.
        """
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
                # merge events, sports, categories, tournaments
                for key in ("events", "sports", "categories", "tournaments"):
                    if key in chunk:
                        merged.setdefault(key, {}).update(chunk[key])
                # merge status
                if "status" in chunk:
                    merged.setdefault("status", {}).update(chunk["status"])
        return merged

    # -- private -------------------------------------------------------

    def _get_json(self, path: str) -> dict:
        url = BETBY_API_BASE + path
        resp = self.session.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Event parser
# ---------------------------------------------------------------------------

def parse_events(snapshot: dict, sport_filter: Optional[str] = None) -> List[dict]:
    """
    Parse the raw snapshot into a flat list of dicts, one per live event.

    Each dict has:
        event_id, team1, team2, sport, category, tournament,
        home_score, away_score, match_time,
        odd_1, odd_X, odd_2,
        total_line, odd_over, odd_under,
        scheduled (datetime)
    """
    events_raw = snapshot.get("events", {})
    sports = snapshot.get("sports", {})
    categories = snapshot.get("categories", {})
    tournaments = snapshot.get("tournaments", {})

    results = []
    for eid, ev in events_raw.items():
        desc = ev.get("desc", {})
        sport_id = desc.get("sport", "")
        sport_info = sports.get(sport_id, {})
        sport_name = sport_info.get("name", f"sport_{sport_id}")

        # optional filter
        if sport_filter:
            if sport_filter.lower() not in sport_name.lower():
                continue

        competitors = desc.get("competitors", [])
        if len(competitors) < 2:
            continue

        team1 = competitors[0].get("name", "?")
        team2 = competitors[1].get("name", "?")

        cat_id = desc.get("category", "")
        tourn_id = desc.get("tournament", "")
        cat_name = categories.get(cat_id, {}).get("name", "")
        tourn_name = tournaments.get(tourn_id, {}).get("name", "")

        # score
        score = ev.get("score", {})
        home_score = score.get("home_score", "")
        away_score = score.get("away_score", "")

        # state / clock
        state = ev.get("state", {})
        clock = state.get("clock", {})
        match_time = clock.get("match_time", "")

        # markets
        markets = ev.get("markets", {})

        # 1x2
        odd_1, odd_x, odd_2 = "", "", ""
        m1x2 = markets.get(MARKET_1X2, {}).get("", {})
        if m1x2:
            odd_1 = m1x2.get("1", {}).get("k", "")
            odd_x = m1x2.get("2", {}).get("k", "")
            odd_2 = m1x2.get("3", {}).get("k", "")

        # fallback: Winner market (no draw)
        if not odd_1 and not odd_2:
            mwinner = markets.get(MARKET_WINNER, {}).get("", {})
            if mwinner:
                odd_1 = mwinner.get("4", {}).get("k", "")
                odd_2 = mwinner.get("5", {}).get("k", "")

        # total (find the first / main variant)
        total_line, odd_over, odd_under = "", "", ""
        m_total = markets.get(MARKET_TOTAL, {})
        if m_total:
            # pick the first specifier key (e.g. "total=2.5")
            for spec_key, outcomes in m_total.items():
                # extract numeric total from key like "total=2.5"
                m = re.search(r"total=([\d.]+)", spec_key)
                if m:
                    total_line = m.group(1)
                    odd_over = outcomes.get("12", {}).get("k", "")
                    odd_under = outcomes.get("13", {}).get("k", "")
                    break  # take first one only

        scheduled_ts = desc.get("scheduled", 0)
        scheduled_dt = datetime.datetime.fromtimestamp(scheduled_ts) if scheduled_ts else None

        results.append({
            "event_id": eid,
            "team1": team1,
            "team2": team2,
            "sport": sport_name,
            "category": cat_name,
            "tournament": tourn_name,
            "home_score": home_score,
            "away_score": away_score,
            "match_time": match_time,
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
# Writer – one CSV per match
# ---------------------------------------------------------------------------

class MatchCSVWriter:
    """Manages one CSV file per live match."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        # event_id → file path
        self._paths: Dict[str, str] = {}

    def write(self, ev: dict) -> None:
        eid = ev["event_id"]
        if eid not in self._paths:
            date = ev["scheduled"].date() if ev["scheduled"] else datetime.date.today()
            path = _csv_path(
                self.output_dir,
                ev["team1"], ev["team2"],
                ev["sport"], ev["tournament"],
                date,
            )
            self._paths[eid] = path
            _write_header_if_needed(path)

        now = datetime.datetime.now().isoformat(timespec="milliseconds")
        row = [
            now,
            ev["match_time"],
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
    output_dir: str = "v2/db",
    interval: float = DEFAULT_POLL_INTERVAL,
    sport_filter: Optional[str] = None,
) -> None:
    client = BetbyClient()
    writer = MatchCSVWriter(output_dir)

    log.info("CoinCasino live-odds scraper started")
    log.info("  output dir : %s", os.path.abspath(output_dir))
    log.info("  interval   : %.1fs", interval)
    log.info("  sport filter: %s", sport_filter or "(all)")
    log.info("Press Ctrl+C to stop.\n")

    cycle = 0
    while True:
        t0 = time.monotonic()
        cycle += 1
        try:
            snapshot = client.fetch_live_snapshot()
            events = parse_events(snapshot, sport_filter=sport_filter)

            for ev in events:
                writer.write(ev)

            # compact status line
            n_total = len(events)
            n_with_odds = sum(1 for e in events if e["odd_1"])
            elapsed = time.monotonic() - t0
            log.info(
                "cycle %4d  |  %3d live events  |  %3d with odds  |  %.2fs",
                cycle, n_total, n_with_odds, elapsed,
            )

        except requests.RequestException as exc:
            log.warning("Network error: %s", exc)
        except Exception:
            log.exception("Unexpected error in cycle %d", cycle)

        # sleep remainder of interval
        elapsed = time.monotonic() - t0
        sleep_time = max(0, interval - elapsed)
        time.sleep(sleep_time)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CoinCasino live-odds scraper (Betby API)"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="v2/db",
        help="Directory for CSV files (default: v2/db)",
    )
    parser.add_argument(
        "-i", "--interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL,
        help=f"Poll interval in seconds (default: {DEFAULT_POLL_INTERVAL})",
    )
    parser.add_argument(
        "-s", "--sport",
        default=None,
        help="Filter by sport name, e.g. 'soccer', 'tennis' (case-insensitive)",
    )
    args = parser.parse_args()

    try:
        run(
            output_dir=args.output_dir,
            interval=args.interval,
            sport_filter=args.sport,
        )
    except KeyboardInterrupt:
        log.info("\nStopped by user.")


if __name__ == "__main__":
    main()

