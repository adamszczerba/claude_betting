"""
CoinCasino Live Soccer Odds Scraper
=====================================
Fetches live soccer match data (scores + odds) every 2s from the Betby API
that powers coincasino.com/en/sports?bt-path=%2Flive

Each live match gets its own CSV file:
    {team1}_vs_{team2}_{tournament}_cc_{date}.csv

Usage:
    python v2/coincasino_scraper.py
    python v2/coincasino_scraper.py --interval 5      # poll every 5s
    python v2/coincasino_scraper.py -o my_data_dir
"""

import argparse
import csv
import datetime
import logging
import os
import re
import time
from typing import Dict, List, Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BETBY_API_BASE = "https://api-h-c7818b61-608.sptpub.com"
BRAND_ID = "2432254656609652736"
LANGUAGE = "en"
BOOKMAKER_TAG = "cc"  # short tag used in filenames

SOCCER_SPORT_ID = "1"  # Betby sport id for real soccer (not eSoccer)

DEFAULT_POLL_INTERVAL = 2.0  # seconds

# Key market IDs
MARKET_1X2 = "1"   # outcomes: 1=home, 2=draw, 3=away
MARKET_TOTAL = "18" # outcomes: 12=over, 13=under  (specifier: total=N)

# Match-status codes relevant for soccer
STATUS_LABELS = {
    6: "1H",       # 1st half
    7: "2H",       # 2nd half
    31: "HT",      # halftime
    32: "AET",     # awaiting extra time
    33: "ETHT",    # extra-time halftime
    41: "ET1",     # 1st extra-time half
    42: "ET2",     # 2nd extra-time half
    50: "PEN",     # penalties
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
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
# Clock computation
# ---------------------------------------------------------------------------

def _compute_match_time(clock: dict) -> str:
    """
    Return a human-readable match minute string like "67:23".

    The API provides:
      - match_time  : "MM:SS" at the moment the clock was last updated
      - stopped     : bool – whether the clock is paused (e.g. halftime)
      - timestamp   : unix-ts (seconds if <=12 digits, ms if 13 digits)

    When the clock is running (stopped=false) we add the elapsed wall-clock
    time since `timestamp` to get the *current* match minute.
    """
    raw = clock.get("match_time", "")
    if not raw:
        return ""

    stopped = clock.get("stopped", True)   # default to stopped if absent
    ts = clock.get("timestamp")

    if stopped or ts is None:
        return raw  # clock frozen -> return as-is

    # normalise timestamp to seconds
    if isinstance(ts, (int, float)) and ts > 1_000_000_000_000:
        ts_sec = ts / 1000.0
    else:
        ts_sec = float(ts)

    # parse "MM:SS" -> total seconds
    parts = raw.split(":")
    try:
        base_min = int(parts[0])
        base_sec = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        return raw

    base_total_sec = base_min * 60 + base_sec

    # elapsed seconds since the clock snapshot
    now_sec = time.time()
    elapsed = max(0.0, now_sec - ts_sec)

    current_total_sec = int(base_total_sec + elapsed)
    mins = current_total_sec // 60
    secs = current_total_sec % 60
    return f"{mins}:{secs:02d}"


# ---------------------------------------------------------------------------
# Betby API client
# ---------------------------------------------------------------------------

class BetbyClient:
    """Thin wrapper around the Betby sptpub REST API used by CoinCasino."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def fetch_live_snapshot(self) -> dict:
        """
        Return the full live-events snapshot (top + rest chunks merged).
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
# Event parser  (soccer only, sport_id == "1")
# ---------------------------------------------------------------------------

def parse_soccer_events(snapshot: dict) -> List[dict]:
    """
    Extract only real soccer events (sport id "1") from the snapshot.
    """
    events_raw = snapshot.get("events", {})
    categories = snapshot.get("categories", {})
    tournaments = snapshot.get("tournaments", {})

    results: list[dict] = []
    for eid, ev in events_raw.items():
        desc = ev.get("desc", {})

        # -- soccer only ------------------------------------------------
        if desc.get("sport") != SOCCER_SPORT_ID:
            continue
        # skip virtual / simulated matches
        if desc.get("virtual", False):
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

        # -- score -------------------------------------------------------
        score = ev.get("score", {})
        home_score = score.get("home_score", "")
        away_score = score.get("away_score", "")

        # -- match time (computed from running clock) --------------------
        state = ev.get("state", {})
        clock = state.get("clock", {})
        match_time = _compute_match_time(clock)

        match_status_code = state.get("match_status")
        match_status = STATUS_LABELS.get(match_status_code,
                                          str(match_status_code) if match_status_code else "")

        # -- 1x2 odds ---------------------------------------------------
        markets = ev.get("markets", {})
        odd_1, odd_x, odd_2 = "", "", ""
        m1x2 = markets.get(MARKET_1X2, {}).get("", {})
        if m1x2:
            odd_1 = m1x2.get("1", {}).get("k", "")
            odd_x = m1x2.get("2", {}).get("k", "")
            odd_2 = m1x2.get("3", {}).get("k", "")

        # -- total (over/under) -----------------------------------------
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
            "category": cat_name,
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
    output_dir: str = "match_database/coincasino",
    interval: float = DEFAULT_POLL_INTERVAL,
) -> None:
    client = BetbyClient()
    writer = MatchCSVWriter(output_dir)

    log.info("CoinCasino live SOCCER odds scraper started")
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

        elapsed = time.monotonic() - t0
        sleep_time = max(0.0, interval - elapsed)
        time.sleep(sleep_time)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CoinCasino live soccer odds scraper (Betby API)"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="match_database/coincasino",
        help="Directory for CSV files (default: match_database/coincasino)",
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

