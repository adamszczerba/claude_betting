"""
Scan match_database/{bookmaker}/{today}/ and return the latest odds row
for every CSV file found.  Bookmaker identity is derived from the directory
name; team/tournament are parsed from the filename.
"""

import csv
import datetime
import os
import re
from typing import Dict, List, Optional

# Bookmaker directories → display‐friendly labels
BOOKMAKERS = {
    "coincasino": "CoinCasino",
    "betfair": "Betfair",
    "bet365": "Bet365",
    "betfair_exchange": "BetfairExchange",
}

# Tags embedded in CSV filenames
_TAG_TO_BOOKMAKER = {"cc": "coincasino", "bf": "betfair",
                     "b365": "bet365", "bfx": "betfair_exchange"}

# Standard 11-column schema shared by all bookmakers
STANDARD_COLUMNS = [
    "timestamp", "match_time", "match_status",
    "home_score", "away_score",
    "odd_1", "odd_X", "odd_2",
    "total_line", "odd_over", "odd_under",
]

# CoinCasino adds 14 more columns
CC_EXTRA_COLUMNS = [
    "odd_dc_1X", "odd_dc_12", "odd_dc_X2",
    "odd_dnb_1", "odd_dnb_2",
    "odd_penalty_yes", "odd_penalty_no",
    "correct_score",
    "corners_line", "odd_corners_over", "odd_corners_under",
    "bookings_line", "odd_bookings_over", "odd_bookings_under",
]

# Regex for the filename convention:
#   {team1}_vs_{team2}_{tournament}_{tag}_{YYYY-MM-DD}.csv
_FILENAME_RE = re.compile(
    r"^(.+?)_vs_(.+?)_(.+?)_("
    + "|".join(_TAG_TO_BOOKMAKER.keys())
    + r")_(\d{4}-\d{2}-\d{2})\.csv$"
)


def _parse_filename(fname: str) -> Optional[dict]:
    """Extract team1, team2, tournament, bookmaker from a CSV filename."""
    m = _FILENAME_RE.match(fname)
    if not m:
        return None
    return {
        "team1": m.group(1),
        "team2": m.group(2),
        "tournament": m.group(3),
        "bookmaker": _TAG_TO_BOOKMAKER[m.group(4)],
        "date": m.group(5),
    }


def _last_csv_row(path: str) -> Optional[Dict[str, str]]:
    """Read only the last data row of a CSV (header + rows).

    Uses seek-from-end to find the last line in O(1) disk reads instead of
    iterating every row from the beginning. For a file with N rows this
    changes complexity from O(N) to O(1).
    """
    try:
        with open(path, "rb") as f:
            # --- header (first line) ---
            header_line = f.readline()
            if not header_line:
                return None
            header = next(csv.reader([header_line.decode("utf-8")]))
            if not header:
                return None

            # --- seek to find last line ---
            chunk_size = 1024
            f.seek(0, 2)  # seek to end
            file_size = f.tell()
            if file_size <= len(header_line):
                return None  # header only, no data rows

            # Read backwards from end to find the start of the last line
            pos = file_size
            buf = b""
            last_line = None
            while pos > 0:
                read_size = min(chunk_size, pos)
                pos -= read_size
                f.seek(pos)
                chunk = f.read(read_size)
                buf = chunk + buf
                lines = buf.split(b"\n")
                non_empty = [ln for ln in lines if ln.strip()]
                if len(non_empty) >= 2:
                    last_line = non_empty[-1]
                    break
                elif len(non_empty) >= 1 and pos == 0:
                    last_line = non_empty[0]
                    break

            if last_line is None:
                return None

            # Handle \r\n line endings
            last_line = last_line.rstrip(b"\r")
            row = next(csv.reader([last_line.decode("utf-8")]))
            if not row:
                return None
            # Pad if row is shorter than header (partial write)
            while len(row) < len(header):
                row.append("")
            return dict(zip(header, row))
    except Exception:
        return None


def scan_today(
    db_root: str = "match_database",
    date: Optional[datetime.date] = None,
) -> List[dict]:
    """Return a list of dicts, one per match file, with latest odds + metadata.

    Each dict has keys:
        bookmaker, team1, team2, tournament, date,
        + all CSV column values from the last row.
    """
    if date is None:
        date = datetime.date.today()
    date_str = str(date)

    results: List[dict] = []
    for bk_dir in BOOKMAKERS:
        day_path = os.path.join(db_root, bk_dir, date_str)
        if not os.path.isdir(day_path):
            continue
        for fname in os.listdir(day_path):
            if not fname.endswith(".csv"):
                continue
            info = _parse_filename(fname)
            if info is None:
                continue
            row = _last_csv_row(os.path.join(day_path, fname))
            if row is None:
                continue
            row["bookmaker"] = info["bookmaker"]
            row["team1"] = info["team1"]
            row["team2"] = info["team2"]
            row["tournament"] = info["tournament"]
            results.append(row)
    return results
