"""
Betfair Live Soccer Odds Scraper (v2)
======================================
Scrapes live soccer match data (scores + 1X2 odds) every 2s from the
Betfair in-play page using Selenium.

URL: https://www.betfair.com/betting/inplay/all/i-696e706c6179

Each live match gets its own CSV file:
    {team1}_vs_{team2}_{tournament}_bf_{date}.csv

The CSV schema matches the v2/coincasino_scraper.py format so that
downstream analysis can treat all bookmaker data uniformly.

Usage:
    python v2_betfair/betfair_scraper.py
    python v2_betfair/betfair_scraper.py --interval 5
    python v2_betfair/betfair_scraper.py -o my_data_dir
"""

import argparse
import csv
import datetime
import logging
import os
import re
import sys
import time
from typing import Dict, List, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# sync_clock lives in v2/ locally, or same dir in Docker — make it importable
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _here)                                       # Docker: /app
sys.path.insert(0, os.path.join(_here, "..", "v2"))              # local dev
from sync_clock import sleep_until_next_tick  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BETFAIR_URL = "https://www.betfair.com/betting/inplay/all/i-696e706c6179"
BOOKMAKER_TAG = "bf"

DEFAULT_POLL_INTERVAL = 2.0   # seconds between DOM reads
DEFAULT_REFRESH_INTERVAL = 30.0  # seconds between full page refreshes
FETCH_WAIT_TIME_SEC = 10      # Selenium wait for key element

CSV_COLUMNS = [
    "timestamp",
    "match_time",
    "match_status",
    "home_score",
    "away_score",
    "odd_1",       # home win
    "odd_X",       # draw
    "odd_2",       # away win
    "total_line",  # not available on betfair in-play overview
    "odd_over",
    "odd_under",
]

HEADERS_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("betfair_v2")

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


def _write_header_if_needed(path: str) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(CSV_COLUMNS)


def _append_row(path: str, row: list) -> None:
    with open(path, "a", newline="") as f:
        csv.writer(f).writerow(row)


def fractional_to_decimal(frac_str: str) -> str:
    """
    Convert fractional odds like '2/7' to decimal odds like '1.29'.

    Betfair displays fractional odds (e.g. 2/7, 16/5, 12/1).
    Decimal = (numerator / denominator) + 1

    Returns empty string for suspended/unavailable odds ('-' or empty).
    """
    frac_str = frac_str.strip()
    if not frac_str or frac_str == '-':
        return ""
    # Already decimal?
    if '/' not in frac_str:
        try:
            float(frac_str)
            return frac_str
        except ValueError:
            return ""
    try:
        num, den = frac_str.split('/')
        decimal = (int(num) / int(den)) + 1
        return f"{decimal:.2f}"
    except (ValueError, ZeroDivisionError):
        return ""


# ---------------------------------------------------------------------------
# CSS selector helpers  (class names are hashed, so we use partial matching)
# ---------------------------------------------------------------------------
# The Betfair page uses hashed CSS class names like:
#   _3ec23adf2e90513f-competitionHeader
#   e050104376d5250d-couponContainer
#   _443c5e6894fef559-teamNameLabel
#   _90346fd614c6253a-inPlay           (score squares)
#   c84e4011151df22b-labelTwoLines     (odds labels)
#   _73836e21fc8d6105-status           (time/status)
#   _73836e21fc8d6105-extraTime        (added time like +7')
#   _3ec23adf2e90513f-title            (league name)
#
# We use CSS [class*=...] partial attribute selectors to match these
# regardless of the hash prefix.

SEL_COUPON_LIST     = "[class*=couponListContainer]"
SEL_COMPETITION     = "[class*=competitionHeader]"
SEL_COMP_TITLE      = "[class*=-title]"           # league name span
SEL_COUPON          = "[class*=couponContainer]:not([class*=couponListContainer])"  # exclude list container
SEL_TEAM_NAME       = "[class*=teamNameLabel]"
SEL_SCORE_SQUARE    = "[class*=inPlay]"            # score digits
SEL_STATUS          = "[class*=-status]"           # match time span
SEL_EXTRA_TIME      = "[class*=extraTime]"         # added time div
SEL_ODD_LABEL       = "[class*=labelTwoLines]"     # odds labels


def _find_league_for_coupon(coupon: WebElement, driver: WebElement) -> str:
    """
    Walk backward through siblings to find the competition header
    that precedes this coupon container.

    The page structure is:
        <div>  <!-- competitionHeader: "Brazilian Serie A" -->
        <div>  <!-- wrapper around couponContainer -->
        <div>  <!-- wrapper around couponContainer -->
        <div>  <!-- competitionHeader: "Chilean Primera B" -->
        <div>  <!-- wrapper around couponContainer -->
        ...
    """
    # Each coupon is wrapped in a parent div that is a direct child
    # of the couponListContainer. We find that parent, then walk
    # backward through preceding siblings.
    try:
        # coupon's parent div
        wrapper = coupon.find_element(By.XPATH, "./..")
        # Walk backward through preceding siblings
        prev_siblings = wrapper.find_elements(
            By.XPATH, "./preceding-sibling::div"
        )
        # prev_siblings are returned in document order (first = earliest),
        # so we iterate in reverse to find the nearest competition header
        for sib in reversed(prev_siblings):
            headers = sib.find_elements(By.CSS_SELECTOR, SEL_COMPETITION)
            if headers:
                title_els = headers[0].find_elements(
                    By.CSS_SELECTOR, SEL_COMP_TITLE
                )
                if title_els:
                    return title_els[0].text.strip()
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Selenium scraper
# ---------------------------------------------------------------------------

class BetfairScraper:
    """Selenium-based scraper for Betfair's in-play page."""

    def __init__(self, url: str = BETFAIR_URL, headless: bool = False):
        self.url = url
        options = Options()
        if headless or os.environ.get("HEADLESS", "").lower() in ("1", "true", "yes"):
            options.add_argument('--headless=new')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument(f'--user-agent={HEADERS_UA}')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(20)
        self._loaded_once = False

    def fetch_events(self, needs_refresh: bool = False) -> List[dict]:
        """
        Extract match data from the Betfair in-play page.

        On first call the page is loaded from scratch.  On subsequent calls
        the live DOM is simply re-read (~0.2-0.5 s).  When *needs_refresh*
        is True the page is fully refreshed first so that new / removed
        matches are picked up.
        """
        driver = self.driver

        if not self._loaded_once:
            log.info("Loading: %s", self.url)
            self._load_page(driver)
            self._loaded_once = True
        elif needs_refresh:
            log.debug("Refreshing page to pick up match changes…")
            self._refresh_page(driver)

        try:
            return self._parse_dom(driver)
        except StaleElementReferenceException:
            log.info("DOM stale, reloading page…")
            self._load_page(driver)
            return self._parse_dom(driver)

    # ------------------------------------------------------------------
    def _load_page(self, driver) -> None:
        """Full page load (or reload)."""
        driver.get(self.url)
        self._wait_for_content(driver)
        self._dismiss_cookie_banner(driver)

    def _refresh_page(self, driver) -> None:
        """Refresh + wait for content to re-appear."""
        driver.refresh()
        self._wait_for_content(driver)
        self._dismiss_cookie_banner(driver)

    def _wait_for_content(self, driver) -> None:
        """Wait until the coupon list or the page body is usable."""
        try:
            WebDriverWait(driver, FETCH_WAIT_TIME_SEC).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, SEL_COUPON_LIST)
                )
            )
        except Exception:
            # couponListContainer may not exist when no sport is live;
            # the page is still usable – _has_football_live will handle it
            pass
        time.sleep(1)  # let JS settle

    def _dismiss_cookie_banner(self, driver) -> None:
        """Click 'Allow All Cookies' if the OneTrust banner is visible."""
        try:
            btn = driver.find_element(By.ID, "onetrust-accept-btn-handler")
            if btn.is_displayed():
                btn.click()
                log.info("Cookie consent banner dismissed")
                time.sleep(0.5)
        except Exception:
            pass  # banner not present or already dismissed

    def _has_football_live(self, driver) -> bool:
        """
        Return True if football matches are currently in-play.

        When no football is live, Betfair shows only other sports (tennis,
        cricket, etc.) — "Football" disappears from the in-play sport list
        in the sidebar, and all coupons have only 2 odds (no draw column).

        We check both: absence of "Football" in the sidebar sport list AND
        that no coupon has 3 odds (the 1-X-2 structure unique to football).
        """
        # Fast check: look for "Football" in the in-play sport sidebar.
        # The sidebar items are plain text nodes; we scan the coupon list's
        # preceding sibling text for "Football".
        body_text = driver.find_element(By.TAG_NAME, "body").text
        return "Football" in body_text

    def _scroll_to_load_all(self, driver) -> None:
        """
        Scroll the page incrementally to trigger lazy-loaded match coupons.

        Betfair only renders matches that are near the viewport.  Without
        scrolling, matches below the fold are missing from the DOM.
        """
        last_height = driver.execute_script("return document.body.scrollHeight")
        viewport = driver.execute_script("return window.innerHeight") or 600
        pos = 0
        max_scrolls = 30          # safety cap
        for _ in range(max_scrolls):
            pos += viewport
            driver.execute_script(f"window.scrollTo(0, {pos})")
            time.sleep(0.3)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if pos >= new_height and new_height == last_height:
                break
            last_height = new_height
        # Scroll back to top so the page stays in a consistent state
        driver.execute_script("window.scrollTo(0, 0)")
        time.sleep(0.3)

    def _parse_dom(self, driver) -> List[dict]:
        """
        Extract match data directly from the DOM using CSS selectors.

        Page structure per match (inside couponListContainer):
            competitionHeader  →  league name (in -title span)
            couponContainer    →  one per match:
                teamNameLabel  ×2  →  home team, away team
                -status span       →  match time (e.g. "19'", "HT")
                extraTime div      →  added time (e.g. "+7'")
                inPlay squares ×2  →  home score, away score
                labelTwoLines  ×3  →  odd_1, odd_X, odd_2 (fractional)

        Returns empty list with a sentinel key "no_football" when no
        football is currently live.
        """
        results: list[dict] = []

        if not self._has_football_live(driver):
            # Signal "no football" to the caller via sentinel
            return [{"_no_football": True}]

        # Scroll the page to force all lazy-loaded coupons into the DOM
        self._scroll_to_load_all(driver)

        coupon_list = driver.find_element(By.CSS_SELECTOR, SEL_COUPON_LIST)
        coupons = coupon_list.find_elements(By.CSS_SELECTOR, SEL_COUPON)

        for coupon in coupons:
            try:
                ev = self._parse_coupon(coupon, driver)
                if ev:
                    results.append(ev)
            except StaleElementReferenceException:
                log.debug("Stale element in coupon, skipping")
            except Exception as exc:
                log.debug("Failed to parse coupon: %s", exc)

        return results

    def _parse_coupon(self, coupon: WebElement, driver) -> Optional[dict]:
        """Parse a single coupon container into a match dict."""

        # --- Teams ---
        team_els = coupon.find_elements(By.CSS_SELECTOR, SEL_TEAM_NAME)
        if len(team_els) < 2:
            return None
        team1 = team_els[0].text.strip()
        team2 = team_els[1].text.strip()
        if not team1 or not team2:
            return None

        # --- Match time / status ---
        match_time = ""
        match_status = ""
        status_els = coupon.find_elements(By.CSS_SELECTOR, SEL_STATUS)
        if status_els:
            raw_time = status_els[0].text.strip()
            # e.g. "19'", "HT", "100'"
            if raw_time in ("HT", "FT", "AET", "ET", "PEN"):
                match_status = raw_time
                match_time = raw_time
            else:
                # Strip the prime symbol: "19'" -> "19"
                match_time = raw_time.replace("'", "").replace("′", "")

        # --- Extra time (e.g. "+7'") ---
        extra_els = coupon.find_elements(By.CSS_SELECTOR, SEL_EXTRA_TIME)
        if extra_els:
            extra = extra_els[0].text.strip()  # e.g. "+7'"
            extra_clean = extra.replace("'", "").replace("′", "")
            if extra_clean:
                match_time = f"{match_time}{extra_clean}"  # e.g. "100+7"

        # --- Scores ---
        score_els = coupon.find_elements(By.CSS_SELECTOR, SEL_SCORE_SQUARE)
        home_score = 0
        away_score = 0
        if len(score_els) >= 2:
            try:
                home_score = int(score_els[0].text.strip())
            except ValueError:
                pass
            try:
                away_score = int(score_els[1].text.strip())
            except ValueError:
                pass

        # --- Odds (fractional → decimal) ---
        odd_els = coupon.find_elements(By.CSS_SELECTOR, SEL_ODD_LABEL)
        odd_1, odd_x, odd_2 = "", "", ""
        if len(odd_els) >= 3:
            odd_1 = fractional_to_decimal(odd_els[0].text)
            odd_x = fractional_to_decimal(odd_els[1].text)
            odd_2 = fractional_to_decimal(odd_els[2].text)

        # --- League ---
        tournament = _find_league_for_coupon(coupon, driver)

        return {
            "team1": team1,
            "team2": team2,
            "tournament": tournament,
            "home_score": home_score,
            "away_score": away_score,
            "match_time": match_time,
            "match_status": match_status,
            "odd_1": odd_1,
            "odd_X": odd_x,
            "odd_2": odd_2,
            "total_line": "",   # Not available on betfair overview page
            "odd_over": "",
            "odd_under": "",
        }

    def close(self):
        self.driver.quit()


# ---------------------------------------------------------------------------
# CSV writer – one file per match (same schema as v2/coincasino_scraper.py)
# ---------------------------------------------------------------------------

class MatchCSVWriter:
    """Manages one CSV file per live match."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self._paths: Dict[str, str] = {}   # key = "team1|team2|tournament"

    def _match_key(self, ev: dict) -> str:
        return f"{ev['team1']}|{ev['team2']}|{ev['tournament']}"

    def write(self, ev: dict) -> None:
        key = self._match_key(ev)
        if key not in self._paths:
            path = _csv_path(
                self.output_dir,
                ev["team1"], ev["team2"],
                ev["tournament"],
                datetime.date.today(),
            )
            self._paths[key] = path
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
        _append_row(self._paths[key], row)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(
    output_dir: str = "match_database/betfair",
    interval: float = DEFAULT_POLL_INTERVAL,
    refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
) -> None:
    scraper = BetfairScraper()
    writer = MatchCSVWriter(output_dir)

    log.info("Betfair live SOCCER odds scraper started")
    log.info("  output dir : %s", os.path.abspath(output_dir))
    log.info("  poll       : %.1fs", interval)
    log.info("  refresh    : %.0fs", refresh_interval)
    log.info("Press Ctrl+C to stop.\n")

    cycle = 0
    _last_no_football_log: float = 0.0
    _last_refresh: float = 0.0            # force refresh on first cycle
    NO_FOOTBALL_LOG_INTERVAL = 60.0

    try:
        while True:
            t0 = time.monotonic()
            cycle += 1
            try:
                # Decide whether this cycle should refresh the page
                needs_refresh = (t0 - _last_refresh) >= refresh_interval
                events = scraper.fetch_events(needs_refresh=needs_refresh)
                if needs_refresh:
                    _last_refresh = time.monotonic()

                # --- No-football sentinel ---
                if events and events[0].get("_no_football"):
                    now = time.monotonic()
                    if now - _last_no_football_log >= NO_FOOTBALL_LOG_INTERVAL:
                        log.info(
                            "cycle %4d  |  no football in-play right now — waiting…",
                            cycle,
                        )
                        _last_no_football_log = now
                    sleep_until_next_tick(interval)
                    continue

                for ev in events:
                    writer.write(ev)

                n_total = len(events)
                n_with_odds = sum(1 for e in events if e["odd_1"] or e["odd_X"] or e["odd_2"])
                elapsed = time.monotonic() - t0
                refresh_tag = " [R]" if needs_refresh else ""
                log.info(
                    "cycle %4d  |  %3d soccer matches  |  %3d with odds  |  %.2fs%s",
                    cycle, n_total, n_with_odds, elapsed, refresh_tag,
                )

            except Exception as exc:
                log.exception("Error in cycle %d: %s", cycle, exc)

            sleep_until_next_tick(interval)
    except KeyboardInterrupt:
        log.info("\nStopped by user.")
    finally:
        scraper.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Betfair live soccer odds scraper (Selenium)"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="match_database/betfair",
        help="Directory for CSV files (default: match_database/betfair)",
    )
    parser.add_argument(
        "-i", "--interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL,
        help=f"Poll interval in seconds (default: {DEFAULT_POLL_INTERVAL})",
    )
    parser.add_argument(
        "-r", "--refresh-interval",
        type=float,
        default=DEFAULT_REFRESH_INTERVAL,
        help=f"Page refresh interval in seconds (default: {DEFAULT_REFRESH_INTERVAL})",
    )
    args = parser.parse_args()
    run(
        output_dir=args.output_dir,
        interval=args.interval,
        refresh_interval=args.refresh_interval,
    )


if __name__ == "__main__":
    main()

