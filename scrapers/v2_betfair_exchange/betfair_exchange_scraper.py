"""
Betfair Exchange Live Soccer Odds Scraper
==========================================
Scrapes live soccer match data (scores + 1X2 odds) every 2s from the
Betfair Exchange in-play page using Selenium.

URL: https://www.betfair.com/exchange/plus/inplay/all

The Exchange shows **back** (buy) and **lay** (sell) decimal prices.
The recorded odd is the average of the best back and best lay price
for each selection (1, X, 2).

Each live match gets its own CSV file:
    {team1}_vs_{team2}_{tournament}_bfx_{date}.csv

The CSV schema matches v2_betfair/betfair_scraper.py and
v2_coincasino/coincasino_scraper.py so that downstream analysis can
treat all bookmaker data uniformly.

Usage:
    python v2_betfair_exchange/betfair_exchange_scraper.py
    python v2_betfair_exchange/betfair_exchange_scraper.py --interval 5
    python v2_betfair_exchange/betfair_exchange_scraper.py -o my_data_dir
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

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# sync_clock lives in v2_coincasino/ locally, or same dir in Docker — make it importable
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _here)                                               # Docker: /app
sys.path.insert(0, os.path.join(_here, "..", "v2_coincasino"))          # local dev
from sync_clock import sleep_until_next_tick  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EXCHANGE_URL = "https://www.betfair.com/exchange/plus/inplay/football"
BOOKMAKER_TAG = "bfx"

DEFAULT_POLL_INTERVAL = 2.0   # seconds between DOM reads
DEFAULT_REFRESH_INTERVAL = 30.0  # seconds between full page refreshes
FETCH_WAIT_TIME_SEC = 15      # Selenium wait for key element

CSV_COLUMNS = [
    "timestamp",
    "match_time",
    "match_status",
    "home_score",
    "away_score",
    "odd_1",       # home win  (average of back & lay)
    "odd_X",       # draw      (average of back & lay)
    "odd_2",       # away win  (average of back & lay)
    "total_line",  # not available on exchange overview
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
log = logging.getLogger("betfair_exchange")

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


def average_back_lay(back_str: str, lay_str: str) -> str:
    """
    Compute the average of back (buy) and lay (sell) decimal prices.

    The Betfair Exchange shows decimal odds directly (e.g. 1.45, 3.20).
    Returns the midpoint as a string with 2 decimal places.
    Returns whichever is available if only one is present.
    Returns empty string if neither is available.
    """
    back_str = back_str.strip() if back_str else ""
    lay_str = lay_str.strip() if lay_str else ""

    back_val = _parse_decimal(back_str)
    lay_val = _parse_decimal(lay_str)

    if back_val is not None and lay_val is not None:
        return f"{(back_val + lay_val) / 2:.2f}"
    elif back_val is not None:
        return f"{back_val:.2f}"
    elif lay_val is not None:
        return f"{lay_val:.2f}"
    return ""


def _parse_decimal(s: str):
    """Try to parse a string as a float. Return None on failure."""
    if not s or s == '-':
        return None
    try:
        val = float(s)
        return val if val > 0 else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# JavaScript snippet that extracts ALL match data in a single call.
#
# The Betfair Exchange in-play page is an AngularJS 1.x SPA.  The DOM
# structure (from actual page inspection) is:
#
#   <bf-coupon-table>                               ← Angular component
#     <table class="coupon-table">
#       <tr ng-repeat-start="(marketId, event) ..."> ← one per match
#         <td>
#           <a class="mod-link"
#              data-event-type-name="football"
#              data-competition-or-venue-name="...">
#             <event-line>
#               <section class="mod-event-line">
#                 <bf-livescores>
#                   <div class="bf-livescores-time-elapsed">
#                     <div class="middle-label">69'</div>   ← time
#                   </div>
#                   <div class="bf-livescores-match-scores">
#                     <div class="scores">
#                       <span class="home">1</span>         ← home score
#                       <span class="away">0</span>         ← away score
#                     </div>
#                   </div>
#                 </bf-livescores>
#                 <ul class="runners">
#                   <li class="name">Team A</li>             ← team names
#                   <li class="name">Team B</li>
#                 </ul>
#               </section>
#             </event-line>
#           </a>
#         </td>
#         <td class="coupon-runners" colspan="3">
#           <div class="coupon-runner">               ← one per selection
#             <ours-price-button type="back">         ← back price
#               <button class="...">
#                 <span class="...">1.45</span>       ← price text
#               </button>
#             </ours-price-button>
#             <ours-price-button type="lay">          ← lay price (if shown)
#               ...
#             </ours-price-button>
#           </div>
#         </td>
#       </tr>
# ---------------------------------------------------------------------------
_JS_EXTRACT_ALL = r"""
var results = [];

// Check if Football content is on the page
var pageText = document.body.innerText || "";
var hasFootball = pageText.indexOf("Football") !== -1 ||
                  pageText.indexOf("Soccer") !== -1;
if (!hasFootball) return {football: false, matches: []};

// Find all event rows in coupon tables
var rows = document.querySelectorAll(
    'table.coupon-table > tbody > tr[ng-repeat-start]'
);
if (rows.length === 0) {
    // Fallback: any tr that contains a mod-link with event data
    rows = document.querySelectorAll('table.coupon-table tr');
}

function getText(el) {
    return el ? (el.innerText || el.textContent || "").trim() : "";
}

for (var i = 0; i < rows.length; i++) {
    var row = rows[i];

    // Find the event link — it has all the metadata as data attributes
    var link = row.querySelector('a.mod-link[data-event-type-name]');
    if (!link) continue;

    // ── Filter: only football events ──
    var sport = (link.getAttribute('data-event-type-name') || '').toLowerCase();
    if (sport !== 'football' && sport !== 'soccer') continue;

    // ── Team names: <ul class="runners"> <li class="name"> ──
    var nameEls = row.querySelectorAll('ul.runners li.name');
    if (nameEls.length < 2) continue;
    var t1 = getText(nameEls[0]);
    var t2 = getText(nameEls[1]);
    if (!t1 || !t2) continue;

    // ── Tournament/competition from data attribute ──
    var compSlug = link.getAttribute('data-competition-or-venue-name') || '';
    // Convert slug "colombian-primera-b" → "Colombian Primera B"
    var tournament = compSlug.replace(/-/g, ' ').replace(/\b\w/g, function(c){
        return c.toUpperCase();
    });

    // ── Scores: <div class="scores"> <span class="home">1</span> ... ──
    var scoreContainer = row.querySelector('.bf-livescores-match-scores .scores');
    var hs = 0, as_score = 0;
    if (scoreContainer) {
        var homeEl = scoreContainer.querySelector('.home');
        var awayEl = scoreContainer.querySelector('.away');
        if (homeEl) hs = parseInt(getText(homeEl)) || 0;
        if (awayEl) as_score = parseInt(getText(awayEl)) || 0;
    }

    // ── Match time/status ──
    var matchTime = "", matchStatus = "";
    // Time elapsed: <div class="middle-label">69'</div>
    var timeEl = row.querySelector('.bf-livescores-time-elapsed .middle-label');
    if (timeEl) {
        var raw = getText(timeEl);
        if (["HT","FT","AET","ET","PEN","END"].indexOf(raw) !== -1) {
            matchStatus = raw;
            matchTime = raw;
        } else {
            matchTime = raw.replace(/['\u2032]/g, "");
        }
    }
    // Bottom label (e.g. "END" for extra time end)
    var bottomEl = row.querySelector('.bf-livescores-time-elapsed .bottom-label');
    if (bottomEl) {
        var bl = getText(bottomEl);
        if (bl) matchStatus = bl;
    }
    // Starting soon / not yet started
    if (!matchTime) {
        var startEl = row.querySelector('.bf-livescores-start-date .label');
        if (startEl) {
            matchTime = getText(startEl);
        }
    }

    // ── Odds: extract from <ours-price-button> or <div class="coupon-runner"> ──
    // The actual DOM renders prices as:
    //   <ours-price-button type="back">
    //     <button class="... back ...">
    //       <label class="Zs3u5 ...">18</label>    ← price
    //       <label class="He6+y ...">£15</label>   ← size
    //     </button>
    //   </ours-price-button>
    var runners = row.querySelectorAll('.coupon-runner, div.coupon-runner');
    var backs = [], lays = [];

    for (var r = 0; r < runners.length; r++) {
        var runner = runners[r];

        // Back price button
        var backBtn = runner.querySelector('ours-price-button[type="back"]');
        var backPrice = "";
        if (backBtn) {
            // The price is in the first <label> inside the <button>
            var labels = backBtn.querySelectorAll('button label');
            if (labels.length >= 1) {
                backPrice = getText(labels[0]);
            }
            // Fallback: try span
            if (!backPrice || backPrice === '-') {
                var priceEl = backBtn.querySelector('span');
                if (priceEl) backPrice = getText(priceEl);
            }
            // Fallback: extract first number from button text
            if (!backPrice || backPrice === '-') {
                var btnEl = backBtn.querySelector('button') || backBtn;
                var btnText = getText(btnEl);
                var priceMatch = btnText.match(/(\d+\.?\d*)/);
                if (priceMatch) backPrice = priceMatch[1];
            }
        }
        backs.push(backPrice);

        // Lay price button
        var layBtn = runner.querySelector('ours-price-button[type="lay"]');
        var layPrice = "";
        if (layBtn) {
            var layLabels = layBtn.querySelectorAll('button label');
            if (layLabels.length >= 1) {
                layPrice = getText(layLabels[0]);
            }
            if (!layPrice || layPrice === '-') {
                var laySpan = layBtn.querySelector('span');
                if (laySpan) layPrice = getText(laySpan);
            }
            if (!layPrice || layPrice === '-') {
                var layBtnEl = layBtn.querySelector('button') || layBtn;
                var layBtnText = getText(layBtnEl);
                var layMatch = layBtnText.match(/(\d+\.?\d*)/);
                if (layMatch) layPrice = layMatch[1];
            }
        }
        lays.push(layPrice);
    }

    // For 1X2: backs[0]=1, backs[1]=X, backs[2]=2
    var b1 = backs[0] || "", bx = backs[1] || "", b2 = backs[2] || "";
    var l1 = lays[0]  || "", lx = lays[1]  || "", l2 = lays[2]  || "";

    // ── Check suspended state ──
    var isSuspended = !!row.querySelector('.state-overlay-container, .suspended-container');

    results.push({
        team1: t1, team2: t2, tournament: tournament,
        home_score: hs, away_score: as_score,
        match_time: matchTime, match_status: isSuspended ? "Suspended" : matchStatus,
        back_1: b1, lay_1: l1,
        back_x: bx, lay_x: lx,
        back_2: b2, lay_2: l2
    });
}

return {football: true, matches: results};
"""


# ---------------------------------------------------------------------------
# Selenium scraper
# ---------------------------------------------------------------------------

class BetfairExchangeScraper:
    """Selenium-based scraper for Betfair Exchange in-play page."""

    def __init__(self, url: str = EXCHANGE_URL, headless: bool = False):
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
        
        # Explicitly set Chrome binary path for Docker containers
        chrome_bin = os.environ.get("CHROME_BIN", "/usr/bin/google-chrome")
        if os.path.exists(chrome_bin):
            options.binary_location = chrome_bin
            log.info(f"Using Chrome binary at: {chrome_bin}")
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(40)
        self._loaded_once = False

    def fetch_events(self, needs_refresh: bool = False) -> List[dict]:
        """
        Extract match data from the Betfair Exchange in-play page.

        On first call the page is loaded from scratch.  On subsequent calls
        the live DOM is simply re-read.  When *needs_refresh* is True the
        page is fully refreshed so that new/removed matches are picked up.
        """
        driver = self.driver

        if not self._loaded_once:
            log.info("Loading: %s", self.url)
            self._load_page(driver)
            self._scroll_to_load_all(driver)
            self._loaded_once = True
        elif needs_refresh:
            log.debug("Refreshing page to pick up match changes…")
            self._refresh_page(driver)
            self._scroll_to_load_all(driver)

        try:
            return self._parse_dom(driver)
        except StaleElementReferenceException:
            log.info("DOM stale, reloading page…")
            self._load_page(driver)
            self._scroll_to_load_all(driver)
            return self._parse_dom(driver)

    # ------------------------------------------------------------------
    def _load_page(self, driver) -> None:
        """Full page load (or reload)."""
        driver.get(self.url)
        self._wait_for_content(driver)
        self._dismiss_cookie_banner(driver)

    def _refresh_page(self, driver) -> None:
        """Refresh + wait for content to re-appear with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                driver.refresh()
                self._wait_for_content(driver)
                self._dismiss_cookie_banner(driver)
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    log.warning(f"Page refresh failed (attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(2)
                else:
                    log.error(f"Page refresh failed after {max_retries} attempts, reloading page")
                    # Full reload as fallback
                    self._load_page(driver)
                    self._scroll_to_load_all(driver)

    def _wait_for_content(self, driver) -> None:
        """Wait until the coupon table is usable."""
        try:
            WebDriverWait(driver, FETCH_WAIT_TIME_SEC).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR,
                     "table.coupon-table, bf-coupon-table")
                )
            )
        except Exception:
            pass
        time.sleep(1.5)  # let AngularJS settle and render prices


    def _dismiss_cookie_banner(self, driver) -> None:
        """Click 'Allow All Cookies' if the OneTrust banner is visible."""
        try:
            btn = driver.find_element(By.ID, "onetrust-accept-btn-handler")
            if btn.is_displayed():
                btn.click()
                log.info("Cookie consent banner dismissed")
                time.sleep(0.5)
        except Exception:
            pass

    def _parse_dom(self, driver) -> List[dict]:
        """
        Extract match data from the DOM using a single JavaScript call.
        """
        data = driver.execute_script(
            "return (function(){" + _JS_EXTRACT_ALL + "})()"
        )

        if not data or not data.get("football"):
            return [{"_no_football": True}]

        results: list[dict] = []
        for m in data.get("matches", []):
            results.append({
                "team1": m["team1"],
                "team2": m["team2"],
                "tournament": m.get("tournament", ""),
                "home_score": m.get("home_score", 0),
                "away_score": m.get("away_score", 0),
                "match_time": m.get("match_time", ""),
                "match_status": m.get("match_status", ""),
                "odd_1": average_back_lay(
                    m.get("back_1", ""), m.get("lay_1", "")
                ),
                "odd_X": average_back_lay(
                    m.get("back_x", ""), m.get("lay_x", "")
                ),
                "odd_2": average_back_lay(
                    m.get("back_2", ""), m.get("lay_2", "")
                ),
                "total_line": "",
                "odd_over": "",
                "odd_under": "",
            })
        return results

    def _scroll_to_load_all(self, driver) -> None:
        """
        Scroll the page incrementally to trigger lazy-loaded match rows.
        """
        last_height = driver.execute_script("return document.body.scrollHeight")
        viewport = driver.execute_script("return window.innerHeight") or 600
        pos = 0
        max_scrolls = 40  # safety cap
        for _ in range(max_scrolls):
            pos += viewport
            driver.execute_script(f"window.scrollTo(0, {pos})")
            time.sleep(0.2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if pos >= new_height and new_height == last_height:
                break
            last_height = new_height
        driver.execute_script("window.scrollTo(0, 0)")
        time.sleep(0.2)

    def close(self):
        self.driver.quit()


# ---------------------------------------------------------------------------
# CSV writer – one file per match (same schema as v2_betfair)
# ---------------------------------------------------------------------------

class MatchCSVWriter:
    """Manages one CSV file per live match."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self._paths: Dict[str, str] = {}

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
    output_dir: str = "match_database/betfair_exchange",
    interval: float = DEFAULT_POLL_INTERVAL,
    refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
) -> None:
    scraper = BetfairExchangeScraper()
    writer = MatchCSVWriter(output_dir)

    log.info("Betfair Exchange live SOCCER odds scraper started")
    log.info("  output dir : %s", os.path.abspath(output_dir))
    log.info("  poll       : %.1fs", interval)
    log.info("  refresh    : %.0fs", refresh_interval)
    log.info("  odds       : average(back, lay)")
    log.info("Press Ctrl+C to stop.\n")

    cycle = 0
    _last_no_football_log: float = 0.0
    _last_refresh: float = 0.0
    NO_FOOTBALL_LOG_INTERVAL = 60.0

    try:
        while True:
            t0 = time.monotonic()
            cycle += 1
            try:
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
                n_with_odds = sum(
                    1 for e in events
                    if e["odd_1"] or e["odd_X"] or e["odd_2"]
                )
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
        description="Betfair Exchange live soccer odds scraper (Selenium)"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="match_database/betfair_exchange",
        help="Directory for CSV files (default: match_database/betfair_exchange)",
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

