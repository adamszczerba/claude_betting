"""
Bet365 Live Soccer Odds Scraper (v2)
=====================================
Scrapes live soccer match data (scores + 1X2 odds) every 2s from the
Bet365 in-play page using Selenium.

URL: https://www.bet365.com/#/IP/B1

Each live match gets its own CSV file:
    {team1}_vs_{team2}_{tournament}_b365_{date}.csv

The CSV schema matches the other v2 scrapers (v2_betfair, v2_coincasino,
v2_betfair_exchange) so that downstream analysis can treat all bookmaker
data uniformly.

Usage:
    python v2_bet365/bet365_scraper.py
    python v2_bet365/bet365_scraper.py --interval 5
    python v2_bet365/bet365_scraper.py -o my_data_dir
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
BET365_URL = "https://www.bet365.com/#/IP/B1"
BOOKMAKER_TAG = "b365"

DEFAULT_POLL_INTERVAL = 2.0   # seconds between DOM reads
DEFAULT_REFRESH_INTERVAL = 60.0  # seconds between full page refreshes
FETCH_WAIT_TIME_SEC = 15      # Selenium wait for key element
SCROLL_STEP_SETTLE_SEC = 0.5
POST_SCROLL_SETTLE_SEC = 1.0
REFRESH_RETRIES = 3

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

HEADERS_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bet365_v2")

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


def parse_decimal_odds(raw: str) -> str:
    """
    Parse a decimal odds string from the Bet365 page.

    Bet365 displays decimal odds directly (e.g. 1.50, 3.20, 12.00).
    Returns empty string for suspended/unavailable odds.
    """
    raw = raw.strip()
    if not raw or raw in ('-', 'OFF', 'SUSP', 'SP'):
        return ""
    try:
        val = float(raw)
        return f"{val:.2f}" if val > 0 else ""
    except ValueError:
        return ""


# ---------------------------------------------------------------------------
# JavaScript snippet that extracts ALL match data in a single call.
#
# Bet365 in-play page DOM structure (hashed class names, partial matching):
#
# The page uses a single-page app with hash routing (#/IP/B1 = in-play).
# The in-play soccer section contains event containers with:
#   - Competition/league headers
#   - Match rows with team names, scores, time, and odds
#
# Key CSS patterns (partial class name matching):
#   [class*="InPlayCompetition"]    → competition header container
#   [class*="InPlayEvent"]          → individual match row
#   [class*="ParticipantName"]      → team name elements
#   [class*="Score"]                → score elements
#   [class*="Timer"]                → match timer / clock
#   [class*="OddsValue"]            → odds display
#   [class*="Fixture"]              → fixture container
#
# Bet365 uses heavily obfuscated class names that rotate, so we use
# multiple fallback strategies to find the right elements.
# ---------------------------------------------------------------------------
_JS_EXTRACT_ALL = r"""
var results = [];
var debugInfo = {
    strategy1_containers: 0,
    strategy2_ovm_fixtures: 0,
    scanned_elements: 0,
    has_football_markers: false
};

var bodyTextEarly = document.body ? (document.body.innerText || "") : "";
var bodyLowerEarly = bodyTextEarly.toLowerCase();
var hasShellMarkers = (
    bodyTextEarly.indexOf("Open Account Offer") !== -1 &&
    bodyTextEarly.indexOf("All Sports") !== -1 &&
    bodyTextEarly.indexOf("In-Play") !== -1
);
var hasOddsNumbers = /\b\d+\.\d{2}\b/.test(bodyTextEarly);
if (
    bodyLowerEarly.indexOf("sorry, you have been blocked") !== -1 ||
    bodyLowerEarly.indexOf("unable to access bet365.com") !== -1 ||
    bodyLowerEarly.indexOf("cloudflare ray id") !== -1
) {
    return {
        football: false,
        matches: [],
        blocked: true,
        block_reason: "cloudflare",
        debug_info: debugInfo
    };
}

if (hasShellMarkers && !hasOddsNumbers) {
    return {
        football: false,
        matches: [],
        blocked: true,
        block_reason: "shell-no-live-markets",
        debug_info: debugInfo
    };
}

// ── Strategy 1: Modern Bet365 in-play DOM ──
// The in-play page groups matches by competition within collapsible sections.
// Each section has a header (league name) and match rows.

// Find the main in-play container
var ipContainer = document.querySelector('.InPlayModule, [class*="InPlayModule"], [class*="ipg-InPlayGrid"]');
if (!ipContainer) {
    // Fallback: use the whole body
    ipContainer = document.body;
}

// Helper to get clean text
function getText(el) {
    return el ? (el.innerText || el.textContent || "").trim() : "";
}

// Helper to check if an element or its ancestors have "Soccer" / "Football" context
function isSoccer(el) {
    var check = el;
    var depth = 0;
    while (check && depth < 15) {
        var txt = check.getAttribute && (check.getAttribute('data-sport') || '');
        if (txt && (txt.toLowerCase().indexOf('soccer') !== -1 || txt.toLowerCase().indexOf('football') !== -1)) return true;
        check = check.parentElement;
        depth++;
    }
    return true; // default: assume soccer since we navigate to soccer in-play
}

// ── Strategy: Parse the Bet365 DOM generically ──
// Bet365 obfuscates class names but keeps a somewhat stable structure:
//   - Competition headers contain the league name
//   - Event rows contain participant names, scores, timer, and odds
//
// We look for common structural patterns.

var currentLeague = "";

// Try to find competition groups (sections with headers + events)
var allElements = ipContainer.querySelectorAll('*');
debugInfo.scanned_elements = allElements.length;

// Build an index of all visible text blocks for team pairs and odds
// Strategy: find elements that look like match containers
// They typically contain exactly 2 team names, a timer, scores, and 3 odds

// Look for recognizable structural patterns
var matchContainers = [];

// Pattern A: div containers with participant-like child structure
var divs = ipContainer.querySelectorAll('div');
for (var i = 0; i < divs.length; i++) {
    var d = divs[i];
    var cn = d.className || '';

    // Competition/league headers - contain league names
    // Usually class contains "Header" or "Competition" and has a short text
    if (cn.match && cn.match(/competition|header|league/i) && !cn.match(/event|match|fixture|odds|score/i)) {
        var hText = getText(d);
        // League headers are typically short (< 80 chars) and don't contain score-like patterns
        if (hText && hText.length < 80 && !hText.match(/^\d+\s*[-:]\s*\d+/)) {
            // Check it's not a nested child with too many children (a real header is simple)
            if (d.children.length < 10) {
                currentLeague = hText.split('\n')[0].trim();
            }
        }
    }

    // Event containers - typically class contains keywords like "Event", "Fixture", "Participant"
    if (cn.match && cn.match(/event|fixture|match/i) && cn.match(/container|wrapper|row/i)) {
        matchContainers.push({el: d, league: currentLeague});
    }
}

// If we didn't find containers via class names, try a structure-based approach
if (matchContainers.length === 0) {
    // Look for elements that contain exactly the right structure:
    // 2 team-name-like text nodes, score digits, timer text, and odds values
    var candidates = ipContainer.querySelectorAll('div, section, article, li');
    for (var i = 0; i < candidates.length; i++) {
        var c = candidates[i];
        var text = getText(c);
        // A match container typically has at least 2 lines and contains digits
        if (text && text.split('\n').length >= 3 && text.match(/\d/)) {
            // Check if it has odds-like numbers (decimal format: X.XX)
            var oddsMatches = text.match(/\b\d+\.\d{2}\b/g);
            if (oddsMatches && oddsMatches.length >= 3) {
                // Check it's not too large (not a parent of many matches)
                var childText = '';
                for (var j = 0; j < Math.min(c.children.length, 5); j++) {
                    childText += getText(c.children[j]) + '\n';
                }
                // If child text also has 3+ odds, this is likely a single match container
                var childOdds = childText.match(/\b\d+\.\d{2}\b/g);
                if (childOdds && childOdds.length >= 3 && c.offsetHeight > 20 && c.offsetHeight < 300) {
                    matchContainers.push({el: c, league: currentLeague});
                }
            }
        }
    }
}
debugInfo.strategy1_containers = matchContainers.length;

// ── Strategy 2: Direct search for the Bet365 IPG (In-Play Grid) structure ──
// Bet365 typically has:
//   .ipg-EventHeader (or similar) for league names
//   .ovm-Fixture / .ovm-FixtureDetailsTwoWay for each match
//   .ovm-ParticipantOddsOnly for each team's odds

// Try the ovm (Odds View Module) selectors
var ovmFixtures = document.querySelectorAll(
    '[class*="ovm-Fixture"], [class*="Fixture"][class*="Detail"], ' +
    '[class*="rcl-ParticipantFixture"]'
);
debugInfo.strategy2_ovm_fixtures = ovmFixtures.length;

if (ovmFixtures.length > 0) {
    results = []; // reset if we find these
    currentLeague = "";

    for (var i = 0; i < ovmFixtures.length; i++) {
        var fix = ovmFixtures[i];

        // Walk up to find the competition header
        var parent = fix.parentElement;
        var depth = 0;
        while (parent && depth < 10) {
            var header = parent.querySelector(
                '[class*="ipg-EventHeader"], [class*="Competition"], [class*="EventHeader"]'
            );
            if (header) {
                var leagueText = getText(header);
                if (leagueText && leagueText.length < 100) {
                    currentLeague = leagueText.split('\n')[0].trim();
                }
                break;
            }
            parent = parent.parentElement;
            depth++;
        }

        // Team names — look for participant name elements
        var teamEls = fix.querySelectorAll(
            '[class*="ParticipantName"], [class*="Participant"]'
        );
        // Filter to actual name elements (not containers)
        var names = [];
        for (var t = 0; t < teamEls.length; t++) {
            var name = getText(teamEls[t]);
            if (name && name.length > 1 && name.length < 60 && !name.match(/^\d+\.?\d*$/)) {
                names.push(name);
            }
        }
        if (names.length < 2) continue;

        var t1 = names[0];
        var t2 = names[1];

        // Scores
        var scoreEls = fix.querySelectorAll(
            '[class*="Score"], [class*="score"]'
        );
        var hs = 0, as_score = 0;
        var scoreNums = [];
        for (var s = 0; s < scoreEls.length; s++) {
            var sv = getText(scoreEls[s]);
            if (sv.match(/^\d+$/)) scoreNums.push(parseInt(sv));
        }
        if (scoreNums.length >= 2) {
            hs = scoreNums[0];
            as_score = scoreNums[1];
        }

        // Match time / status
        var matchTime = "", matchStatus = "";
        var timerEls = fix.querySelectorAll(
            '[class*="Timer"], [class*="Clock"], [class*="timer"], [class*="clock"], ' +
            '[class*="Time"], [class*="time"], [class*="Status"]'
        );
        for (var ti = 0; ti < timerEls.length; ti++) {
            var tv = getText(timerEls[ti]);
            if (!tv) continue;
            if (["HT", "FT", "AET", "ET", "PEN", "Half Time", "Full Time"].indexOf(tv) !== -1) {
                matchStatus = tv.replace("Half Time", "HT").replace("Full Time", "FT");
                matchTime = matchStatus;
                break;
            }
            // Time like "45:00", "90+3", "67", etc.
            var timeMatch = tv.match(/(\d{1,3})[':]\d{2}/);
            if (timeMatch) {
                matchTime = tv.replace(/['\u2032]/g, "");
                break;
            }
            timeMatch = tv.match(/^(\d{1,3})[\u2032']*$/);
            if (timeMatch) {
                matchTime = timeMatch[1];
                break;
            }
        }

        // Odds — Bet365 uses decimal odds
        var oddsEls = fix.querySelectorAll(
            '[class*="OddsValue"], [class*="Odds"], [class*="odds"], ' +
            '[class*="Price"], [class*="price"]'
        );
        var oddsValues = [];
        for (var o = 0; o < oddsEls.length; o++) {
            var ov = getText(oddsEls[o]);
            if (ov && ov.match(/^\d+\.\d+$/)) {
                oddsValues.push(ov);
            }
        }
        var o1 = oddsValues[0] || "";
        var ox = oddsValues[1] || "";
        var o2 = oddsValues[2] || "";

        results.push({
            team1: t1, team2: t2, tournament: currentLeague,
            home_score: hs, away_score: as_score,
            match_time: matchTime, match_status: matchStatus,
            odd_1: o1, odd_x: ox, odd_2: o2
        });
    }
}

// ── Strategy 3: last-resort full-text parse ──
// If nothing else worked, try to find match patterns in the page text
if (results.length === 0) {
    // Check if the page has soccer/football content at all
    var bodyText = document.body.innerText || "";
    debugInfo.has_football_markers = (
        bodyText.indexOf("Soccer") !== -1 ||
        bodyText.indexOf("Football") !== -1 ||
        bodyText.indexOf("In-Play") !== -1 ||
        bodyText.indexOf("Live") !== -1
    );
    if (bodyText.indexOf("Soccer") === -1 && bodyText.indexOf("Football") === -1 &&
        bodyText.indexOf("In-Play") === -1 && bodyText.indexOf("Live") === -1) {
        return {football: false, matches: [], debug_info: debugInfo};
    }
    // Page has content but we couldn't parse it — return empty matches
    // so the scraper knows the page loaded but found no parseable matches
    return {football: true, matches: [], debug_info: debugInfo};
}

return {football: true, matches: results, debug_info: debugInfo};
"""


# ---------------------------------------------------------------------------
# Selenium scraper
# ---------------------------------------------------------------------------

class Bet365Scraper:
    """Selenium-based scraper for Bet365's in-play page."""

    def __init__(self, url: str = BET365_URL, headless: bool = False):
        self.url = url
        options = Options()
        if headless or os.environ.get("HEADLESS", "").lower() in ("1", "true", "yes"):
            options.add_argument('--headless=new')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--window-size=1920,3000')
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
        Extract match data from the Bet365 in-play page.

        On first call the page is loaded from scratch.  On subsequent calls
        the live DOM is simply re-read (~0.2-0.5 s).  When *needs_refresh*
        is True the page is fully refreshed first so that new / removed
        matches are picked up.
        """
        driver = self.driver

        if not self._loaded_once:
            log.info("Loading: %s", self.url)
            self._load_page(driver)
            self._scroll_to_load_all(driver)
            time.sleep(POST_SCROLL_SETTLE_SEC)
            self._loaded_once = True
        elif needs_refresh:
            log.debug("Refreshing page to pick up match changes…")
            self._refresh_page(driver)
            self._scroll_to_load_all(driver)
            time.sleep(POST_SCROLL_SETTLE_SEC)

        try:
            events = self._parse_dom(driver)
            if not events:
                # On shell/home pages, force navigation once and retry extraction.
                self._ensure_inplay_soccer_view(driver)
                self._scroll_to_load_all(driver)
                time.sleep(POST_SCROLL_SETTLE_SEC)
                retry_events = self._parse_dom(driver)
                if retry_events:
                    return retry_events
            return events
        except StaleElementReferenceException:
            log.info("DOM stale, reloading page…")
            self._load_page(driver)
            self._scroll_to_load_all(driver)
            time.sleep(POST_SCROLL_SETTLE_SEC)
            return self._parse_dom(driver)

    # ------------------------------------------------------------------
    def _load_page(self, driver) -> None:
        """Full page load (or reload)."""
        driver.get(self.url)
        self._wait_for_content(driver)
        self._dismiss_cookie_banner(driver)
        self._ensure_inplay_soccer_view(driver)

    def _refresh_page(self, driver) -> None:
        """Refresh + wait for content to re-appear, with retries."""
        for attempt in range(REFRESH_RETRIES):
            try:
                driver.get(self.url)   # hash-routing: refresh via re-navigate
                self._wait_for_content(driver)
                self._dismiss_cookie_banner(driver)
                self._ensure_inplay_soccer_view(driver)
                return
            except Exception as e:
                if attempt < REFRESH_RETRIES - 1:
                    log.warning(
                        "Refresh failed (attempt %d/%d): %s",
                        attempt + 1,
                        REFRESH_RETRIES,
                        e,
                    )
                    time.sleep(2)
                else:
                    log.error("Refresh failed after %d attempts; forcing full reload", REFRESH_RETRIES)
                    self._load_page(driver)
                    return

    def _wait_for_content(self, driver) -> None:
        """Wait until the in-play content is usable."""
        try:
            # Wait for any element that indicates the page loaded
            WebDriverWait(driver, FETCH_WAIT_TIME_SEC).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR,
                     '[class*="InPlay"], [class*="Fixture"], '
                     '[class*="ovm-Fixture"], [class*="ipg-"]')
                )
            )
        except Exception:
            # Page may not have in-play content; that's OK
            pass
        time.sleep(2.0)  # let the SPA JS settle

    def _dismiss_cookie_banner(self, driver) -> None:
        """Dismiss the cookie consent banner if visible."""
        try:
            # Bet365 uses different cookie banners
            for selector in [
                "#onetrust-accept-btn-handler",
                "[class*='CookieConsent'] button",
                "[class*='cookie'] button[class*='accept']",
                ".ccm-CookieConsentPopup_Accept",
            ]:
                try:
                    btn = driver.find_element(By.CSS_SELECTOR, selector)
                    if btn.is_displayed():
                        btn.click()
                        log.info("Cookie consent banner dismissed")
                        time.sleep(0.5)
                        return
                except Exception:
                    continue

            # Text-based fallback for current bet365 consent dialog
            for xpath in [
                "//button[contains(normalize-space(.), 'Accept All')]",
                "//button[contains(normalize-space(.), 'Accept all')]",
                "//*[self::a or self::button or self::div][contains(normalize-space(.), 'Accept All')]",
            ]:
                try:
                    btn = driver.find_element(By.XPATH, xpath)
                    if btn.is_displayed():
                        driver.execute_script("arguments[0].click();", btn)
                        log.info("Cookie consent accepted via text fallback")
                        time.sleep(0.8)
                        return
                except Exception:
                    continue
        except Exception:
            pass

    def _ensure_inplay_soccer_view(self, driver) -> None:
        """Best-effort navigation to In-Play > Soccer if shell page is shown."""
        try:
            inplay_clicked = driver.execute_script(
                """
                function clickLabel(label) {
                    const nodes = Array.from(document.querySelectorAll('a, button, div, span'));
                    let clicked = false;
                    for (const n of nodes) {
                        const txt = (n.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                        if (!txt || txt.indexOf(label.toLowerCase()) === -1) continue;
                        const r = n.getBoundingClientRect();
                        const visible = r.width > 0 && r.height > 0 && r.top >= 0 && r.bottom <= window.innerHeight;
                        if (visible) {
                            n.click();
                            clicked = true;
                            break;
                        }
                    }
                    return clicked;
                }

                return clickLabel('In-Play');
                """
            )

            soccer_clicked = False
            if inplay_clicked:
                time.sleep(0.8)
                soccer_clicked = driver.execute_script(
                    """
                    function clickLabel(label) {
                        const nodes = Array.from(document.querySelectorAll('a, button, div, span'));
                        let clicked = false;
                        for (const n of nodes) {
                            const txt = (n.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                            if (!txt || txt.indexOf(label.toLowerCase()) === -1) continue;
                            try {
                                n.scrollIntoView({block: 'center'});
                            } catch (_) {}
                            n.click();
                            clicked = true;
                            break;
                        }
                        return clicked;
                    }

                    return clickLabel('Soccer');
                    """
                )

            if inplay_clicked or soccer_clicked:
                log.info(
                    "Navigation assist: inplay_clicked=%s soccer_clicked=%s",
                    inplay_clicked,
                    soccer_clicked,
                )
                time.sleep(1.5)
        except Exception:
            pass

    def _parse_dom(self, driver) -> List[dict]:
        """
        Extract match data from the DOM using a single JavaScript call.

        One ``execute_script`` roundtrip replaces hundreds of Selenium
        ``find_element`` calls, keeping parse time low.
        """
        try:
            data = driver.execute_script(
                "return (function(){" + _JS_EXTRACT_ALL + "})()"
            )
        except Exception as e:
            log.error(f"JavaScript extraction failed: {e}")
            return []

        if not data:
            log.warning("JavaScript returned no data")
            return []

        if data.get("blocked"):
            reason = data.get("block_reason", "unknown")
            log.error("Bet365 page is blocked by anti-bot protection (%s)", reason)
            return [{"_blocked": True, "reason": reason}]
            
        if not data.get("football"):
            log.info("No football/soccer content detected on page")
            return [{"_no_football": True}]

        matches = data.get("matches", [])
        debug_info = data.get("debug_info", {})
        log.info(f"Extracted {len(matches)} matches from DOM")
        
        if len(matches) == 0:
            log.warning("Football content detected but no matches parsed - possible DOM structure change")
            if debug_info:
                log.warning(
                    "Selector debug: strategy1_containers=%s strategy2_ovm_fixtures=%s scanned_elements=%s has_football_markers=%s",
                    debug_info.get("strategy1_containers"),
                    debug_info.get("strategy2_ovm_fixtures"),
                    debug_info.get("scanned_elements"),
                    debug_info.get("has_football_markers"),
                )

        results: list[dict] = []
        for m in matches:
            results.append({
                "team1": m["team1"],
                "team2": m["team2"],
                "tournament": m.get("tournament", ""),
                "home_score": m.get("home_score", 0),
                "away_score": m.get("away_score", 0),
                "match_time": m.get("match_time", ""),
                "match_status": m.get("match_status", ""),
                "odd_1": parse_decimal_odds(m.get("odd_1", "")),
                "odd_X": parse_decimal_odds(m.get("odd_x", "")),
                "odd_2": parse_decimal_odds(m.get("odd_2", "")),
                "total_line": "",
                "odd_over": "",
                "odd_under": "",
            })
        return results

    def _scroll_to_load_all(self, driver) -> None:
        """
        Scroll the page incrementally to trigger lazy-loaded match rows.

        Bet365 lazy-loads matches as the user scrolls.  Without scrolling,
        only matches near the viewport are in the DOM.
        """
        last_height = driver.execute_script("return document.body.scrollHeight")
        viewport = driver.execute_script("return window.innerHeight") or 600
        pos = 0
        max_scrolls = 40          # safety cap
        for _ in range(max_scrolls):
            pos += viewport
            driver.execute_script(f"window.scrollTo(0, {pos})")
            time.sleep(SCROLL_STEP_SETTLE_SEC)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if pos >= new_height and new_height == last_height:
                break
            last_height = new_height
        # Scroll back to top so the page stays in a consistent state
        driver.execute_script("window.scrollTo(0, 0)")
        time.sleep(SCROLL_STEP_SETTLE_SEC)

    def close(self):
        self.driver.quit()


# ---------------------------------------------------------------------------
# CSV writer – one file per match (same schema as other v2 scrapers)
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
    output_dir: str = "match_database/bet365",
    interval: float = DEFAULT_POLL_INTERVAL,
    refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
) -> None:
    scraper = Bet365Scraper()
    writer = MatchCSVWriter(output_dir)

    log.info("Bet365 live SOCCER odds scraper started")
    log.info("  output dir : %s", os.path.abspath(output_dir))
    log.info("  poll       : %.1fs", interval)
    log.info("  refresh    : %.0fs", refresh_interval)
    log.info("Press Ctrl+C to stop.\n")

    cycle = 0
    _last_no_football_log: float = 0.0
    _last_blocked_log: float = 0.0
    _last_refresh: float = 0.0            # force refresh on first cycle
    NO_FOOTBALL_LOG_INTERVAL = 60.0
    BLOCKED_LOG_INTERVAL = 60.0

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

                # --- Blocked-by-anti-bot sentinel ---
                if events and events[0].get("_blocked"):
                    now = time.monotonic()
                    if now - _last_blocked_log >= BLOCKED_LOG_INTERVAL:
                        log.error(
                            "cycle %4d  |  bet365 access blocked (%s); check VPN exit node or rotate config",
                            cycle,
                            events[0].get("reason", "unknown"),
                        )
                        _last_blocked_log = now
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
        description="Bet365 live soccer odds scraper (Selenium)"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="match_database/bet365",
        help="Directory for CSV files (default: match_database/bet365)",
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

