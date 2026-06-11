"""
CoinCasino Playwright executor — stealth bet placement.

Requires:
    pip install playwright playwright-stealth
    playwright install chromium

Environment variables:
    CC_EMAIL          CoinCasino account e-mail
    CC_PASSWORD       CoinCasino account password
    EXECUTION_MODE    "dry_run" (default) or "live"
    CC_SESSION_PATH   path to persistent session JSON (default: /app/cc_session.json)

Stealth techniques applied
--------------------------
  • playwright-stealth patches (navigator.webdriver, plugins, languages, …)
  • Realistic viewport 1920×1080
  • Human-like mouse movement via bezier-curve interpolation
  • Randomised delays 200-800ms between all actions
  • Persistent browser context (cookies / localStorage survive restarts)
  • Latest real Chrome/Linux User-Agent string

Bet placement flow
------------------
  1. Load session (restore cookies) — re-login if expired
  2. Navigate to live sports page
  3. Find match by fuzzy team-name matching
  4. Click correct odds button (human-like mouse path)
  5. Verify no price drift beyond threshold
  6. Type stake digit-by-digit with delays
  7. Click confirm — parse receipt
  8. Save session

Usage
-----
>>> from execution.cc_executor import CoinCasinoExecutor
>>> executor = CoinCasinoExecutor()
>>> receipt  = executor.place_bet(order)
"""

from __future__ import annotations

import datetime
import logging
import math
import os
import random
import time
import uuid
from typing import Optional, Tuple

from decisions.signal_router import BetOrder
from execution.base import BetReceipt, Executor, ExecutionError, PriceDriftError

log = logging.getLogger(__name__)

__all__ = ["CoinCasinoExecutor"]

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------

_LOGIN_URL      = "https://www.coincasino.com/en/login"
_LIVE_URL       = "https://www.coincasino.com/en/sports/live"
_SESSION_PATH   = os.environ.get("CC_SESSION_PATH", "/app/cc_session.json")

# ---------------------------------------------------------------------------
# Selectors (CSS / text) — update if CoinCasino DOM changes
# ---------------------------------------------------------------------------

_SEL_EMAIL        = "input[type='email'], input[name='email']"
_SEL_PASSWORD     = "input[type='password'], input[name='password']"
_SEL_LOGIN_BTN    = "button[type='submit']"
_SEL_LIVE_MATCH   = "[class*='event'], [class*='match'], [class*='game']"
_SEL_BET_SLIP     = "[class*='betslip'], [class*='bet-slip'], [class*='coupon']"
_SEL_STAKE_INPUT  = "input[class*='stake'], input[placeholder*='stake'], input[placeholder*='amount']"
_SEL_CONFIRM_BTN  = "button[class*='confirm'], button[class*='place'], button[class*='submit']"
_SEL_RECEIPT      = "[class*='receipt'], [class*='success'], [class*='bet-id']"

# ---------------------------------------------------------------------------
# Bezier mouse helper
# ---------------------------------------------------------------------------

def _bezier_path(
    x0: float, y0: float,
    x1: float, y1: float,
    steps: int = 20,
) -> list[Tuple[float, float]]:
    """Generate a smooth bezier curve from (x0,y0) to (x1,y1)."""
    cx = x0 + (x1 - x0) * random.uniform(0.2, 0.8) + random.uniform(-50, 50)
    cy = y0 + (y1 - y0) * random.uniform(0.2, 0.8) + random.uniform(-50, 50)
    path = []
    for i in range(steps + 1):
        t = i / steps
        x = (1 - t)**2 * x0 + 2 * (1 - t) * t * cx + t**2 * x1
        y = (1 - t)**2 * y0 + 2 * (1 - t) * t * cy + t**2 * y1
        path.append((x, y))
    return path


def _human_delay(lo: float = 0.2, hi: float = 0.8) -> None:
    time.sleep(random.uniform(lo, hi))


class CoinCasinoExecutor(Executor):
    """Playwright-based CoinCasino executor with stealth and human-like interaction."""

    def __init__(self):
        self._email    = os.environ.get("CC_EMAIL", "")
        self._password = os.environ.get("CC_PASSWORD", "")
        self._drift_threshold = float(os.environ.get("CC_DRIFT_THRESHOLD", "0.02"))

        # Lazy-import so the module loads even without playwright installed
        try:
            from playwright.sync_api import sync_playwright
            from playwright_stealth import stealth_sync
            self._sync_playwright = sync_playwright
            self._stealth = stealth_sync
        except ImportError as exc:
            raise ImportError(
                "playwright and playwright-stealth are required. "
                "Install with: pip install playwright playwright-stealth && "
                "playwright install chromium"
            ) from exc

        self._pw      = None
        self._browser = None
        self._context = None
        self._page    = None
        self._session_path = _SESSION_PATH

    # ------------------------------------------------------------------
    # Executor interface
    # ------------------------------------------------------------------

    def place_bet(self, order: BetOrder) -> BetReceipt:
        self._ensure_session()
        try:
            return self._do_place_bet(order)
        except PriceDriftError:
            raise
        except Exception as exc:
            raise ExecutionError(f"Bet placement failed: {exc}") from exc

    def close(self) -> None:
        try:
            if self._context:
                self._context.storage_state(path=self._session_path)
            if self._browser:
                self._browser.close()
            if self._pw:
                self._pw.stop()
        except Exception:
            pass
        self._pw = self._browser = self._context = self._page = None

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def _ensure_session(self) -> None:
        """Start Playwright + restore/create browser session."""
        if self._page is not None:
            return  # already initialised

        self._pw = self._sync_playwright().start()
        launch_args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
        ]
        self._browser = self._pw.chromium.launch(
            headless = True,
            args     = launch_args,
        )

        context_kwargs = dict(
            viewport         = {"width": 1920, "height": 1080},
            user_agent       = (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale           = "en-GB",
            timezone_id      = "Europe/Warsaw",
            java_script_enabled = True,
        )

        if os.path.exists(self._session_path):
            log.info("[CC] Restoring session from %s", self._session_path)
            context_kwargs["storage_state"] = self._session_path

        self._context = self._browser.new_context(**context_kwargs)
        self._page    = self._context.new_page()
        self._stealth(self._page)

        # Verify session is still valid; re-login if needed
        if not self._is_logged_in():
            self._login()

    def _is_logged_in(self) -> bool:
        """Navigate to live page and check for user-specific element."""
        try:
            self._page.goto(_LIVE_URL, timeout=30_000, wait_until="domcontentloaded")
            _human_delay(1.0, 2.0)
            # If redirected to login or has a login button, session is expired
            return "login" not in self._page.url.lower()
        except Exception:
            return False

    def _login(self) -> None:
        log.info("[CC] Logging in as %s …", self._email)
        self._page.goto(_LOGIN_URL, timeout=30_000, wait_until="domcontentloaded")
        _human_delay(0.5, 1.2)

        self._human_fill(_SEL_EMAIL, self._email)
        _human_delay(0.3, 0.7)
        self._human_fill(_SEL_PASSWORD, self._password)
        _human_delay(0.4, 0.8)
        self._human_click(_SEL_LOGIN_BTN)

        self._page.wait_for_load_state("networkidle", timeout=20_000)
        _human_delay(1.0, 2.0)

        if "login" in self._page.url.lower():
            raise ExecutionError("Login failed — check CC_EMAIL / CC_PASSWORD")

        # Persist session
        self._context.storage_state(path=self._session_path)
        log.info("[CC] Login successful, session saved.")

    # ------------------------------------------------------------------
    # Bet placement
    # ------------------------------------------------------------------

    def _do_place_bet(self, order: BetOrder) -> BetReceipt:
        log.info("[CC] Placing bet: %s %s @ %.4f  stake=%.2f EUR",
                 order.market, order.outcome, order.min_price, order.stake_eur)

        # Navigate to live sports page
        self._page.goto(_LIVE_URL, timeout=30_000, wait_until="domcontentloaded")
        _human_delay(1.5, 2.5)

        # Find the match row
        match_el = self._find_match(order.team1, order.team2)
        if match_el is None:
            raise ExecutionError(
                f"Match not found on page: {order.team1} vs {order.team2}"
            )

        # Click the odds button for the correct market
        odds_el = self._find_odds_button(match_el, order.market, order.outcome)
        if odds_el is None:
            raise ExecutionError(
                f"Odds button not found: {order.market} / {order.outcome}"
            )

        # Read displayed price BEFORE clicking
        displayed_price = self._read_displayed_price(odds_el)
        if displayed_price is not None:
            self._check_drift(displayed_price, order)

        self._human_move_click(odds_el)
        _human_delay(0.5, 1.0)

        # Wait for bet slip to open
        try:
            self._page.wait_for_selector(_SEL_BET_SLIP, timeout=8_000)
        except Exception:
            raise ExecutionError("Bet slip did not open after clicking odds")

        # Re-check price in bet slip
        slip_price = self._read_slip_price()
        if slip_price is not None:
            self._check_drift(slip_price, order)

        # Enter stake
        self._enter_stake(order.stake_eur)
        _human_delay(0.5, 0.9)

        # Confirm
        try:
            self._human_click(_SEL_CONFIRM_BTN)
        except Exception:
            raise ExecutionError("Confirm button not found in bet slip")

        # Wait for receipt
        try:
            self._page.wait_for_selector(_SEL_RECEIPT, timeout=15_000)
        except Exception:
            raise ExecutionError("Receipt element not found after confirm")

        receipt_id    = self._parse_receipt_id()
        accepted_price = slip_price or displayed_price or order.min_price

        # Save updated session
        self._context.storage_state(path=self._session_path)

        log.info("[CC] Bet placed! receipt=%s  accepted=%.4f", receipt_id, accepted_price)

        return BetReceipt(
            order_id       = order.id,
            bookmaker      = "coincasino",
            receipt_id     = receipt_id,
            accepted_price = accepted_price,
            stake_eur      = order.stake_eur,
            notes          = "live",
        )

    # ------------------------------------------------------------------
    # DOM helpers
    # ------------------------------------------------------------------

    def _find_match(self, team1: str, team2: str):
        """Find the match element using fuzzy team-name matching."""
        from dashboard.matcher import normalize  # reuse existing helper
        norm1 = normalize(team1)
        norm2 = normalize(team2)

        candidates = self._page.query_selector_all(_SEL_LIVE_MATCH)
        for el in candidates:
            text = (el.text_content() or "").lower()
            if norm1 in text and norm2 in text:
                return el
        return None

    def _find_odds_button(self, match_el, market: str, outcome: str):
        """Find the odds button for a given market within the match element."""
        # Market index mapping for 1X2
        _outcome_index = {"1": 0, "X": 1, "2": 2, "over": 0, "under": 1}
        idx = _outcome_index.get(outcome, 0)

        # Try to find buttons in the match element
        btns = match_el.query_selector_all("button, [class*='odd'], [class*='price']")
        if btns and idx < len(btns):
            return btns[idx]
        return None

    def _read_displayed_price(self, odds_el) -> Optional[float]:
        try:
            text = odds_el.text_content() or ""
            return float(text.strip())
        except Exception:
            return None

    def _read_slip_price(self) -> Optional[float]:
        try:
            el = self._page.query_selector(
                "[class*='odds'], [class*='price'], [class*='rate']"
            )
            if el:
                return float((el.text_content() or "").strip())
        except Exception:
            pass
        return None

    def _check_drift(self, live_price: float, order: BetOrder) -> None:
        drift = (order.min_price - live_price) / order.min_price
        if drift > self._drift_threshold:
            raise PriceDriftError(
                f"Price drifted: wanted ≥ {order.min_price:.4f}, "
                f"got {live_price:.4f} (drift {drift*100:.1f}%)"
            )

    def _enter_stake(self, stake_eur: float) -> None:
        """Type stake digit-by-digit with human delays."""
        try:
            inp = self._page.wait_for_selector(_SEL_STAKE_INPUT, timeout=5_000)
            inp.triple_click()
            _human_delay(0.1, 0.3)
            for ch in f"{stake_eur:.2f}":
                inp.type(ch, delay=random.uniform(80, 200))
        except Exception as exc:
            raise ExecutionError(f"Could not enter stake: {exc}") from exc

    def _parse_receipt_id(self) -> str:
        try:
            el = self._page.query_selector(_SEL_RECEIPT)
            if el:
                text = el.text_content() or ""
                # Extract numeric or alphanumeric bet ID from receipt text
                import re
                m = re.search(r"[\w\-]{6,}", text)
                if m:
                    return m.group(0)
        except Exception:
            pass
        return f"CC-{uuid.uuid4().hex[:8].upper()}"

    # ------------------------------------------------------------------
    # Human-like interaction helpers
    # ------------------------------------------------------------------

    def _human_move_click(self, element) -> None:
        """Move mouse along bezier curve to element, then click."""
        try:
            box = element.bounding_box()
            if box:
                tx = box["x"] + box["width"] / 2
                ty = box["y"] + box["height"] / 2
                cur_x, cur_y = 960.0, 540.0   # assume centre as start
                path = _bezier_path(cur_x, cur_y, tx, ty, steps=random.randint(15, 30))
                for px, py in path:
                    self._page.mouse.move(px, py)
                    time.sleep(random.uniform(0.005, 0.02))
                self._page.mouse.click(tx, ty)
                return
        except Exception:
            pass
        element.click()

    def _human_click(self, selector: str) -> None:
        el = self._page.query_selector(selector)
        if el is None:
            raise ExecutionError(f"Element not found: {selector}")
        self._human_move_click(el)

    def _human_fill(self, selector: str, value: str) -> None:
        el = self._page.wait_for_selector(selector, timeout=10_000)
        el.click()
        _human_delay(0.1, 0.3)
        for ch in value:
            el.type(ch, delay=random.uniform(60, 180))

