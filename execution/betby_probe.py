"""
Betby API probe — intercept XHR requests after Playwright login to identify
the authenticated bet-placement REST endpoint used by the Betby platform
(which powers CoinCasino, LVBet and others).

Running this script once after login dumps:
  - All XHR/fetch request URLs
  - Authorization / session headers
  - Request body for any POST to paths containing "bet"

If a usable endpoint is found, cc_executor.py can be updated to use the REST
API directly (auth header only) instead of full UI automation — making
placement faster and more reliable.

Usage
-----
    python -m execution.betby_probe

Results are saved to execution/betby_api_findings.json
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)s  %(message)s")

_OUTPUT = os.path.join(os.path.dirname(__file__), "betby_api_findings.json")
_LIVE_URL = "https://www.coincasino.com/en/sports/live"
_SESSION_PATH = os.environ.get("CC_SESSION_PATH", "/app/cc_session.json")

_findings: list = []


def _on_request(request) -> None:
    url = request.url
    method = request.method
    headers = dict(request.headers)
    auth = headers.get("authorization", headers.get("x-session-token", ""))
    body = ""
    try:
        body = request.post_data or ""
    except Exception:
        pass

    entry = {
        "url":    url,
        "method": method,
        "auth":   auth[:60] + "…" if len(auth) > 60 else auth,
        "body":   body[:500] if body else "",
    }

    # Log anything that looks like a bet or auth endpoint
    if any(kw in url.lower() for kw in ("bet", "place", "auth", "token", "session")):
        log.info("Interesting request: %s %s", method, url)
        if auth:
            log.info("  Auth header: %s…", auth[:40])
        if body:
            log.info("  Body: %s", body[:200])
        _findings.append(entry)


def run_probe() -> None:
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import stealth_sync
    except ImportError:
        print("playwright not installed — skipping probe.")
        return

    email    = os.environ.get("CC_EMAIL", "")
    password = os.environ.get("CC_PASSWORD", "")

    if not email or not password:
        print("Set CC_EMAIL and CC_PASSWORD environment variables before probing.")
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx_kwargs: dict = dict(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        if os.path.exists(_SESSION_PATH):
            ctx_kwargs["storage_state"] = _SESSION_PATH

        context = browser.new_context(**ctx_kwargs)
        page    = context.new_page()
        stealth_sync(page)

        page.on("request", _on_request)

        log.info("Navigating to live sports page …")
        page.goto(_LIVE_URL, timeout=30_000, wait_until="domcontentloaded")
        time.sleep(5)

        # If not logged in — log in
        if "login" in page.url.lower():
            log.info("Not logged in — authenticating …")
            from execution.cc_executor import _LOGIN_URL
            page.goto(_LOGIN_URL, timeout=30_000)
            time.sleep(1)
            page.fill("input[type='email']", email)
            time.sleep(0.5)
            page.fill("input[type='password']", password)
            time.sleep(0.5)
            page.click("button[type='submit']")
            page.wait_for_load_state("networkidle", timeout=20_000)
            context.storage_state(path=_SESSION_PATH)

        log.info("Observing network traffic for 15 seconds …")
        time.sleep(15)

        browser.close()

    with open(_OUTPUT, "w") as f:
        json.dump(_findings, f, indent=2)

    log.info("Probe complete. %d interesting requests recorded.", len(_findings))
    log.info("Results saved to %s", _OUTPUT)

    # Print auth token if found
    for f in _findings:
        if f.get("auth"):
            log.info("Auth token found: %s", f["auth"])
            break


if __name__ == "__main__":
    run_probe()

