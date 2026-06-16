"""
Orchestrator — main poll loop + REST API.

Polls match_database/ on every sync-clock tick, runs the full analytics →
decisions pipeline, writes BetOrders to the ledger, and exposes a lightweight
FastAPI endpoint for the dashboard and the executor container to consume.

REST endpoints (port 8051)
--------------------------
  GET /                      new fast Vanilla-JS dashboard (index.html)
  GET /api/signals          active value/arb signals (last 60 s)
  GET /api/odds             latest grouped odds table (pre-computed)
  GET /api/logs             latest Docker container log tails
  GET /api/bets             all bet records from ledger
  GET /api/pnl              daily/total P&L summary
  GET /api/health           heartbeat
  GET /stream               Server-Sent Events stream (signals+odds+logs+bets+pnl)

Docker
------
  Built by docker/orchestrator/Dockerfile
  Mounts match_database/ (read-only) and ledger/ (read-write)
  No VPN needed — purely internal.

Local development
-----------------
  source .venv/bin/activate
  python -m orchestrator.main
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Make project root importable regardless of working directory
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s  %(levelname)s  %(name)s  %(message)s",
    datefmt  = "%H:%M:%S",
)
log = logging.getLogger("orchestrator")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_ROOT       = os.environ.get("DB_ROOT",   os.path.join(_ROOT, "match_database"))
LEDGER_PATH   = os.environ.get("LEDGER_PATH", os.path.join(_ROOT, "ledger", "bets.db"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))
API_PORT      = int(os.environ.get("ORCHESTRATOR_PORT", "8051"))
SIGNAL_TTL_S  = 60   # signals older than this are removed from the cache

# Docker containers to stream logs from
_LOG_CONTAINERS: Dict[str, str] = {
    "scraper-coincasino":      "CoinCasino",
    "scraper-betfair":         "Betfair",
    "scraper-betfair-exchange":"BetfairExchange",
    "scraper-bet365":          "Bet365",
    "scraper-lvbet":           "LvBet",
    "scraper-pinnacle":        "Pinnacle",
    "scraper-sts":             "STS",
}
_LOG_TAIL = 30
_LOG_REFRESH_SEC = 5

# ---------------------------------------------------------------------------
# Shared state (guarded by _lock)
# ---------------------------------------------------------------------------
_lock           = threading.Lock()
_latest_signals: List[dict]  = []
_latest_grouped: List[dict]  = []
_latest_logs:    dict        = {}   # {container: {label, running, logs}}
_latest_bets:    List[dict]  = []
_latest_pnl:     dict        = {}

# Module-level ledger reference (set in main())
_ledger: Optional[Any] = None

# ---------------------------------------------------------------------------
# SSE infrastructure
# ---------------------------------------------------------------------------
_sse_event_loop: Optional[asyncio.AbstractEventLoop] = None
_sse_queues:     List[asyncio.Queue] = []
_sse_queues_lock = threading.Lock()


def _push_sse_event() -> None:
    """Serialise shared state and push to all connected SSE clients.

    Called from the poll thread; bridges into the asyncio event loop via
    call_soon_threadsafe so the async generators can yield the payload.
    """
    if _sse_event_loop is None:
        return
    try:
        with _lock:
            payload = json.dumps({
                "signals": list(_latest_signals),
                "odds":    list(_latest_grouped),
                "logs":    dict(_latest_logs),
                "bets":    list(_latest_bets),
                "pnl":     dict(_latest_pnl),
                "ts":      datetime.datetime.now().isoformat(),
            }, default=str)
    except Exception as exc:
        log.warning("SSE serialisation error: %s", exc)
        return

    with _sse_queues_lock:
        queues = list(_sse_queues)

    for q in queues:
        try:
            _sse_event_loop.call_soon_threadsafe(q.put_nowait, payload)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Docker log helpers (best-effort — silent if docker not available)
# ---------------------------------------------------------------------------

def _docker_inspect(container: str) -> bool:
    try:
        r = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", container],
            capture_output=True, text=True, timeout=2,
        )
        return r.stdout.strip() == "true"
    except Exception:
        return False


def _docker_tail(container: str) -> str:
    try:
        r = subprocess.run(
            ["docker", "logs", "--tail", str(_LOG_TAIL), container],
            capture_output=True, text=True, timeout=2,
        )
        return (r.stdout or r.stderr or "(no output)").strip()
    except subprocess.TimeoutExpired:
        return "(timeout)"
    except FileNotFoundError:
        return "(docker not found)"
    except Exception as exc:
        return f"(error: {exc})"


def _refresh_logs() -> None:
    """Fetch logs for all registered containers; update _latest_logs."""
    result: dict = {}
    for cname, label in _LOG_CONTAINERS.items():
        running = _docker_inspect(cname)
        logs    = _docker_tail(cname) if running else "(container not running)"
        result[cname] = {"label": label, "running": running, "logs": logs}
    with _lock:
        _latest_logs.clear()
        _latest_logs.update(result)


def _logs_loop() -> None:
    """Background thread: refresh Docker logs every _LOG_REFRESH_SEC seconds."""
    log.info("Logs refresh loop started (interval=%ds)", _LOG_REFRESH_SEC)
    while True:
        try:
            _refresh_logs()
        except Exception as exc:
            log.debug("Log refresh error: %s", exc)
        time.sleep(_LOG_REFRESH_SEC)


# ---------------------------------------------------------------------------
# Imports (deferred so we can log import errors gracefully)
# ---------------------------------------------------------------------------

def _fetch_rows():
    """Fetch odds rows from Redis streams, falling back to CSV scan."""
    try:
        from scrapers.shared.stream_consumer import read_latest_from_streams, is_redis_available
        if is_redis_available():
            rows = read_latest_from_streams()
            if rows:
                return rows
    except Exception:
        pass
    return scan_today(db_root=DB_ROOT)


def _import_modules():
    global scan_today, build_grouped_table, weighted_consensus
    global scan_value, scan_arb, RiskManager, SignalRouter
    global Ledger, _load_config

    from dashboard.data_service import scan_today
    from dashboard.matcher import build_grouped_table
    from analytics.consensus import weighted_consensus
    from analytics.value import scan_value
    from analytics.arbitrage import scan_arb
    from decisions.risk_manager_v2 import RiskManager, _load_config
    from decisions.signal_router import SignalRouter
    from ledger.ledger import Ledger


# ---------------------------------------------------------------------------
# Poll loop (runs in a background thread)
# ---------------------------------------------------------------------------

def _poll_loop(ledger: "Ledger", risk: "RiskManager", router: "SignalRouter") -> None:
    from v2_coincasino.sync_clock import sleep_until_next_tick

    log.info("Poll loop started (interval=%.1fs, db_root=%s)", POLL_INTERVAL, DB_ROOT)

    while True:
        try:
            _run_cycle(ledger, risk, router)
        except Exception as exc:
            log.exception("Cycle error: %s", exc)
        sleep_until_next_tick(POLL_INTERVAL)


def _run_cycle(ledger: "Ledger", risk: "RiskManager", router: "SignalRouter") -> None:
    now = datetime.datetime.now()

    rows = _fetch_rows()

    # Staleness filter
    cfg = _load_config()
    max_age = cfg.get("risk", {}).get("max_odds_age_sec", 0)
    if max_age > 0:
        fresh_rows = []
        stale_count = 0
        for r in rows:
            ts_str = r.get("timestamp", "")
            if ts_str:
                try:
                    ts = datetime.datetime.fromisoformat(ts_str)
                    age_sec = (now - ts).total_seconds()
                    if age_sec <= max_age:
                        fresh_rows.append(r)
                    else:
                        stale_count += 1
                except ValueError:
                    fresh_rows.append(r)
            else:
                fresh_rows.append(r)
        if stale_count:
            log.debug("Dropped %d stale row(s) (age > %ds)", stale_count, max_age)
        rows = fresh_rows

    grouped = build_grouped_table(rows)

    if not grouped:
        log.debug("No grouped matches available.")
        # Still push SSE so browser gets empty update
        _push_sse_event()
        return

    fair_map = weighted_consensus(grouped)
    vt       = cfg.get("value_thresholds", {})

    value_signals = scan_value(
        grouped,
        fair_map,
        main_threshold_pct  = vt.get("main_markets", 3.0),
        niche_threshold_pct = vt.get("niche_markets", 5.0),
    )
    arb_signals = scan_arb(grouped)

    all_signals = [*value_signals, *arb_signals]
    log.info("Cycle: %d matches, %d value, %d arb",
             len(grouped), len(value_signals), len(arb_signals))

    # Serialise signals for API / SSE
    serialised: List[dict] = []
    for sig in value_signals:
        serialised.append({
            "type":           "VALUE",
            "match_key":      sig.match_key,
            "team1":          sig.team1,
            "team2":          sig.team2,
            "tournament":     sig.tournament,
            "bookmaker":      sig.bookmaker,
            "market":         sig.market,
            "bookmaker_odds": sig.bookmaker_odds,
            "fair_odds":      sig.fair_odds,
            "edge_pct":       round(sig.edge_pct, 2),
            "timestamp":      sig.timestamp.isoformat(),
        })
    for sig in arb_signals:
        serialised.append({
            "type":                  "ARB",
            "match_key":             sig.match_key,
            "team1":                 sig.team1,
            "team2":                 sig.team2,
            "tournament":            sig.tournament,
            "guaranteed_profit_pct": round(sig.guaranteed_profit_pct, 2),
            "legs": [{"bookmaker": l.bookmaker,
                      "market":    l.market,
                      "odds":      l.odds}
                     for l in sig.legs],
            "timestamp":             sig.timestamp.isoformat(),
        })

    # Update shared state
    try:
        bets = ledger.get_all_bets()
        pnl  = ledger.get_pnl_summary()
    except Exception:
        bets, pnl = [], {}

    with _lock:
        _latest_signals.clear()
        _latest_signals.extend(serialised)
        _latest_grouped.clear()
        _latest_grouped.extend(grouped)
        _latest_bets.clear()
        _latest_bets.extend(bets)
        _latest_pnl.clear()
        _latest_pnl.update(pnl)

    # Push SSE to all connected browsers
    _push_sse_event()

    # Decisions
    ledger.expire_stale_orders()

    for sig in all_signals:
        cc_data: Optional[dict] = None
        match_key = sig.match_key if hasattr(sig, "match_key") else ""
        for g in grouped:
            mk = f"{g['team1']}_vs_{g['team2']}_{g['tournament']}"
            if mk == match_key:
                cc_data = g.get("odds", {}).get("coincasino")
                break

        if not risk.allow_signal(sig, cc_data):
            continue

        order = router.to_order(sig)
        if order is None:
            continue

        snapshot = getattr(sig, "odds_snapshot", {})
        ledger.record_order(order, odds_snapshot=snapshot)
        risk.record_bet(order.match_key, order.stake_eur)
        log.info("BetOrder queued: %s  %s  %.2f EUR  edge=%+.1f%%",
                 order.match_key, order.market, order.stake_eur, order.edge_pct)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

def _build_api(ledger: "Ledger"):
    try:
        from contextlib import asynccontextmanager
        from fastapi import FastAPI, Request
        from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
    except ImportError:
        log.warning("fastapi not installed — REST API disabled.")
        return None

    @asynccontextmanager
    async def _lifespan(application: "FastAPI"):
        """Capture the asyncio event loop on startup for SSE bridging."""
        global _sse_event_loop
        _sse_event_loop = asyncio.get_event_loop()
        log.info("SSE event loop captured.")
        yield  # app runs here

    app = FastAPI(title="Betting Orchestrator API", version="2.0",
                  lifespan=_lifespan)

    # ── Serve new Vanilla-JS dashboard ───────────────────────────────────
    _DASHBOARD_HTML = os.path.join(_ROOT, "dashboard", "index.html")

    @app.get("/")
    def dashboard_ui():
        if os.path.exists(_DASHBOARD_HTML):
            return FileResponse(_DASHBOARD_HTML, media_type="text/html")
        return JSONResponse({"error": "dashboard/index.html not found"}, status_code=404)

    # ── Server-Sent Events stream ─────────────────────────────────────────
    @app.get("/stream")
    async def sse_stream(request: Request):
        global _sse_event_loop
        if _sse_event_loop is None:
            _sse_event_loop = asyncio.get_event_loop()

        q: asyncio.Queue = asyncio.Queue(maxsize=20)
        with _sse_queues_lock:
            _sse_queues.append(q)

        async def _gen():
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        data = await asyncio.wait_for(q.get(), timeout=30.0)
                        yield f"data: {data}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
            finally:
                with _sse_queues_lock:
                    try:
                        _sse_queues.remove(q)
                    except ValueError:
                        pass

        return StreamingResponse(
            _gen(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # ── REST endpoints ────────────────────────────────────────────────────
    @app.get("/api/health")
    def health():
        return {"status": "ok", "timestamp": datetime.datetime.now().isoformat()}

    @app.get("/api/signals")
    def signals():
        cutoff = datetime.datetime.now() - datetime.timedelta(seconds=SIGNAL_TTL_S)
        with _lock:
            active = [
                s for s in _latest_signals
                if datetime.datetime.fromisoformat(s["timestamp"]) > cutoff
            ]
        return JSONResponse(active)

    @app.get("/api/odds")
    def odds():
        with _lock:
            return JSONResponse(list(_latest_grouped))

    @app.get("/api/logs")
    def logs():
        with _lock:
            return JSONResponse(dict(_latest_logs))

    @app.get("/api/bets")
    def bets():
        return JSONResponse(ledger.get_all_bets())

    @app.get("/api/pnl")
    def pnl():
        return JSONResponse(ledger.get_pnl_summary())

    return app


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    _import_modules()

    cfg    = _load_config()
    ledger = Ledger(LEDGER_PATH)
    risk   = RiskManager(cfg)
    router = SignalRouter(cfg)

    log.info("Orchestrator starting. DB_ROOT=%s  LEDGER=%s", DB_ROOT, LEDGER_PATH)

    # Initial log fetch (so the first SSE event includes log data)
    threading.Thread(target=_refresh_logs, daemon=True).start()

    # Start poll loop
    t = threading.Thread(target=_poll_loop, args=(ledger, risk, router), daemon=True)
    t.start()

    # Start log refresh loop
    threading.Thread(target=_logs_loop, daemon=True).start()

    # Start API
    app = _build_api(ledger)
    if app is not None:
        try:
            import uvicorn
            log.info("REST API + SSE stream starting on port %d …", API_PORT)
            uvicorn.run(app, host="0.0.0.0", port=API_PORT, log_level="warning")
        except ImportError:
            log.warning("uvicorn not installed — REST API disabled.")
            t.join()
    else:
        t.join()


if __name__ == "__main__":
    main()

