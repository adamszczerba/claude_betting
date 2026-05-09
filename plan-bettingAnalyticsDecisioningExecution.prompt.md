# Betting System — Full Architecture

> Last updated: 2026-05-08
> Status: **Approved, pending implementation**

---

## Overview

This system extends the existing 7-scraper odds-collection pipeline with three
new layers: **Analytics** (consensus odds + value/arb detection), **Decisioning**
(risk management + stake sizing), and **Execution** (Playwright-based bet
placement on CoinCasino only, through the existing ProtonVPN Poland tunnel).

---

## System Diagram

```
┌──────────────────────────── Existing Layer ─────────────────────────────┐
│                                                                          │
│  coincasino ──┐                                                          │
│  betfair ─────┤                                                          │
│  betfair_ex ──┼──► match_database/<bookmaker>/<date>/*.csv              │
│  bet365 ──────┤                                                          │
│  pinnacle ────┤         (7 Docker containers, WireGuard VPN)            │
│  lvbet ───────┤                                                          │
│  sts ─────────┘                                                          │
│                                                                          │
│  dashboard/app.py  →  http://127.0.0.1:8050  (Plotly Dash)              │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                polls match_database/ every 2 s
                              │
                              ▼
┌──────────────────────── Orchestrator ───────────────────────────────────┐
│  orchestrator/main.py                                                    │
│                                                                          │
│  1. scan_today() + build_grouped_table()  ← reuse dashboard helpers     │
│  2. Analytics Engine                                                     │
│       analytics/overround.py   — remove bookmaker margin                │
│       analytics/consensus.py   — weighted median implied probability     │
│       analytics/value.py       — emit ValueSignal (edge %)              │
│       analytics/arbitrage.py   — emit ArbSignal (guaranteed profit %)   │
│  3. Decisioning Engine                                                   │
│       decisions/risk_manager.py — filters (edge threshold, leagues, …)  │
│       decisions/kelly.py        — ¼-Kelly stake sizing                  │
│       decisions/signal_router.py → write BetOrder to ledger/bets.db     │
│                                                                          │
│  REST API (FastAPI):  /api/signals  /api/bets  /api/pnl                 │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ BetOrder rows in ledger/bets.db
                                ▼
┌──────────────────────── Executor (Docker) ──────────────────────────────┐
│  docker/executor/  — python:3.12-slim + Playwright Chromium             │
│  WireGuard: ProtonVPN Poland  (same config as scraper-coincasino)       │
│                                                                          │
│  execution/cc_executor.py                                                │
│    - playwright-stealth patches (fingerprint masking)                   │
│    - persistent browser context (cookies / localStorage)                │
│    - human-like mouse paths + randomised delays (200–800 ms)            │
│    - price drift guard (abort if slip > threshold)                      │
│    - DryRunExecutor (default) / LiveExecutor (EXECUTION_MODE=live)      │
│                                                                          │
│  execution/betby_probe.py  — optional: reverse-engineer authenticated   │
│    Betby REST endpoints for faster/stabler placement (Playwright        │
│    fallback if unavailable)                                              │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ receipt
                                ▼
┌──────────────────────── Ledger ─────────────────────────────────────────┐
│  ledger/bets.db  (SQLite)                                                │
│                                                                          │
│  bets(id, created_at, bookmaker, match_key, market, outcome,            │
│        stake, requested_price, accepted_price, status)                  │
│  signals(bet_id, odds_snapshot_json)                                     │
│  results(bet_id, settled_at, pnl)                                        │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                      dashboard REST API
                                ▼
┌──────────────────────── Extended Dashboard ─────────────────────────────┐
│  dashboard/app.py   (new tabs added to existing Dash app)               │
│                                                                          │
│  Tab 1 — Scraper Logs      (existing)                                   │
│  Tab 2 — Odds Comparison   (existing)                                   │
│  Tab 3 — Opportunities     (ValueSignal / ArbSignal live table)         │
│  Tab 4 — Bet Ledger        (placed bets, status, cumulative P&L chart)  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Layout (target state)

```
Betting/
├── analytics/
│   ├── __init__.py
│   ├── overround.py        # remove_overround(odds, method) → fair_odds
│   ├── consensus.py        # weighted_consensus(grouped_rows) → fair_odds per market
│   ├── value.py            # scan_value(grouped_rows) → list[ValueSignal]
│   └── arbitrage.py        # scan_arb(grouped_rows)   → list[ArbSignal]
│
├── decisions/
│   ├── __init__.py
│   ├── risk_manager.py     # RiskManager(config) → filter(signal) → bool
│   ├── kelly.py            # kelly_stake(edge, odds, bankroll, fraction=0.25)
│   ├── signal_router.py    # SignalRouter: signals → BetOrder stream
│   └── config.yaml         # thresholds, league whitelist, bankroll, limits
│
├── execution/
│   ├── __init__.py
│   ├── base.py             # abstract Executor.place_bet(order) → BetReceipt
│   ├── cc_executor.py      # CoinCasino Playwright executor (stealth)
│   ├── betby_probe.py      # investigate Betby auth API endpoints
│   └── dry_run.py          # DryRunExecutor — logs only, no real bets
│
├── ledger/
│   ├── __init__.py
│   ├── schema.py           # SQLite table definitions + migrations
│   └── ledger.py           # record_order(), record_receipt(), settle_bet(), pnl()
│
├── orchestrator/
│   ├── __init__.py
│   ├── main.py             # poll loop + analytics + decisions + REST API
│   └── Dockerfile
│
├── docker/
│   ├── executor/
│   │   ├── Dockerfile      # python:3.12-slim + Playwright + WireGuard
│   │   └── entrypoint.sh   # same WG pattern as coincasino entrypoint
│   └── … (existing)
│
├── mathematics/
│   └── event_probability.py   (extended with overround removal)
│
├── dashboard/
│   ├── app.py              (+ Opportunities and Bet Ledger tabs)
│   └── … (existing)
│
└── plan-bettingAnalyticsDecisioningExecution.prompt.md   (this file)
```

---

## Module Specifications

### `analytics/overround.py`

Extends `mathematics/event_probability.py`.

```
remove_overround(odds: list[float], method: "normalize" | "shin" | "power")
    → fair_odds: list[float]
```

- **normalize**: scale implied probs so they sum to 1
- **shin**: Shin (1993) model — accounts for insider-info asymmetry
- **power**: solve for k such that `sum((1/o)^k) = 1`, then `p_fair = (1/o)^k`

### `analytics/consensus.py`

```python
BOOKMAKER_WEIGHTS = {
    "pinnacle":         1.0,   # sharpest market
    "betfair_exchange": 0.85,
    "bet365":           0.5,
    "betfair":          0.5,
    "coincasino":       0.4,
    "lvbet":            0.35,
    "sts":              0.35,
}

weighted_consensus(grouped_rows: list[dict]) → dict[match_key, FairOdds]
```

Computes weighted median of de-juiced implied probabilities.
Returns `FairOdds(odd_1, odd_X, odd_2, odd_over, odd_under)` as decimal odds.

### `analytics/value.py`

```python
@dataclass
class ValueSignal:
    match_key: str          # "TeamA_vs_TeamB_Tournament"
    bookmaker: str          # "coincasino"
    market: str             # "odd_1" | "odd_X" | "odd_2" | "odd_over" | "odd_under"
    bookmaker_odds: float
    fair_odds: float
    edge_pct: float         # (bookmaker_odds / fair_odds - 1) * 100
    timestamp: datetime
    odds_snapshot: dict     # full grouped row at signal time
```

Threshold configurable in `decisions/config.yaml` (default: `+3.0%` main, `+5.0%` niche).

### `analytics/arbitrage.py`

```python
@dataclass
class ArbSignal:
    match_key: str
    legs: list[ArbLeg]      # each leg: bookmaker, market, outcome, odds
    guaranteed_profit_pct: float   # (1 - sum(1/odds_i)) * 100
    timestamp: datetime
```

Two-way (1X2 home/away only) and three-way arb detection across aligned bookmaker groups.

---

### `decisions/config.yaml`

```yaml
bankroll_eur: 1000.0
kelly_fraction: 0.25          # ¼-Kelly

value_thresholds:
  main_markets: 3.0           # % minimum edge for odd_1, odd_X, odd_2
  niche_markets: 5.0          # % for over/under

risk:
  max_stake_eur: 50.0
  max_concurrent_bets: 3
  max_exposure_per_match_eur: 100.0
  suspend_on_status: ["HT", "FT", "AET", "PEN"]  # no bets during breaks

leagues:
  whitelist: []               # empty = all leagues allowed
  blacklist: ["Esoccer", "Virtual"]
```

### `decisions/kelly.py`

```
kelly_stake(edge_pct, decimal_odds, bankroll, fraction=0.25) → stake_eur
    b = decimal_odds - 1
    p = implied_probability(fair_odds)
    q = 1 - p
    kelly_full = (b*p - q) / b
    stake = bankroll * kelly_full * fraction
    return clamp(stake, min=1.0, max=config.max_stake_eur)
```

---

### `execution/cc_executor.py` — CoinCasino Playwright

#### Stealth Stack

| Technique | Implementation |
|---|---|
| Fingerprint masking | `playwright-stealth` Python library |
| Headless evasion | `--headless=new` Chrome flag |
| `navigator.webdriver` | patched to `undefined` |
| Viewport | 1920×1080, realistic device scale |
| User-Agent | latest real Chrome/Linux UA |
| Mouse movement | bezier-curve interpolation via `page.mouse.move()` |
| Timing | randomised delays 200–800 ms between actions |
| Session persistence | `browser.new_context(storage_state="cc_session.json")` |

#### Bet Placement Flow

```
1. load_session()          — restore cookies from cc_session.json
   if expired → login(email, password) → save_session()

2. navigate to live sports page
   → wait for match list to render

3. find_match(team1, team2)
   → fuzzy match using normalize() from dashboard/matcher.py
   → scroll match into view

4. click_odds(market)
   → human-like move → click odds button
   → bet slip panel opens

5. check_price(current_price)
   → if |current_price - order.min_price| / order.min_price > drift_threshold:
       raise PriceDriftError → abort, log

6. enter_stake(stake_eur)
   → clear field → type digit-by-digit with delays

7. confirm_bet()
   → click confirm button
   → wait for receipt element
   → parse receipt_id, accepted_price

8. return BetReceipt(id, accepted_price, timestamp)
```

#### Betby API Probe (parallel investigation)

`execution/betby_probe.py` — on first run after login, extract and log:
- `Authorization` / `X-Session-Token` headers from XHR requests
- Bet placement endpoint (likely `POST /api/v1/bets` or similar)
- Request payload structure

If confirmed, `cc_executor.py` switches to API mode (faster, more reliable) with
Playwright only for session token renewal.

---

### `orchestrator/main.py`

```python
while True:
    rows = scan_today(db_root=DB_ROOT)
    grouped = build_grouped_table(rows)
    fair = weighted_consensus(grouped)

    signals = scan_value(grouped, fair) + scan_arb(grouped)

    for sig in signals:
        if risk_manager.filter(sig):
            order = signal_router.to_order(sig, bankroll)
            ledger.record_order(order)
            # executor picks up from ledger — decoupled

    sleep_until_next_tick(interval=2.0)
```

FastAPI endpoints (port 8051):
- `GET /api/signals` — active value/arb signals (last 60 s)
- `GET /api/bets` — all bet records from ledger
- `GET /api/pnl` — daily/total P&L summary

---

### Docker Services (additions to `docker-compose.yml`)

```yaml
  orchestrator:
    build: ./orchestrator
    container_name: orchestrator
    volumes:
      - ./match_database:/app/match_database:ro
      - ./ledger:/app/ledger
    ports:
      - "8051:8051"
    restart: unless-stopped

  executor:
    build:
      context: .
      dockerfile: docker/executor/Dockerfile
    container_name: executor-coincasino
    cap_add:
      - NET_ADMIN
    sysctls:
      - net.ipv4.conf.all.src_valid_mark=1
      - net.ipv6.conf.all.disable_ipv6=0
    volumes:
      - ./ledger:/app/ledger
      - ./vpns/coincasino/warszawa1_protonvpn-PL-88.conf:/etc/wireguard/wg0.conf:ro
      - ./execution/cc_session.json:/app/cc_session.json
    shm_size: "2g"
    environment:
      - EXECUTION_MODE=dry_run     # change to "live" after validation
      - CC_EMAIL=${CC_EMAIL}
      - CC_PASSWORD=${CC_PASSWORD}
    restart: unless-stopped
```

---

## Extended Dashboard Tabs

### Tab 3 — Opportunities

| Column | Description |
|---|---|
| Match | Team A vs Team B |
| Tournament | League name |
| Market | odd_1 / odd_X / odd_2 / odd_over / odd_under |
| Type | VALUE or ARB |
| Bookmaker | CoinCasino / Pinnacle / … |
| Edge % | e.g. +4.2% |
| Fair Odds | consensus fair price |
| BK Odds | bookmaker offered price |
| Kelly Stake | recommended stake (EUR) |
| Age | seconds since signal detected |

ARB rows highlighted red; VALUE rows highlighted green.
Auto-refreshes every 2 s from orchestrator `/api/signals`.

### Tab 4 — Bet Ledger

- Table: all bets, sortable by date/status/P&L
- Status badges: `PENDING` / `PLACED` / `SETTLED_WIN` / `SETTLED_LOSS` / `ABORTED`
- Cumulative P&L line chart (by day)
- Summary KPIs: total bets, win rate, ROI %, total P&L (EUR)

---

## Implementation Phases

| Phase | Scope | Gate to next phase |
|---|---|---|
| **1 — Analytics** | `analytics/` + unit tests | Signals match manual inspection |
| **2 — Decisioning** | `decisions/` + config YAML | Risk filters proven on historical data |
| **3 — Orchestrator** | `orchestrator/` + Dashboard tabs 3–4 | Signals visible in Dashboard |
| **4 — Dry Run** | `execution/dry_run.py` + `ledger/` | 7 days paper trading, positive expectation |
| **5 — Betby Probe** | `execution/betby_probe.py` | Determine if API placement is available |
| **6 — Live Execution** | `execution/cc_executor.py` + `docker/executor/` | Manual sign-off, set `EXECUTION_MODE=live` |

---

## Key Constraints & Gotchas

1. **Account stealth is critical** — CoinCasino will limit/ban accounts that bet with mechanical patterns. Vary stake ±1-2%, randomise confirmation timing, simulate normal browsing between bets.
2. **Dry-run is the default** — `EXECUTION_MODE=live` must be set explicitly. Default is always `dry_run`.
3. **Price drift guard** — never accept odds more than configurable threshold (default 2%) below the signal price.
4. **Session persistence** — `cc_session.json` stored in `execution/` and mounted into executor container. Rotate on login failures.
5. **Suspend on status changes** — no bets placed during `HT`, `FT`, `AET`, `PEN` — match state must be `1H` or `2H` (or `ET1`/`ET2`) with at least 5 minutes remaining.
6. **CSV append-only** — existing scrapers never rewrite files; analytics reads last row only (already handled by `data_service._last_csv_row()`).
7. **Clock sync** — orchestrator must use `sleep_until_next_tick(2.0)` to align with scraper poll boundaries.

