## system overview
## module boundaries
## data flow
## diagrams
## domain concepts
- stays open for new bet types, new bookmakers, new signal providers
- only football and live betting, but should be open for more sports and prematch betting


# Betting System -- Technical Architecture Contract

> Last updated: 2026-06-15
> Status: **Live (v1) -> Target (v2)** -- current modules running; this file is both current contract and evolution roadmap
> Purpose: **Contract between modules** -- authoritative source of truth for module boundaries, data shapes, and integration rules.
> Details: Each module has its own `.md` or in-file docstring; this file is the map, not the territory.

---

# 1. System Overview

## 1.1 Vision

End-to-end autonomous betting system that:

1. **Collects** live and prematch odds as timeseries from many bookmakers.
2. **Generates signals** from multiple independent sources (market inefficiency, ML models, live external information).
3. **Decides** whether to bet, how much, and on what — fusing all available signals.
4. **Executes** bets on selected bookmakers.
5. **Learns** from historical outcomes to improve future decisions.
6. **Presents** state to a human operator for observation and optional approval.

## 1.2 Scope Evolution

| Dimension | Current (v1) | Target (v2) |
|-----------|-------------|-------------|
| Sports | Football only | Football, Tennis, Basketball, Hockey |
| Bet timing | Live only | Live + Prematch |
| Bet types | 1X2, Over/Under | + Corners, Cards, Handicaps, Correct Score, Player Props |
| Signal sources | Comparator only | Comparator + ML + Web Search + Live Data |
| Execution | CoinCasino only | CoinCasino + extensible to others |
| Data storage | CSV files | CSV (real-time) + Historical DB (timeseries + outcomes) |
| Decision making | Rule-based thresholds | ML model + configurable rules + human-in-the-loop |

## 1.2 Current Pipeline (v1 -- Live)

```
[Scrapers x7] --> [match_database/ CSVs] --> [Orchestrator]
                                                    |
                                          +---------v---------+
                                     [Analytics]          [Dashboard]
                                          |
                                     [Decisions]
                                          |
                                       [Ledger]
                                          |
                                       [Executor]
``

## 1.3 Target Pipeline (v2 -- Live)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA COLLECTION LAYER                        │
│                                                                     │
│  [Scraper x N]  ──dual──►  match_database/<sport>/<bk>/<date>/*.csv │
│  (per bookmaker,   write    (append-only, audit trail)              │
│   per sport,                                                       │
│   isolated VPN)       │                                            │
│                        └──XADD──► Redis Stream "odds:<bk>"         │
│                                  (MAXLEN 10000, ~3h window)        │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────┼─────────────────────────────────────┐
│                    REAL-TIME CONSUMERS                               │
│                               │                                     │
│  ┌──────────────┐  ┌──────────┴───┐  ┌──────────────────┐         │
│  │ Comparator   │  │  ML Signal   │  │  Dashboard       │         │
│  │ Signal       │  │  Provider    │  │  (live odds,     │         │
│  │ Provider     │  │  (future)    │  │   <50ms latency) │         │
│  │ (XREAD)      │  │              │  │                  │         │
│  └──────┬───────┘  └───────┬──────┘  └──────────────────┘         │
│         │                  │                                        │
│         └──────────────────┼─────────────────────┐                  │
│                            │                     │                  │
│              ┌─────────────┴──────────────┐      │                  │
│              │  Signal Bus (Redis pub/sub)│      │                  │
│              │  "signals:all" channel     │      │                  │
│              └─────────────┬──────────────┘      │                  │
└────────────────────────────┼─────────────────────┼──────────────────┘
                             │                     │
                             │
┌────────────────────────────┼─────────────────────────────────────────┐
│                     DECISION LAYER                                    │
│                            │                                         │
│              ┌─────────────▼──────────────┐                          │
│              │     Decision Engine        │                          │
│              │                            │                          │
│              │  1. Signal Aggregator      │                          │
│              │  2. ML Value Assessor      │                          │
│              │  3. Risk Manager           │                          │
│              │  4. Kelly / Stake Sizer    │                          │
│              │  5. Human Approval Gate    │                          │
│              └─────────────┬──────────────┘                          │
│                            │ BetOrder                                │
└────────────────────────────┼─────────────────────────────────────────┘
                             │
┌────────────────────────────┼─────────────────────────────────────────┐
│                     EXECUTION LAYER                                   │
│                            │                                         │
│              ┌─────────────▼──────────────┐                          │
│              │     Executor Router        │                          │
│              │                            │                          │
│              │  ┌─────────┐ ┌──────────┐  │                          │
│              │  │CoinCasino│ │ Future   │  │                          │
│              │  │Executor  │ │ Executor │  │                          │
│              │  └─────────┘ └──────────┘  │                          │
│              └─────────────┬──────────────┘                          │
│                            │                                         │
│              ┌─────────────▼──────────────┐                          │
│              │     Ledger (SQLite)        │                          │
│              │     bets, signals, results │                          │
│              └────────────────────────────┘                          │
└──────────────────────────────────────────────────────────────────────┘
                             │
┌────────────────────────────┼─────────────────────────────────────────┐
│                     OBSERVABILITY LAYER                               │
│                            │                                         │
│              ┌─────────────▼──────────────┐                          │
│              │     Dashboard (Dash)       │                          │
│              │                            │                          │
│              │  - Live odds comparison    │                          │
│              │  - Signal feed             │                          │
│              │  - Bet ledger & P&L        │                          │
│              │  - ML model metrics        │                          │
│              │  - Scraper health          │                          │
│              │  - Human approval queue    │                          │
│              └────────────────────────────┘                          │
└──────────────────────────────────────────────────────────────────────┘
```

---

# 2. Module Boundaries

| Module | Path | Role | Transport | Status |
|--------|------|------|-----------|--------|
| **Scrapers** | `v2_*/` | Collect live/prematch odds → CSV | file write | ✅ Running |
| **Data Ingestor** | `ingestion/` | CSV → Historical DB pipeline | file read → DB write | 🔲 Planned |
| **Historical DB** | `database/` | Timeseries storage for odds + outcomes | PostgreSQL/InfluxDB | 🔲 Planned |
| **Comparator Signal** | `signals/comparator/` | Cross-bookmaker value & arb detection | Signal Bus pub | ✅ (refactor) |
| **ML Signal** | `signals/ml/` | Trained model predictions | Signal Bus pub | 🔲 Planned |
| **Web Search Signal** | `signals/web/` | Live news/event intelligence | Signal Bus pub | 🔲 Planned |
| **Live Data Signal** | `signals/live_data/` | Match state signals (score, time, momentum) | Signal Bus pub | 🔲 Planned |
| **Signal Bus** | `signals/bus.py` | Typed pub/sub message broker | Redis/NATS | 🔲 Planned |
| **Decision Engine** | `decisions/` | Signal fusion, risk, sizing, approval | in-process | ✅ (extend) |
| **Ledger** | `ledger/` | Persistent bet records | SQLite | ✅ Running |
| **Executor** | `execution/` | Place bets on bookmakers | HTTP + browser | ✅ (extend) |
| **Orchestrator** | `orchestrator/` | Poll loop, REST API, SSE | HTTP :8051 | ✅ Running |
| **Dashboard** | `dashboard/` | Observability UI | HTTP :8050 | ✅ Running |
| **ML Trainer** | `ml/` | Model training on historical data | offline batch | 🔲 Planned |
| **Data Cleaner** | `maintenance/` | Purge corrupted historical records | DB write | 🔲 Planned |

### 4.1 Data Collection Layer

#### 4.1.1 Scrapers (existing, extended)

Current `v2_*` scrapers remain the real-time data source. Extensions for target:

**Multi-sport support**: Each scraper adds sport detection and tagging.
```
match_database/<sport>/<bookmaker>/<YYYY-MM-DD>/<team1>_vs_<team2>_<tournament>_<tag>_<date>.csv
```
- `sport`: `football`, `tennis`, `basketball`, `hockey`
- Detected from page structure or API response metadata

**Prematch support**: Separate poll mode with longer intervals (30–60 s).
```python
# prematch mode
scraper.run(mode="prematch", interval=30)  # polls upcoming events
scraper.run(mode="live", interval=2)       # current behavior
```

**Extended bet types**: CSV schema grows with new market columns (append-only, never reorder):
```csv
# Standard 11 (existing)
timestamp,match_time,match_status,home_score,away_score,odd_1,odd_X,odd_2,total_line,odd_over,odd_under

# Extended markets (appended per scraper capability)
,handicap_line,odd_handicap_home,odd_handicap_away
,corners_line,odd_corners_over,odd_corners_under
,cards_line,odd_cards_over,odd_cards_under
,correct_score_1_0,correct_score_2_0,correct_score_2_1,...
```

**Rules**:
- Each sport gets its own subdirectory under `match_database/`.
- Sport key is canonical lowercase: `football`, `tennis`, `basketball`, `hockey`.
- Prematch CSVs go to `match_database/<sport>/<bk>/<date>/prematch/` to separate from live.
- All existing rules from `ARCHITECTURE.md` §3.1 still apply (append-only, sync-clock, one file per match).






#### 4.2.6 Recent State Cache (recommended)

To make decision-time lookups fast and deterministic, maintain a small recent-state cache (Redis recommended in MVP).

Contents:
- per `canonical_match_id` latest market snapshot(s): best available odds per market, timestamp, `is_suspended`, `scraper_name` and `scraper_version`.
- per bookmaker latest `row_fingerprint` and `observed_at` to allow quick provenance checks.

Usage:
- Decision engine consults the recent-state cache for freshest market data immediately before creating an order; signals are advisory and should reference the cache snapshot for final odds.
- Executors should re-fetch fresh odds from the cache (or directly from the bookmaker) and apply price-drift guards before placing a bet.

Operational note: implement cache as a small Redis instance alongside the signal-bus in Phase 2. This keeps infra light while providing the benefits of a "source of current truth" for decision-time operations.

---

### 4.3 Decision Layer









#### 4.4.2 Bookmaker Executors

Each bookmaker gets its own executor implementation:

```
execution/
├── base.py                 # Executor ABC (existing)
├── dry_run.py              # DryRunExecutor (existing)
├── cc_executor.py          # CoinCasinoExecutor (existing)
├── betfair_executor.py     # BetfairExecutor (planned)
├── pinnacle_executor.py    # PinnacleExecutor (planned)
└── router.py               # ExecutorRouter (new)
```

**Rules**:
- Each executor runs in its own container with its own VPN.
- Executors poll the orchestrator for PENDING orders (existing pull model).
- New executors implement the `Executor` ABC — no changes to orchestrator or ledger needed.
- Price drift guard applies per-executor (existing rule).

#### 4.4.3 Order Lifecycle & Idempotency

Execution must be expressed as a strict state machine so retries, partial fills and rejections are handled deterministically.

Recommended lifecycle states:
- CREATED: decision engine emitted order and persisted to ledger
- PENDING_APPROVAL: awaiting human approval (optional)
- APPROVED: ready to dispatch
- DISPATCHED: sent to executor for placement
- PLACED: bookmaker acknowledged bet (includes bet reference)
- PARTIALLY_FILLED: if marketplace supports partial fills (keep for completeness)
- REJECTED: bookmaker rejected order (with reason)
- EXPIRED: order expired before placement
- SETTLED: outcome received and P&L computed
- VOIDED: voided/cancelled by bookmaker

Idempotency & retry rules:
- Every order has a stable `order_id` (UUID) persisted in the ledger before dispatch.
- Executors must be idempotent: receiving the same `order_id` twice must not double-place a bet.
- Retries must be limited and backoff-aware; after N attempts transition to REJECTED with diagnostic logged.
- Before final `PLACED` state, executors must check the recent-state cache for price drift and either refresh stake/odds or abort.

Auditability:
- Store executor responses (`executor_response` JSON) and bookmaker bet references in the ledger row for the order.

---

### 4.5 Data Maintenance

#### 4.5.1 Data Cleaner (new)

```python
# maintenance/cleaner.py
class DataCleaner:
    """
    Periodically reviews historical data quality:
    - Detect corrupted CSV files (missing columns, bad timestamps)
    - Identify matches with no settlement data
    - Flag odds anomalies (e.g., odds > 1000, negative odds)
    - Purge records older than retention period
    - Generate data quality reports
    """
    def audit(self, sport: str, date_range: tuple) -> QualityReport: ...
    def clean(self, report: QualityReport) -> int: ...
    def purge(self, before: str) -> int: ...
```

**Rules**:
- Runs as a scheduled job (daily or weekly).
- Never deletes data without generating an audit report first.
- Corrupted records are flagged, not silently deleted.
- Retention policy configurable in `decisions/config.yaml`.

---

# 3. Data Flow

(target)

```
[Scraper containers x N]
  │
  ├──► match_database/<sport>/<bk>/<date>/*.csv  (real-time, append-only)
  │              │
  │              ├──► [Ingestor] ──► [Historical DB]  (timeseries, permanent)
  │              │                        │
  │              │                        ├──► [ML Trainer] ──► ml/models/
  │              │                        │
  │              │                        └──► [Data Cleaner] (audit, purge)
  │              │
  │              └──► [Comparator Signal] ──┐
  │                                         │
  │    [Historical DB] ──► [ML Signal] ─────┤
  │                                         │
  │    [News APIs] ──► [Web Search Signal] ─┤
  │                                         │
  │    [CSV latest] ──► [Live Data Signal] ──┤
  │                                         │
  │                                         ▼
  │                              ┌─── [Signal Bus] ───┐
  │                              │    (Redis/NATS)    │
  │                              └────────┬───────────┘
  │                                       │
  │                              ┌────────▼───────────┐
  │                              │  Decision Engine    │
  │                              │  (fuse, risk, size) │
  │                              └────────┬───────────┘
  │                                       │ BetOrder
  │                              ┌────────▼───────────┐
  │                              │  Approval Gate      │
  │                              │  (optional)         │
  │                              └────────┬───────────┘
  │                                       │ approved BetOrder
  │                              ┌────────▼───────────┐
  │                              │  Executor Router    │
  │                              │  ┌────┐ ┌────┐      │
  │                              │  │ CC │ │ BF │ ...  │
  │                              │  └────┘ └────┘      │
  │                              └────────┬───────────┘
  │                                       │ BetReceipt
  │                              ┌────────▼───────────┐
  │                              │  Ledger (SQLite)    │
  │                              └────────┬───────────┘
  │                                       │
  │                              ┌────────▼───────────┐
  │                              │  Dashboard          │
  │                              │  (observe, approve) │
  │                              └────────────────────┘
```

---

# 4. Diagrams

## 4.1 Current System Diagram (v1)

```
[Scraper containers]
  v2_coincasino -----+
  v2_betfair --------+
  v2_bet365 ---------+------------------> match_database/<bk>/<date>/*.csv
  v2_betfair_exchange+                              |
  v2_pinnacle -------+                              | read (scan_today)
  v2_lvbet ----------+                              v
  v2_sts ------------+                    dashboard/data_service.py
                                                    |
                                          dashboard/matcher.py
                                          build_grouped_table()
                                                    |
                              +---------------------+------------------+
                              |                                        |
                     analytics/                                 dashboard/app.py
                    (consensus, value, arb)                     (live view)
                              |
                     decisions/
                    (risk_manager, kelly, router)
                              | BetOrder
                              v
                        ledger/bets.db <-----------------------------+
                              |                                     |
                       orchestrator/main.py                         |
                       REST API :8051                               |
                              |                                     |
                 +------------+                                     +
                 |                                                  |
           dashboard/    executor/                           BetReceipt write
           app.py        executor_loop.py                          |
           (bets/P&L)         |                                   |
                              v                                   |
                      cc_executor.py ------------ CoinCasino ------+
```

## 4.2 Target System Diagram (v2)

```
[Scraper containers x N]
  │
  ├──► match_database/<sport>/<bk>/<date>/*.csv  (real-time, append-only)
  │              │
  │              ├──► [Ingestor] ──► [Historical DB]  (timeseries, permanent)
  │              │                        │
  │              │                        ├──► [ML Trainer] ──► ml/models/
  │              │                        │
  │              │                        └──► [Data Cleaner] (audit, purge)
  │              │
  │              └──► [Comparator Signal] ──┐
  │                                         │
  │    [Historical DB] ──► [ML Signal] ─────┤
  │                                         │
  │    [News APIs] ──► [Web Search Signal] ─┤
  │                                         │
  │    [CSV latest] ──► [Live Data Signal] ──┤
  │                                         │
  │                                         ▼
  │                              ┌─── [Signal Bus] ───┐
  │                              │    (Redis/NATS)    │
  │                              └────────┬───────────┘
  │                                       │
  │                              ┌────────▼───────────┐
  │                              │  Decision Engine    │
  │                              │  (fuse, risk, size) │
  │                              └────────┬───────────┘
  │                                       │ BetOrder
  │                              ┌────────▼───────────┐
  │                              │  Approval Gate      │
  │                              │  (optional)         │
  │                              └────────┬───────────┘
  │                                       │ approved BetOrder
  │                              ┌────────▼───────────┐
  │                              │  Executor Router    │
  │                              │  ┌────┐ ┌────┐      │
  │                              │  │ CC │ │ BF │ ...  │
  │                              │  └────┘ └────┘      │
  │                              └────────┬───────────┘
  │                                       │ BetReceipt
  │                              ┌────────▼───────────┐
  │                              │  Ledger (SQLite)    │
  │                              └────────┬───────────┘
  │                                       │
  │                              ┌────────▼───────────┐
  │                              │  Dashboard          │
  │                              │  (observe, approve) │
  │                              └────────────────────┘
```

## 4.3 Signal Flow Diagram

```
+-------------+   +-------------+   +-------------+   +-------------+
| Comparator  |   |     ML      |   | Web Search  |   | Live Data   |
|  Provider   |   |  Provider   |   |  Provider   |   |  Provider   |
+------+------+   +------+------+   +------+------+   +------+------+
       |                 |                 |                 |
       | Signal          | Signal          | Signal          | Signal
       | (COMPARATOR)    | (ML)            | (WEB_SEARCH)    | (LIVE_DATA)
       |                 |                 |                 |
       +--------+--------+--------+--------+--------+--------+
                |                 |                 |
                +-------------+---+-----------------+
                              |
                     +--------v--------+
                     |   Signal Bus    |
                     |  (Redis/NATS)   |
                     +--------+--------+
                              |
                     +--------v--------+
                     | Decision Engine |
                     |  (aggregate,    |
                     |   fuse, size)   |
                     +--------+--------+
                              | BetOrder
                     +--------v--------+
                     | Approval Gate   |
                     | (optional)      |
                     +--------+--------+
                              |
                     +--------v--------+
                     | Executor Router |
                     |  +---+  +---+    |
                     |  |CC |  |BF |... |
                     |  +---+  +---+    |
                     +-----------------+
```




**Shared volumes**:
- `match_database/` — scrapers write; ingestor, orchestrator, dashboard read
- `ledger/` — orchestrator + executor read/write
- `ml/models/` — trainer writes; ml-signal reads
- `execution/cc_session.json` — executor rw

---

# Appendix A: Module Detail References

| Module | Detailed documentation |
|--------|----------------------|
| All scrapers (v2_*) | `AGENTS.md` + each scraper's module docstring |
| `sync_clock` | `v2_coincasino/sync_clock.py` docstring |
| `data_service` | `dashboard/data_service.py` docstring |
| `matcher` | `dashboard/matcher.py` docstring |
| `analytics/*` | `plan-bettingAnalyticsDecisioningExecution.prompt.md` |
| `decisions/*` | `plan-bettingAnalyticsDecisioningExecution.prompt.md` + `decisions/config.yaml` |
| `ledger/*` | `ledger/schema.py` + `ledger/ledger.py` docstrings |
| `orchestrator` | `orchestrator/main.py` docstring |
| `execution/*` | `plan-bettingAnalyticsDecisioningExecution.prompt.md` |
| Docker / VPN setup | `AGENTS.md` + `docker/*/entrypoint.sh` |
| Dashboard | `dashboard/app.py` |
| Known issues / TODOs | `todos.py`, `AGENTS.md` |

# Appendix B: Extension Points

### Adding a new signal provider
1. Create `signals/<name>/provider.py` implementing the `SignalProvider` interface.
2. Define signal payload schema in `signals/<name>/schema.py`.
3. Add provider weight to `signal_weights` in `decisions/config.yaml`.
4. Deploy as a new Docker service publishing to the Signal Bus.
5. No changes to other modules needed.

### Adding a new sport
1. Extend scrapers with sport detection and tagging.
2. Add sport directory to `match_database/<sport>/`.
3. Add sport config section to `decisions/config.yaml`.
4. Train sport-specific ML model (when ready).
5. Add sport to `BOOKMAKER_WEIGHTS` and `BOOKMAKERS` if sport-specific bookmakers exist.

### Adding a new bookmaker
1. Create `v2_<bk>/` scraper following v2 pattern.
2. Create `execution/<bk>_executor.py` implementing `Executor` ABC.
3. Add Docker service + VPN config.
4. Add weight to `BOOKMAKER_WEIGHTS` in `analytics/consensus.py`.
5. Add to `BOOKMAKERS` in `dashboard/data_service.py`.

### Adding a new market type
1. Extend CSV schema (append columns).
2. Add market key to canonical list (§5.4 in ARCHITECTURE.md).
3. Update signal providers to detect value in the new market.
4. Add threshold to `value_thresholds` in config.
5. Update analytics consensus to handle the new market group.
