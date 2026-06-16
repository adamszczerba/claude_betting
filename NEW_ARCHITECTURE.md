# Betting System — Target Architecture

> Status: **Target** — evolution roadmap from current `ARCHITECTURE.md`  
> Purpose: **End-state design** for multi-sport, multi-signal, ML-driven live & prematch betting  
> Gap reference: Current implementation → see `ARCHITECTURE.md`

---

## 1. Vision

End-to-end autonomous betting system that:

1. **Collects** live and prematch odds as timeseries from many bookmakers.
2. **Generates signals** from multiple independent sources (market inefficiency, ML models, live external information).
3. **Decides** whether to bet, how much, and on what — fusing all available signals.
4. **Executes** bets on selected bookmakers.
5. **Learns** from historical outcomes to improve future decisions.
6. **Presents** state to a human operator for observation and optional approval.

### Scope Evolution

| Dimension | Current (v1) | Target (v2) |
|-----------|-------------|-------------|
| Sports | Football only | Football, Tennis, Basketball, Hockey |
| Bet timing | Live only | Live + Prematch |
| Bet types | 1X2, Over/Under | + Corners, Cards, Handicaps, Correct Score, Player Props |
| Signal sources | Comparator only | Comparator + ML + Web Search + Live Data |
| Execution | CoinCasino only | CoinCasino + extensible to others |
| Data storage | CSV files | CSV (real-time) + Historical DB (timeseries + outcomes) |
| Decision making | Rule-based thresholds | ML model + configurable rules + human-in-the-loop |

---

## 2. System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA COLLECTION LAYER                        │
│                                                                     │
│  [Scraper x N]  ──►  match_database/<sport>/<bk>/<date>/*.csv       │
│  (per bookmaker,     (append-only, real-time, sync-clock)           │
│   per sport,                                                      │
│   isolated VPN)       ┌──────────────────────────────────┐         │
│                       │   Historical Timeseries DB        │         │
│                       │   (PostgreSQL / InfluxDB)         │         │
│                       │   - odds movement                 │         │
│                       │   - match events (goals, cards)   │         │
│                       │   - settlement results            │         │
│                       └──────────┬───────────────────────┘         │
└──────────────────────────────────┼──────────────────────────────────┘
                                   │
┌──────────────────────────────────┼──────────────────────────────────┐
│                     SIGNAL PROVIDER LAYER                            │
│                                  │                                   │
│  ┌──────────────┐  ┌────────────┴───┐  ┌──────────────────┐        │
│  │  Comparator   │  │  ML / Time-    │  │  Web Search      │        │
│  │  Signal       │  │  series Signal │  │  Signal          │        │
│  │  Provider     │  │  Provider      │  │  Provider        │        │
│  │              │  │                │  │                  │        │
│  │ Cross-bk     │  │ Trained on     │  │ Live news,       │        │
│  │ odds diff,   │  │ historical     │  │ radio, video     │        │
│  │ arb, value   │  │ odds patterns  │  │ processing       │        │
│  └──────┬───────┘  └───────┬────────┘  └────────┬─────────┘        │
│         │                  │                     │                   │
│         └──────────────────┼─────────────────────┘                   │
│                            │                                         │
│              ┌─────────────┴──────────────┐                          │
│              │   Signal Bus (Redis/NATS)  │                          │
│              │   pub/sub, typed messages  │                          │
│              └─────────────┬──────────────┘                          │
└────────────────────────────┼─────────────────────────────────────────┘
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

## 3. Module Registry

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

---

## 4. Layer-by-Layer Contracts

### 4.1 Data Collection Layer


#### 4.1.2 Historical Database (new)


#### 4.1.3 Identity & Matching (new)


---


#### 4.2.2 Comparator Signal Provider (refactor from existing `analytics/`)

Extracts current `analytics/` logic into a standalone signal-producing process.

```python
# signals/comparator/provider.py
class ComparatorSignalProvider:
    """
    Reads latest CSV rows, computes fair odds via consensus,
    detects value bets and arbitrage, publishes signals.
    """
    def scan(self) -> list[Signal]:
        rows = scan_today(DB_ROOT)
        grouped = build_grouped_table(rows)
        fair = weighted_consensus(grouped)
        
        signals = []
        for sig in scan_value(grouped, fair):
            signals.append(self._to_signal(sig, "COMPARATOR"))
        for sig in scan_arb(grouped):
            signals.append(self._to_signal(sig, "COMPARATOR"))
        return signals
```

**Existing code reuse**: `analytics/overround.py`, `analytics/consensus.py`, `analytics/value.py`, `analytics/arbitrage.py` move into this provider with minimal changes.

#### 4.2.3 ML / Timeseries Signal Provider (new)

Uses trained models to assess whether current odds represent value based on historical patterns.

```python
# signals/ml/provider.py
class MLSignalProvider:
    """
    Queries Historical DB for current match context,
    runs ML model inference, publishes value signals.
    """
    def __init__(self, model_registry: ModelRegistry):
        self.models = model_registry
    
    def scan(self) -> list[Signal]:
        active_matches = self._get_active_matches()
        signals = []
        for match in active_matches:
            features = self._extract_features(match)
            prediction = self.models.predict(match.sport, features)
            if prediction.is_value:
                signals.append(self._to_signal(match, prediction))
        return signals
    
    def _extract_features(self, match) -> dict:
        """
        Features:
        - Odds movement trajectory (last 10, 30, 60 min)
        - Current match state (time, score, momentum)
        - Historical odds for similar matchups
        - Bookmaker margin trends
        - Volume/liquidity indicators (if available)
        """
        ...
```

**Model types** (per sport):
- **Odds trajectory model**: Predicts where odds will move next (LSTM/Transformer on timeseries).
- **Value assessment model**: Classifies current odds as value/non-value (gradient boosting on engineered features).
- **Outcome probability model**: Estimates true event probability independent of bookmaker odds.

**Training pipeline** (`ml/`):
```python
# ml/trainer.py
class ModelTrainer:
    def train(self, sport: str, model_type: str, 
              train_start: str, train_end: str) -> Model:
        """Pull historical data, engineer features, train, validate."""
        ...
    
    def backtest(self, model: Model, test_start: str, test_end: str) -> Metrics:
        """Evaluate model on held-out period."""
        ...
```

**Rules**:
- Models are versioned and stored in `ml/models/<sport>/<model_type>/v<N>/`.
- ML provider runs inference every 5–10 seconds (not every 2s poll — models are slower).
- ML provider publishes signals with `confidence` reflecting model certainty.
- Training is offline (separate process, not in the live pipeline).

#### 4.2.4 Web Search Signal Provider (new)

Monitors external information sources for live intelligence not reflected in odds.

```python
# signals/web/provider.py
class WebSearchSignalProvider:
    """
    Searches for live information:
    - Breaking news (injuries, red cards, weather)
    - Live radio/text commentary
    - Social media sentiment shifts
    - Video processing (goal detection, possession stats)
    """
    def __init__(self, sources: list[NewsSource]):
        self.sources = sources
    
    def scan(self) -> list[Signal]:
        active_matches = self._get_active_matches()
        signals = []
        for match in active_matches:
            for source in self.sources:
                intel = source.fetch(match)
                if intel.is_actionable:
                    signals.append(self._to_signal(match, intel))
        return signals
```

**Source types**:
- **News APIs**: Breaking sports news (injuries, lineup changes, weather alerts).
- **Live commentary APIs**: Text/radio commentary feeds (goal alerts, possession shifts).
- **Video processing**: Future — computer vision on live streams for momentum detection.
- **Social media**: Sentiment analysis on match-related posts.

**Rules**:
- Web search provider runs on its own schedule (news checks every 30–60 s).
- Signals include source attribution in `payload` for auditability.
- Low-confidence signals are published but tagged — decision engine can weight them.

#### 4.2.5 Live Data Signal Provider (new)

Derives signals from the current match state itself (score, time, momentum).

```python
# signals/live_data/provider.py
class LiveDataSignalProvider:
    """
    Analyzes current match state to generate signals:
    - Goal just scored → odds will shift, is there a window?
    - Match approaching end → draw probability changes
    - Momentum shift (possession, shots) → upcoming goal likelihood
    """
    def scan(self) -> list[Signal]:
        ...
```

**Rules**:
- Reads from the same CSV files (latest row) or Historical DB.
- Complements the comparator — comparator says "odds are wrong", live data says "here's why".

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

#### 4.3.1 Decision Engine (extended from current `decisions/`)

The decision engine is the **only consumer** of signals from the bus. It fuses all signal types into a single betting decision.

```python
# decisions/engine.py
class DecisionEngine:
    """
    Fuses signals from all providers into betting decisions.
    
    Pipeline:
    1. Aggregate signals per match (from Signal Bus)
    2. Score each opportunity (weighted signal fusion)
    3. ML value assessment (optional override)
    4. Risk filtering
    5. Kelly stake sizing
    6. Human approval gate (optional)
    7. Emit BetOrder to executor
    """
    
    def __init__(self, config: DecisionConfig, 
                 signal_bus: SignalBus,
                 risk_manager: RiskManager,
                 ledger: Ledger):
        ...
    
    def evaluate(self) -> list[BetOrder]:
        # 1. Collect all active signals
        signals = self.signal_bus.get_recent(last_n_sec=60)
        grouped = self._group_by_match(signals)
        
        orders = []
        for match_id, match_signals in grouped.items():
            # 2. Fuse signals into a composite score
            composite = self._fuse_signals(match_signals)
            
            # 3. Check edge vs the per-market threshold (maps to value_thresholds in config).
            #    main: odd_1/X/2 · niche: over/under/corners/cards · prematch: prematch markets
            min_edge = self._threshold_for(composite.market, composite.is_prematch)
            if composite.edge_pct < min_edge:
                continue
            if composite.confidence < self.config.min_composite_confidence:
                continue
            
            # 4. Global safety gate (kill switch + daily loss limit) BEFORE per-bet risk.
            if not self.risk_manager.trading_enabled():
                break  # kill switch active or daily loss limit hit → stop all betting
            
            # 5. Risk filter
            if not self.risk_manager.filter(composite):
                continue
            
            # 6. Size the bet
            stake = kelly_stake(
                edge_pct=composite.edge_pct,
                odds=composite.best_odds,
                bankroll=self.config.bankroll_eur,
                fraction=self.config.kelly_fraction,
            )
            stake = min(stake, self.config.max_stake_eur)  # hard cap
            
            # 7. Create order
            order = self._create_order(composite, stake)
            orders.append(order)
        
        return orders
    
    def _fuse_signals(self, signals: list[Signal]) -> CompositeSignal:
        """
        Weighted fusion of signals from different providers.
        
        Weights (configurable):
        - COMPARATOR: 0.40  (market-based, most reliable)
        - ML: 0.30          (model-based, needs validation)
        - WEB_SEARCH: 0.15  (external info, variable quality)
        - LIVE_DATA: 0.15   (contextual, complementary)
        """
        ...
```

#### 4.3.2 Signal Fusion Model

```python
@dataclass
class CompositeSignal:
    match_id: str
    canonical_match_id: str     # cross-bookmaker id (§4.1.3) — used for exposure/correlation
    match_key: str
    sport: str
    team1: str
    team2: str
    tournament: str
    market: str
    direction: str              # "BACK" | "LAY"
    best_odds: float            # best available odds
    best_bookmaker: str         # where to execute
    best_odds_snapshot_ts: datetime  # ts of the odds snapshot used (price-drift guard, §4.4.3)
    edge_pct: float             # fused edge estimate
    confidence: float           # fused confidence
    match_status: str           # "", "HT", "FT", ... — read by RiskManager (§4.3.3)
    is_prematch: bool           # prematch vs live — read by RiskManager (§4.3.3)
    signal_count: int           # how many signals contributed
    signal_breakdown: dict      # {signal_type: {count, avg_edge, avg_confidence}}
    timestamp: datetime
```

**Fusion rules**:
- At least **2 independent signal types** must agree on direction before a bet is placed.
- If signals conflict (one says BACK, another says LAY), the opportunity is skipped.
- Composite `edge_pct` is a weighted average, weighted by `confidence × provider_weight`.
- Composite `confidence` is higher when more independent sources agree.

#### 4.3.3 Risk Manager (extended)

Current `decisions/risk_manager.py` extended with:

```python
class RiskManager:
    def trading_enabled(self) -> bool:
        """
        Global safety gate, checked BEFORE any per-bet logic.
        Returns False (halt ALL betting) if any of:
        - manual kill switch is ON (config.kill_switch or runtime flag from dashboard)
        - cumulative realized loss today >= config.max_daily_loss_eur
        - recent-state cache / signal bus is stale (no fresh data within staleness_sec)
        """
        if self.config.kill_switch:
            return False
        if self._realized_loss_today() >= self.config.max_daily_loss_eur:
            return False
        if self._data_is_stale():
            return False
        return True

    def filter(self, composite: CompositeSignal) -> bool:
        # Existing checks
        if composite.match_status in SUSPEND_STATUSES:
            return False
        if composite.is_suspended:
            return False  # market suspended at the bookmaker — never bet
        if self._concurrent_bets() >= self.config.max_concurrent_bets:
            return False
        if self._exposure(composite.canonical_match_id) >= self.config.max_exposure_per_match_eur:
            return False
        if self._sport_exposure(composite.sport) >= self.config.max_exposure_per_sport_eur:
            return False
        # New: signal agreement requirement
        if composite.signal_count < self.config.min_signal_sources:
            return False
        # New: ML model confidence gate
        if composite.signal_breakdown.get("ML", {}).get("avg_confidence", 0) < self.config.min_ml_confidence:
            return False  # ML signal too uncertain
        # New: prematch vs live risk profiles
        if composite.is_prematch:
            return self._prematch_risk_check(composite)
        return True
```

#### 4.3.4 Human Approval Gate (new)

Optional human-in-the-loop for high-stakes or low-confidence bets.

```python
# decisions/approval.py
class ApprovalGate:
    """
    Routes bets to human approval queue based on rules:
    - Stake > approval_threshold_eur
    - Confidence < auto_approve_confidence
    - New sport/market not yet validated
    - ML model in probation period
    """
    def requires_approval(self, order: BetOrder) -> bool: ...
    def submit_for_approval(self, order: BetOrder) -> None: ...
    def approve(self, order_id: str) -> None: ...
    def reject(self, order_id: str, reason: str) -> None: ...
    def get_pending(self) -> list[BetOrder]: ...
```

**Rules**:
- Approval queue is visible in the Dashboard.
- Orders expire if not approved within `expiry_sec`.
- Auto-approve mode can be enabled to bypass the gate entirely.
- Approval decisions are logged for audit.

---

### 4.4 Execution Layer

#### 4.4.1 Executor Router (extended)

Routes orders to the correct bookmaker executor. Currently only CoinCasino; target supports multiple.

```python
# execution/router.py
class ExecutorRouter:
    """
    Routes BetOrders to the appropriate bookmaker executor.
    Selection based on:
    - order.bookmaker (primary)
    - Fallback to alternative executor if primary unavailable
    - Dry-run mode for testing
    """
    def __init__(self, executors: dict[str, Executor]):
        self.executors = executors
    
    def route(self, order: BetOrder) -> BetReceipt:
        executor = self.executors.get(order.bookmaker)
        if executor is None:
            raise NoExecutorError(f"No executor for {order.bookmaker}")
        return executor.place_bet(order)
```

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

## 5. Configuration (extended)

`decisions/config.yaml` grows to cover new modules:

```yaml
# ── Bankroll & Sizing ──
bankroll_eur: 1000.0
kelly_fraction: 0.25

# ── Signal Thresholds ──
value_thresholds:
  main_markets: 3.0         # % — odd_1, odd_X, odd_2
  niche_markets: 5.0        # % — odd_over, odd_under, corners, cards
  prematch_markets: 2.0     # % — prematch (tighter margins)

# ── Signal Fusion ──
signal_weights:
  comparator: 0.40
  ml: 0.30
  web_search: 0.15
  live_data: 0.15

min_signal_sources: 2       # at least N independent signal types must agree
min_composite_confidence: 0.6

# ── ML Model ──
ml:
  enabled: false             # toggle ML signal provider
  min_confidence: 0.7        # minimum model confidence to consider signal
  model_registry_path: "ml/models/"
  retrain_interval_days: 7

# ── Risk ──
risk:
  kill_switch: false           # master OFF switch — halts ALL betting when true
  max_daily_loss_eur: 200.0    # halt betting once realized loss today reaches this
  staleness_sec: 10            # halt if no fresh odds within this window (safety)
  max_stake_eur: 50.0
  max_concurrent_bets: 3
  max_exposure_per_match_eur: 100.0
  max_exposure_per_sport_eur: 300.0
  suspend_on_status: ["HT", "FT", "AET", "PEN"]
  drift_threshold_pct: 2.0

# ── Human Approval ──
approval:
  enabled: false
  auto_approve: true         # bypass gate
  stake_threshold_eur: 25.0  # require approval above this
  min_confidence_auto: 0.8   # auto-approve above this confidence

# ── Sports ──
sports:
  enabled: ["football"]      # add "tennis", "basketball", "hockey" as ready
  football:
    leagues:
      whitelist: []
      blacklist: ["Esoccer", "Virtual"]
  tennis:
    leagues:
      whitelist: []
      blacklist: []
  basketball:
    leagues:
      whitelist: []
      blacklist: []
  hockey:
    leagues:
      whitelist: []
      blacklist: []

# ── Data Maintenance ──
maintenance:
  retention_days: 365
  audit_interval_days: 7
  auto_purge: false

# ── Signal Bus ──
signal_bus:
  backend: "redis"           # "redis" | "nats"
  host: "signal-bus"
  port: 6379
  signal_ttl_sec: 300
```

---

## 6. Docker Services (extended)

| Service | Container | VPN | Ports | New? |
|---------|-----------|-----|-------|------|
| `scraper-coincasino` | `v2_coincasino/` | ProtonVPN PL | — | |
| `scraper-betfair` | `v2_betfair/` | ProtonVPN UK | — | |
| `scraper-bet365` | `v2_bet365/` | ProtonVPN UK | — | |
| `scraper-betfair-exchange` | `v2_betfair_exchange/` | ProtonVPN UK | — | |
| `scraper-pinnacle` | `v2_pinnacle/` | ProtonVPN UK | — | |
| `scraper-lvbet` | `v2_lvbet/` | ProtonVPN PL | — | |
| `scraper-sts` | `v2_sts/` | ProtonVPN PL | — | |
| `signal-bus` | Redis/NATS | none | 6379 | ✅ |
| `comparator-signal` | `signals/comparator/` | none | — | ✅ (refactor) |
| `ml-signal` | `signals/ml/` | none | — | ✅ |
| `web-signal` | `signals/web/` | none | — | ✅ |
| `live-data-signal` | `signals/live_data/` | none | — | ✅ |
| `historical-db` | PostgreSQL+TimescaleDB | none | 5432 | ✅ |
| `ingestor` | `ingestion/` | none | — | ✅ |
| `orchestrator` | `docker/orchestrator/` | none | 8051 | |
| `executor-coincasino` | `docker/executor/` | ProtonVPN PL | — | |
| `dashboard` | `dashboard/` | none | 8050 | |
| `ml-trainer` | `ml/` (offline) | none | — | ✅ |

**Shared volumes**:
- `match_database/` — scrapers write; ingestor, orchestrator, dashboard read
- `ledger/` — orchestrator + executor read/write
- `ml/models/` — trainer writes; ml-signal reads
- `execution/cc_session.json` — executor rw

---

## 7. Data Flow (target)

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

## 8. Migration Path (current → target)

### Phase 1: Foundation (now)
- [x] Scrapers running (v2_* pattern)
- [x] Comparator analytics (value + arb detection)
- [x] Decision engine (rule-based)
- [x] Ledger (SQLite)
- [x] Executor (CoinCasino dry-run)
- [x] Dashboard (observability)
- [x] Orchestrator (REST API + SSE)

### Phase 2: Signal Bus + Historical DB
- [ ] Implement canonical `matching` module / service (see §4.1.3)
- [ ] Build CSV → DB ingestor (start with validation + provenance; DB can be sqlite for MVP or Postgres for Phase 2)
- [ ] Add recent-state cache (small Redis instance) and a lightweight signal-bus (Redis pub/sub) — keep infra minimal for Phase 2
- [ ] Refactor `analytics/` into comparator signal provider that publishes to the bus and writes provenance to the ingestor
- [ ] Update orchestrator/decision engine to consult the recent-state cache and consume signals from the bus (toggleable)
- [ ] Deploy PostgreSQL + TimescaleDB (optional in Phase 2; required before full ML backfilling)

### Phase 3: ML Pipeline
- [ ] Build feature engineering pipeline from historical DB
- [ ] Gate: ensure data readiness (30+ days stable history, settled outcomes, low match-mismatch rate) before training
- [ ] Train initial odds trajectory model (football, 1X2 market)
- [ ] Deploy ML signal provider (toggleable in config)
- [ ] Implement signal fusion in decision engine (weighting, recency decay, conflict rules)
- [ ] Backtest on historical data and validate lift vs comparator-only baseline

### Phase 4: Multi-Sport
- [ ] Extend scrapers with sport detection
- [ ] Add tennis scrapers (Betfair, Pinnacle)
- [ ] Add basketball scrapers
- [ ] Add hockey scrapers
- [ ] Sport-specific ML models

### Phase 5: Additional Signal Sources
- [ ] Web search signal provider (news APIs)
- [ ] Live data signal provider (match state analysis)
- [ ] Video processing pipeline (future)

### Phase 6: Execution Expansion
- [ ] Betfair executor
- [ ] Pinnacle executor
- [ ] Multi-bookmaker execution with best-price routing

### Phase 7: Human-in-the-Loop
- [ ] Approval gate in decision engine
- [ ] Dashboard approval queue UI
- [ ] Mobile notifications for approval requests

### Phase 8: Prematch Betting
- [ ] Prematch scraper mode
- [ ] Prematch-specific ML models
- [ ] Prematch risk profile in config

---

## 9. Cross-Cutting Rules

### 9.1 Process Isolation
Each signal provider is a **separate process** (and optionally a separate container).
Communication is **only** via the Signal Bus. This enables:
- Independent deployment and scaling
- Fault isolation (one provider crash doesn't affect others)
- Easy addition of new signal types
- Different polling cadences per provider

### 9.2 Sport Abstraction
All modules use `sport` as a first-class dimension:
- CSV paths include sport
- Historical DB partitions by sport
- ML models are per-sport
- Risk limits can be per-sport
- Config enables/disables sports independently
 
Operational note: each sport has unique timing and state semantics. Implement sport-specific adapters that map raw scraper fields into a small common runtime schema:
- `event_phase` (period/half/set)
- `clock_value` (seconds or mm:ss)
- `clock_running` (bool)
- `score` object

Adapters handle conversion and normalization so the rest of the system (signals, decision engine, ML) can rely on a consistent interface rather than football-only assumptions.

### 9.3 Market Extensibility
New bet types (corners, cards, handicaps, player props) are added by:
1. Extending CSV schema (append columns, never reorder)
2. Adding market key to canonical list
3. Updating signal providers to handle the new market
4. Adding threshold in config
5. No changes to bus, ledger, or executor interfaces

### 9.4 Clock Synchronization
All real-time processes (scrapers, signal providers, orchestrator) use:

```python
from scrapers.v2_coincasino import sleep_until_next_tick

sleep_until_next_tick(interval=N)
```
Signal providers may use different intervals based on their data source latency.

### 9.5 Data Lineage
Every signal includes enough metadata to trace back to its source. In addition, DB rows and ledger entries should include provenance to enable end-to-end replay and debugging:
- `signal_type` identifies the provider
- `provider_name` and `provider_version`
- `payload` includes raw data and intermediate calculations
- `dedupe_key` and `source_poll_id` for deduplication and tracing
- `odds_snapshot` in ledger links bets to the signals that triggered them
- `canonical_match_id` and `matching_confidence` (from the matcher)
- `scraper_name`, `scraper_version`, `row_fingerprint`, `observed_at`, `ingested_at` in historical rows

Operational note: preserve raw JSON blobs (optionally compressed) but prefer normalized columns for queries. Ensure provenance fields are indexed for debug workflows.

### 9.6 Money Safety (non-negotiable)
Because the system places real money autonomously, these guards are MANDATORY and always evaluated:
- **Kill switch**: `risk.kill_switch` (config) plus a runtime toggle from the Dashboard. When ON, the decision engine emits zero orders and in-flight orders are not dispatched.
- **Daily loss limit**: when realized loss for the day reaches `risk.max_daily_loss_eur`, all betting halts until the next day or manual reset.
- **Staleness guard**: never bet on data older than `risk.staleness_sec`; stale odds = no bet.
- **Hard stake cap**: every stake is clamped to `risk.max_stake_eur` regardless of Kelly output.
- **Dry-run default**: new executors and new sports/markets START in dry-run; promotion to live requires explicit config + a clean dry-run record.
- **Idempotent execution**: a single `order_id` can never place two bets (see §4.4.3).
- **Suspended markets**: `is_suspended` rows never produce orders.

### 9.7 Edge & Sizing Unit Consistency
`edge_pct` is expressed in **percent** in config thresholds (e.g. `3.0` = 3%) but Kelly sizing needs a **fraction**.
The decision engine MUST convert (`edge = edge_pct / 100`) before calling `kelly_stake`. Kelly uses decimal `best_odds`.
Document the unit at every boundary to avoid 100× sizing bugs.

---

## 10. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Signal transport | Start with Redis (pub/sub + recent-cache), upgrade to Redis Streams/NATS | Keep infra light initially; evolve when replay/consumer-group needs arise |
| Historical DB | PostgreSQL + TimescaleDB | Mature, excellent timeseries support, SQL queries for ML |
| Signal providers as processes | Yes | Fault isolation, independent scaling, easy to add new types |
| ML model per sport | Yes | Different sports have different dynamics |
| Human approval gate | Optional, off by default | Speed for live betting; enable for high-stakes |
| CSV + DB dual storage | Yes | CSV for real-time (low latency), DB for history (queryable) |
| Executor per bookmaker | Yes | Each bookmaker has unique API/browser requirements |
| Signal fusion weighted average | Start simple | Can evolve to ML-based fusion later |
| Prematch separate directory | Yes | Different cadence, different risk profile, avoids confusion |

---

## 11. Extension Points

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


