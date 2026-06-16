signal providers:
- web searcher signal provider: searches for live information not based on data from bookmakers
- timeseries signal provider: produce signals based on timeseries for given bookmaker and trained ML algorithm
- comparator signal provider: produce signals based on comparing odds across bookmakers
- software stays open for introducing more signal providers


### 4.2 Signal Provider Layer

Signal providers are **independent processes** that publish typed messages to the Signal Bus.
They can run on different schedules, use different data sources, and be deployed independently.

#### 4.2.1 Signal Bus (new)

**Technology**: Redis Streams (simple, already likely available) or NATS (lighter, purpose-built).

**Message schema** (all signals):
```python
@dataclass
class Signal:
    signal_id: str              # UUID
    signal_type: str            # "COMPARATOR" | "ML" | "WEB_SEARCH" | "LIVE_DATA"
    timestamp: datetime         # when signal was generated
    sport: str                  # "football", "tennis", etc.
    match_id: str               # per-bookmaker raw match identifier
    canonical_match_id: str     # cross-bookmaker id from matcher (§4.1.3)
    match_key: str              # human-readable "TeamA_vs_TeamB_Tournament"
    team1: str
    team2: str
    tournament: str
    market: str                 # "odd_1", "odd_X", "odd_2", "odd_over", etc.
    direction: str              # "BACK" (bet for) | "LAY" (bet against) | "NEUTRAL"
    confidence: float           # 0.0–1.0
    edge_pct: float             # estimated edge percentage
    payload: dict               # signal-type-specific data
    ttl_sec: int                # signal expires after N seconds
    dedupe_key: str             # deterministic key (§4.2.1) for dedup across republishes
    provider_name: str          # which provider emitted this (data lineage, §9.5)
    provider_version: str       # provider git SHA / tag
```

**Transport contract**:
```python
# signals/bus.py
class SignalBus:
    def publish(self, signal: Signal) -> None: ...
    def subscribe(self, signal_types: list[str], callback) -> None: ...
    def get_recent(self, match_id: str, last_n_sec: int = 60) -> list[Signal]: ...
```

**Rules**:
- Signals are **immutable** — once published, never modified.
- Signals have a **TTL** — the decision engine ignores expired signals.
- Each signal provider is a **separate process** (can be separate container).
- Signal providers MUST NOT communicate with each other directly — only via the bus.
- The bus persists signals for at least 5 minutes (for decision engine consumption).

Additional bus/producer rules (MVP-focused):
- Signals MUST include a deterministic `dedupe_key` computed by the provider. Suggested components: `provider_name`, `source_event_id` (or `canonical_match_id`), `market`, `direction`, rounded `edge_pct`, and `snapshot_ts` (or `observed_at`).
- Decision engine must be able to query "recent signals" per `canonical_match_id` and ignore duplicates sharing the same `dedupe_key` within the configured window.
- Providers should avoid republishing materially identical signals within a short window unless `edge_pct` or `confidence` changed beyond a configured delta.

Design note: start with a lightweight Redis-backed implementation for the bus (recent-signal store + pub/sub) to keep infra simple in Phase 2. Upgrade to Redis Streams or NATS when replay/consumer-group features are required.


### 4.2 Signal Provider Layer

Signal providers are **independent processes** that publish typed messages to the Signal Bus.
They can run on different schedules, use different data sources, and be deployed independently.

#### 4.2.1 Signal Bus (new)

**Technology**: Redis Streams (simple, already likely available) or NATS (lighter, purpose-built).

**Message schema** (all signals):
```python
@dataclass
class Signal:
    signal_id: str              # UUID
    signal_type: str            # "COMPARATOR" | "ML" | "WEB_SEARCH" | "LIVE_DATA"
    timestamp: datetime         # when signal was generated
    sport: str                  # "football", "tennis", etc.
    match_id: str               # per-bookmaker raw match identifier
    canonical_match_id: str     # cross-bookmaker id from matcher (§4.1.3)
    match_key: str              # human-readable "TeamA_vs_TeamB_Tournament"
    team1: str
    team2: str
    tournament: str
    market: str                 # "odd_1", "odd_X", "odd_2", "odd_over", etc.
    direction: str              # "BACK" (bet for) | "LAY" (bet against) | "NEUTRAL"
    confidence: float           # 0.0–1.0
    edge_pct: float             # estimated edge percentage
    payload: dict               # signal-type-specific data
    ttl_sec: int                # signal expires after N seconds
    dedupe_key: str             # deterministic key (§4.2.1) for dedup across republishes
    provider_name: str          # which provider emitted this (data lineage, §9.5)
    provider_version: str       # provider git SHA / tag
```

**Transport contract**:
```python
# signals/bus.py
class SignalBus:
    def publish(self, signal: Signal) -> None: ...
    def subscribe(self, signal_types: list[str], callback) -> None: ...
    def get_recent(self, match_id: str, last_n_sec: int = 60) -> list[Signal]: ...
```

**Rules**:
- Signals are **immutable** — once published, never modified.
- Signals have a **TTL** — the decision engine ignores expired signals.
- Each signal provider is a **separate process** (can be separate container).
- Signal providers MUST NOT communicate with each other directly — only via the bus.
- The bus persists signals for at least 5 minutes (for decision engine consumption).

Additional bus/producer rules (MVP-focused):
- Signals MUST include a deterministic `dedupe_key` computed by the provider. Suggested components: `provider_name`, `source_event_id` (or `canonical_match_id`), `market`, `direction`, rounded `edge_pct`, and `snapshot_ts` (or `observed_at`).
- Decision engine must be able to query "recent signals" per `canonical_match_id` and ignore duplicates sharing the same `dedupe_key` within the configured window.
- Providers should avoid republishing materially identical signals within a short window unless `edge_pct` or `confidence` changed beyond a configured delta.

Design note: start with a lightweight Redis-backed implementation for the bus (recent-signal store + pub/sub) to keep infra simple in Phase 2. Upgrade to Redis Streams or NATS when replay/consumer-group features are required.

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