assess bet value, decide if place the bet, provide bet sizing

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

