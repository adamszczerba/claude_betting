# role
scrap data from bookmakers and store
always run in docker containers with vpn

# architecture
    1 sraper for 1 bookmaker in 1 folder

db file format
```
match_database/<sport>/<bookmaker>/<YYYY-MM-DD>/<team1>_vs_<team2>_<tournament>_<tag>_<date>.csv
```

```
# Standard 11 (existing)
timestamp,match_time,match_status,home_score,away_score,odd_1,odd_X,odd_2,total_line,odd_over,odd_under
```

database
**Purpose**: Persistent timeseries storage for ML training, backtesting, and long-term analytics.
CSV files are ephemeral (daily); the DB is permanent.

**Technology**: PostgreSQL with TimescaleDB extension (or InfluxDB if simpler).

**Schema**:
```sql
-- Odds timeseries (one row per scraper poll per match per bookmaker)
CREATE TABLE odds_timeseries (
    ts              TIMESTAMPTZ NOT NULL,
    sport           TEXT NOT NULL,
    bookmaker       TEXT NOT NULL,
    match_id        TEXT NOT NULL,       -- per-bookmaker raw match identifier
    canonical_match_id TEXT,             -- cross-bookmaker id from matcher (§4.1.3)
    matching_confidence FLOAT,           -- 0.0–1.0 confidence of the canonical match
    team1           TEXT NOT NULL,
    team2           TEXT NOT NULL,
    tournament      TEXT NOT NULL,
    match_time      TEXT,                -- "67:23", "HT", etc.
    match_status    TEXT DEFAULT '',
    home_score      INT,
    away_score      INT,
    -- 1X2 market
    odd_1           FLOAT,
    odd_X           FLOAT,
    odd_2           FLOAT,
    -- Over/Under market
    total_line      FLOAT,
    odd_over        FLOAT,
    odd_under       FLOAT,
    -- Handicap market
    handicap_line   FLOAT,
    odd_handicap_home FLOAT,
    odd_handicap_away FLOAT,
    -- Extended markets (nullable)
    corners_line    FLOAT,
    odd_corners_over FLOAT,
    odd_corners_under FLOAT,
    cards_line      FLOAT,
    odd_cards_over  FLOAT,
    odd_cards_under FLOAT,
    -- Metadata / Provenance
    is_prematch     BOOLEAN DEFAULT FALSE,
    -- provenance & auditing
    observed_at     TIMESTAMPTZ DEFAULT now(), -- when scraper observed this row
    ingested_at     TIMESTAMPTZ DEFAULT now(), -- when ingested into DB
    scraper_name    TEXT,    -- e.g. v2_betfair
    scraper_version TEXT,    -- git SHA / tag of scraper parser used
    source_poll_id  TEXT,    -- opaque id from scraper run (for debugging)
    row_fingerprint TEXT,    -- short hash of row payload for dedupe / debug
    parse_confidence FLOAT,  -- 0.0–1.0 if parser can express uncertainty
    is_suspended    BOOLEAN DEFAULT FALSE, -- market suspended flag
    market_available_1x2 BOOLEAN DEFAULT TRUE,
    market_available_ou BOOLEAN DEFAULT TRUE,
    raw_json        JSONB               -- full scraper output for debugging
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('odds_timeseries', 'ts');

-- Dedupe / upsert target: one row per (bookmaker, match, market snapshot, ts).
-- Ingestor upserts on this; replays are idempotent.
CREATE UNIQUE INDEX uniq_odds_snapshot
    ON odds_timeseries (bookmaker, match_id, ts, row_fingerprint);

-- Query helpers for signal providers / decision engine.
CREATE INDEX idx_odds_canonical ON odds_timeseries (canonical_match_id, ts DESC);
CREATE INDEX idx_odds_provenance ON odds_timeseries (scraper_name, scraper_version, observed_at);


-- Match outcomes (for settlement and ML labels)
CREATE TABLE match_results (
    match_id        TEXT PRIMARY KEY,
    sport           TEXT NOT NULL,
    team1           TEXT NOT NULL,
    team2           TEXT NOT NULL,
    tournament      TEXT NOT NULL,
    kickoff         TIMESTAMPTZ,
    final_score_home INT,
    final_score_away INT,
    result_1x2      TEXT,               -- "1", "X", "2"
    total_goals     INT,
    result_ou       TEXT,               -- "over", "under"
    -- Extended results
    corners_total   INT,
    cards_total     INT,
    settled_at      TIMESTAMPTZ
);

-- Bet settlement (analytics MIRROR of ledger, for ML labels / backtesting).
-- NOTE: the SQLite ledger (§3 "Ledger") remains the single source of truth
-- for money. This table is a denormalized copy for queryable history only.
CREATE TABLE bet_settlements (
    bet_id          TEXT PRIMARY KEY,    -- references ledger bets.id
    match_id        TEXT NOT NULL,
    market          TEXT NOT NULL,
    outcome         TEXT NOT NULL,
    settled_at      TIMESTAMPTZ,
    pnl_eur         FLOAT,
    won             BOOLEAN
);
```

**Ingestion pipeline** (`ingestion/`):
```python
# ingestion/ingestor.py
class HistoricalIngestor:
    """Reads CSV files from match_database/, deduplicates, writes to DB."""
    
    def ingest_today(self, db_root: str) -> int:
        """Scan all CSVs for today, upsert into odds_timeseries."""
        ...
    
    def backfill(self, start_date: str, end_date: str) -> int:
        """Bulk import historical CSVs."""
        ...
```

**Rules**:
- Ingestor runs as a sidecar process (or cron job), not inside scraper containers.
- CSVs are the source of truth for real-time; DB is the source of truth for history.
- DB rows MUST include provenance (which scraper, version, poll id and observed timestamp) to allow replay and debugging.
- `raw_json` column preserves full scraper output for future reprocessing; parsers should produce a stable `row_fingerprint`.
- Do NOT rely solely on a naive hash for cross-bookmaker identity. Use the dedicated `matching` subsystem (see §4.1.3) to obtain canonical_match_id for cross-bookmaker joins.
