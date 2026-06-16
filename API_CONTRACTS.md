# API between modules, change requires direct approval. Agent trust in that file until something clearly fails.

## Data Flow

```
Scraper ──CSV write──► match_database/<bookmaker>/<date>/*.csv   (audit trail)
   │
   └──XADD──► Redis Stream ("odds:<bookmaker>")
                     ├──XREAD──► Comparator Signal Provider
                     ├──XREAD──► Orchestrator
                     └──XREAD──► Dashboard

Signal Provider ──publish──► Redis Pub/Sub ("signals:all") ──subscribe──► Decision Engine
                                                                 ──subscribe──► Ledger
```

## Redis Streams (Raw Odds)

- **Stream key format:** `odds:<bookmaker>` (e.g., `odds:betfair`, `odds:bet365`, `odds:coincasino`)
- **Message format:** JSON object per match per poll cycle:
  ```json
  {
    "timestamp": "2026-04-06T14:06:05.107",
    "match_time": "67:23",
    "match_status": "",
    "home_score": "1",
    "away_score": "0",
    "odd_1": "2.10",
    "odd_X": "3.40",
    "odd_2": "4.50",
    "total_line": "2.5",
    "odd_over": "1.85",
    "odd_under": "2.00",
    "team1": "Liverpool",
    "team2": "Arsenal",
    "tournament": "Premier League",
    "bookmaker": "betfair"
  }
  ```
- **Trimming:** MAXLEN ~10000 per stream (~3h of data)
- **Transport:** XREAD (no consumer groups)
- **Dual-write:** CSV write is unchanged; stream publish is best-effort (failure logged, not fatal)

## Redis Pub/Sub (Signal Objects)

- **Channel:** `signals:all`
- **Message format:** Signal Bus `Signal.to_json()` (see `signals/bus.py`)
- **Publishers:** Comparator, ML, Web Search, Live Data signal providers
- **Subscribers:** Decision Engine, Dashboard

## CSV Storage (Unchanged)

- Path: `match_database/<bookmaker>/<YYYY-MM-DD>/<team1>_vs_<team2>_<tournament>_<tag>_<date>.csv`
- Schema: standardized columns per AGENTS.md
- Write pattern: append-only, one file per match, header written once