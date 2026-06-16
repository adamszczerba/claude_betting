# Task scheduled for agent to be done.

1. explore what is the best way to store data from bookmakers to be memory efficient, good for ml training and available for decision making in real time.
2. how scraper communicate with signal provider
3. how decision maker communicate with signal provider
4. how many processes that system needs

---

## Redis Streams — Scraper → Signal Provider IPC

### Problem

All scrapers write to CSV files; orchestrator and signal providers read those CSVs.
This creates I/O bottleneck (~550 file open/close ops per 2s cycle) and adds 2-4s
latency from scraper write to signal provider read.

### Solution

Scrapers publish odds snapshots to Redis Streams (dual-write: CSV + stream). Current
consumers (CSV readers) keep working unchanged. New consumers (signal providers,
dashboard) subscribe directly to the stream for real-time data.

### Architecture

```
Scraper ──CSV write──► match_database/<bookmaker>/<date>/*.csv   (audit trail, unchanged)
   │
   └──XADD──► Redis Stream ("odds:<bookmaker>")
                     │
                     ├──XREAD──► Comparator Signal Provider (replaces scan_today())
                     ├──XREAD──► Dashboard (live odds, <50ms latency)
                     └──XREAD──► ML Signal Provider (future)
```

### Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Stream key | `odds:<bookmaker>` (per-bookmaker) | Consumer can subscribe to specific bookmakers; avoids parsing a single huge stream |
| Stream format | One JSON message per match per poll cycle (snapshot) | Simple, matches current behavior, idempotent — consumers just take latest per match |
| Transport | XREAD with manual position tracking | No consumer groups needed — only one instance each of orchestrator and dashboard. Simpler, less overhead |
| Redis trimming | MAXLEN ~10000 per stream (approx 3h at 2s interval × ~50 matches × 7 bookmakers) | Bounds memory; old data irrelevant after match ends |
| Scraper changes | Dual-write: keep existing CSV write, add stream publish | Zero risk to current pipeline; stream consumers added incrementally |
| Shared module | `scrapers/shared/stream_publisher.py` | All scrapers import from one place — connection handling, serialization, error handling in one spot |
| Redis client | `redis-py` (already planned in requirements) | Standard, well-maintained, supports streams natively |
| Signal Bus pub/sub | **Keeps existing** (`signals/bus.py` Redis pub/sub for signal objects) | Stream is for **raw odds**; pub/sub is for **signals** — different data, different consumers. Don't merge. |

### Steps

1. Add `redis` service to `docker-compose.yml` (`redis:7-alpine`, port 6379)
2. Add `redis` Python package to `requirements.txt`
3. Create `scrapers/shared/stream_publisher.py`:
   - Connects to Redis (host from env var, default `localhost`)
   - Function `publish_snapshot(bookmaker: str, rows: list[dict])` — serializes each row as JSON, XADD to `odds:<bookmaker>` with MAXLEN ~10000
   - Silently logs errors (stream publish failure must not break CSV write path)
4. Update each scraper (`v2_betfair`, `v2_bet365`, `v2_coincasino`, `v2_betfair_exchange`, `v2_pinnacle`, `v2_lvbet`, `v2_sts`, `v2_sbobet`):
   - After CSV write, call `publish_snapshot(bookmaker_tag, rows)`
   - Add `REDIS_HOST` env var (in docker-compose, set to `redis`)
5. Update `ComparatorSignalProvider` (`signals/comparator/provider.py`):
   - Add optional Redis stream consumer mode: XREAD from all `odds:*` streams, build grouped table from latest messages
   - Keep CSV scan as fallback (feature flag or auto-detect Redis availability)
6. Update orchestrator poll loop: consume from Redis stream instead of (or alongside) CSV scan
7. Update dashboard: add Redis stream subscription for live odds display
8. Update `API_CONTRACTS.md` to reflect final architecture

### Expected Improvement

End-to-end latency drops from 2-4s to <50ms. Eliminates ~200 file reads per cycle.
CSV files remain as audit trail — no data loss if Redis is down.

### Prerequisites

- `signals/bus.py` already has Redis backend — but this is for **signal pub/sub**, not odds streams. Separate concern.
- Each scraper container needs network access to Redis service (add to docker-compose network).
- No changes to CSV schema or file naming.
