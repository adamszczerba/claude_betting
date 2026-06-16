# Task scheduled for agent to be done.

1. explore what is the best way to store data from bookmakers to be memory efficient, good for ml training and available for decision making in real time.
2. how scraper communicate with signal provider
3. how decision maker communicate with signal provider
4. how many processes that system needs
## Option A — Redis Streams for Scraper → Signal Provider IPC (Next Sprint)

**Problem:** All scrapers write to CSV files; orchestrator and signal providers read those CSVs. This creates I/O bottleneck (~550 file open/close ops per 2s cycle) and adds 2-4s latency from scraper write to signal provider read.

**Solution:** Scrapers publish odds snapshots directly to Redis Streams. Orchestrator and signal providers subscribe. CSVs remain as append-only audit trail only.

**Architecture:**
```
Scraper ──publish──► Redis Stream ("odds") ──subscribe──► Orchestrator
                                                    ──subscribe──► Comparator Provider
                                                    ──subscribe──► ML Provider (future)
                                                    ──subscribe──► Dashboard
```

**Steps:**
1. Add `redis` service to `docker-compose.yml` (official `redis:7-alpine` image)
2. Update `config.yaml` signal bus backend from `memory` to `redis` (URL: `redis://redis:6379`)
3. Add `redis` Python package to `requirements.txt`
4. Create `scraper_publisher.py` module: after each poll cycle, serialize odds snapshot to JSON, publish to Redis Stream (`XADD odds * data <json>`)
5. Update orchestrator: replace `scan_today()` with Redis Stream consumer (`XREAD` or consumer group)
6. Update `ComparatorSignalProvider`: consume from Redis instead of scanning CSVs
7. Keep CSV writes as-is (audit trail, no change to scraper write path)

**Expected improvement:** End-to-end latency drops from 2-4s to <50ms. Eliminates ~200 file reads per cycle.

**Prerequisites:** Redis must be deployed. Signal Bus (`signals/bus.py`) already supports Redis backend — just needs Redis running and config updated.
