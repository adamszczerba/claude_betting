# Architecture decisions. Agent fills with problem desc, solution selected and reason why this solution is chosen. Birefly.

Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Signal transport | Redis pub/sub for signals ("signals:all" channel) | Typed Signal objects between providers and decision engine |
| Odds transport | Redis Streams per bookmaker ("odds:<bk>", MAXLEN 10000) | Real-time odds distribution from scrapers to consumers; ~3h window covers in-progress matches |
| Scraper output | Dual-write: CSV (audit trail) + Redis Stream (real-time) | CSV unchanged for backward compatibility; stream publish is best-effort (failure logged, never breaks scraper) |
| Stream consumer pattern | XREAD (no consumer groups) | Only one instance each of orchestrator and dashboard; consumer groups add unnecessary complexity |
| Stream data format | One JSON message per match per poll cycle (snapshot) | Simple, idempotent, matches current CSV row format |
| Shared publisher | `scrapers/shared/stream_publisher.py` | All 8 scrapers import from one place; connection handling and error handling centralized |
| Consumer fallback | Redis first, CSV scan as fallback | Graceful degradation if Redis is unavailable |
| Historical DB | PostgreSQL + TimescaleDB | Mature, excellent timeseries support, SQL queries for ML |
| Signal providers as processes | Yes | Fault isolation, independent scaling, easy to add new types |
| ML model per sport | Yes | Different sports have different dynamics |
| Human approval gate | Optional, off by default | Speed for live betting; enable for high-stakes |
| CSV + DB dual storage | Yes | CSV for real-time (low latency), DB for history (queryable) |
| Executor per bookmaker | Yes | Each bookmaker has unique API/browser requirements |
| Signal fusion weighted average | Start simple | Can evolve to ML-based fusion later |
| Prematch separate directory | Yes | Different cadence, different risk profile, avoids confusion |