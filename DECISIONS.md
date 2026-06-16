# Architecture decisions. Agent fills with problem desc, solution selected and reason why this solution is chosen. Birefly.

Key Design Decisions

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