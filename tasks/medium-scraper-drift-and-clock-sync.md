# Scraper drift, anti-bot resilience and clock sync

**Difficulty:** medium | **Area:** ops | **Status:** open

## Problem
Scrapers silently break. A site redesign, an anti-bot change or a VPN exit going bad
produces missing data, or worse, subtly wrong data — and the consensus absorbs it without
complaint. Separately, containers with drifting clocks make every timestamp-based mechanism
in this task list unreliable.

## Why it matters
Silent degradation is the worst failure mode: the system keeps emitting signals, they are
just wrong now. Clock drift in particular breaks staleness TTLs, latency measurement and
CLV timing simultaneously, and does it invisibly.

## Fix
- **Per-scraper health**: rows/minute, parse failure rate, price-change rate, time since last
  successful scrape. Alert on deviation from each scraper's own baseline, not a global one.
- **Sanity gates**: overround outside a plausible band, odds outside a plausible range, a
  price series that has not moved while every other book has — all indicate a broken parse.
- **Clock sync**: NTP in every container; assert bounded skew at startup; timestamp at the
  scrape boundary, not at write time.
- **VPN health**: per-exit success rate; rotate automatically on degradation.

## Acceptance criteria
- [ ] Per-scraper health metrics exported and alerting
- [ ] Sanity gates reject implausible rows before they reach the consensus
- [ ] Clock skew bounded and asserted
- [ ] Health visible on the dashboard
