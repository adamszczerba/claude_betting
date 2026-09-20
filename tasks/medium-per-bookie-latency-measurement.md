# Measure end-to-end latency per bookmaker

**Difficulty:** medium | **Area:** live | **Status:** open

## Problem
Nothing in the system knows how long it takes for a price change at a bookmaker to become
visible to the consensus. Scrapers poll at different intervals through different VPN exits,
so the answer differs per book by an unknown and probably large amount.

## Why it matters
In-play operators target sub-one-second latency specifically because slower books get picked
off. The same logic applies in reverse: **if your consensus is slower than the book you are
comparing against, every edge you find is you being the slow one.** Without measuring, there
is no way to tell a real edge from a latency artefact — and the artefacts will look like the
best opportunities in the system.

## Fix
- Stamp every price with scrape time and publish time.
- Estimate propagation delay per book: cross-correlate each book's price series against the
  anchor's and find the lag that maximises correlation.
- Export per-book latency as a metric; alert when a book degrades.
- Feed the measurement into staleness TTLs and into weighting — a structurally slow book
  should not carry the same weight in live as pre-match.

## Acceptance criteria
- [ ] Scrape and publish timestamps on every price
- [ ] Per-book lag estimated from stored history and re-estimated periodically
- [ ] Latency visible on the dashboard, alerting on degradation

## Related
`hard-price-staleness-ttl`, `hard-empirical-bookmaker-weights`
