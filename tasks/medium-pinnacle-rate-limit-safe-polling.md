# Pinnacle polling within rate limits

**Difficulty:** medium | **Area:** data sources | **Status:** open

## Problem
Pinnacle's odds endpoints are aggressively rate limited — as low as one request per two
minutes per endpoint per sport on some tiers. Naive polling gets throttled or blocked, and
throttled anchor data is worse than no anchor data because it is silently stale.

## Why it matters
Pinnacle is the highest-weighted input. Two-minute-old anchor prices in a live market are
not a benchmark, they are a liability — and without `hard-price-staleness-ttl` the system
cannot even tell.

## Options
1. **Delta polling** — official API `/odds?since=<last>` returns only changes, drastically
   cutting request volume for the same coverage. Cheapest correct fix.
2. **Streaming reseller** — several vendors expose Pinnacle over SSE/WebSocket with
   ~15-40ms price-change-to-client latency (pinnodds.com, pinnapi.com). Costs money, removes
   the problem entirely.
3. **Scrape** — current approach; fragile and no better on latency.

## Acceptance criteria
- [ ] Measured actual latency and request budget of the current approach
- [ ] `since`-based delta polling implemented, or a streaming source adopted
- [ ] Throttling detected and surfaced rather than silently degrading
- [ ] Choice recorded in `DECISIONS.md`

## Reference
https://github.com/pinnacleapi/pinnacleapi-documentation
