# Account for the Betfair in-play bet delay

**Difficulty:** easy | **Area:** live | **Status:** open

## Problem
Betfair applies an in-play bet delay of roughly 1-12 seconds depending on sport and market.
The value is published per market in the `betDelay` field of `listMarketBook`. The exchange
price observed at time t is therefore not executable at t — and, more importantly for a
pricing engine, a price that has already absorbed information you have not yet seen.

## Why it matters
Betfair Exchange is one of the highest-weighted inputs. Treating its price as contemporaneous
with a scraped bookmaker price introduces a systematic, sport-dependent skew into the
consensus at exactly the moments that matter (right after a goal or a point).

## Fix
Capture `betDelay` alongside the price in `scrapers/v2_betfair_exchange`, store it, and make
it available to the consensus so exchange prices can be time-shifted or downweighted during
the delay window.

## Acceptance criteria
- [ ] `betDelay` captured per market and persisted with the price row
- [ ] Consensus can apply the shift or exclude exchange prices inside the window
- [ ] Documented in `API_CONTRACTS.md`

## Reference
https://support.developer.betfair.com/hc/en-us/articles/360002825652
