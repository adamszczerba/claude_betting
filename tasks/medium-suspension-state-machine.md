# Distinguish suspended / not-offered / stale / live

**Difficulty:** medium | **Area:** live | **Status:** open

## Problem
The pipeline has one representation of a price: a number, or `None`. There is no way to
express *why* a number is missing or how much to trust one that is present. Scrapers
typically keep returning the last-seen value when a market is suspended.

## Why it matters
Books suspend instantly on a goal, red card or VAR check — that is the single most important
risk control an in-play book has. A frozen pre-goal price treated as a live quote produces
an enormous phantom edge at precisely the moment the true price has moved furthest. This is
the highest-severity live failure mode after staleness.

## Fix
Introduce an explicit per-price state: `LIVE`, `SUSPENDED`, `NOT_OFFERED`, `STALE`.
- Scrapers detect and emit suspension where the site exposes it (most do, visually).
- Where it is not exposed, infer: an unchanged price while other books move is suspicious;
  an unchanged price across a detected score change is suspended.
- Only `LIVE` prices enter the consensus. The others are recorded — they are valuable both
  for diagnosing feeds and as signal in their own right (see `medium-availability-as-signal`).

## Acceptance criteria
- [ ] Price state in the CSV schema and Redis stream payload
- [ ] Each `scrapers/v2_*` emits state where detectable
- [ ] Consensus consumes only `LIVE`
- [ ] `API_CONTRACTS.md` updated

## Depends on
`hard-price-staleness-ttl` (shares the timestamp plumbing)
