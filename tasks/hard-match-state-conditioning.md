# Condition the fair price on match state

**Difficulty:** hard | **Area:** live | **Status:** open

## Problem
The consensus aggregates prices with no knowledge of the game. It cannot tell the difference
between two books genuinely disagreeing and two books looking at different scorelines
because one has not received the goal yet.

## Why it matters
In-play, the dominant source of cross-book disagreement is not opinion — it is **information
arrival timing**. Books receive score updates from different suppliers at different times;
the spread between the fastest and slowest can be several seconds, and that is the whole
ballgame. Averaging across that spread produces a price for a match state that no longer
exists, and the error is largest precisely when the opportunity looks biggest.

Staleness TTLs (`hard-price-staleness-ttl`) catch the case where a book has not *updated*.
They do not catch the case where a book has updated promptly using stale information.

## Approach
- **Independent state feed**: score, red cards, match clock, and ideally a goal/VAR event
  stream. Candidates: Flashscore/LiveScore scraping (cheap), or a data provider — Sportradar,
  Genius Sports, Stats Perform (Stats Perform quote ~0.5s latency on their 2026 World Cup
  rights; commercial feeds generally target sub-second because that is the threshold at which
  in-play books get picked off).
- **State versioning**: tag every price with the match state it was observed under. Only
  aggregate prices sharing a state version.
- **Event freeze**: on a detected state change, discard the consensus and rebuild from prices
  observed after the change. Do not blend across the boundary.
- **Cross-check**: a book still pricing the old state after the state feed has moved is
  lagging, not disagreeing — exclude it and record the lag. This measurement is itself
  valuable: it tells you which books to never trust in the first seconds after an event.

## Acceptance criteria
- [ ] Independent match-state feed integrated, with its own latency measured
- [ ] Prices tagged with observed state version; aggregation only within a version
- [ ] Consensus freezes and rebuilds across state changes rather than blending
- [ ] Per-book post-event lag tracked

## Related
`hard-price-staleness-ttl`, `medium-suspension-state-machine`,
`medium-per-bookie-latency-measurement` — four faces of the same underlying problem;
design the timestamp/state plumbing once.
