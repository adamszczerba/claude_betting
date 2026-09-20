# Build a coverage matrix: who offers what, at what size, at what margin

**Difficulty:** medium | **Area:** availability | **Status:** open

## Problem
The system knows prices. It does not know **availability** — which books offer a given
(event, market, line) at all, at what limit, with what margin. Availability is half the
stated goal of the fair-price engine and is currently absent.

## Why it matters
- A price backed by eight books is a different-quality estimate than one backed by three,
  and the consensus should say which it is.
- Coverage gaps are where the exploitable prices live: markets that few books price are
  markets few books price *well*.
- You cannot reason about whether a market is worth scraping without knowing who covers it.

## Fix
Materialise a coverage matrix per event: rows = (market_type, line), columns = bookmakers,
cells = {offered, state, margin, limit, last_update}. Persist it and expose it on the
dashboard. It is a reporting artefact and a runtime input both.

## Acceptance criteria
- [ ] Coverage matrix computed per event and persisted
- [ ] Per-book, per-market margin tracked over time
- [ ] Dashboard view showing coverage gaps and margin by market
- [ ] Consensus can read `n_books` per market from it

## Related
`medium-limits-and-depth-capture`, `medium-availability-as-signal`
