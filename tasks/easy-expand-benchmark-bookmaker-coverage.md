# Add more benchmark bookmakers

**Difficulty:** easy (per scraper) | **Area:** data sources | **Status:** open

## Problem
Eight books are scraped. More *independent* opinions improve the consensus; more copies of
the same opinion do not (see `hard-correlated-feed-detection`).

## Candidates, ranked by marginal information

**Tier 0/1 — genuine originators, highest value:**
- Matchbook — matchbook.com, developers.matchbook.com (exchange, API)
- Smarkets — smarkets.com, developer.smarkets.com (exchange, API)
- Nova88 / IBCBet, Singbet (Crown), Bet ISN, 3ET — normally only reachable via a broker,
  see `medium-evaluate-broker-api`

**Benchmark / follower books — useful as lag detectors and coverage fill:**
- Superbet, Fortuna, Betclic, Etoto, Fuksiarz (deep markets, fast in-play, used purely as
  benchmark — none of these are playable targets)
- Bwin, Unibet, Betsson, 1xBet (broad European coverage)

## Note
These are all **benchmark inputs**. The playable book is hypothetical and fee-free — see
`easy-document-playable-book-model`.

## Acceptance criteria
- [ ] Each new scraper follows the `scrapers/v2_*` contract (CSV + Redis stream)
- [ ] Weight assigned in `BOOKMAKER_WEIGHTS` before it goes live
- [ ] Correlation against existing books checked before granting weight > 0.2
