# Evaluate a betting broker as a data + execution source

**Difficulty:** medium | **Area:** data sources | **Status:** open

## Problem
The tier-0/1 Asian books that matter most for a live consensus — Nova88/IBCBet, Singbet
(Crown), Bet ISN, 3ET, 188Bet, Sharpbet — are not practically scrapeable individually. They
are reached through brokers.

## The trade
A broker gives you, behind **one** integration:
- prices from Pinnacle (as PS3838), SBOBet, Betfair, Matchbook, Nova88, Singbet and others
- **executable liquidity with real limits** — not a scraped number, a price you can hit
- no VPN-per-scraper maintenance burden

Against: cost, account/KYC requirements, and a dependency on a single intermediary.

## Candidates
- AsianConnect88 — asianconnect88.com
- VOdds — vodds.com
- Sportmarket Pro — sportmarket.com
- BetInAsia — betinasia.com

## Why it matters here
Worth taking seriously even though the playable book is hypothetical: the value is on the
**benchmark** side. Six sharp books behind one API, with real limits attached, replaces a
large share of the scraper fleet and its maintenance burden — and limits are data the
scrapers cannot get at all (`medium-limits-and-depth-capture`).

## Acceptance criteria
- [ ] API docs and actual coverage reviewed for at least two brokers
- [ ] Cost compared against scraper maintenance and VPN overhead
- [ ] Latency and rate limits established
- [ ] Recommendation in `DECISIONS.md`
