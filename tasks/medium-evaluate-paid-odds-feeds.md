# Evaluate paid odds feeds against scraper maintenance

**Difficulty:** medium | **Area:** data sources | **Status:** open

## Problem
Eight scrapers, eight VPN configs, and every one of them a standing maintenance liability.
Some of that is buyable.

## Candidates
| Provider | URL | Note |
|---|---|---|
| The Odds API | the-odds-api.com | Free tier, cheapest entry, polling-based |
| BetsAPI | betsapi.com | Cheap, strong in-play and Asian/bet365 coverage |
| OpticOdds | opticodds.com | Sub-second streaming, sharp-book focused |
| OddsJam | oddsjam.com/odds-api | 100+ books, streaming, ~$500-1000+/mo, gated pricing |
| OddsMatrix | oddsmatrix.com | Operator-grade pre-live + live |
| SportsGameOdds | sportsgameodds.com | Publishes pricing |

Directory of 30+: sportsapis.dev

## What to establish
- **Latency** — a polling feed is useless for live regardless of coverage. Measure, do not
  trust the marketing number.
- **Coverage** — which of the books that actually matter are included, at what market depth.
  100 books is worth less than the right 6.
- **Whether limits/depth come with it** — the thing scrapers cannot provide.
- **Cost vs scraper maintenance**, honestly accounted.

## Acceptance criteria
- [ ] Two or three trialled on real live matches, latency measured end to end
- [ ] Coverage checked against the books actually weighted in the consensus
- [ ] Build-vs-buy recommendation per bookmaker tier in `DECISIONS.md`

## Related
`medium-evaluate-broker-api` — overlapping, decide together.
