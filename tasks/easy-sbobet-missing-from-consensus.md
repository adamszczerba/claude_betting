# SBOBet is scraped but never reaches the consensus

**Difficulty:** easy | **Area:** aggregation | **Status:** open

## Problem
`analytics/consensus.py:107` iterates `BOOKMAKER_WEIGHTS.items()` and looks up each key in
the group. `sbobet` is absent from `BOOKMAKER_WEIGHTS` (`analytics/consensus.py:37-45`),
so `scrapers/v2_sbobet` output is silently discarded from every fair price.

## Why it matters
SBOBet is a tier-0/1 Asian market maker — high in-play limits on AH/totals, one of the
few books that originates rather than copies. It is arguably the second most valuable
input after Pinnacle. Dropping it costs accuracy for zero reason.

## Fix
Add `"sbobet": 0.80` (starting value, revisit under `hard-empirical-bookmaker-weights`).
Then invert the loop so it iterates over bookmakers **present in the group** and warns on
any bookmaker with data but no weight — this class of bug should be loud, not silent.

## Acceptance criteria
- [ ] `sbobet` contributes to `weighted_consensus()` output
- [ ] A bookmaker present in group data but missing from `BOOKMAKER_WEIGHTS` logs a warning
- [ ] Test asserts every scraper directory under `scrapers/v2_*` has a weight entry
