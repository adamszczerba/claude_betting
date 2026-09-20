# Over/Under is aggregated without the line

**Difficulty:** medium | **Area:** market identity | **Status:** open

## Problem
`_OVERUNDER = ["odd_over", "odd_under"]` (`analytics/consensus.py:50`) carries no line.
`total_line` exists in the CSV schema (`dashboard/data_service.py:30`) but the consensus
never reads it. So Over 2.5 at one book is weighted-medianed with Over 3.5 at another.

## Why it matters
These are different markets with different true probabilities — Over 2.5 vs Over 3.5 in a
typical football match differs by roughly 20 percentage points. The resulting "fair" total
price is meaningless, and any edge it reports against a playable book is noise.

## Fix
Make the line part of market identity throughout: group by `(market_type, total_line)`,
aggregate within a line only, and emit a fair price per line rather than one `odd_over`.
`FairOdds` needs to become a collection of priced markets, not five fixed fields.

## Acceptance criteria
- [ ] Consensus keyed by `(market_type, line)`
- [ ] Books quoting different lines never aggregated together
- [ ] `FairOdds` returns per-line fair prices; `analytics/value.py` and
      `signals/comparator/provider.py` updated for the new shape
- [ ] Test with two books on different lines asserts they stay separate

## Related
`hard-canonical-market-grammar` generalises this; `hard-alternate-line-interpolation`
handles comparing across lines once they are separated.
