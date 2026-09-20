# Weighted median degenerates on thin markets

**Difficulty:** easy | **Area:** aggregation | **Status:** open

## Problem
`_weighted_median()` returns the first value whose cumulative weight crosses half the
total. With two books at weights 1.00 (pinnacle) and 0.35 (sts), the anchor's value always
wins — the result is labelled "consensus" but is a single book. With three mid-weight
books the result is equally arbitrary.

## Why it matters
A consensus of one is not a consensus, and the system currently cannot tell the difference.
Confidence downstream should scale with how many independent opinions actually contributed.

## Fix
- Require N >= 3 contributing books (configurable) to emit a consensus price.
- Below threshold, emit anchor-only price explicitly flagged as `source="anchor"` with a
  widened uncertainty band, or emit nothing.
- Attach `n_books` and `contributing_books` to every `FairOdds`.

## Acceptance criteria
- [ ] `FairOdds` carries `n_books` and the list of contributing bookmakers
- [ ] Below-threshold groups are flagged, not silently emitted as consensus
- [ ] Threshold configurable; default documented in `DECISIONS.md`

## Related
`medium-fair-price-uncertainty-interval`, `hard-empirical-bookmaker-weights`
