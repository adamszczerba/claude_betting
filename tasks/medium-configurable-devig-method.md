# Devig method is hardcoded to `normalize`

**Difficulty:** medium | **Area:** devigging | **Status:** open

## Problem
`analytics/overround.py` implements `normalize`, `shin` and `power`. `_fair_from_group()`
calls `remove_overround(raw_odds, method="normalize")` with the method fixed
(`analytics/consensus.py:117`). Two of three implementations are unreachable.

## Why it matters
On balanced lines all three methods agree to within a rounding error. On lopsided in-play
prices — a 1.15 favourite after the second goal, exactly the situation being targeted —
they diverge by several percentage points, which is larger than the edge being sought.
`normalize` (multiplicative) is the crudest of the three: it spreads margin proportionally
to implied probability, which is the opposite of how books actually load vig.

## Fix
- Thread `method` through `weighted_consensus()` as a parameter, config-driven.
- Allow per-market-type override (a two-way total and a three-way 1X2 do not need the same
  treatment).
- Run all three over stored history and pick on calibration, not on preference.

## Acceptance criteria
- [ ] Method configurable at call site and via config
- [ ] Per-market-type override supported
- [ ] Comparison over stored history recorded in `DECISIONS.md`
- [ ] Default changed from `normalize` if the evidence says so

## Depends on
`medium-backtest-harness`, `medium-calibration-curves`
