# Guard against devigging an incomplete market

**Difficulty:** easy | **Area:** devigging | **Status:** open

## Problem
`remove_overround()` normalises whatever list it is handed. If a scraper returns 1 and 2
but not X (suspended, or a parse miss), and the caller passes the partial list, the margin
is computed against an incomplete outcome set and the "fair" odds come out badly wrong —
too long, in the direction that looks like value.

`_fair_from_group()` currently skips groups where any column is `None`, which happens to be
correct — but the invariant lives nowhere and nothing enforces it for future callers or
for new market types.

## Why it matters
A partial-market devig produces exactly the failure mode that costs money: a fabricated
edge that looks plausible.

## Fix
Make the market completeness requirement explicit in `analytics/overround.py`: accept a
declared market type (`1X2`, `OU`, `AH`, `BTTS`), assert the expected number of mutually
exclusive and exhaustive outcomes, raise otherwise.

## Acceptance criteria
- [ ] `remove_overround()` takes a market type and validates outcome count
- [ ] Partial input raises rather than returning plausible-looking numbers
- [ ] Test covers the 2-of-3 case for 1X2
