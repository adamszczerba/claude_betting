# Canonical market grammar

**Difficulty:** hard | **Area:** market identity | **Status:** open

## Problem
The schema hardcodes five fields: `odd_1`, `odd_X`, `odd_2`, `odd_over`, `odd_under` (plus a
`total_line` the consensus ignores). Real books quote a market space that does not fit this:
Asian handicaps including quarter lines (-0.25, -0.75) that split the stake across two
outcomes, European handicaps, draw-no-bet, totals at many lines, team totals, both-teams-to-
score, correct score, period and minute-band markets, corners, cards.

Worse, the same economic bet appears under different names: 1X2 and AH 0.0 are not the same
market, but DNB and AH 0.0 are. Over 2.5 and Under 2.5 are one market, not two.

## Why it matters
Without a canonical grammar, every market beyond the hardcoded five needs bespoke handling
per bookmaker, and equivalent markets cannot be pooled into one consensus. It caps how much
of the book's actual offering the engine can ever price — which is half the stated goal.

## Fix
Represent a market as a structured key rather than a column name:

    (market_type, period, line, side, participant)

with an equivalence layer that maps book-specific naming onto it, and explicit rules for
genuinely equivalent formulations (DNB ≡ AH 0.0). Quarter lines need first-class support —
a -0.25 handicap is half a stake on 0.0 and half on -0.5 and must devig as such.

## Acceptance criteria
- [ ] Structured market key replacing hardcoded columns
- [ ] Per-bookmaker mapping layer from raw market names to canonical keys
- [ ] Quarter/split handicaps modelled correctly, including in devigging
- [ ] Equivalence rules explicit, documented and tested
- [ ] Migration path for the existing CSV corpus (do not lose stored history)

## Related
`medium-total-line-in-market-identity` is the narrow first slice of this.
`hard-alternate-line-interpolation` builds on it.
