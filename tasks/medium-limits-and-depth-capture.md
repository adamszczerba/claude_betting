# Capture limits and exchange depth

**Difficulty:** medium | **Area:** availability | **Status:** open

## Problem
The system stores a price with no notion of how much of it exists. A price with a 5-unit max
stake and a price with a 5000-unit max stake are stored identically.

## Why it matters
- **Size validates the price.** A sharp book's willingness to take real money at a number is
  what makes that number informative. Pinnacle at max limits is a far stronger signal than
  Pinnacle at a reduced limit, and the current system cannot distinguish them.
- **Exchange depth is a better weight than a constant.** Betfair/Smarkets/Matchbook weights
  should track matched volume and available-to-back size, not a hardcoded 0.85 — exchange
  prices on thin markets are close to meaningless.
- **Edge size is bounded by available size.** An edge you can stake 5 units into is not worth
  the operational risk of acting on.

## Fix
Capture available-to-back/lay size and depth from exchange APIs, and max stake from books
that expose it. Feed into both weighting and signal filtering.

## Acceptance criteria
- [ ] Exchange depth captured (at least best price + size, ideally top 3 levels)
- [ ] Max stake captured where exposed
- [ ] Exchange weight scales with depth rather than being constant
- [ ] Signals carry a maximum actionable size
