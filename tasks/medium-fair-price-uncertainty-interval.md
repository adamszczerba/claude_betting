# Emit an uncertainty band, not just a point estimate

**Difficulty:** medium | **Area:** aggregation | **Status:** open

## Problem
`weighted_consensus()` returns a single number per outcome. The spread of opinion behind
that number is discarded.

## Why it matters
Disagreement across books is itself signal. Tight agreement between five independent books
means the fair price is well determined and a deviation is real. Wide disagreement usually
means something is broken — a stale feed, a mismatched event, a different line, one book
that has seen a goal the others have not. Those are exactly the cases that produce the
largest apparent edges, and they are all false.

Filtering on "edge > X% **and** spread < Y%" removes most of the failure modes in this
task list as a side effect.

## Fix
Return a dispersion measure alongside each fair price: weighted standard deviation of the
contributing devigged probabilities, or an inter-book range. Expose it through
`signals/comparator/provider.py` so the decision layer can gate on it.

## Acceptance criteria
- [ ] `FairOdds` carries a dispersion measure per outcome
- [ ] Dispersion propagated to signal consumers
- [ ] Decision layer can gate on maximum dispersion
- [ ] Distribution of dispersion values inspected over stored history to pick a sane default

## Related
`easy-thin-market-degeneracy-guard`
