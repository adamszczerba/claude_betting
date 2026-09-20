# Consensus probabilities are not renormalised

**Difficulty:** easy | **Area:** aggregation | **Status:** open

## Problem
`_fair_from_group()` takes a weighted median **per outcome independently**
(`analytics/consensus.py:126-137`). The median of per-outcome distributions does not
preserve the sum-to-one constraint, so the emitted `FairOdds` is not a coherent
probability distribution. `odd_1/odd_X/odd_2` can imply 0.97 or 1.04 total.

## Why it matters
Every downstream EV calculation in `analytics/value.py` assumes the fair price is a real
probability. A 3% systematic sum error is the same order of magnitude as the edges being
hunted — it manufactures or hides signals outright.

## Fix
After the per-outcome median, renormalise: `p_i /= sum(p)`. Log the pre-normalisation sum
as a diagnostic — a consistently large deviation means the aggregation method itself is
wrong (see `medium-logit-space-aggregation`).

## Acceptance criteria
- [ ] Sum of fair probabilities per market == 1.0 within 1e-9
- [ ] Pre-normalisation sum exposed as a field for monitoring
- [ ] Test with deliberately disagreeing books asserts the invariant holds
