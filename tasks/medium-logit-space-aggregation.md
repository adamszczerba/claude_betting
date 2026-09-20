# Replace the weighted median with a smoother estimator

**Difficulty:** medium | **Area:** aggregation | **Status:** open

## Problem
The weighted median is robust to a single bad quote, but it is a step function: as prices
tick, the output jumps discontinuously from one book's value to another's. It also gives no
sum-to-one guarantee and discards all information from non-median books.

## Why it matters
In live betting the fair price is sampled continuously and compared against a moving
playable price. A jumpy estimator produces signals that appear and vanish on the estimator's
own discontinuities rather than on real market movement — false positives that are expensive
to discover in production.

## Fix
Evaluate against the current median, over stored history:
- **Weighted mean in logit space** — `logit(p) = log(p/(1-p))`, average, invert. Natural
  scale for probabilities, well-behaved near 0 and 1, smooth.
- **Trimmed weighted mean** — drop the extreme quotes, average the rest. Keeps most of the
  median's outlier robustness without the discontinuity.

Renormalise after either (see `easy-consensus-probabilities-dont-sum-to-one`).

## Acceptance criteria
- [ ] At least two estimators implemented behind a common interface
- [ ] Compared on calibration and on tick-to-tick stability (variance of the fair price
      series when underlying inputs are unchanged)
- [ ] Choice recorded in `DECISIONS.md`
