# Interpolate between lines

**Difficulty:** hard | **Area:** pricing | **Status:** open

## Problem
The anchor quotes Over 2.5. The book under comparison quotes Over 2.75, or AH -0.75. Without
a way to move between lines, these simply do not compare, and the pair is dropped.

## Why it matters
Books deliberately offer different lines, and in-play they move lines rather than just
prices. If the engine can only compare identical lines, it discards a large fraction of all
available comparisons — including, systematically, the ones where a book has moved its line
in a way that created value. The dropped comparisons are not a random sample.

## Approach
- **Fit a distribution, not a curve.** A goals distribution (bivariate Poisson, or a fitted
  supremacy/total parameterisation) calibrated to the lines that *are* quoted gives every
  other line consistently, and guarantees internal coherence — Over 2.5 and Over 3.5 derived
  from one distribution cannot contradict each other, which ad-hoc interpolation does not
  guarantee.
- **Quarter lines** split the stake across two adjacent whole/half lines and must be
  decomposed before devigging, not after.
- **Asian handicap ↔ totals ↔ 1X2** are all views of the same underlying score distribution.
  Fitting once and deriving all three is both more accurate and more coherent than treating
  them as unrelated markets.
- **In-play**, refit on current state: elapsed time, score, red cards materially change the
  distribution and the naive pre-match fit is badly wrong after the first goal.

## Acceptance criteria
- [ ] Score distribution fitted from observed anchor lines
- [ ] Any line derivable, including quarter lines, with correct stake decomposition
- [ ] Coherence checked: derived prices agree with directly quoted prices where both exist
- [ ] In-play refitting conditioned on match state
- [ ] Interpolated prices flagged as such and carry wider uncertainty

## Depends on
`hard-canonical-market-grammar`, `medium-total-line-in-market-identity`
## Shares machinery with
`hard-derived-fair-price-for-unanchored-markets`
