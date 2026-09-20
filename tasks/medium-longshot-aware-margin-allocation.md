# Margin is not distributed proportionally across outcomes

**Difficulty:** medium | **Area:** devigging | **Status:** open

## Problem
The multiplicative method assumes each outcome carries margin in proportion to its implied
probability. Empirically, books load **more** margin onto longshots — this is the
favourite-longshot bias, and it is well documented across decades of market data.

## Why it matters
Assuming proportional allocation systematically underestimates favourites' true probability
and overestimates longshots'. In-play, where overrounds run 7-12% versus 4-6% pre-match,
the error roughly doubles. It biases in a consistent direction, so it does not average out
over many bets — it accumulates.

## Fix
- Prefer Shin (models insider-information asymmetry, directly corrects the bias) or power
  (never leaves [0,1], no feasibility failures) over multiplicative.
- Estimate the actual margin allocation per bookmaker from stored history: regress realised
  outcome frequency against each book's devigged probability, bucketed by probability. The
  shape of the residual tells you which method fits that book.
- Consider a per-bookmaker devig method rather than one global choice — soft books and
  sharp books shade differently.

## Acceptance criteria
- [ ] Per-bookmaker devig method supported
- [ ] Residual-vs-probability analysis run over stored history
- [ ] Findings documented in `DECISIONS.md`

## References
Shin (1993); https://cran.r-project.org/web/packages/implied/vignettes/introduction.html
