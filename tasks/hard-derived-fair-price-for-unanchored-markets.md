# Derived fair prices for markets the anchors do not quote

**Difficulty:** hard | **Area:** pricing | **Status:** open

## Problem
Sharp books price a narrow set of markets in-play: 1X2, Asian handicap, totals, sometimes
next goal. Soft and retail books price far more — corners, cards, team totals, both teams to
score, player markets, period and minute-band markets. For those, there is no consensus to
compute, because there are not enough independent quotes to form one.

## Why it matters
This is where the mispricing actually lives. Sharp books have pinned the main markets; the
margin on derivatives is where soft books are weakest and least attentive. But a consensus
engine returns nothing there, and — worse — a consensus formed from three soft books that
all copy the same supplier is a confident number with no information behind it
(`hard-correlated-feed-detection`).

## Approach
Separate the two pricing regimes explicitly and never let them be confused:
- **Consensus-priced markets** — enough independent quotes. Current path.
- **Model-priced markets** — derive from related markets. A goals distribution fitted to the
  match total and supremacy gives team totals, BTTS and correct score. Corners and cards
  correlate with supremacy, game state and match tempo. In-play, all of these must be
  conditioned on elapsed time and current state.
- **Unpriceable** — say so. Emitting nothing is correct and far cheaper than emitting a
  number with unknown error.

The classification must be explicit in the output, and model-priced markets must carry wider
uncertainty bands than consensus-priced ones. The failure mode to avoid is a model price
flowing downstream indistinguishable from a consensus price.

## Acceptance criteria
- [ ] Market classification (consensus / model / unpriceable) explicit and per-market
- [ ] Goals-distribution model (bivariate Poisson or similar) fitted and validated against
      consensus prices on markets where both exist — that overlap is the model's test set
- [ ] Model prices carry their own, wider uncertainty
- [ ] Signals record which regime produced the fair price

## Depends on
`hard-canonical-market-grammar`, `medium-calibration-curves`
