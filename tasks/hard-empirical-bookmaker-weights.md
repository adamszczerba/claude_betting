# Estimate bookmaker weights from data instead of guessing them

**Difficulty:** hard | **Area:** aggregation | **Status:** open

## Problem
`BOOKMAKER_WEIGHTS` is a hand-written table of intuitions. Pinnacle 1.00, exchange 0.85,
bet365 0.50, and so on. They may be roughly right; nothing in the system checks.

## Why it matters
The weights determine the fair price. Guessed weights mean a guessed fair price with a
veneer of rigour. More usefully: the right weight is **measurable**, because the market
tells you who leads and who follows.

## Approach
- **Lead/lag estimation.** Cross-correlate each book's devigged probability series against
  the others. A book that consistently moves first earns weight; a book whose moves are
  predicted by others' earlier moves is carrying no independent information and should
  approach zero regardless of brand.
- **Predictive accuracy.** Score each book's devigged probabilities against realised outcomes
  (log loss). Weight by out-of-sample skill.
- **Condition on context.** Sharpness is not one number. A book can be sharp pre-match and
  slow in-play, sharp on 1X2 and hopeless on totals, sharp in the Premier League and
  guessing in Ekstraklasa. Estimate per (phase, market type, competition tier) where sample
  size allows, and back off to a pooled estimate where it does not.
- **Re-estimate on a schedule.** Books change pricing providers and traders.

## Acceptance criteria
- [ ] Lead/lag and log-loss estimated per book over stored history
- [ ] Weights conditioned on match phase and market type, with shrinkage to a pooled prior
      where data is thin
- [ ] Re-estimation job on a schedule; weight changes logged and reviewable
- [ ] Empirical weights beat the hand-written table on the backtest, or the hand-written
      table stays and that result is recorded

## Depends on
`medium-backtest-harness`, `medium-per-bookie-latency-measurement`
