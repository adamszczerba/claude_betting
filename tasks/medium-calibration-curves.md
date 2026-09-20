# Calibration: do 30% fair prices win 30% of the time?

**Difficulty:** medium | **Area:** validation | **Status:** open

## Problem
The fair price is never checked against reality.

## Why it matters
CLV says the engine agrees with the sharp market. Calibration says the engine is *right*.
They fail differently and both are needed: a badly calibrated engine can still show positive
CLV on a subset of markets while being systematically wrong elsewhere — most commonly at the
tails, which is where in-play spends most of its time.

## Fix
- Bucket emitted fair probabilities (deciles, plus finer buckets below 0.05 and above 0.95
  where in-play lives), plot realised frequency against predicted.
- Report Brier score and log loss against the baseline
  (`easy-naive-baseline-benchmark`) — calibration plots hide miscalibration that scoring
  rules expose.
- Slice by: market type, minute-of-match bucket, number of contributing books, dispersion.
  Aggregate calibration can look fine while a specific slice is badly broken.

## Acceptance criteria
- [ ] Calibration computed over stored history with outcome joins
- [ ] Brier / log loss against baseline
- [ ] Sliced by market type, match phase, `n_books`, dispersion
- [ ] Results on the dashboard, refreshed on a schedule

## Depends on
`medium-backtest-harness` (needs match outcomes joined to stored odds)
