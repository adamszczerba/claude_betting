# Detect correlated feeds and stop double-counting

**Difficulty:** hard | **Area:** aggregation | **Status:** open

## Problem
The consensus treats every bookmaker as an independent opinion. Many are not. A large share
of European books buy pricing from the same handful of suppliers, or copy a market leader
with a fixed markup. bet365, LVBET and STS may substantially be one opinion wearing three
hats.

## Why it matters
Three copies of one opinion outvote one genuine independent source. Adding books then makes
the consensus *worse* while making it look more robust — more inputs, tighter apparent
agreement, higher false confidence. It also breaks
`medium-fair-price-uncertainty-interval`: dispersion across correlated books understates
true uncertainty, so the dispersion gate stops protecting anything.

This is the most likely reason a multi-book consensus loses to the anchor alone, which makes
`easy-naive-baseline-benchmark` a useful early diagnostic for it.

## Approach
- Compute residual correlation between books after removing the common market component —
  raw price correlation is near 1 for everything and tells you nothing.
- Cluster books by residual correlation. Books in a cluster share one opinion.
- Allocate weight per **cluster**, then split within it, rather than per book.
- Watch for regime change: a book switching supplier shows up as a correlation structure
  break, and should trigger re-estimation.

## Acceptance criteria
- [ ] Residual correlation matrix computed over stored history
- [ ] Clustering implemented; cluster membership visible and reviewable
- [ ] Weight allocated per cluster, not per book
- [ ] Effective independent sample size reported alongside `n_books`
- [ ] Correlation structure re-checked on a schedule

## Depends on
`medium-backtest-harness`, `hard-empirical-bookmaker-weights`
