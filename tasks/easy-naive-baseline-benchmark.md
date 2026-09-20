# Benchmark the consensus against a Pinnacle-only baseline

**Difficulty:** easy | **Area:** validation | **Status:** open

## Problem
There is no evidence that multi-book consensus beats the simplest possible alternative:
take Pinnacle, devig it with the power method, done.

## Why it matters
Multi-book consensus systems frequently *lose* to the anchor alone, because adding soft
books adds more noise than information. If the baseline wins, ship the baseline — it is
also far cheaper to run and has fewer failure modes.

## Fix
Implement `analytics/baseline.py` exposing `anchor_fair(group, book="pinnacle", method="power")`
with the same interface as `weighted_consensus()`. Run both over stored history and compare
on calibration and CLV.

## Acceptance criteria
- [ ] Baseline implemented behind the same interface
- [ ] Head-to-head comparison over stored `match_database/` history
- [ ] Result written to `DECISIONS.md` with the numbers

## Depends on
`medium-backtest-harness`, `medium-calibration-curves`, `medium-clv-logging`
