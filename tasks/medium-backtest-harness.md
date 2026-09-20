# Backtest harness over stored history

**Difficulty:** medium | **Area:** validation | **Status:** open

## Problem
Every methodological choice in this task list — devig method, estimator, weights, TTLs,
thresholds — is currently decided by argument rather than by evidence. There is no way to
replay stored data through a candidate configuration.

## Why it matters
This is the unblocking task for most of the validation and aggregation work. Without it, the
devig comparison, the estimator comparison, the weight estimation and the baseline benchmark
are all guesses. With it they are half a day's work each.

The data already exists: `match_database/<bookie>/<date>/*.csv` is a growing corpus and is
the most valuable asset in the project.

## Fix
- Replay engine that feeds stored rows through the consensus pipeline in timestamp order,
  honouring the real arrival times so staleness and latency behave as they did live.
- Join match outcomes (needs a results source — Flashscore or an API).
- Configuration sweep: run N configurations, output calibration / CLV / signal counts.

## Acceptance criteria
- [ ] Deterministic replay of stored history in arrival order
- [ ] Match outcomes joined
- [ ] Config sweep producing a comparison table
- [ ] Runs on the existing corpus without needing new data collection

## Protect
The stored CSV corpus is irreplaceable. Back it up before any schema migration.
