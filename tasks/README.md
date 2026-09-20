# Task index — live fair-price consensus engine

One file per task. The `easy-` / `medium-` / `hard-` filename prefix is a difficulty hint
for picking a model: `easy-` tasks are localised and well specified, `hard-` tasks need
design work and touch multiple modules.

Source research: [`research/2026-09-20-live-fair-price-consensus.md`](../research/2026-09-20-live-fair-price-consensus.md)

**Standing assumption:** the playable bookmaker is hypothetical and fee-free (CoinCasino as
stand-in). Every other bookmaker — including all regionally licensed ones — is a benchmark
input only, never a betting target. See `easy-document-playable-book-model`.

## Suggested order

The dependency chain that unblocks the most: `hard-price-staleness-ttl` →
`medium-backtest-harness` → everything in validation and aggregation, which is currently
decided by argument rather than evidence.

Quick wins first: the four confirmed code defects (`easy-sbobet-missing-from-consensus`,
`easy-consensus-probabilities-dont-sum-to-one`, `medium-total-line-in-market-identity`,
`medium-configurable-devig-method`) are real bugs in the current consensus path, not
speculative improvements.

## Easy

| Task | Area |
|---|---|
| [sbobet-missing-from-consensus](easy-sbobet-missing-from-consensus.md) | aggregation — **confirmed bug** |
| [consensus-probabilities-dont-sum-to-one](easy-consensus-probabilities-dont-sum-to-one.md) | aggregation — **confirmed bug** |
| [thin-market-degeneracy-guard](easy-thin-market-degeneracy-guard.md) | aggregation |
| [devig-subset-guard](easy-devig-subset-guard.md) | devigging |
| [betfair-betdelay-timeshift](easy-betfair-betdelay-timeshift.md) | live |
| [naive-baseline-benchmark](easy-naive-baseline-benchmark.md) | validation |
| [expand-benchmark-bookmaker-coverage](easy-expand-benchmark-bookmaker-coverage.md) | data sources |
| [document-playable-book-model](easy-document-playable-book-model.md) | domain |

## Medium

| Task | Area |
|---|---|
| [total-line-in-market-identity](medium-total-line-in-market-identity.md) | market identity — **confirmed bug** |
| [configurable-devig-method](medium-configurable-devig-method.md) | devigging — **confirmed gap** |
| [longshot-aware-margin-allocation](medium-longshot-aware-margin-allocation.md) | devigging |
| [logit-space-aggregation](medium-logit-space-aggregation.md) | aggregation |
| [fair-price-uncertainty-interval](medium-fair-price-uncertainty-interval.md) | aggregation |
| [suspension-state-machine](medium-suspension-state-machine.md) | live |
| [per-bookie-latency-measurement](medium-per-bookie-latency-measurement.md) | live |
| [pinnacle-rate-limit-safe-polling](medium-pinnacle-rate-limit-safe-polling.md) | data sources |
| [coverage-matrix](medium-coverage-matrix.md) | availability |
| [availability-as-signal](medium-availability-as-signal.md) | availability |
| [limits-and-depth-capture](medium-limits-and-depth-capture.md) | availability |
| [clv-logging](medium-clv-logging.md) | validation |
| [calibration-curves](medium-calibration-curves.md) | validation |
| [backtest-harness](medium-backtest-harness.md) | validation — **unblocks most others** |
| [scraper-drift-and-clock-sync](medium-scraper-drift-and-clock-sync.md) | ops |
| [evaluate-broker-api](medium-evaluate-broker-api.md) | data sources |
| [evaluate-paid-odds-feeds](medium-evaluate-paid-odds-feeds.md) | data sources |
| [study-existing-arb-ev-tools](medium-study-existing-arb-ev-tools.md) | research |

## Hard

| Task | Area |
|---|---|
| [price-staleness-ttl](hard-price-staleness-ttl.md) | live — **highest priority** |
| [event-id-registry](hard-event-id-registry.md) | market identity |
| [canonical-market-grammar](hard-canonical-market-grammar.md) | market identity |
| [empirical-bookmaker-weights](hard-empirical-bookmaker-weights.md) | aggregation |
| [correlated-feed-detection](hard-correlated-feed-detection.md) | aggregation |
| [match-state-conditioning](hard-match-state-conditioning.md) | live |
| [derived-fair-price-for-unanchored-markets](hard-derived-fair-price-for-unanchored-markets.md) | pricing |
| [alternate-line-interpolation](hard-alternate-line-interpolation.md) | pricing |

## Clusters worth designing together

- **Timestamp/state plumbing**: `hard-price-staleness-ttl`, `medium-suspension-state-machine`,
  `medium-per-bookie-latency-measurement`, `hard-match-state-conditioning` — four symptoms of
  one missing abstraction. Design the price envelope once.
- **Market identity**: `medium-total-line-in-market-identity`, `hard-canonical-market-grammar`,
  `hard-event-id-registry` — the narrow fix is a slice of the general one.
- **Score distribution**: `hard-alternate-line-interpolation`,
  `hard-derived-fair-price-for-unanchored-markets` — same model, two uses.
- **Build vs buy**: `medium-evaluate-broker-api`, `medium-evaluate-paid-odds-feeds`,
  `medium-pinnacle-rate-limit-safe-polling` — one decision.
