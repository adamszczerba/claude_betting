# Log closing line value against the anchor

**Difficulty:** medium | **Area:** validation | **Status:** open

## Problem
There is no record of how the fair price compared to the market's final word, so there is no
way to tell whether the engine is any good.

## Why it matters
CLV is the standard validation metric for betting models, and the evidence behind it is
strong: measured against sharp closing lines over large samples, beating the close
correlates close to one-to-one with long-run yield. Consistent positive CLV over ~1000
observations is near-conclusive evidence of edge; consistent negative CLV is near-conclusive
evidence of its absence. It is also observable long before realised P&L is, because it has
vastly lower variance than results do.

## Fix
For every signal the system emits, persist:
- the fair price at signal time and the playable price taken
- the anchor price at signal time
- the anchor **closing** price (requires a settle-time job to capture the close)
- for in-play: the anchor price at N seconds after the signal, as an in-play analogue of the
  close — the true in-play "closing line" is the price just before the market resolves

Report CLV distribution, not just the mean.

## Acceptance criteria
- [ ] Every signal persisted with fair / playable / anchor prices and timestamps
- [ ] Closing price capture job
- [ ] In-play CLV proxy defined and documented in `DECISIONS.md`
- [ ] CLV distribution reported on the dashboard

## Related
`ledger/` already persists decisions — extend rather than duplicate.
