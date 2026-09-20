# Timestamps and staleness TTL through the consensus path

**Difficulty:** hard | **Area:** live | **Status:** open  | **Priority: highest**

## Problem
Nothing in the consensus path knows when a price was observed. `_fair_from_group()` reads
whatever value is in the group and treats every book as contemporaneous. Scrapers poll at
different intervals through different VPN exits, so in practice a fair price can combine a
Pinnacle quote from t with an STS quote from t-9s.

## Why it matters
This is the single most damaging correctness issue for live pricing, and it is worse than a
random error because it is *directional*. When the market moves — a goal, a red card — slow
books lag in a known direction, so the consensus is systematically dragged toward the
pre-event price for several seconds. The apparent edge against a fast playable book is
largest exactly when the consensus is most wrong. Every other improvement in this list is
built on top of a number that is wrong in a way this task fixes.

## Fix
1. **Capture** an observation timestamp at the scrape boundary (not at write time) on every
   price, in the CSV schema and the Redis stream payload.
2. **Exclude, do not downweight.** A price older than the TTL carries no information about
   the current market; downweighting it still lets it pull the estimate. Drop it.
3. **TTL by phase**: 2-5s in live, much looser pre-match. Consider per-book TTLs informed by
   `medium-per-bookie-latency-measurement` — a structurally slow book needs a shorter useful
   life, not a longer one.
4. **Emit consensus age** alongside the price so consumers can gate on it, and record how
   often each book is excluded for staleness — a book excluded most of the time in live
   should not be carrying live weight at all.

## Acceptance criteria
- [ ] Observation timestamp on every price, end to end through CSV and Redis
- [ ] Clock skew between containers bounded and asserted (see `medium-scraper-drift-and-clock-sync`)
- [ ] Stale prices excluded from the consensus, with per-book exclusion rates recorded
- [ ] Consensus carries its own age; downstream can gate on it
- [ ] `API_CONTRACTS.md` updated

## Related
`medium-suspension-state-machine` shares this plumbing — do them together.
