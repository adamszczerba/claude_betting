# Event matching across bookmakers

**Difficulty:** hard | **Area:** market identity | **Status:** open | **Priority: high**

## Problem
`dashboard/matcher.py` matches events by normalised team names and tournament. Books disagree
about everything: team naming (Śląsk Wrocław / Slask Wroclaw / Slask), sponsor names, youth
and reserve suffixes, women's fixtures, league naming and tiering, neutral venues, reversed
home/away, and kickoff times that differ by minutes across feeds.

## Why it matters
This is the unglamorous problem that silently ruins everything downstream. A wrong match
produces a consensus built from two different games — which yields an enormous apparent
edge, since the prices are unrelated. A missed match quietly shrinks the book count and
degrades the consensus without any error surfacing. Both failures are invisible without
deliberate instrumentation.

It is also the highest-leverage place to invest: every additional bookmaker added to the
system multiplies the matching burden, and matching quality caps the value of every other
improvement.

## Fix
- **Event ID registry**: canonical event IDs keyed on (normalised participants, kickoff
  within a tolerance window, competition), with per-bookmaker alias tables that persist and
  accumulate. Manual resolutions must be remembered permanently — this is where the value
  compounds.
- **Layered matching**: exact alias hit, then normalised exact, then fuzzy with a confidence
  score. Never auto-accept below a threshold.
- **Explicit unmatched queue**, surfaced on the dashboard for human review; each resolution
  writes back to the alias table.
- **Mismatch detection as a safety net**: a group whose books disagree wildly is more likely
  mis-joined than genuinely disagreeing. Feed dispersion back in as a matching alarm.

## Acceptance criteria
- [ ] Persistent alias tables per bookmaker, populated by both automatic and manual matches
- [ ] Confidence score on every match; sub-threshold matches quarantined not auto-joined
- [ ] Unmatched/low-confidence queue visible and resolvable from the dashboard
- [ ] Match rate and estimated false-join rate tracked as metrics
- [ ] Regression corpus of known-hard cases with expected outcomes

## Related
`medium-fair-price-uncertainty-interval` provides the dispersion signal used as the alarm.
