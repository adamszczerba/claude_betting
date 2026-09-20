# Treat availability itself as a pricing signal

**Difficulty:** medium | **Area:** availability | **Status:** open

## Problem
Availability is currently treated as a data-collection concern. It is also information.

## Rules worth encoding
- **Anchor absent ⇒ suspicion.** If Pinnacle or the exchange does not price a market in-play,
  it is usually because they cannot price it confidently. Any apparent edge on a soft book in
  that market deserves a heavy penalty, not a green light.
- **Everyone suspended but one ⇒ that one is stale.** A lone live quote while the market has
  suspended is almost always a lagging feed, not an opportunity. This is the phantom-edge
  pattern that looks most attractive and is most reliably wrong.
- **Sudden coverage withdrawal ⇒ information arrived.** Books pulling a market together is a
  strong prior that something happened that your score feed has not delivered yet.
- **Limit collapse ⇒ the book thinks it is wrong.** A sharp book cutting max stake on a
  market is its own opinion about the reliability of its price.

## Why it matters
These rules kill entire categories of false positive using data the system is already close
to having, and they need no model — just state that is currently thrown away.

## Acceptance criteria
- [ ] Rules implemented as explicit, named, individually testable gates
- [ ] Each gate logged when it fires, so its hit rate can be measured
- [ ] Rules documented in `DOMAIN.md`

## Depends on
`medium-suspension-state-machine`, `medium-coverage-matrix`
