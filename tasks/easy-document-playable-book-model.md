# Define the playable-book model: hypothetical and fee-free

**Difficulty:** easy | **Area:** domain | **Status:** open

## Decision to record
The playable bookmaker is **hypothetical and fee-free** (CoinCasino is the working
stand-in). No turnover tax, no stake deduction, no winnings tax.

Every other scraped book — including all regionally licensed ones — is a **benchmark input
only**. They contribute to the fair price; they are never a betting target.

## Why it matters
This constraint decides whether the comparator needs an effective-odds layer at all. Under
the fee-free model, quoted odds == effective odds, and `analytics/value.py` can compare
directly against consensus with no adjustment. Getting this wrong in either direction
(applying a deduction that does not exist, or omitting one that does) shifts every EV
number by several percent — the same magnitude as the edges being hunted.

## Task
- Record the decision and its rationale in `DECISIONS.md`.
- State it in `DOMAIN.md` as a modelling assumption, so it is revisited deliberately rather
  than discovered later.
- Add a single `effective_odds(book, quoted)` seam in `analytics/value.py` that is identity
  for fee-free books. Cheap to add now; avoids a scattered retrofit if a real playable book
  with a deduction is ever added.

## Acceptance criteria
- [ ] `DECISIONS.md` and `DOMAIN.md` updated
- [ ] Single `effective_odds()` seam exists, identity by default
- [ ] No tax/fee arithmetic anywhere else in the codebase
