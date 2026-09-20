# Study existing arb/EV tools

**Difficulty:** medium | **Area:** research | **Status:** open

> Split out as its own task at the user's request.

## Task
Study how the established commercial tools in this space solve the same problems — to learn
from, not to buy.

| Tool | URL | Why it is worth a look |
|---|---|---|
| **BetBurger** | betburger.com | Arb/value scanner, very wide book coverage. Their filter design shows which failure modes matter enough to expose as user controls. |
| **RebelBetting** | rebelbetting.com | Arb + value betting. Publish unusually candid material on stale odds, bet acceptance rates and why arbs fail. |
| **OddsMonkey** | oddsmonkey.com | Matched-betting oriented. Strongest on market/event matching and normalisation — the boring problem this project also has. |
| **Pinnacle Odds Dropper** | pinnacleoddsdropper.com | Narrow and deep: Pinnacle line movement as signal. Good published writing on devigging and CLV. |
| **Outlier** | outlier.bet | Devigging and fair-odds tooling, with clear public documentation of their method choices. |
| **Trademate Sports** | tradematesports.com | Value betting on sharp-book-derived fair odds. Publish on CLV validation and bankroll management. |

## Questions to answer for each
1. Which book do they anchor to, and how do they weight the rest?
2. Which devig method, and do they vary it by market?
3. How do they handle staleness and suspension — what do they tell users about failed bets?
4. How do they match events and markets across books?
5. What do they surface about availability, limits and depth?
6. What do they claim about latency, and what does their UI imply about the real number?
7. Which markets do they refuse to cover, and why? (The gaps are as informative as the
   coverage — they mark where this approach stops working.)

## Deliverable
A comparison note in `research/`, plus any resulting concrete tasks added to `tasks/`.
Particularly: anything they treat as a first-class concern that this project currently
ignores.

## Acceptance criteria
- [ ] All six reviewed against the seven questions
- [ ] Findings written to `research/`
- [ ] New tasks filed for gaps worth closing
