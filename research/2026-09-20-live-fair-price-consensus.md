# Research: live betting fair-price consensus — bookmakers, sources, problems

**Date:** 2026-09-20
**Status:** complete — do not re-run this research. Tasks extracted to [`tasks/`](../tasks/README.md).

---

## The question (verbatim)

> this is sports betting software with scrapers
> one of the key problems it to find live betting price consensus in the market.
> it should focus both on price corettness but also on various bets availability
> make a research on internet and come back with list of bookmakers, urls, problems to be solved to create that 'fair price' calcualtor
> it will be then used to compare with offers i have abailable for playing

**Follow-up clarification:**

> dont worry about polish task
> polish bookmakers can be used as benchamrk also, because i will not use it
> the playable bookmaker will be hipotetical, tax free (for example coincasino)

### Standing constraint that follows

The playable bookmaker is **hypothetical and fee-free** — CoinCasino as stand-in. No
turnover tax, no stake deduction, no winnings tax. Quoted odds == effective odds.

Every other scraped bookmaker, including all regionally licensed ones, is a **benchmark
input only**: it contributes to the fair price and is never a betting target. Any
turnover-tax or effective-odds arithmetic is therefore out of scope. (An earlier draft of
this research treated Polish books as playable and made the 12% stake deduction a central
concern — that framing is superseded and should not be reintroduced.)

---

## Repo state at time of research

Scraped: `bet365, betfair, betfair_exchange, coincasino, lvbet, pinnacle, sbobet, sts`
(`scrapers/v2_*`).

`analytics/consensus.py` devigs each book then takes a weighted median of implied
probabilities, using a hand-written `BOOKMAKER_WEIGHTS` table.
`analytics/overround.py` implements `normalize`, `shin`, `power`.

---

## 1. Bookmakers, tiered by what they contribute

A consensus is **not** an average of everyone. Books split into price **originators** (they
discover the price), **fast followers** (they copy but carry real risk), and **noise** (they
copy slowly and shade for recreational flow). Weighting noise books at ≥0.3 actively
corrupts the estimate.

### Tier 0 — originators / anchors (weight 0.8–1.0)

| Book | Why | Access |
|---|---|---|
| **Pinnacle** (PS3838 mirror) | Industry benchmark. 2–3% overround on major football, does not limit winners, in-play margins far below the field. Its closing line is the standard every model is validated against. | pinnacle.com · api.pinnacle.com · docs: github.com/pinnacleapi/pinnacleapi-documentation |
| **Betfair Exchange** | Real money, two-sided. Gives price **and depth** — the dimension no scraped bookmaker feed has. | developer.betfair.com — REST + Exchange Stream API (push) |
| **SBOBet** | Asian market maker, huge in-play limits on AH/totals. Already scraped. | sbobet.com |
| **Circa Sports** | US-side originator (NFL/NBA); irrelevant if football/EU-only. | circasports.com |

### Tier 1 — sharp Asian / high-limit cross-checks (0.5–0.8)

IBCBet/**Nova88**, **Singbet (Crown)**, **Bet ISN**, **3ET**, **188Bet**, **Sharpbet**.
Not practically scrapeable individually — reached through **brokers**:

- AsianConnect88 — asianconnect88.com
- VOdds — vodds.com (one API → Pinnacle/SBObet/Betfair/Matchbook/Nova88/Singbet)
- Sportmarket Pro — sportmarket.com
- BetInAsia — betinasia.com

A broker API is worth serious consideration: tier-0/1 prices **plus executable liquidity
with real limits** behind one integration, instead of N scrapers behind N VPN configs.

### Tier 2 — exchanges (liquidity-weighted, 0.4–0.7)

**Smarkets** (smarkets.com, developer.smarkets.com), **Matchbook** (matchbook.com,
developers.matchbook.com), **Prophet X**. Thin outside home markets — weight by matched
volume, not by name.

### Tier 3 — soft / European retail (0.1–0.3, or 0.0)

bet365, Bwin, Unibet, William Hill, Betsson, 1xBet. Margins 6–10% on 1X2, much worse on
derivatives. Use as the comparison target and as **staleness detectors** — if bet365 moved
and the anchor did not, the anchor feed is lagging.

### Tier 4 — benchmark-only regional books (weight low, never a target)

Deep markets and fast in-play, useful for coverage and lag detection:
STS, Fortuna, Superbet, Betclic, LVBET, Etoto, forBET, TOTALbet, Betfan, Fuksiarz, PZBuk,
Betters, Betcris, AdmiralBet, LeBull, ComeOn, Traf.

Currently scraped from this group: STS, LVBET. Highest-value additions by market depth:
Superbet, Fortuna, Betclic, Etoto, Fuksiarz.

---

## 2. Data sources beyond direct scraping

**Aggregator APIs**
- The Odds API — the-odds-api.com — free tier, cheapest entry, polling-based
- OddsJam — oddsjam.com/odds-api — 100+ books, sub-second streaming, ~$500–1000+/mo, gated
- OpticOdds — opticodds.com — sub-second streaming, sharp-book focused
- BetsAPI — betsapi.com — cheap, strong in-play + bet365/Asian coverage
- OddsMatrix — oddsmatrix.com — operator-grade pre-live + live
- SportsGameOdds — sportsgameodds.com — publishes pricing
- Directory of 30+: sportsapis.dev

**Comparison sites** (scrape targets / validation ground truth)
- oddsportal.com — best for historical opening→closing movement; the backtest ground truth
- betexplorer.com — historical + archive
- oddspedia.com — strongest live in-play comparison coverage
- oddschecker.com — UK-heavy

**Match-state feeds** (needed to price live at all)
Sportradar, Genius Sports, Stats Perform (~0.5s quantified latency on their 2026 World Cup
rights). Flashscore/LiveScore scraping is the cheap option.

**Pinnacle streaming resellers** — pinnodds.com, pinnapi.com (~15–40ms price-change-to-client
over SSE/WebSocket), relevant because the official odds endpoints are rate-limited to as
little as 1 req / 2 min per endpoint per sport.

---

## 3. Problems to solve

Each is now a file in [`tasks/`](../tasks/README.md).

**A. Identity & alignment** — event matching across books; canonical market grammar
`(market_type, period, line, side, participant)`; line grouping (Over 2.5 ≠ Over 3.5).

**B. Devigging** — method choice matters most on lopsided in-play prices, where methods
diverge by more than the edge being sought; margin is loaded onto longshots, not spread
proportionally; in-play overrounds run 7–12% vs 4–6% pre-match, so devig error roughly
doubles; never devig a partial market.

**C. Aggregation** — weights should be measured (lead/lag, log loss), not guessed;
correlated feeds double-count one opinion; weighted median is jumpy and does not renormalise;
thin markets degenerate to the anchor; emit an uncertainty band, since dispersion is itself
the best false-positive filter in the system.

**D. Live** — staleness (the #1 issue: consensus mixes prices captured seconds apart, and the
lag is directional); suspension vs not-offered vs stale vs live; latency asymmetry (if your
consensus is slower than the book you compare against, every "edge" is you being slow);
Betfair `betDelay` of 1–12s; match-state conditioning (books disagree because of information
arrival timing, not opinion); Pinnacle rate limits.

**E. Availability** — coverage matrix (who offers what, at what limit, what margin);
availability as signal (anchor absent ⇒ suspicion; everyone suspended but one ⇒ that one is
stale); limits and depth; derived prices for markets anchors do not quote; alternate-line
interpolation.

**F. Validation** — CLV against the anchor's close (beating the close correlates ~1:1 with
long-run yield and has far lower variance than P&L); calibration curves and scoring rules;
backtest harness over the stored CSV corpus; beat the naive Pinnacle-only baseline or ship
the baseline.

**G. Ops** — scraper drift and silent degradation; clock sync (breaks staleness, latency
measurement and CLV timing at once, invisibly); anti-bot; build-vs-buy.

---

## 4. Confirmed defects in the current code

1. **`sbobet` never reaches the consensus** — `analytics/consensus.py:107` iterates
   `BOOKMAKER_WEIGHTS.items()`; `sbobet` is absent from the dict (`consensus.py:37-45`).
2. **Over/Under aggregated without the line** — `_OVERUNDER` (`consensus.py:50`) ignores
   `total_line`, which exists in `dashboard/data_service.py:30`.
3. **Devig method hardcoded** to `normalize` (`consensus.py:117`); `shin` and `power` are
   implemented and unreachable.
4. **No timestamp or staleness filter anywhere in the consensus path** — fatal for live.
5. **Fair probabilities do not sum to 1** — per-outcome weighted median with no
   renormalisation (`consensus.py:126-137`).

---

## Sources

- https://betherosports.com/blog/devigging-methods-explained
- https://help.outlier.bet/en/articles/8208129-how-to-devig-odds-comparing-the-methods
- https://cran.r-project.org/web/packages/implied/vignettes/introduction.html
- https://en.wikipedia.org/wiki/Favourite-longshot_bias
- https://www.pinnacleoddsdropper.com/guides/how-to-devig-pinnacle-s-odds-for-betting-on-soft-books
- https://help.outlier.bet/en/articles/9922960-how-sportsbooks-set-odds-soft-vs-sharp-books
- https://valuebetfactory.com/betting-education/sharpest-sportsbooks
- https://betmetricslab.com/betting-sites/sports-bet-brokers-agents/
- https://asianodds.com/en/bookmaker-reviews
- https://oddspapi.io/blog/best-odds-apis-2026-comparison/
- https://sportsgameodds.com/blog/comparing-odds-api-providers
- https://sportsapis.dev/
- https://docs.developer.betfair.com/display/1smk3cen4v3lu3yomq5qye0ni/Exchange+Stream+API
- https://support.developer.betfair.com/hc/en-us/articles/360002825652
- https://github.com/pinnacleapi/pinnacleapi-documentation
- https://sharpapi.io/learn/how-to-get-pinnacle-odds-api
- https://track360.io/blog/in-play-betting-margins-operator-economics-2026
- https://bet.report/en/blog/bookmaker-margins-explained/
- https://theplayoffs.news/en/the-data-behind-in-play-in-the-uk-latency-feeds/
- https://www.tonyspicks.com/2026/05/12/live-betting-latency-which-sportsbooks-update-fastest-during-play/
- https://sccgmanagement.com/sccg-articles/2026/03/06/the-sports-betting-data-odds-ecosystem/
- https://www.pinnacle.com/betting-resources/en/educational/what-is-closing-line-value-clv-in-sports-betting
- https://asianodds.com/en/closing-line-value
- https://punter2pro.com/best-odds-comparison-sites/
- https://www.similarweb.com/website/oddsportal.com/competitors/
- https://www.karlwhelan.com/Papers/Overround.pdf
