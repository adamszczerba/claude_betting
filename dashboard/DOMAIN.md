This is web app, that shows system status and let user select best bets.
The goal is to get big ROI keeping the bankroll stable.
Has tabs:
- 1st shows logs from scrapers that are running (see scrapers dir)
- 2nd shows all matches:
    - ordered by: first bookmakers i can bet on, then expected value on bet
    - for given match, show all markets, label bets with best odds
    - if system recognize value bet, highlight it, show expected value and button. Button opens bet details (see signal signals and decisions dir)
    - first column is a match name, second in bookmaker, 3rd is match time, score and status, 4th and following are possible bets with odds
- 3rd has filter and shows best opportunities from last X hours (there is a filter to select up to week time horizon)
- 4rd shows bets that has been approved - with their outcome
- 5th is a analytics tab. User can select match from database and see plots from all bookmakers that was or are available:
    x axis is match time, y is 1X2 odds in 0-1 scale (pricing like vonds), each bookmaker has different color, there are also scores visible on y axis.

When button is clicked, there is pop up with details and 2 buttons: "place bet" and "cancel". If user clicks "place bet".
If place bet is clicked, close pop up and record decision, then track it with outcome and ROI, store for 1 year. Show it in 4rd tab.
User manually place that bets and that is not a scope of the system.

There is no bankroll risk management in dashboard
there is no auto execution in dashboard
