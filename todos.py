# rename Betting/v2 to Betting/v2_coincasino to be consitient with v2_betfair
# for both coincasino and betfair ensure that fetch is every 2s, cannot be less frequent  ✅ DONE — wall-clock aligned via v2/sync_clock.py
# exlore if betfair offers other deals than 1x2
# explore if coincasino offers other deals than 1x2  ✅ DONE — API has 4 soccer markets: 1x2, Total O/U, Double Chance, Draw No Bet (no corners/cards). All odds from API, not UI-calculated. DC+DNB now scraped.
# matches are not updated when scrapers are rexecuted with run.scraper.sh, fix it. match_database should be updated often
