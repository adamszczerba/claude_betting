That is betting e2e system.

Ultimate goal:
- betting prematch and live tennis, basketball, football, hockey.
- betting decision made based on many singals: 
  - current state based on live data (score, time, etc)
  - live information not based on data from bookmakers: radio transmit, live news updates, video processing
  - bookmaker inefficiency founded based on comarping with other bookmakers
  - ml model assessing if current price is undervaluing given match time serie (odds, pints, time to end)
- database storing historical match data (odds, points, time movement, etc) for training ml model and for betting decision making
- ml model for assessing if current offer is a value bet
- betting bot placing bets based on player commands


Software modules:
# each modele have own interface and documentation. It is described in .md file in their own directory. Thant .md handle both info for human and for AI agent.
- data collector (scrap bookmaker data as timeseries and saves down to db)
- database (match's timeseries)
- 
- web searcher signal provider (searches for live information not based on data from bookmakers, produce signals)
- timeseries signal provider (produce signals based on timeseries for given bookmaker and trained ML algorithm)
- comparator signal provider (produce signals based on comparing odds across bookmakers)
- # todo software should be open for more signal providers
- # todo signals providers should be separate process? how they communicate with decision maker
- 
- decision maker: assess bet value, decide if place the bet, provide bet sizing
- executor: place bet based on decision maker output
# todo: it should be also module to visit past matches in db and delete them if are too corrupted

- ui for player to observe state and approve suggested bets

# todo: data collectors and executor should be executed in different docker containers to use different vpns
# todo: software should be open for more bookmakers
# todo: it should be open for new kind of bets. for example for football corners, cards, handicaps

First version we are implementing:
only live betting, only football
execution only on conincasino, other bookmakers are for signal generation and value bet detection, but not for execution.

