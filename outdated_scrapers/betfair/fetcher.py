import datetime
import time
from dataclasses import dataclass
import pandas as pd
import re
import os

from IPython.sphinxext.custom_doctests import float_doctest
from jedi.inference.gradual.typeshed import try_to_load_stub_cached
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
logger = logging.getLogger()

BETFAIR_ID = 'bf'

# tuning knobs
POLL_INTERVAL_SEC = 2.0          # desired interval between updates
FETCH_WAIT_TIME_SEC = 2          # how long WebDriverWait waits for the key element

class MatchState:
    def __init__(self, match_time: str, score1: int, score2: int, bet_1_odd: float, bet_x_odd: float, bet_2_odd: float):
        self.timestamp = datetime.datetime.now()
        self.match_time = match_time
        self.score1 = score1
        self.score2 = score2
        self.bet_1_odd = bet_1_odd
        self.bet_x_odd = bet_x_odd
        self.bet_2_odd = bet_2_odd

    def __repr__(self):
        return f"MatchState(time={self.timestamp}, match_time={self.match_time}, score1={self.score1}, score2={self.score2}, bet_1_odd={self.bet_1_odd}, bet_x_odd={self.bet_x_odd}, bet_2_odd={self.bet_2_odd})"


class MatchIdentifier:
    def __init__(self, team1: str, team2: str, league: str, date: datetime.date, bookmaker: str):
        self.bookmaker = bookmaker
        self.team1 = team1
        self.team2 = team2
        self.league = league
        self.date = date

    def get_filename(self) -> str:
        return f"{self.team1}_vs_{self.team2}_{self.league}_{self.bookmaker}_{self.date}.csv"

    def __eq__(self, other):
        if not isinstance(other, MatchIdentifier):
            return False
        return (self.team1 == other.team1 and
                self.team2 == other.team2 and
                self.league == other.league and
                self.date == other.date and
                self.bookmaker == other.bookmaker)

    def __hash__(self):
        return hash((self.team1, self.team2, self.league, self.date, self.bookmaker))

    def __repr__(self):
        return f"MatchIdentifier(team1={self.team1}, team2={self.team2}, league={self.league}, date={self.date}, bookmaker={self.bookmaker})"


class Match:
    def __init__(self, id: MatchIdentifier):
        self.id = id
        self.state_history: list[MatchState] = []
        logger.info('Match created: {self.id}')

    def add_state(self, state: MatchState):
        logger.info(f'State added to match {state}')
        self.state_history.append(state)

    def save_state_history(self):
        states_history = pd.DataFrame([vars(s) for s in self.state_history])
        filename = self.id.get_filename()
        db_dir = '/outdated_scrapers/betfair/db'
        os.makedirs(db_dir, exist_ok=True)
        path = os.path.join(db_dir, filename)

        # append instead of rewriting whole file every time
        write_header = not os.path.exists(path)
        states_history.to_csv(path, mode='a', header=write_header, index=False)
        # clear in-memory buffer after flush
        self.state_history.clear()

def dedup_consecutive_live(elems: list[str]) -> list[str]:
    result = []
    prev_live = False
    for e in elems:
        if e == 'Live':
            if prev_live:
                continue
            prev_live = True
        else:
            prev_live = False
        result.append(e)
    return result


class Executor:
    def __init__(self):
        self.matches: dict[MatchIdentifier, Match] = {}
        self.poll_interval_sec = POLL_INTERVAL_SEC

    def update_matches(self, new_states: dict[MatchIdentifier, MatchState]):
        for match_id, state in new_states.items():
            if match_id not in self.matches:
                self.matches[match_id] = Match(match_id)

            self.matches[match_id].add_state(state)

    def run(self):
        scraper = BetfairScraper()
        try:
            while True:
                loop_start = time.time()
                logger.debug('Next fetch started.')

                # fetch page text
                fetch_start = time.time()
                current_page_state = scraper.fetch_raw()
                fetch_time = time.time() - fetch_start

                # parse + update
                parse_start = time.time()
                matchid_to_matchstate = scraper.parse_raw(current_page_state)
                self.update_matches(matchid_to_matchstate)
                parse_time = time.time() - parse_start

                loop_time = time.time() - loop_start
                logger.info(
                    f"Loop timing: fetch={fetch_time:.3f}s, parse+update={parse_time:.3f}s, total={loop_time:.3f}s"
                )

                # sleep to maintain ~poll_interval_sec between loops
                remaining = self.poll_interval_sec - loop_time
                if remaining > 0:
                    time.sleep(remaining)
        finally:
            for match in self.matches.values():
                match.save_state_history()
            scraper.close()


class BetfairScraper():

    def __init__(self):
        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument(
            '--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        self.driver = webdriver.Chrome(options=options)
        # set shorter page load timeout; we only need the live page quickly
        self.driver.set_page_load_timeout(10)

    def extract_leftover(self, leftover: str):

        # 'CA Votuporanguense U20 2 - 0 Falcon FC U20 58′'
        match_time = leftover.split(' ')[-1]
        teams_and_scores = ' '.join(leftover.split(' ')[:-1])
        team1_score1, score2_team2 = teams_and_scores.split('-')
        *team1_words, score1 = [x for x in team1_score1.split(' ') if x]
        team1 = ' '.join(team1_words)
        score2, *team2_words = [x for x in score2_team2.split(' ') if x]
        team2 = ' '.join(team2_words)

        return team1, team2, int(score1), int(score2), match_time

    def extract_oods(self, odds_candidates: list[str]):

        if len(odds_candidates) == 3:
            return odds_candidates[0], odds_candidates[1], odds_candidates[2]
        elif len(odds_candidates) < 3:
            # one odd is not loaded correctly, it is missing on UI, so whitespace is at the beginning of other odd
            odds = []
            for candidate in odds_candidates:
                if candidate.startswith(' '):
                    odds.append(0.0)
                    odds.append(float(candidate.lstrip()))
                else:
                    odds.append(float(candidate))

            if len(odds) != 3:
                raise ValueError(f'Cannot extract odds from match data: {match}')

            return odds[0], odds[1], odds[2]
        else:
            raise ValueError(f'Cannot extract odds from match data: {match}')



        return odds


    def parse_league(self, league_elements: list[str]):

        league_name = league_elements[0]
        matches_elements = league_elements[4:]
        BETS_SUSPENDED = 'SUSPENDAT'

        matches_elements = dedup_consecutive_live(matches_elements)

        live_positions = [i for i, elem in enumerate(matches_elements) if elem == 'Live']
        match_end_positions = [l+2 for l in live_positions]
        match_start_positions = [0] + match_end_positions[:-1]

        matches = []
        for start, end in zip(match_start_positions, match_end_positions):
            match_elems = matches_elements[start:end]
            matches.append(match_elems)

        match_id_to_state = {}
        for match in matches:
            for idx, val in enumerate(match):
                if val == 'Live':
                    live_idx = idx

            try:
                float(match[live_idx-1])
            except ValueError:
                logger.warning(f'Removing: {match[live_idx - 1]} - no-float value before "Live" in match: {match[-1]}')

                if match[live_idx-1].startswith(' '):
                    # this means that odd is not available, so we can fill it with 0
                    match[live_idx - 1] = '0.0'
                else:
                    match.remove(match[live_idx - 1])
                    live_idx -= 1

            # if odds' field starts with ' ' it means that previous odd is not availale (cannot be executed),
            # so lets fill it with 0

            if match[0] == BETS_SUSPENDED: # TODO add a note that betting is suspended
                odd1, oddX, odd2 = self.extract_oods(match[1:live_idx])
                assert match[live_idx] == 'Live'
                leftover = match[live_idx+1]
            else:
                odd1, oddX, odd2 = self.extract_oods(match[0:live_idx])
                assert match[live_idx] == 'Live'
                leftover = match[live_idx+1]

            team1, team2, score1, score2, match_time = self.extract_leftover(leftover)


            match_state = MatchState(
                match_time=match_time,
                score1=score1,
                score2=score2,
                bet_1_odd=odd1,
                bet_x_odd=oddX,
                bet_2_odd=odd2
            )
            match_id = MatchIdentifier(
                bookmaker=BETFAIR_ID,
                team1=team1,
                team2=team2,
                league=league_name,
                date=datetime.date.today(),
            )
            match_id_to_state[match_id] = match_state

        return match_id_to_state




    def parse_raw(self, raw: str) -> dict[MatchIdentifier, MatchState]:
        """
        Parse raw text to extract live match data.
        Expected format: "Live\nTeam1 score1 - score2 Team2 time′\n1\nX\n2\nodd1\noddX\nodd2"
        """
        matches = {}
        lines = raw.split('\n')


        PREMATCH_START_PHRASE = 'În curând'
        for idx, line in enumerate(lines):
            if line == PREMATCH_START_PHRASE:
                lines = lines[:idx]
                break


        x_positions =[]
        for position, line in enumerate(lines):
            if line == 'X':
                x_positions.append(position)

        league_positions = [pos - 2 for pos in x_positions]
        league_positions_ends = league_positions[1:] + [len(lines)]

        leagues = []
        for begin, end in zip(league_positions, league_positions_ends):
            leagues.append(lines[begin:end])

        matches = {}
        for l in leagues:
            league_matches = self.parse_league(l)
            matches.update(league_matches)

        return matches

    def fetch_raw(self, url: str = "https://www.betfair.ro/sport/inplay", wait_time: int = FETCH_WAIT_TIME_SEC) -> str:
         driver = self.driver

         print(f"Fetching: {url}")
         # use get only for the first call; afterward, just refresh to reduce overhead
         if not hasattr(self, "_loaded_once"):
             self._loaded_once = True
             driver.get(url)
         else:
             driver.refresh()

        # wait only for the minimal container you actually need (more specific than .zone-container)
         WebDriverWait(driver, wait_time).until(
             EC.presence_of_element_located((By.CLASS_NAME, "zone-container"))
         )

         # reading .text is O(size of DOM), can be heavy; if possible, narrow this down
         zone_container: str = driver.find_element(By.CLASS_NAME, "zone-container").text
         return zone_container

    def close(self):
        self.driver.quit()

if __name__ == "__main__":
    executor = Executor()
    executor.run()
