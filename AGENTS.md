# Betting Scraper Project Guidelines

## rules
never add .csv to git, keep all csv in match_database, if you need some for testing, clean it later 

## handling knowledge in .md files
Top level software architecture is described in ARCHITECTURE.md. Keep it updated. For more details on software modules, see their respective .md files in their directories.
Decisions with explanation are in DECISIONS.md. Use it to answer questions yourself. Update it when you make decision.
Problem domain and motivation is in DOMAIN.md. Use it to understand the goal and motivation of the project. Update it when you have new insights about the domain.
API between main system components is in API_CONTRACTS.md. Trust it until you have clear evidence that it is wrong. Update it when you make decision about API change.
Use TASKS.md to track your work and progress. Update it when you start working on something or finish it.

##
## env and run


## testing
At the end of each task, ask if run whole system

## version control
- **Always `git add` all new and modified files after completing a change** — run `git add -A` or `git add .` before committing
- Only `match_database/` is excluded from tracking (via `.gitignore`); everything else must be committed
- Make commits for each meaningful change, msg starts with '[AGENT]'

## architecture
## dev rules



## Architecture

### v2_* Pattern (Modern, Preferred)
All active scrapers follow the **v2_*** naming convention (`v2_betfair/`, `v2_bet365/`, `v2_coincasino/`, `v2_betfair_exchange/`):
- **Functional design** with modular helper functions
- **Single JavaScript execution** for fast DOM parsing (<0.2s for 50+ matches)
- **Streaming CSV writes** (append on every poll, never rewrite entire file)
- **Standardized schema** across all bookmakers
- **Shared utilities**: `sync_clock.py` for synchronized wall-clock polling, `MatchCSVWriter` pattern

### Legacy Code
The `betfair/` directory contains old object-oriented code (kept for reference only). Do not follow these patterns for new work.

## Build and Test

### Running Scrapers
```bash
# Build & start all scrapers with VPNs
docker compose up -d --build

# View logs for specific scraper
docker compose logs -f betfair

# Stop all containers
docker compose down

# Launch with automatic log terminals (gnome-terminal, xterm, etc.)
./run_scrapers.sh
```

### Testing
```bash
# Activate virtual environment
source .venv/bin/activate

# Run unit tests (pytest)
python -m pytest v2_betfair/test_betfair_scraper.py -v
python -m pytest v2_bet365/test_bet365_scraper.py -v
python -m pytest v2_betfair_exchange/test_betfair_exchange_scraper.py -v
```

**Testing Philosophy**: Unit tests focus on data transformation functions (odds parsing, format conversion). Live scraping is tested manually via Docker.

### Direct Scraper Development
```bash
# API-based scraper (no browser required)
python v2_coincasino/coincasino_scraper.py --interval 5 -o output_dir

# Selenium-based scrapers (requires Chrome)
python v2_betfair/betfair_scraper.py --interval 2 -o output_dir
```

## Code Conventions

### Data Storage Structure
All scraped data goes to `match_database/<bookmaker>/<YYYY-MM-DD>/`:
```
match_database/
├── betfair/2026-04-07/TeamA_vs_TeamB_League_bf_2026-04-07.csv
├── bet365/2026-04-07/TeamA_vs_TeamB_League_b365_2026-04-07.csv
├── coincasino/2026-04-07/TeamA_vs_TeamB_League_cc_2026-04-07.csv
└── betfair_exchange/2026-04-07/TeamA_vs_TeamB_League_bfx_2026-04-07.csv
```

### CSV Schema (Standardized)
**All scrapers MUST use this column order**:
```csv
timestamp,match_time,match_status,home_score,away_score,odd_1,odd_X,odd_2,total_line,odd_over,odd_under
```
- `timestamp`: ISO 8601 with milliseconds (e.g., `2026-04-06T14:06:05.107`)
- `match_time`: Match minute (e.g., `67:23` or `HT`)
- `match_status`: `HT`, `FT`, `AET`, `PEN`, or empty during live play
- `odd_1/X/2`: Decimal odds for home win / draw / away win
- `total_line/odd_over/odd_under`: Over/under market (if available)

**CoinCasino Extended Schema**: Has 13 additional columns (double chance, draw no bet, penalties, correct score, corners, bookings) — see `v2_coincasino/coincasino_scraper.py` docstring.

### File Naming Convention
```
{team1}_vs_{team2}_{tournament}_{bookmaker_tag}_{date}.csv
```
- Sanitize special characters: `[\/*?:"<>|]` → `_`
- Tags: `bf` (Betfair), `b365` (Bet365), `cc` (CoinCasino), `bfx` (Betfair Exchange)

### Clock Synchronization Pattern
**All scrapers MUST use synchronized polling** via `sync_clock.py`:

```python
from scrapers.v2_coincasino import sleep_until_next_tick

while True:
    events = scraper.fetch_events()
    for event in events:
        writer.write(event)
    sleep_until_next_tick(interval=2.0)  # Aligns to wall-clock boundaries
```
**Why**: Docker containers share the host kernel clock → sub-millisecond synchronization across all scrapers without external coordination. This ensures timestamps align for cross-bookmaker analysis.

### MatchCSVWriter Pattern
One CSV file per match, lazy file creation, append-only writes:
```python
writer = MatchCSVWriter(output_dir="match_database/betfair/2026-04-07")
writer.write({
    "team1": "Liverpool",
    "team2": "Arsenal",
    "tournament": "Premier League",
    ...
})
```
File created on first write, header written once, subsequent writes append rows.

## Key Technologies

- **Python 3.12** (venv at `.venv/`)
- **Selenium WebDriver** + Chrome/Chromium (for Betfair, Bet365, Betfair Exchange)
- **Docker** with WireGuard VPN tunneling (ProtonVPN configs in `vpns/`)
- **requests** (for CoinCasino API scraping)

## Docker + VPN Architecture

### Container Isolation
Each scraper runs in its own container with an isolated WireGuard VPN tunnel:
- **CoinCasino**: ProtonVPN Poland (fast API calls)
- **Betfair/Bet365/Exchange**: ProtonVPN UK (geo-restricted sites)

**Key**: VPN tunnels exist only inside container network namespace → **host networking is never affected**.

### VPN Configuration
WireGuard configs mounted as read-only volumes from `vpns/<scraper>/`:
```yaml
volumes:
  - ./vpns/betfair/manchester1_protonvpn-UK-232.conf:/etc/wireguard/wg0.conf:ro
```
Swap VPN configs without rebuilding containers.

### Entrypoint Script Pattern
All `docker/*/entrypoint.sh` scripts follow this pattern:
1. Extract Address/DNS from WireGuard config
2. Strip `wg-quick` extensions, create clean config
3. Create WireGuard interface manually (no `wg-quick` in slim containers)
4. Configure policy routing (fwmark + table 51820)
5. Verify public IP: `curl -s https://ifconfig.me`
6. Launch Python scraper with `exec python /app/scraper.py -o /app/db`

**Why manual setup?** Full control over routing rules, works in `python:3.12-slim` base image.

## Scraper-Specific Notes

### Betfair (v2_betfair/)
- **Challenge**: Hashed CSS class names → use partial matching `[class*=teamNameLabel]`
- **Odds Format**: Fractional → decimal conversion (e.g., `"2/7"` → `"1.29"`)
- **Performance**: JavaScript injection extracts all matches in one call (~0.2s)

### Bet365 (v2_bet365/)
- **Challenge**: Heavily obfuscated DOM, multiple fallback strategies for element detection
- **Odds Format**: Decimal (native)

### CoinCasino (v2_coincasino/)
- **Advantage**: API-based (REST), no browser overhead → fastest scraper
- **Clock Feature**: Computes live match time from API clock + wall-clock elapsed time
- **Markets**: Most comprehensive (13 additional columns beyond standard schema)

### Betfair Exchange (v2_betfair_exchange/)
- **Odds Calculation**: Average of back (buy) and lay (sell) prices
- **Purpose**: Exchange odds provide market consensus (different from bookmaker fixed odds)

## Known Issues & TODOs

See `todos.py` and `betfair/todo`:
- **Bug**: Match time "90+10" parsed as "10" (needs regex fix)
- **Enhancement**: Add row to store match suspension state
- **Data Continuity**: Betfair page refresh mechanism needed to avoid missing data during reload

### Recently Fixed (April 2026)
- ✅ **Chrome/ChromeDriver detection**: Added explicit Chrome binary path and ChromeDriver installation in Dockerfiles to fix "NoSuchDriverException" errors
- ✅ **Betfair Exchange timeouts**: Added retry logic with fallback to full page reload for "timeout receiving message from renderer" errors
- ✅ **Better bet365 logging**: Enhanced error handling and diagnostic logging to identify DOM parsing issues

## Development Workflow

1. **Activate venv**: `source .venv/bin/activate`
2. **Run scraper locally** (for debugging): `python v2_betfair/betfair_scraper.py`
3. **Write unit tests** for data transformation functions
4. **Build Docker image**: `docker compose build betfair`
5. **Test in container**: `docker compose up betfair`
6. **Check VPN**: `docker exec scraper-betfair curl -s https://ifconfig.me`

## Troubleshooting

### Selenium ChromeDriver Issues
**Symptoms**: `NoSuchDriverException`, "Unable to obtain driver for chrome", "error sending request"

**Solution**: Ensure Dockerfile includes:
```dockerfile
# Install ChromeDriver matching Chrome version
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1) && \
    CHROMEDRIVER_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_VERSION}") && \
    wget -q "https://storage.googleapis.com/chrome-for-testing-public/${CHROMEDRIVER_VERSION}/linux64/chromedriver-linux64.zip" && \
    unzip chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf chromedriver-linux64.zip chromedriver-linux64

ENV CHROME_BIN=/usr/bin/google-chrome
```

And scraper code explicitly sets Chrome binary:
```python
chrome_bin = os.environ.get("CHROME_BIN", "/usr/bin/google-chrome")
if os.path.exists(chrome_bin):
    options.binary_location = chrome_bin
```

### Selenium Timeout Errors
**Symptoms**: "TimeoutException: timeout: Timed out receiving message from renderer"

**Solution**: Add retry logic with fallback to full page reload:
```python
def _refresh_page(self, driver) -> None:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            driver.refresh()
            self._wait_for_content(driver)
            return
        except Exception as e:
            if attempt < max_retries - 1:
                log.warning(f"Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                # Full reload as fallback
                self._load_page(driver)
```

### Bet365 Not Finding Matches
**Symptoms**: "Football content detected but no matches parsed"

**Check**:
1. Verify page is loading: `docker exec scraper-bet365 curl -I https://www.bet365.com`
2. Check logs for JavaScript errors
3. May indicate DOM structure change — inspect live page and update `_JS_EXTRACT_ALL` selectors

### VPN Connection Issues
**Check public IP**: `docker exec scraper-betfair curl -s https://ifconfig.me`
- Should show VPN exit node IP, not your real IP
- UK scrapers: Betfair, Bet365, Exchange
- Poland scraper: CoinCasino

## Critical Gotchas

1. **Never use `wg-quick`** in Dockerfiles → manual WireGuard setup required for slim images
2. **CSV writes are append-only** → never rewrite entire file (causes data loss during concurrent reads)
3. **Match time format**: Must handle special values (`HT`, `90+5`, penalty shootouts)
4. **Selenium containers need shm_size: "2g"** → Chrome fails without sufficient shared memory
5. **Clock synchronization is critical** → always use `sleep_until_next_tick()` for polling
6. **Betfair has 3 separate sources**: Sportsbook (`v2_betfair/`), Exchange (`v2_betfair_exchange/`), and legacy (`betfair/`) — only use v2_* modules

## Reference Documentation

- Main README: `README.md` (Docker commands)
- Betfair philosophy: `betfair/README` (source of truth for odds benchmarking)
- Each scraper has detailed docstring header with usage examples
