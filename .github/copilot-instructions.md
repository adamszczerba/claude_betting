# Betting Scraper Project Guidelines

## ⚠️ CRITICAL COPILOT BEHAVIOR RULES (STRICT ENFORCEMENT)

- **STRICT FILE ACCESS**: DO NOT open, read, or search any file using tools (e.g., `read_file`, `open_file`, `file_search`, `grep_search`, `list_dir`) unless it has been explicitly provided in the user's current context or manually attached by the user.
- **ASK FOR PERMISSION**: If you determine that reading a new file is absolutely necessary to fulfill the user's request, you **MUST ask the user for explicit permission first**. State clearly which file you want to open and why.
- **NO AUTONOMOUS EXPLORATION**: Do not autonomously explore the codebase, search for files, or read files outside the provided context without prior user approval.
- **Rely on Context**: Base your answers and code modifications strictly on the files and information already provided in the conversation context.

---

This project scrapes live soccer betting odds from multiple bookmakers (Betfair, Bet365, CoinCasino, Betfair Exchange) using isolated Docker containers with WireGuard VPN tunneling. The goal is value bets detection.


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

## Key Technologies

- **Python 3.12** (venv at `.venv/`)
- **Selenium WebDriver** + Chrome/Chromium (for Betfair, Bet365, Betfair Exchange)
- **Docker** with WireGuard VPN tunneling (ProtonVPN configs in `vpns/`)
- **requests** (for CoinCasino API scraping)
