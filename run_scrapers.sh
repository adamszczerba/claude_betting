#!/bin/bash
# run_scrapers.sh
# Builds & launches all scrapers, each inside its own Docker container.
# VPN-tunnelled scrapers use isolated WireGuard tunnels:
#   - CoinCasino  → ProtonVPN Poland   (vpns/coincasino/)
#   - Betfair     → ProtonVPN UK       (vpns/betfair/)
#   - Pinnacle    → ProtonVPN UK       (vpns/pinnacle/)
# No-VPN scrapers (Polish bookmakers, publicly accessible):
#   - LVBet
#   - STS
#
# Scraped CSV files are persisted on the host under:
#   match_database/<bookmaker>/
#
# Logs are shown in separate terminal windows when a supported terminal
# emulator is found; otherwise they are merged (labeled) into this terminal.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Helpers ──────────────────────────────────────────────────────────────
DOCKER="docker"
command -v docker &>/dev/null || { echo "ERROR: docker not found"; exit 1; }

# Use sudo only when the current user is not in the docker group
if ! docker info &>/dev/null 2>&1; then
    echo "[*] Docker requires sudo — authenticating now..."
    sudo -v                       # cache credentials once
    DOCKER="sudo docker"
fi

export COMPOSE="$DOCKER compose"

# Detect a GUI terminal emulator
find_terminal() {
    for t in gnome-terminal konsole xfce4-terminal xterm; do
        command -v "$t" &>/dev/null && echo "$t" && return
    done
}

open_log_terminal() {
    local title="$1"
    local container="$2"
    local term
    term=$(find_terminal)

    # Use "docker logs" directly — avoids compose path issues and sudo
    # re-prompts in spawned terminals.
    local cmd="$DOCKER logs -f --tail=100 $container"

    case "$term" in
        gnome-terminal)
            gnome-terminal --title="$title" -- bash -c "echo '=== $title ==='; $cmd; exec bash" &
            ;;
        konsole)
            konsole --title "$title" -e bash -c "echo '=== $title ==='; $cmd; exec bash" &
            ;;
        xfce4-terminal)
            xfce4-terminal --title="$title" -e "bash -c 'echo \"=== $title ===\"; $cmd; exec bash'" &
            ;;
        xterm)
            xterm -title "$title" -e bash -c "echo '=== $title ==='; $cmd; exec bash" &
            ;;
        *)
            return 1   # no terminal found
            ;;
    esac
}

# ── Main ─────────────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════╗"
echo "║         Betting Scrapers — Docker Launcher          ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  CoinCasino       VPN: ProtonVPN Poland (WireGuard) ║"
echo "║  Betfair          VPN: ProtonVPN UK     (WireGuard) ║"
echo "║  Betfair Exchange VPN: ProtonVPN UK     (WireGuard) ║"
echo "║  Bet365           VPN: ProtonVPN UK     (WireGuard) ║"
echo "║  Pinnacle         VPN: ProtonVPN UK     (WireGuard) ║"
echo "║  LVBet            No VPN                            ║"
echo "║  STS              No VPN                            ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# 1. Build & start containers
echo "[*] Building & starting scraper containers..."
$COMPOSE up -d --build
echo ""

# 2. Show running containers
echo "[*] Running containers:"
$DOCKER ps --filter "name=scraper-" --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"
echo ""

# 3. Display where CSVs are saved
echo "[*] Match data will be stored in:"
echo "      match_database/coincasino/"
echo "      match_database/betfair/"
echo "      match_database/betfair_exchange/"
echo "      match_database/bet365/"
echo "      match_database/pinnacle/"
echo "      match_database/lvbet/"
echo "      match_database/sts/"
echo ""

# 4. Start the dashboard
echo "[*] Starting Betting Dashboard on http://127.0.0.1:8050 ..."
# Kill any stale dashboard instance
fuser -k 8050/tcp &>/dev/null || true
sleep 1
# Activate venv and launch dashboard in background
(
    source "$SCRIPT_DIR/.venv/bin/activate" 2>/dev/null || true
    nohup python "$SCRIPT_DIR/dashboard/app.py" > /tmp/dashboard.log 2>&1 &
)
sleep 2
if curl -s -o /dev/null -w '' http://127.0.0.1:8050/ 2>/dev/null; then
    echo "[*] Dashboard is live at http://127.0.0.1:8050"
    # Try to open in browser
    xdg-open http://127.0.0.1:8050 2>/dev/null || true
else
    echo "[!] Dashboard may not have started — check /tmp/dashboard.log"
fi
echo ""

# 5. Open logs
TERM_EMU=$(find_terminal)

if [ -n "$TERM_EMU" ]; then
    echo "[*] Opening live log terminals ($TERM_EMU)..."
    sleep 2
    open_log_terminal "CoinCasino Scraper (PL VPN)" "scraper-coincasino"
    sleep 1
    open_log_terminal "Betfair Scraper (UK VPN)"    "scraper-betfair"
    sleep 1
    open_log_terminal "Betfair Exchange Scraper (UK VPN)" "scraper-betfair-exchange"
    sleep 1
    open_log_terminal "Bet365 Scraper (UK VPN)" "scraper-bet365"
    sleep 1
    open_log_terminal "Pinnacle Scraper (UK VPN)" "scraper-pinnacle"
    sleep 1
    open_log_terminal "LVBet Scraper (no VPN)" "scraper-lvbet"
    sleep 1
    open_log_terminal "STS Scraper (no VPN)" "scraper-sts"
    echo ""
    echo "[*] Done! Seven terminal windows should now show live logs."
else
    echo "[*] No GUI terminal found — showing merged logs below."
    echo "    Press Ctrl+C to stop following logs (containers keep running)."
    echo ""
    # Follow all services in this terminal — docker compose labels each line
    $COMPOSE logs -f --tail=50 coincasino betfair betfair_exchange bet365 pinnacle lvbet sts
fi

echo ""
echo "[*] To stop all scrapers:  $COMPOSE down"

