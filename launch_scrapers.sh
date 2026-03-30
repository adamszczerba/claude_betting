#!/bin/bash
# launch_scrapers.sh
# Builds & starts both scraper containers, then opens two terminal windows
# showing live logs from each. Host networking is NOT affected.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Building & starting scraper containers..."
sudo docker compose up -d --build

echo ""
echo "==> Containers started. Opening log terminals..."

# Detect available terminal emulator
open_terminal() {
    local title="$1"
    local cmd="$2"

    if command -v gnome-terminal &>/dev/null; then
        gnome-terminal --title="$title" -- bash -c "$cmd; exec bash" &
    elif command -v xterm &>/dev/null; then
        xterm -title "$title" -e bash -c "$cmd; exec bash" &
    elif command -v konsole &>/dev/null; then
        konsole --title "$title" -e bash -c "$cmd; exec bash" &
    elif command -v xfce4-terminal &>/dev/null; then
        xfce4-terminal --title="$title" -e "bash -c '$cmd; exec bash'" &
    else
        echo "No supported terminal emulator found. Run manually:"
        echo "  $cmd"
    fi
}

sleep 2

open_terminal "CoinCasino Scraper (PL VPN)" \
    "echo '=== CoinCasino Scraper — ProtonVPN Poland ===' && sudo docker compose -f $SCRIPT_DIR/docker-compose.yml logs -f coincasino"

sleep 1

open_terminal "Betfair Scraper (UK VPN)" \
    "echo '=== Betfair Scraper — ProtonVPN UK ===' && sudo docker compose -f $SCRIPT_DIR/docker-compose.yml logs -f betfair"

echo ""
echo "==> Done! Two terminal windows should open with live logs."
echo "    CSV files will appear in:"
echo "      match_database/coincasino/<YYYY-MM-DD>/"
echo "      match_database/betfair/<YYYY-MM-DD>/"
echo ""
echo "    To stop all scrapers:  sudo docker compose down"

