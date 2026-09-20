#!/bin/bash
# Prints what is missing to run the project. Exit 1 if anything is missing.
cd "$(dirname "$0")/.."
miss=0
chk(){ if eval "$2" &>/dev/null; then echo "OK      $1"; else echo "MISSING $1  -> $3"; miss=1; fi; }
chk "docker"            "command -v docker"            "sudo apt install docker.io docker-compose-v2 (scrapers)"
chk "python venv"       "[ -x .venv/bin/python ]"      "sudo apt install python3-venv && ./scripts/setup_env.sh"
chk "python deps"       ".venv/bin/python -c 'import redis,pandas,fastapi,uvicorn,dash,rapidfuzz,yaml'" "./scripts/setup_env.sh"
chk "redis (optional)"  "command -v redis-server || (command -v docker)" "sudo apt install redis-server (bus falls back to memory)"
chk "match_database/"   "[ -d match_database ]"        "run scrapers, or ./scripts/run_local.sh (works empty)"
chk "vpn confs"         "[ -f vpns/pinnacle/manchester1_protonvpn-UK-232.conf ]" "restore vpns/*"
exit $miss
