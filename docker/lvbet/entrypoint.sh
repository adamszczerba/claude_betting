#!/bin/bash
set -e
echo "[entrypoint] No VPN required for LVBet."
echo "[entrypoint] Public IP:"
curl -s --max-time 10 https://ifconfig.me || echo "(could not reach ifconfig.me)"
echo ""
echo "[entrypoint] Starting LVBet scraper..."
exec python /app/lvbet_scraper.py -o /app/db "$@"
