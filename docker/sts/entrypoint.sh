#!/bin/bash
set -e
echo "[entrypoint] No VPN required for STS."
echo "[entrypoint] Public IP:"
curl -s --max-time 10 https://ifconfig.me || echo "(could not reach ifconfig.me)"
echo ""
echo "[entrypoint] Starting STS scraper..."
exec python /app/sts_scraper.py -o /app/db "$@"
