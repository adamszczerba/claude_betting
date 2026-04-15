# LVBet scraper — lightweight Python, no VPN needed
FROM python:3.12-slim

# Minimal dependencies (no WireGuard)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir requests

WORKDIR /app

# Copy scraper code
COPY v2_lvbet/lvbet_scraper.py /app/lvbet_scraper.py
COPY v2_coincasino/sync_clock.py /app/sync_clock.py

# Copy entrypoint
COPY docker/lvbet/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Data output directory
RUN mkdir -p /app/db

ENTRYPOINT ["/entrypoint.sh"]

