# Pinnacle scraper — lightweight Python + WireGuard (UK VPN)
FROM python:3.12-slim

# Install WireGuard tools and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wireguard-tools \
        iproute2 \
        iptables \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir requests

WORKDIR /app

# Copy scraper code
COPY v2_pinnacle/pinnacle_scraper.py /app/pinnacle_scraper.py
COPY v2_coincasino/sync_clock.py /app/sync_clock.py

# Copy VPN config (will be overridden by volume mount if desired)
COPY vpns/pinnacle/*.conf /etc/wireguard/wg0.conf

# Copy entrypoint
COPY docker/pinnacle/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Data output directory
RUN mkdir -p /app/db

ENTRYPOINT ["/entrypoint.sh"]

