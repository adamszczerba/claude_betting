# CoinCasino scraper — lightweight Python + WireGuard
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
COPY v2/coincasino_scraper.py /app/coincasino_scraper.py

# Copy VPN config (will be overridden by volume mount if desired)
COPY vpns/coincasino/*.conf /etc/wireguard/wg0.conf

# Copy entrypoint
COPY docker/coincasino/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Data output directory
RUN mkdir -p /app/db

ENTRYPOINT ["/entrypoint.sh"]

