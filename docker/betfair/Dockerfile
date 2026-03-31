# Betfair scraper — Python + Selenium + Chrome + WireGuard
FROM python:3.12-slim

# Install WireGuard, Chrome, and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wireguard-tools \
        iproute2 \
        iptables \
        curl \
        wget \
        gnupg \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
        > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir selenium requests

WORKDIR /app

# Copy scraper code
COPY v2_betfair/betfair_scraper.py /app/betfair_scraper.py
COPY v2/sync_clock.py /app/sync_clock.py

# Copy VPN config
COPY vpns/betfair/*.conf /etc/wireguard/wg0.conf

# Copy entrypoint
COPY docker/betfair/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Run Chrome in headless mode (no display in container)
ENV HEADLESS=1

# Data output directory
RUN mkdir -p /app/db

ENTRYPOINT ["/entrypoint.sh"]

