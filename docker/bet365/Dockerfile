# Bet365 scraper — Python + Selenium + Chrome + WireGuard
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

# Install ChromeDriver
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1) && \
    CHROMEDRIVER_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_VERSION}") && \
    wget -q "https://storage.googleapis.com/chrome-for-testing-public/${CHROMEDRIVER_VERSION}/linux64/chromedriver-linux64.zip" && \
    unzip chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf chromedriver-linux64.zip chromedriver-linux64

# Python deps
RUN pip install --no-cache-dir selenium requests

WORKDIR /app

# Copy scraper code
COPY v2_bet365/bet365_scraper.py /app/bet365_scraper.py
COPY v2_coincasino/sync_clock.py /app/sync_clock.py

# Copy VPN config (same UK proxy as betfair)
COPY vpns/betfair/*.conf /etc/wireguard/wg0.conf

# Copy entrypoint
COPY docker/bet365/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Run Chrome in headless mode (no display in container)
ENV HEADLESS=1
ENV CHROME_BIN=/usr/bin/google-chrome

# Data output directory
RUN mkdir -p /app/db

ENTRYPOINT ["/entrypoint.sh"]

