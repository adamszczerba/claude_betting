FROM python:3.12-slim

LABEL description="CoinCasino Playwright executor — stealth bet placement via ProtonVPN Poland"

# System deps: WireGuard, curl, Playwright system libraries
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wireguard-tools \
        iproute2 \
        iptables \
        curl \
        wget \
        unzip \
        ca-certificates \
        fonts-liberation \
        libasound2 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libcups2 \
        libdbus-1-3 \
        libdrm2 \
        libgbm1 \
        libglib2.0-0 \
        libgtk-3-0 \
        libnspr4 \
        libnss3 \
        libx11-6 \
        libxcomposite1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxrandr2 \
        libxss1 \
        libxtst6 \
        xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir \
        playwright \
        playwright-stealth \
        rapidfuzz \
        pyyaml

# Install Playwright browser (Chromium only)
RUN playwright install chromium

WORKDIR /app

# Copy all modules required by the executor
COPY analytics/        /app/analytics/
COPY decisions/        /app/decisions/
COPY ledger/           /app/ledger/
COPY execution/        /app/execution/
COPY dashboard/matcher.py        /app/dashboard/matcher.py
COPY dashboard/data_service.py   /app/dashboard/data_service.py
COPY dashboard/__init__.py       /app/dashboard/__init__.py
COPY v2_coincasino/sync_clock.py /app/v2_coincasino/sync_clock.py
COPY v2_coincasino/__init__.py   /app/v2_coincasino/__init__.py

RUN mkdir -p /app/ledger

COPY docker/executor/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PYTHONPATH=/app
ENV EXECUTION_MODE=dry_run
ENV CC_SESSION_PATH=/app/cc_session.json

ENTRYPOINT ["/entrypoint.sh"]

