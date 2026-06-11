FROM python:3.12-slim

LABEL description="Betting Orchestrator — analytics, decisions, REST API"

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        fastapi \
        uvicorn[standard] \
        rapidfuzz \
        pyyaml

WORKDIR /app

# Copy all project modules needed by orchestrator
COPY analytics/       /app/analytics/
COPY decisions/       /app/decisions/
COPY ledger/          /app/ledger/
COPY orchestrator/    /app/orchestrator/
COPY dashboard/data_service.py  /app/dashboard/data_service.py
COPY dashboard/matcher.py       /app/dashboard/matcher.py
COPY dashboard/__init__.py      /app/dashboard/__init__.py
COPY dashboard/index.html       /app/dashboard/index.html
COPY v2_coincasino/sync_clock.py /app/v2_coincasino/sync_clock.py
COPY v2_coincasino/__init__.py   /app/v2_coincasino/__init__.py

RUN mkdir -p /app/ledger /app/match_database

ENV PYTHONPATH=/app
ENV DB_ROOT=/app/match_database
ENV LEDGER_PATH=/app/ledger/bets.db
ENV POLL_INTERVAL=2.0
ENV ORCHESTRATOR_PORT=8051

EXPOSE 8051

CMD ["python", "-m", "orchestrator.main"]

