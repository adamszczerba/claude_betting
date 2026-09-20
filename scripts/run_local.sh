#!/bin/bash
# Run orchestrator + dashboard API locally (no docker) on :8051, then run tests.
cd "$(dirname "$0")/.."
mkdir -p match_database ledger
export DB_ROOT=$PWD/match_database LEDGER_PATH=$PWD/ledger/bets.db POLL_INTERVAL=2.0 ORCHESTRATOR_PORT=8051
[ "$1" = test ] && exec .venv/bin/python -m pytest tests dashboard -q -x
exec .venv/bin/python -m orchestrator.main
