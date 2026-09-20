#!/bin/bash
# One-time local env setup (needs: sudo apt install python3-venv, or uv).
cd "$(dirname "$0")/.."
if command -v uv &>/dev/null; then uv venv .venv && uv pip install -p .venv/bin/python -r requirements.txt
else python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; fi
