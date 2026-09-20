# Quickstart (read this instead of exploring; keep token use low)
- Layout: scrapers/v2_* (CSV -> match_database/<bookie>/<date>/ + Redis stream), analytics/, signals/, decisions/, orchestrator/main.py (poll loop + FastAPI :8051), ledger/, dashboard/index.html, execution/.
- Entry points: `./scripts/preflight.sh` (what's missing) -> `./scripts/setup_env.sh` -> `./scripts/run_local.sh` (orchestrator) / `./scripts/run_local.sh test` (pytest).
- Full system: `docker compose up -d --build` (scrapers need VPN confs in vpns/, redis service included). Root main.py is empty/unused.
- Redis optional locally: signals/bus.py falls back to in-memory.
- Docs: ARCHITECTURE.md, API_CONTRACTS.md, DECISIONS.md, TASKS.md; only read the one you need. Rules in AGENTS.md (never commit csv; commit msgs start '[AGENT]').
