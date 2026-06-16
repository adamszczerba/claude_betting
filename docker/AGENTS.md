## 5.12 Docker Services

(extended)

| Service | Container | VPN | Ports | New? |
|---------|-----------|-----|-------|------|
| `scraper-coincasino` | `v2_coincasino/` | ProtonVPN PL | — | |
| `scraper-betfair` | `v2_betfair/` | ProtonVPN UK | — | |
| `scraper-bet365` | `v2_bet365/` | ProtonVPN UK | — | |
| `scraper-betfair-exchange` | `v2_betfair_exchange/` | ProtonVPN UK | — | |
| `scraper-pinnacle` | `v2_pinnacle/` | ProtonVPN UK | — | |
| `scraper-lvbet` | `v2_lvbet/` | ProtonVPN PL | — | |
| `scraper-sts` | `v2_sts/` | ProtonVPN PL | — | |
| `signal-bus` | Redis/NATS | none | 6379 | ✅ |
| `comparator-signal` | `signals/comparator/` | none | — | ✅ (refactor) |
| `ml-signal` | `signals/ml/` | none | — | ✅ |
| `web-signal` | `signals/web/` | none | — | ✅ |
| `live-data-signal` | `signals/live_data/` | none | — | ✅ |
| `historical-db` | PostgreSQL+TimescaleDB | none | 5432 | ✅ |
| `ingestor` | `ingestion/` | none | — | ✅ |
| `orchestrator` | `docker/orchestrator/` | none | 8051 | |
| `executor-coincasino` | `docker/executor/` | ProtonVPN PL | — | |
| `dashboard` | `dashboard/` | none | 8050 | |
| `ml-trainer` | `ml/` (offline) | none | — | ✅ |