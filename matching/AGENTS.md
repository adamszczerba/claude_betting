Problem: different bookmakers use different team and competition labels, timestamps and occasionally omit metadata. Cross-bookmaker signals and historical joins require a stable canonical identity.

Design:
- `source_event_id` — optional, scraper-provided id when the bookmaker exposes a stable identifier (keep in `raw_json` and `source_poll_id`).
- `canonical_match_id` — produced by a dedicated `matching` service/module. The matcher uses normalized team names, competition aliases, kickoff window heuristics and fuzzy matching to emit a canonical id and a `matching_confidence` score.
- `matching_map` — a small mapping table maintained by the ingestor that links `source_event_id` (and scraper+row_fingerprint) to `canonical_match_id` with timestamps and manual overrides.

Rules:
- All signal providers and the decision engine MUST reference `canonical_match_id` when correlating events across bookmakers.
- The matcher SHOULD emit `matching_confidence`; the decision engine can apply stricter gates (e.g., only use signals with confidence >= 0.7 for automated bets).
- Manual override capability: admin can patch `matching_map` entries for high-value leagues to fix recurring mismatches.

Operational note: implement the matcher as a small Python module initially (library + CLI) that runs in the ingestor process; promote to a dedicated microservice only if scaling/matching throughput demands it.
