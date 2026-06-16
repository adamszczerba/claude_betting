"""
Fetch recent Docker container logs for each registered scraper.

Uses ``docker logs --tail N`` with a short timeout so the dashboard
callback never blocks for long.
"""

import subprocess
from typing import Dict

# Container names as declared in docker-compose.yml
CONTAINERS: Dict[str, str] = {
    "scraper-coincasino": "CoinCasino",
    "scraper-betfair": "Betfair",
    "scraper-betfair-exchange": "BetfairExchange",
    "scraper-bet365": "Bet365",
}

TAIL_LINES = 30
TIMEOUT_SEC = 2


def _fetch_logs(container: str, tail: int = TAIL_LINES) -> str:
    """Return the last *tail* log lines for *container*, or an error string."""
    try:
        result = subprocess.run(
            ["docker", "logs", "--tail", str(tail), container],
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SEC,
        )
        # Docker logs may come on stderr (python logging goes to stderr)
        out = result.stdout or result.stderr or "(no output)"
        return out.strip()
    except subprocess.TimeoutExpired:
        return "(timeout fetching logs)"
    except FileNotFoundError:
        return "(docker not found)"
    except Exception as exc:
        return f"(error: {exc})"


def _container_running(container: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", container],
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SEC,
        )
        return result.stdout.strip() == "true"
    except Exception:
        return False


def get_all_logs() -> Dict[str, dict]:
    """Return {container_name: {"label": ..., "running": bool, "logs": str}}."""
    out: Dict[str, dict] = {}
    for cname, label in CONTAINERS.items():
        running = _container_running(cname)
        logs = _fetch_logs(cname) if running else "(container not running)"
        out[cname] = {"label": label, "running": running, "logs": logs}
    return out
