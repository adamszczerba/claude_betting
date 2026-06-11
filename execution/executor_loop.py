"""
Executor poll loop — runs inside the Docker executor container.

Polls ledger/bets.db for PENDING orders, places them via the appropriate
executor (DryRunExecutor or CoinCasinoExecutor based on EXECUTION_MODE),
then writes the receipt back into the ledger.

Environment
-----------
  EXECUTION_MODE    "dry_run" (default) | "live"
  LEDGER_PATH       path to bets.db (default: /app/ledger/bets.db)
  CC_EMAIL          CoinCasino login e-mail (required for live mode)
  CC_PASSWORD       CoinCasino login password (required for live mode)
  CC_SESSION_PATH   path to saved Playwright session JSON

Loop interval: 2 seconds (aligned to sync_clock wall-clock ticks).
"""

from __future__ import annotations

import datetime
import logging
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)s  %(name)s  %(message)s",
    datefmt= "%H:%M:%S",
)
log = logging.getLogger("executor_loop")

LEDGER_PATH    = os.environ.get("LEDGER_PATH", "/app/ledger/bets.db")
EXECUTION_MODE = os.environ.get("EXECUTION_MODE", "dry_run").lower()
POLL_SEC       = 2.0


def _build_executor():
    if EXECUTION_MODE == "live":
        log.warning("⚠  EXECUTION_MODE=live — REAL BETS WILL BE PLACED ⚠")
        from execution.cc_executor import CoinCasinoExecutor
        return CoinCasinoExecutor()
    else:
        log.info("EXECUTION_MODE=dry_run — paper trading only.")
        from execution.dry_run import DryRunExecutor
        return DryRunExecutor()


def main() -> None:
    from ledger.ledger import Ledger
    from execution.base import PriceDriftError, ExecutionError
    from v2_coincasino.sync_clock import sleep_until_next_tick

    ledger   = Ledger(LEDGER_PATH)
    executor = _build_executor()

    log.info("Executor loop started.  mode=%s  ledger=%s", EXECUTION_MODE, LEDGER_PATH)

    try:
        while True:
            _process_pending(ledger, executor)
            sleep_until_next_tick(POLL_SEC)
    except KeyboardInterrupt:
        log.info("Shutting down executor.")
    finally:
        executor.close()


def _process_pending(ledger, executor) -> None:
    from execution.base import PriceDriftError, ExecutionError
    from decisions.signal_router import BetOrder

    pending = ledger.get_pending_orders()
    if not pending:
        return

    log.info("%d pending order(s) to process.", len(pending))

    for row in pending:
        order = BetOrder(
            id          = row["id"],
            created_at  = datetime.datetime.fromisoformat(row["created_at"]),
            bookmaker   = row["bookmaker"],
            match_key   = row["match_key"],
            team1       = row.get("team1", ""),
            team2       = row.get("team2", ""),
            tournament  = row.get("tournament", ""),
            market      = row["market"],
            outcome     = row["outcome"],
            stake_eur   = row["stake_eur"],
            min_price   = row["min_price"],
            edge_pct    = row.get("edge_pct", 0.0),
            signal_type = row.get("signal_type", "VALUE"),
            expiry_sec  = row.get("expiry_sec", 30),
        )

        # Check expiry
        age = (datetime.datetime.now() - order.created_at).total_seconds()
        if age > order.expiry_sec:
            ledger.update_status(order.id, "EXPIRED")
            log.info("Order %s expired (age=%.0fs)", order.id[:8], age)
            continue

        try:
            receipt = executor.place_bet(order)
            ledger.record_receipt(
                order_id        = order.id,
                accepted_price  = receipt.accepted_price,
                placed_at       = receipt.placed_at,
                requested_price = order.min_price,
            )
            log.info("Placed: %s  receipt=%s  price=%.4f",
                     order.id[:8], receipt.receipt_id, receipt.accepted_price)

        except PriceDriftError as e:
            ledger.update_status(order.id, "ABORTED")
            log.warning("Price drift — order %s aborted: %s", order.id[:8], e)

        except Exception as e:
            ledger.update_status(order.id, "ABORTED")
            log.error("Execution error — order %s aborted: %s", order.id[:8], e)


if __name__ == "__main__":
    main()

