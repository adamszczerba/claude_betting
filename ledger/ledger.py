"""
Ledger: thread-safe bet record management on top of SQLite.

Usage
-----
>>> from ledger.ledger import Ledger
>>> db = Ledger("/app/ledger/bets.db")
>>> db.record_order(order)
>>> db.record_receipt(order_id, accepted_price=2.25, placed_at=datetime.now())
>>> db.settle_bet(order_id, pnl_eur=12.50)
>>> summary = db.get_pnl_summary()
"""

from __future__ import annotations

import datetime
import json
import sqlite3
import threading
from typing import Any, Dict, List, Optional

from ledger.schema import init_db, DB_PATH
from decisions.signal_router import BetOrder

__all__ = ["Ledger"]


class Ledger:
    """Thread-safe bet ledger backed by SQLite."""

    def __init__(self, path: str = DB_PATH):
        self._path = path
        self._conn = init_db(path)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def record_order(self, order: BetOrder,
                     odds_snapshot: Optional[dict] = None) -> None:
        """Insert a new BetOrder (status=PENDING)."""
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO bets
                  (id, created_at, bookmaker, match_key, team1, team2, tournament,
                   market, outcome, stake_eur, min_price, edge_pct,
                   signal_type, status, expiry_sec)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    order.id, order.created_at.isoformat(),
                    order.bookmaker, order.match_key,
                    order.team1, order.team2, order.tournament,
                    order.market, order.outcome,
                    order.stake_eur, order.min_price,
                    order.edge_pct, order.signal_type,
                    order.status, order.expiry_sec,
                ),
            )
            if odds_snapshot is not None:
                self._conn.execute(
                    "INSERT OR REPLACE INTO signals (bet_id, odds_snapshot_json) VALUES (?,?)",
                    (order.id, json.dumps(odds_snapshot)),
                )
            self._conn.commit()

    def record_receipt(
        self,
        order_id:       str,
        accepted_price: float,
        placed_at:      Optional[datetime.datetime] = None,
        requested_price: Optional[float]            = None,
    ) -> None:
        """Mark an order as PLACED after execution succeeds."""
        placed_at = placed_at or datetime.datetime.now()
        with self._lock:
            self._conn.execute(
                """
                UPDATE bets
                SET status = 'PLACED',
                    accepted_price = ?,
                    requested_price = COALESCE(?, requested_price),
                    placed_at = ?
                WHERE id = ?
                """,
                (accepted_price, requested_price,
                 placed_at.isoformat(), order_id),
            )
            self._conn.commit()

    def update_status(self, order_id: str, status: str) -> None:
        """Generic status update (e.g. ABORTED, EXPIRED)."""
        with self._lock:
            self._conn.execute(
                "UPDATE bets SET status = ? WHERE id = ?",
                (status, order_id),
            )
            self._conn.commit()

    def settle_bet(
        self,
        order_id:   str,
        pnl_eur:    float,
        notes:      str = "",
        settled_at: Optional[datetime.datetime] = None,
    ) -> None:
        """Record the outcome of a placed bet."""
        settled_at = settled_at or datetime.datetime.now()
        status = "SETTLED_WIN" if pnl_eur >= 0 else "SETTLED_LOSS"
        with self._lock:
            self._conn.execute(
                "UPDATE bets SET status = ? WHERE id = ?",
                (status, order_id),
            )
            self._conn.execute(
                """
                INSERT INTO results (bet_id, settled_at, pnl_eur, notes)
                VALUES (?, ?, ?, ?)
                """,
                (order_id, settled_at.isoformat(), pnl_eur, notes),
            )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_pending_orders(self) -> List[Dict[str, Any]]:
        """Return all orders with status PENDING."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM bets WHERE status = 'PENDING' ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_all_bets(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Return recent bets, newest first."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM bets ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_pnl_summary(self) -> Dict[str, Any]:
        """Return aggregate P&L statistics."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS total_bets,
                    SUM(CASE WHEN status = 'PLACED' THEN 1 ELSE 0 END) AS open_bets,
                    SUM(CASE WHEN status = 'SETTLED_WIN' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN status = 'SETTLED_LOSS' THEN 1 ELSE 0 END) AS losses,
                    SUM(stake_eur) AS total_staked
                FROM bets
                WHERE status IN ('PLACED','SETTLED_WIN','SETTLED_LOSS')
                """
            ).fetchone()
            pnl_row = self._conn.execute(
                "SELECT COALESCE(SUM(pnl_eur), 0) AS total_pnl FROM results"
            ).fetchone()
            daily = self._conn.execute(
                """
                SELECT DATE(settled_at) AS day, SUM(pnl_eur) AS daily_pnl
                FROM results
                GROUP BY day
                ORDER BY day
                """
            ).fetchall()

        total_bets    = row["total_bets"] or 0
        wins          = row["wins"]       or 0
        losses        = row["losses"]     or 0
        total_staked  = row["total_staked"] or 0.0
        total_pnl     = pnl_row["total_pnl"]
        settled       = wins + losses
        win_rate      = (wins / settled * 100.0) if settled > 0 else 0.0
        roi           = (total_pnl / total_staked * 100.0) if total_staked > 0 else 0.0

        return {
            "total_bets":   total_bets,
            "open_bets":    row["open_bets"] or 0,
            "wins":         wins,
            "losses":       losses,
            "win_rate_pct": round(win_rate, 1),
            "total_staked": round(total_staked, 2),
            "total_pnl":    round(total_pnl, 2),
            "roi_pct":      round(roi, 1),
            "daily_pnl":    [{"day": r["day"], "pnl": r["daily_pnl"]} for r in daily],
        }

    def expire_stale_orders(self) -> int:
        """Mark PENDING orders that have exceeded their expiry_sec as EXPIRED.

        Returns the number of expired orders.
        """
        now = datetime.datetime.now()
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE bets
                SET status = 'EXPIRED'
                WHERE status = 'PENDING'
                  AND CAST((JULIANDAY(?) - JULIANDAY(created_at)) * 86400 AS INTEGER)
                      > expiry_sec
                """,
                (now.isoformat(),),
            )
            self._conn.commit()
        return cur.rowcount

