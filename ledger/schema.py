"""
SQLite schema definitions and migration for the bet ledger.

Tables
------
bets        — one row per BetOrder (placed or pending)
signals     — full odds snapshot JSON attached to each bet
results     — settlement records (pnl, settled_at)

Usage
-----
>>> from ledger.schema import init_db
>>> conn = init_db("/app/ledger/bets.db")
"""

from __future__ import annotations

import sqlite3
from typing import Optional

__all__ = ["init_db", "DB_PATH"]

DB_PATH = "ledger/bets.db"

_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS bets (
    id               TEXT PRIMARY KEY,
    created_at       TEXT NOT NULL,
    bookmaker        TEXT NOT NULL DEFAULT 'coincasino',
    match_key        TEXT NOT NULL,
    team1            TEXT NOT NULL DEFAULT '',
    team2            TEXT NOT NULL DEFAULT '',
    tournament       TEXT NOT NULL DEFAULT '',
    market           TEXT NOT NULL,
    outcome          TEXT NOT NULL,
    stake_eur        REAL NOT NULL,
    min_price        REAL NOT NULL,
    requested_price  REAL,
    accepted_price   REAL,
    edge_pct         REAL NOT NULL DEFAULT 0,
    signal_type      TEXT NOT NULL DEFAULT 'VALUE',
    status           TEXT NOT NULL DEFAULT 'PENDING',
    placed_at        TEXT,
    expiry_sec       INTEGER NOT NULL DEFAULT 30
);

CREATE TABLE IF NOT EXISTS signals (
    bet_id              TEXT PRIMARY KEY REFERENCES bets(id) ON DELETE CASCADE,
    odds_snapshot_json  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    bet_id       TEXT    NOT NULL REFERENCES bets(id) ON DELETE CASCADE,
    settled_at   TEXT    NOT NULL,
    pnl_eur      REAL    NOT NULL,
    notes        TEXT
);

CREATE INDEX IF NOT EXISTS idx_bets_status    ON bets(status);
CREATE INDEX IF NOT EXISTS idx_bets_match_key ON bets(match_key);
CREATE INDEX IF NOT EXISTS idx_results_bet_id ON results(bet_id);
"""


def init_db(path: str = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite database at *path*, apply schema, return connection."""
    import os
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(_DDL)
    conn.commit()
    return conn

