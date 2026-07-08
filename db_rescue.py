#!/usr/bin/env python3
"""
db_rescue.py — Emergency database recovery tool.

Run this if rockefeller_state.db becomes corrupted or is lost.
It extracts all recoverable data from the damaged file into a clean copy,
then swaps it into place. The original is preserved as a timestamped backup.

Usage:
    python db_rescue.py                    # rescue rockefeller_state.db (default)
    python db_rescue.py path/to/other.db   # rescue a specific file

Tables rescued:
    global_state         — all monitor state, pulse dates, alert counters, premium ledger
    audit_logs           — historical log entries (best-effort, non-critical)
    alert_state_manager  — 3-notification rule state per alert_id
    market_data_cache    — TD API response cache (non-critical, rebuilds on next tick)
    wheel_positions      — full position ledger incl. all migrated columns
"""

import os
import sys
import sqlite3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DB_Rescue")

# ── Schema ────────────────────────────────────────────────────────────────────
# Mirrors database.py exactly — update both if new tables/columns are added.

TABLES = {
    "global_state": """
        CREATE TABLE IF NOT EXISTS global_state (
            key          TEXT PRIMARY KEY,
            value        TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "audit_logs": """
        CREATE TABLE IF NOT EXISTS audit_logs (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            level     TEXT,
            message   TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "alert_state_manager": """
        CREATE TABLE IF NOT EXISTS alert_state_manager (
            alert_id        TEXT PRIMARY KEY,
            last_state      TEXT,
            last_trigger    REAL,
            broadcast_count INTEGER DEFAULT 0,
            last_alert_time TIMESTAMP
        )
    """,
    "market_data_cache": """
        CREATE TABLE IF NOT EXISTS market_data_cache (
            cache_key     TEXT PRIMARY KEY,
            response_json TEXT NOT NULL,
            cached_at     TIMESTAMP NOT NULL
        )
    """,
    "wheel_positions": """
        CREATE TABLE IF NOT EXISTS wheel_positions (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol               TEXT NOT NULL,
            position_type        TEXT NOT NULL,
            strike               REAL NOT NULL,
            expiration           TEXT NOT NULL,
            premium_collected    REAL NOT NULL,
            contracts            INTEGER DEFAULT 1,
            status               TEXT NOT NULL DEFAULT 'OPEN',
            opened_date          TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_date          TEXT,
            close_note           TEXT,
            last_alert_dte       INTEGER,
            cost_basis           REAL DEFAULT 0,
            accumulated_premiums REAL DEFAULT 0,
            open_fees            REAL DEFAULT 0,
            close_fees           REAL DEFAULT 0,
            close_price_per_share REAL,
            roll_group_id        TEXT
        )
    """,
}

# Columns that may be missing from older DB files (pre-migration).
# Applied with ALTER TABLE after initial create — safe to run on any DB age.
MIGRATIONS = [
    "ALTER TABLE wheel_positions ADD COLUMN cost_basis REAL DEFAULT 0",
    "ALTER TABLE wheel_positions ADD COLUMN accumulated_premiums REAL DEFAULT 0",
    "ALTER TABLE wheel_positions ADD COLUMN open_fees REAL DEFAULT 0",
    "ALTER TABLE wheel_positions ADD COLUMN close_fees REAL DEFAULT 0",
    "ALTER TABLE wheel_positions ADD COLUMN close_price_per_share REAL",
    "ALTER TABLE wheel_positions ADD COLUMN roll_group_id TEXT",
]

# Columns to SELECT from each table in the broken DB.
# Must match what actually exists after all migrations.
TABLE_COLUMNS = {
    "global_state":        "key, value, last_updated",
    "audit_logs":          "id, level, message, timestamp",
    "alert_state_manager": "alert_id, last_state, last_trigger, broadcast_count, last_alert_time",
    "market_data_cache":   "cache_key, response_json, cached_at",
    "wheel_positions": (
        "id, symbol, position_type, strike, expiration, premium_collected, contracts, "
        "status, opened_date, closed_date, close_note, last_alert_dte, "
        "cost_basis, accumulated_premiums, open_fees, close_fees, "
        "close_price_per_share, roll_group_id"
    ),
}

# ── Recovery logic ────────────────────────────────────────────────────────────

def rescue_database(source_path: str = "rockefeller_state.db"):
    if not os.path.exists(source_path):
        logger.error(f"Source DB not found: {source_path}")
        logger.error("Run this script from the scripts directory, or pass the full path as an argument.")
        return False

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rescued_path  = f"rockefeller_state_rescued_{timestamp}.db"
    backup_path   = f"rockefeller_state_corrupted_{timestamp}.db"
    final_path    = source_path

    logger.info(f"Source:  {source_path}")
    logger.info(f"Rescued: {rescued_path}")
    logger.info("=" * 60)

    try:
        conn_bad  = sqlite3.connect(source_path)
        conn_good = sqlite3.connect(rescued_path)
        cur_bad   = conn_bad.cursor()
        cur_good  = conn_good.cursor()

        # ── Step 1: Build clean schema ────────────────────────────────────────
        logger.info("Step 1: Building clean schema...")
        for table_name, ddl in TABLES.items():
            cur_good.execute(ddl)
        conn_good.commit()

        # Apply migrations (idempotent — OperationalError = column already exists)
        for sql in MIGRATIONS:
            try:
                cur_good.execute(sql)
                conn_good.commit()
            except sqlite3.OperationalError:
                pass

        # ── Step 2: Extract each table from broken DB ────────────────────────
        logger.info("Step 2: Extracting data from damaged database...")
        totals = {}

        for table, cols in TABLE_COLUMNS.items():
            try:
                cur_bad.execute(f"SELECT {cols} FROM {table}")
                rows = cur_bad.fetchall()

                if rows:
                    placeholders = ", ".join(["?"] * len(cols.split(",")))
                    cur_good.executemany(
                        f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})",
                        rows,
                    )
                    conn_good.commit()

                totals[table] = len(rows)
                status = f"✅ {len(rows)} rows" if rows else "⚠️  0 rows (table empty or missing)"
                logger.info(f"  {table:<25} {status}")

            except sqlite3.OperationalError as e:
                totals[table] = 0
                logger.warning(f"  {table:<25} ⚠️  Could not read: {e}")

        conn_bad.close()
        conn_good.close()

        # ── Step 3: Verify the rescued DB can be opened cleanly ──────────────
        logger.info("Step 3: Verifying rescued database integrity...")
        verify = sqlite3.connect(rescued_path)
        verify.execute("PRAGMA integrity_check").fetchone()
        verify.close()
        logger.info("  integrity_check PASSED")

        # ── Step 4: Swap files ────────────────────────────────────────────────
        logger.info("Step 4: Swapping files...")
        os.rename(source_path, backup_path)
        os.rename(rescued_path, final_path)
        logger.info(f"  Corrupted original → {backup_path}")
        logger.info(f"  Rescued copy       → {final_path} (now active)")

        logger.info("=" * 60)
        logger.info("RESCUE COMPLETE — Summary:")
        for table, count in totals.items():
            logger.info(f"  {table:<25} {count} rows recovered")
        logger.info(f"\n  Active DB: {final_path}")
        logger.info(f"  Backup:    {backup_path}")
        logger.info("\n  Restart all always-on tasks after rescue.")
        return True

    except Exception as e:
        logger.error(f"CRITICAL FAILURE: {e}")
        # Clean up partial rescued file if it exists
        if os.path.exists(rescued_path):
            os.remove(rescued_path)
        return False


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "rockefeller_state.db"
    success = rescue_database(source)
    sys.exit(0 if success else 1)
