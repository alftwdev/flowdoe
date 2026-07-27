#!/usr/bin/env python3
"""
db_tools.py — Unified database maintenance utility.

Replaces audit.py (daily cron), db_rescue.py (emergency recovery),
and seed_cef_premiums.py (one-time setup) with a single file.

Usage:
    python db_tools.py                  # daily maintenance (same as audit.py)
    python db_tools.py --rescue         # emergency DB recovery (same as db_rescue.py)
    python db_tools.py --rescue /path/to/other.db
    python db_tools.py --seed-premiums  # one-time CEF z-score initialization

PythonAnywhere cron (daily maintenance — keep this entry, remove audit.py entry):
    09:39 UTC    python db_tools.py
"""

import os
import sys
import sqlite3
import logging
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DB_Tools")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# ── Daily maintenance (was audit.py) ─────────────────────────────────────────

def run_daily_maintenance(db_path: str = "rockefeller_state.db"):
    """
    Daily cron: prune stale alert locks (> 24h), cap audit_logs at 500 rows,
    VACUUM the DB. Runs in ~1s; zero TD API calls.
    """
    from database import EcosystemDatabase
    db_full = os.path.join(BASE_DIR, db_path)
    logger.info("Starting daily DB maintenance...")
    try:
        with sqlite3.connect(db_full, timeout=10.0) as conn:
            cur = conn.cursor()
            purge_threshold = (datetime.now() - timedelta(hours=24)).isoformat()
            cur.execute("DELETE FROM alert_state_manager WHERE last_alert_time < ?", (purge_threshold,))
            purged_alerts = cur.rowcount
            cur.execute(
                "DELETE FROM audit_logs WHERE id NOT IN "
                "(SELECT id FROM audit_logs ORDER BY id DESC LIMIT 500)"
            )
            purged_logs = cur.rowcount
            conn.commit()
            logger.info(f"Purged {purged_alerts} stale alert locks, {purged_logs} old log entries.")

        with sqlite3.connect(db_full, timeout=10.0) as conn:
            conn.isolation_level = None  # VACUUM requires auto-commit
            conn.execute("VACUUM")
            logger.info("VACUUM complete.")

        EcosystemDatabase().purge_expired_cache()
        logger.info("Daily maintenance done.")
        return True
    except Exception as e:
        logger.critical(f"Daily maintenance failed: {e}")
        return False


# ── Emergency recovery (was db_rescue.py) ────────────────────────────────────

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
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol                TEXT NOT NULL,
            position_type         TEXT NOT NULL,
            strike                REAL NOT NULL,
            expiration            TEXT NOT NULL,
            premium_collected     REAL NOT NULL,
            contracts             INTEGER DEFAULT 1,
            status                TEXT NOT NULL DEFAULT 'OPEN',
            opened_date           TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_date           TEXT,
            close_note            TEXT,
            last_alert_dte        INTEGER,
            cost_basis            REAL DEFAULT 0,
            accumulated_premiums  REAL DEFAULT 0,
            open_fees             REAL DEFAULT 0,
            close_fees            REAL DEFAULT 0,
            close_price_per_share REAL,
            roll_group_id         TEXT
        )
    """,
}

MIGRATIONS = [
    "ALTER TABLE wheel_positions ADD COLUMN cost_basis REAL DEFAULT 0",
    "ALTER TABLE wheel_positions ADD COLUMN accumulated_premiums REAL DEFAULT 0",
    "ALTER TABLE wheel_positions ADD COLUMN open_fees REAL DEFAULT 0",
    "ALTER TABLE wheel_positions ADD COLUMN close_fees REAL DEFAULT 0",
    "ALTER TABLE wheel_positions ADD COLUMN close_price_per_share REAL",
    "ALTER TABLE wheel_positions ADD COLUMN roll_group_id TEXT",
]

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


def rescue_database(source_path: str = "rockefeller_state.db"):
    """Emergency recovery: extract all recoverable data → clean DB → swap in place."""
    if not os.path.exists(source_path):
        logger.error(f"Source DB not found: {source_path}")
        return False

    timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    rescued_path = f"rockefeller_state_rescued_{timestamp}.db"
    backup_path  = f"rockefeller_state_corrupted_{timestamp}.db"

    logger.info(f"Source:  {source_path}")
    logger.info(f"Rescued: {rescued_path}")
    logger.info("=" * 60)

    try:
        conn_bad  = sqlite3.connect(source_path)
        conn_good = sqlite3.connect(rescued_path)
        cur_bad   = conn_bad.cursor()
        cur_good  = conn_good.cursor()

        logger.info("Step 1: Building clean schema...")
        for ddl in TABLES.values():
            cur_good.execute(ddl)
        conn_good.commit()
        for sql in MIGRATIONS:
            try:
                cur_good.execute(sql)
                conn_good.commit()
            except sqlite3.OperationalError:
                pass

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
                status = f"✅ {len(rows)} rows" if rows else "⚠️  0 rows"
                logger.info(f"  {table:<25} {status}")
            except sqlite3.OperationalError as e:
                totals[table] = 0
                logger.warning(f"  {table:<25} ⚠️  Could not read: {e}")

        conn_bad.close()
        conn_good.close()

        logger.info("Step 3: Verifying rescued DB integrity...")
        verify = sqlite3.connect(rescued_path)
        verify.execute("PRAGMA integrity_check").fetchone()
        verify.close()
        logger.info("  integrity_check PASSED")

        logger.info("Step 4: Swapping files...")
        os.rename(source_path, backup_path)
        os.rename(rescued_path, source_path)
        logger.info(f"  Corrupted → {backup_path} | Rescued → {source_path} (active)")

        logger.info("RESCUE COMPLETE. Restart all always-on tasks after rescue.")
        for table, count in totals.items():
            logger.info(f"  {table:<25} {count} rows recovered")
        return True

    except Exception as e:
        logger.error(f"CRITICAL FAILURE: {e}")
        if os.path.exists(rescued_path):
            os.remove(rescued_path)
        return False


# ── CEF premium seed (was seed_cef_premiums.py) ───────────────────────────────

def seed_cef_premiums():
    """
    One-time setup: initialize CLM/CRF premium z-score mu/sigma in DB.
    Run once on a new environment. After ~20 trading days the daily
    cef_calibrate cron (22:30 UTC) takes over with empirical data.
    """
    from analytics import HighFidelityAnalyticsEngine
    engine = HighFidelityAnalyticsEngine()
    for ticker in ["CLM", "CRF"]:
        logger.info(f"Seeding {ticker} premium z-score baseline...")
        result = engine.calibrate_cef_premium_zscore(ticker)
        if result:
            logger.info(
                f"  {ticker}: mu={result['mu']:.2f}% sigma={result['sigma']:.2f}% "
                f"source={result['source']} — DB updated."
            )
        else:
            logger.warning(f"  {ticker}: calibration returned empty — check DB connectivity.")
    logger.info("Seed complete. monitor.py and cef_calibrate will maintain these values going forward.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ecosystem DB maintenance utility")
    parser.add_argument("--rescue",         nargs="?", const="rockefeller_state.db",
                        metavar="PATH",     help="Emergency DB recovery (default: rockefeller_state.db)")
    parser.add_argument("--seed-premiums",  action="store_true",
                        help="One-time CLM/CRF z-score initialization")
    args = parser.parse_args()

    if args.rescue:
        success = rescue_database(args.rescue)
    elif args.seed_premiums:
        seed_cef_premiums()
        success = True
    else:
        success = run_daily_maintenance()

    sys.exit(0 if success else 1)
