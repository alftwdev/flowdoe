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
    python db_tools.py --seed-tax-character CLM --roc 58 --qdi 42 --ord 0 --year 2025
    python db_tools.py --seed-tax-character CRF --roc 61 --qdi 39 --ord 0 --year 2025

    Values come from Box 1a (ordinary), 1b (qualified), 2a (cap gains), 3 (ROC)
    on the annual 1099-DIV. Run once each January after the form arrives.
    Displayed in Sunday personal_scorecard Pushover and Q1 morning brief.

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
    prune dated global_state keys older than 45 days, VACUUM the DB.
    Runs in ~1s; zero TD API calls.
    """
    import re
    from database import EcosystemDatabase
    db_full = os.path.join(BASE_DIR, db_path)
    logger.info("Starting daily DB maintenance...")
    try:
        with sqlite3.connect(db_full, timeout=10.0) as conn:
            cur = conn.cursor()

            # Prune stale alert locks (> 24h)
            purge_threshold = (datetime.now() - timedelta(hours=24)).isoformat()
            cur.execute("DELETE FROM alert_state_manager WHERE last_alert_time < ?", (purge_threshold,))
            purged_alerts = cur.rowcount

            # Cap audit_logs at 500 rows
            cur.execute(
                "DELETE FROM audit_logs WHERE id NOT IN "
                "(SELECT id FROM audit_logs ORDER BY id DESC LIMIT 500)"
            )
            purged_logs = cur.rowcount

            # Prune dated global_state keys (e.g. market_analysis_morning_call_2026-06-20)
            # Keys with YYYY-MM-DD suffix older than 45 days are dedup sentinels that never auto-expire.
            cutoff_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
            cur.execute("SELECT key FROM global_state")
            all_keys = [r[0] for r in cur.fetchall()]
            date_pattern = re.compile(r"_(\d{4}-\d{2}-\d{2})$")
            stale_dated = [k for k in all_keys
                           if (m := date_pattern.search(k)) and m.group(1) < cutoff_date]
            if stale_dated:
                cur.executemany("DELETE FROM global_state WHERE key = ?", [(k,) for k in stale_dated])
                logger.info(f"Pruned {len(stale_dated)} stale dated global_state keys.")
            else:
                logger.info("No stale dated keys to prune.")

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


def purge_stale_data(db_path: str = "rockefeller_state.db"):
    """
    One-time cleanup: drops dead tables (youtube_videos, youtube_key_points, users),
    removes orphaned global_state keys (TSP, staking, deprecated forex state),
    and grades overdue PENDING signal_ledger entries.

    Run once:  python db_tools.py --purge-stale
    """
    db_full = os.path.join(BASE_DIR, db_path)
    logger.info("Running one-time stale data purge...")

    dead_tables = ["youtube_videos", "youtube_key_points", "users"]
    orphaned_key_prefixes = ["tsp_", "staking_yields", "EUR/USD_", "GBP/USD_",
                             "USD/JPY_", "wargame_", "test_poison_key"]
    orphaned_exact = ["btc_spy_correlation_sync"]

    try:
        with sqlite3.connect(db_full, timeout=10.0) as conn:
            cur = conn.cursor()

            # Drop dead tables
            dropped = []
            for tbl in dead_tables:
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {tbl}")
                    dropped.append(tbl)
                except Exception as e:
                    logger.warning(f"Could not drop {tbl}: {e}")
            if dropped:
                logger.info(f"Dropped tables: {', '.join(dropped)}")

            # Remove orphaned global_state keys
            cur.execute("SELECT key FROM global_state")
            all_keys = [r[0] for r in cur.fetchall()]
            to_delete = []
            for k in all_keys:
                if k in orphaned_exact:
                    to_delete.append(k)
                elif any(k.startswith(p) for p in orphaned_key_prefixes):
                    to_delete.append(k)
            if to_delete:
                cur.executemany("DELETE FROM global_state WHERE key = ?", [(k,) for k in to_delete])
                logger.info(f"Removed {len(to_delete)} orphaned global_state keys:")
                for k in to_delete:
                    logger.info(f"  - {k}")

            # Grade overdue PENDING signal_ledger entries
            today_str = datetime.now().strftime("%Y-%m-%d")
            cur.execute(
                "SELECT id, ticker, predicted_direction, entry_price, target_date "
                "FROM signal_ledger WHERE outcome = 'PENDING' AND target_date < ?",
                (today_str,)
            )
            overdue = cur.fetchall()
            for row in overdue:
                sig_id, ticker, direction, entry_price, target_date = row
                logger.info(f"Signal {sig_id} ({ticker} {direction} from {target_date}) is overdue — marking EXPIRED")
                cur.execute(
                    "UPDATE signal_ledger SET outcome = 'EXPIRED', graded_date = ? WHERE id = ?",
                    (today_str, sig_id)
                )
            if overdue:
                logger.info(f"Graded {len(overdue)} overdue PENDING signals as EXPIRED.")

            conn.commit()

        # VACUUM after dropping tables
        with sqlite3.connect(db_full, timeout=10.0) as conn:
            conn.isolation_level = None
            conn.execute("VACUUM")
            logger.info("VACUUM complete.")

        logger.info("Stale data purge complete.")
        return True
    except Exception as e:
        logger.critical(f"Purge failed: {e}")
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


# ── CEF distribution tax character (annual — run once after 1099-DIV arrives) ─

def seed_tax_character(ticker: str, roc_pct: float, qdi_pct: float,
                       ord_pct: float, year: int):
    """
    Store CLM/CRF 1099-DIV tax character in DB.

    Source: IRS 1099-DIV received in January for the prior tax year.
      Box 1a (total ordinary dividends) → split into Box 1b (qualified) + remainder (ordinary)
      Box 2a (total capital gain distributions) — rare for CLM/CRF, include in ord_pct if present
      Box 3 (non-dividend distributions / return of capital) → roc_pct

    roc_pct + qdi_pct + ord_pct must sum to 100.

    After running this, personal_scorecard (Sunday Pushover) and the Q1 morning
    brief will show the after-tax effective yield alongside the headline yield.
    """
    from database import EcosystemDatabase
    total = roc_pct + qdi_pct + ord_pct
    if abs(total - 100.0) > 0.5:
        logger.error(f"Percentages must sum to 100 (got {total:.1f}). Aborting.")
        return False

    ticker = ticker.upper()
    if ticker not in ("CLM", "CRF"):
        logger.error("ticker must be CLM or CRF")
        return False

    db   = EcosystemDatabase()
    data = {
        "ticker":   ticker,
        "year":     year,
        "roc_pct":  round(roc_pct, 1),
        "qdi_pct":  round(qdi_pct, 1),
        "ord_pct":  round(ord_pct, 1),
        "recorded": datetime.now().isoformat(),
    }
    db.update_state(f"{ticker.lower()}_dist_tax_char", data)
    logger.info(f"Stored {ticker} {year} tax character: ROC={roc_pct:.1f}% | QDI={qdi_pct:.1f}% | Ord={ord_pct:.1f}%")
    logger.info(f"  Will appear in Sunday personal_scorecard and Q1 morning briefs.")
    return True


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ecosystem DB maintenance utility")
    parser.add_argument("--rescue",            nargs="?", const="rockefeller_state.db",
                        metavar="PATH",        help="Emergency DB recovery")
    parser.add_argument("--seed-premiums",     action="store_true",
                        help="One-time CLM/CRF z-score initialization")
    parser.add_argument("--seed-tax-character", metavar="TICKER",
                        help="Store 1099-DIV tax character for CLM or CRF (requires --roc/--qdi/--ord/--year)")
    parser.add_argument("--purge-stale",       action="store_true",
                        help="One-time cleanup: drop dead tables, remove orphaned keys, grade overdue signals")
    parser.add_argument("--roc",  type=float, default=0.0, help="Return of capital %%")
    parser.add_argument("--qdi",  type=float, default=0.0, help="Qualified dividend income %%")
    parser.add_argument("--ord",  type=float, default=0.0, help="Ordinary dividend %%")
    parser.add_argument("--year", type=int,   default=datetime.now().year - 1,
                        help="Tax year of the 1099-DIV (default: prior year)")
    args = parser.parse_args()

    if args.rescue:
        success = rescue_database(args.rescue)
    elif args.seed_premiums:
        seed_cef_premiums()
        success = True
    elif args.seed_tax_character:
        success = seed_tax_character(
            args.seed_tax_character, args.roc, args.qdi, args.ord, args.year
        )
    elif args.purge_stale:
        success = purge_stale_data()
    else:
        success = run_daily_maintenance()

    sys.exit(0 if success else 1)
