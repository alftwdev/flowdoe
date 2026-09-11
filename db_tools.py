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
    One-time cleanup: drops dead tables (users),
    removes orphaned global_state keys (TSP, staking, deprecated forex state),
    and grades overdue PENDING signal_ledger entries.

    Run once:  python db_tools.py --purge-stale
    """
    db_full = os.path.join(BASE_DIR, db_path)
    logger.info("Running one-time stale data purge...")

    dead_tables = ["users"]
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


# ── RO Cycle History Seeder ───────────────────────────────────────────────────

def seed_ro_history():
    """
    Seed the ro_cycle_log and ro_cycle_events tables with all known RO cycle data.
    Safe to re-run: uses INSERT OR REPLACE for cycles, INSERT OR IGNORE for events.

    Sources: CLAUDE.md §0-G, direct session observations.
    Run once after deployment, then monitor.py keeps the active cycle updated.

    Usage: python db_tools.py --seed-ro-history
    """
    import json
    from database import EcosystemDatabase
    db = EcosystemDatabase()

    now = datetime.now().isoformat()

    # ── Cycle records ─────────────────────────────────────────────────────────

    cycles = [
        # ── 2025 CLM (COMPLETE) ───────────────────────────────────────────────
        # Formula: 112% × NAV (CLM-specific; CRF was 104% × NAV same cycle)
        # N-2 estimated: ~Feb 21 2025 (59-day heuristic back from Apr 21 record date)
        # Key lesson: record date WAS the cycle low — the 2025 pattern.
        # 2026 RO front-loaded the sell-off before record date — this pattern may NOT repeat.
        {
            "ticker": "CLM", "cycle_year": 2025, "status": "COMPLETE",
            "formula": "112% × NAV",
            "n2_filed_date": "2025-02-21",   # estimated (59d before record date)
            "n2_price": 7.35,
            "n2_nav": 5.90,                  # derived: sub_price / 1.12 = 6.61 / 1.12 ≈ 5.90
            "n2_premium_pct": 24.6,          # (7.35 / 5.90 - 1) × 100
            "n2a_effective_date": None,      # not captured
            "record_date_est": "2025-04-21",
            "record_date_actual": "2025-04-21",
            "expiration_date_est": "2025-05-16",
            "expiration_date_actual": "2025-05-16",
            "exdiv_date": "2025-05-15",
            "exdiv_drop": -0.1224,           # pre-2026 CLM monthly dist ($0.1224/mo)
            "sub_price_estimated": 6.61,
            "sub_price_actual": 6.61,        # 112% × $5.90 NAV
            "cycle_low_price": 6.92,
            "cycle_low_date": "2025-04-21",  # low coincided exactly with record date
            "cycle_low_days_from_n2": 59,
            "price_at_record_date": 6.92,
            "nav_at_record_date": 5.90,
            "premium_at_record_date": 17.3,  # (6.92 / 5.90 - 1) × 100
            "post_exp_1mo_price": 7.88,      # Jun 5, 2025 — full mean reversion underway
            "recovery_from_low_pct": 13.9,   # (7.88 - 6.92) / 6.92 × 100
            # Tier zones were not pre-established in 2025 with this framework
            "tier1_low": None, "tier1_high": None,
            "tier2_low": None, "tier2_high": None,
            "tier3_low": None, "tier3_high": None,
            "tier4_low": None, "tier4_high": None,
            "actual_sell_price": None,       # not recorded in this system
            "actual_sell_date": None,
            "actual_reentry_price": 6.92,    # open-market buyers at record date price
            "actual_reentry_date": "2025-04-21",
            "reentry_path": "RECORD_DATE_LOW",
            "bottom_at_record_date": 1,      # YES — the defining 2025 pattern
            "lessons_json": json.dumps([
                "Record date = cycle low in 2025. Open-market buyers at $6.92 beat RO subscribers ($6.61) — spread $0.31, price recovered $0.96 to $7.88 by Jun 5.",
                "25-day subscription window saw price APPRECIATION ($6.92→$7.32), not continued selling.",
                "CLM 2025 formula was most aggressive (112% × NAV vs CRF's 104% × NAV). Verify formula per N-2 filing each cycle.",
                "In 2025, CLM traded at ~25% premium when N-2 was filed — market had room to compress gradually toward sub price over 84 days.",
                "Pre-N-2 warning signals were NOT captured in 2025. Future cycles: log any dark pool or vol spike before the EDGAR N-2 appears.",
                "Tier zone framework was not pre-established. Build tier zones immediately upon N-2 detection for all future cycles.",
                "2026 WARNING: The 2025 record-date=low pattern may NOT repeat if the market front-loads the sell-off (as in 2026).",
            ]),
            "created_at": now, "updated_at": now,
        },

        # ── 2025 CRF (COMPLETE) ───────────────────────────────────────────────
        # Concurrent with CLM 2025 cycle. Formula: 104% × NAV.
        # Detailed price data not captured in same granularity as CLM.
        {
            "ticker": "CRF", "cycle_year": 2025, "status": "COMPLETE",
            "formula": "104% × NAV",
            "n2_filed_date": "2025-02-21",   # concurrent with CLM (estimated)
            "n2_price": None,                # not captured
            "n2_nav": None,
            "n2_premium_pct": None,
            "record_date_est": "2025-04-21",
            "record_date_actual": "2025-04-21",
            "expiration_date_est": "2025-05-16",
            "expiration_date_actual": "2025-05-16",
            "exdiv_date": "2025-05-15",
            "exdiv_drop": -0.1176,           # pre-2026 CRF monthly dist ($0.1176/mo)
            "sub_price_estimated": None,     # not captured
            "cycle_low_price": None,
            "cycle_low_date": "2025-04-21",  # assumed concurrent with CLM
            "cycle_low_days_from_n2": 59,
            "bottom_at_record_date": 1,      # consistent with CLM 2025 pattern
            "lessons_json": json.dumps([
                "CRF 2025 cycle ran concurrent with CLM. Formula was 104% × NAV (consistent across all prior CRF cycles).",
                "Detailed CRF price data not captured for 2025. Prioritize capturing both tickers equally in future cycles.",
                "CRF formula has been 104% × NAV every cycle. CLM formula has varied (107–112% × NAV in prior cycles; 104% in 2026).",
            ]),
            "created_at": now, "updated_at": now,
        },

        # ── 2026 CLM (ACTIVE as of Sept 9, 2026) ─────────────────────────────
        # N-2 filed Aug 14, 2026. Major difference from 2025: the 2027 distribution
        # preview (lower than 2026) caused the market to front-load the sell-off
        # within 3 days of the N-2 (vs gradual compression over 84 days in 2025).
        # By Day 11 (Aug 25), CLM was already BELOW 2027 fair value (~$6.97).
        # The 2025 "record date = bottom" pattern may NOT hold in 2026.
        {
            "ticker": "CLM", "cycle_year": 2026, "status": "ACTIVE",
            "formula": "104% × NAV",
            "n2_filed_date": "2026-08-14",
            "n2_price": 7.35,
            "n2_nav": 6.73,                  # from N-2 filing (CEFConnect Aug 16)
            "n2_premium_pct": 9.2,           # (7.35 / 6.73 - 1) × 100
            "n2_avg_vol_ratio": 1.0,         # baseline; Aug 17 spiked to 4.6×
            "pre_n2_warning_date": None,     # no pre-N-2 signals captured before Aug 14
            "pre_n2_warning_type": None,
            "n2a_effective_date": None,      # estimated ~Sept 12–15, 2026 (TBD)
            "record_date_est": "2026-10-12",
            "record_date_actual": None,      # TBD — update after 424B3 filing
            "expiration_date_est": "2026-11-06",
            "expiration_date_actual": None,
            "exdiv_date": "2026-09-15",
            "exdiv_drop": -0.1215,           # confirmed 2026 dist: $0.1215/mo
            "sub_price_estimated": 6.56,     # 104% × $6.31 NAV (Aug 21 CEFConnect)
            "sub_price_actual": None,        # TBD — set at record date close
            "cycle_low_price": 6.65,         # intraday 52w low Aug 25; close $6.74
            "cycle_low_date": "2026-08-25",  # may update if lower price occurs
            "cycle_low_days_from_n2": 11,    # 52w low hit Day 11 — front-loaded
            "price_at_record_date": None,    # TBD
            "nav_at_record_date": None,      # TBD — Oct NAV lock is the key catalyst
            "premium_at_record_date": None,
            "post_exp_1mo_price": None,
            "recovery_from_low_pct": None,
            # Tier zones established Aug 25, 2026
            "tier1_low": 6.65, "tier1_high": 6.80,
            "tier1_start_date": "2026-08-25", "tier1_end_date": "2026-09-14",
            "tier2_low": 6.50, "tier2_high": 6.65,
            "tier2_start_date": "2026-09-15", "tier2_end_date": "2026-09-19",
            "tier3_low": 6.30, "tier3_high": 6.55,
            "tier3_start_date": "2026-10-08", "tier3_end_date": "2026-10-16",
            "tier4_low": 6.00, "tier4_high": 6.30,
            "actual_sell_price": None,       # update when dodge executed
            "actual_sell_date": None,
            "actual_reentry_price": None,
            "actual_reentry_date": None,
            "reentry_path": None,
            "bottom_at_record_date": 0,      # 2026 pattern: low hit before record date (Day 11)
            "lessons_json": json.dumps([
                "2026 is structurally different from 2025: Cornerstone Aug 17 press release revealed 2027 distribution preview ($0.1103/mo vs $0.1215/mo current). Market repriced to 2027 FV (~$6.97) within 3 days of N-2. Sell-off front-loaded before record date.",
                "By Day 11 (Aug 25), CLM traded BELOW 2027 FV ($6.97) at $6.74 close / $6.65 intraday. The 2025 'record date = bottom' pattern does NOT appear to be repeating.",
                "2026 formula: 104% × NAV ONLY — no market price floor. Most aggressive CLM formula ever recorded (prior cycles: max(107–112% × NAV, 65–90% × market price)). Verify N-2 filing every cycle.",
                "Tactical shift: With 52w premium avg at 19.60% and current premium at ~6.8%, the recovery driver is premium mean-reversion (structural), not just RO overhang clearing.",
                "Sept 15 ex-div creates mechanical drop window: CRF post-ex-div likely lands at/below sub-price ($6.37) — highest-conviction open-market entry.",
                "Oct NAV lock (end of October) determines 2027 distribution rate. Board sets at 21% of Oct NAV. Higher Oct NAV = higher 2027 dist = higher 2027 FV = stronger recovery.",
                "N-2/A effectiveness (~Sept 12–15) + ex-div (Sept 15) landing in same week = dual-pressure catalyst. Watch for convergence and be ready to deploy Tier 2 capital.",
                "Cash reserve buffer: hold back dry powder even after Tier 2 deployment in case Tier 3 scenario materializes near Oct 12 record date.",
                "UPDATE this lessons_json after 424B3 filing, record date, expiration, and 1-month post recovery are known.",
            ]),
            "created_at": now, "updated_at": now,
        },

        # ── 2026 CRF (ACTIVE as of Sept 9, 2026) ─────────────────────────────
        {
            "ticker": "CRF", "cycle_year": 2026, "status": "ACTIVE",
            "formula": "104% × NAV",
            "n2_filed_date": "2026-08-14",
            "n2_price": 7.12,
            "n2_nav": 6.18,                  # CEFConnect at time of N-2
            "n2_premium_pct": 15.2,          # (7.12 / 6.18 - 1) × 100
            "pre_n2_warning_date": None,
            "pre_n2_warning_type": None,
            "n2a_effective_date": None,      # estimated ~Sept 12–15, 2026 (TBD)
            "record_date_est": "2026-10-12",
            "record_date_actual": None,
            "expiration_date_est": "2026-11-06",
            "expiration_date_actual": None,
            "exdiv_date": "2026-09-15",
            "exdiv_drop": -0.1176,           # confirmed 2026 dist: $0.1176/mo
            "sub_price_estimated": 6.37,     # 104% × $6.12 NAV (Aug 21 CEFConnect)
            "sub_price_actual": None,
            "cycle_low_price": 6.44,         # intraday 52w low Aug 25; close $6.48
            "cycle_low_date": "2026-08-25",
            "cycle_low_days_from_n2": 11,
            "price_at_record_date": None,
            "nav_at_record_date": None,
            "premium_at_record_date": None,
            "post_exp_1mo_price": None,
            "recovery_from_low_pct": None,
            "tier1_low": 6.40, "tier1_high": 6.55,
            "tier1_start_date": "2026-08-25", "tier1_end_date": "2026-09-14",
            "tier2_low": 6.25, "tier2_high": 6.40,
            "tier2_start_date": "2026-09-15", "tier2_end_date": "2026-09-19",
            "tier3_low": 6.00, "tier3_high": 6.25,
            "tier3_start_date": "2026-10-08", "tier3_end_date": "2026-10-16",
            "tier4_low": 5.75, "tier4_high": 6.00,
            "actual_sell_price": None,
            "actual_sell_date": None,
            "actual_reentry_price": None,
            "actual_reentry_date": None,
            "reentry_path": None,
            "bottom_at_record_date": 0,
            "lessons_json": json.dumps([
                "CRF 2026 cycle concurrent with CLM. Formula 104% × NAV (consistent with all prior CRF cycles).",
                "CRF post-ex-div (Sept 15) expected to land at/near sub-price ($6.37) — historically best open-market entry window for CRF specifically.",
                "CRF 52w premium avg: 18.46%. Current premium (~5.56%) near multi-year low. Recovery driver: premium mean-reversion.",
                "UPDATE this lessons_json after actual record date, expiration, and recovery data are captured.",
            ]),
            "created_at": now, "updated_at": now,
        },
    ]

    # ── Events timeline ───────────────────────────────────────────────────────

    events = [
        # 2025 CLM
        ("CLM", 2025, "2025-02-21", "N2_FILED",    7.35, 5.90, 24.6, None, None, None,
         "N-2 filed. Formula: 112% × NAV. Estimated from 59-day record-date heuristic.", "EDGAR_watcher"),
        ("CLM", 2025, "2025-04-21", "RECORD_DATE",  6.92, 5.90, 17.3, None, None, None,
         "Record date. Cycle low coincided exactly with record date — the defining 2025 pattern. "
         "Open-market price $6.92 vs sub price $6.61; market buyers above sub price but recovered fast.", "manual"),
        ("CLM", 2025, "2025-04-21", "PRICE_LOW",    6.92, 5.90, 17.3, None, None, None,
         "Cycle low. Bottom was HERE at record date, not at expiration.", "manual"),
        ("CLM", 2025, "2025-05-15", "EXDIV",        7.10, None, None, None, None, None,
         "Ex-div day. Intraday low ~$7.10 — mechanical drop of ~$0.1224. Price above cycle low.", "manual"),
        ("CLM", 2025, "2025-05-16", "EXPIRATION",   7.32, None, None, None, None, None,
         "Expiration. Price $7.32 — higher than record date ($6.92). Subscription window was appreciation, not selling.", "manual"),
        ("CLM", 2025, "2025-06-05", "RECOVERY_CHECKPOINT", 7.88, None, None, None, None, None,
         "1 month post-expiration. Full mean reversion underway. Recovery from cycle low: +$0.96 (+13.9%).", "manual"),

        # 2026 CLM
        ("CLM", 2026, "2026-08-14", "N2_FILED",     7.35, 6.73,  9.2, None, None, None,
         "N-2 filed. Formula: 104% × NAV (no market price floor — most aggressive CLM formula ever). "
         "52w premium avg: 19.60%. Premium at filing: 9.2%.", "EDGAR_watcher"),
        ("CLM", 2026, "2026-08-17", "CAPITULATION",  6.94, 6.73, None, 8620000, 4.6, 0.1,
         "Cornerstone press release day: 2027 dist preview ($0.1103/mo vs $0.1215/mo current). "
         "CLM dropped $7.35 → $6.94 (–5.6%) on 8.62M vol (4.6× avg). SPY ~flat. CEF-specific event. "
         "Market front-loaded the entire 2027 rerating in one session.", "monitor.py"),
        ("CLM", 2026, "2026-08-25", "PRICE_LOW",     6.65, 6.31,  5.4, None, None, None,
         "52-week low intraday ($6.65); close $6.74. NAV updated to $6.31 (CEFConnect Aug 21). "
         "Premium compressed to 6.81% vs 19.60% avg. Day 11 post-N-2. "
         "Tier zones established. Tactical: waiting for Tier 2 (Sept 15–19).", "monitor.py"),
        ("CLM", 2026, "2026-08-25", "OBSERVATION",   6.74, 6.31,  6.8, None, None, None,
         "Sub price estimated $6.56 (104% × $6.31 NAV). CLM trading above sub price but below 2027 FV ($6.97). "
         "Tier 1 zone: $6.65–$6.80. Recovery driver: premium mean-reversion from 6.8% to 19.6% avg.", "manual"),
        ("CLM", 2026, "2026-09-09", "OBSERVATION",   None, None,  None, None, None, None,
         "Day 26 post-N-2. Price drifted lower (anticipated). Still Tier 1 zone. "
         "Awaiting N-2/A effectiveness (~Sept 12–15) and ex-div (Sept 15) for Tier 2 entry window. "
         "Cash surplus held back in reserve for potential Tier 3 scenario.", "manual"),

        # 2026 CRF
        ("CRF", 2026, "2026-08-14", "N2_FILED",     7.12, 6.18, 15.2, None, None, None,
         "N-2 filed concurrent with CLM. Formula: 104% × NAV (consistent with all prior CRF cycles). "
         "52w premium avg: 18.46%. Premium at filing: 15.2%.", "EDGAR_watcher"),
        ("CRF", 2026, "2026-08-17", "CAPITULATION",  None, 6.18, None, None, None, 0.1,
         "Cornerstone press release day — same catalyst as CLM. CRF dropped concurrent with CLM. "
         "Exact volume not captured. SPY ~flat.", "monitor.py"),
        ("CRF", 2026, "2026-08-25", "PRICE_LOW",     6.44, 6.12,  5.2, None, None, None,
         "52-week low intraday ($6.44); close $6.48. NAV updated to $6.12 (CEFConnect Aug 21). "
         "Premium compressed to 5.56% vs 18.46% avg. "
         "CRF post-ex-div (Sept 15) expected to land at/near sub price ($6.37) — highest-conviction entry.", "monitor.py"),
        ("CRF", 2026, "2026-09-09", "OBSERVATION",   None, None, None, None, None, None,
         "Day 26 post-N-2. Price drifted lower. Still Tier 1 zone. "
         "Waiting for Sept 15 ex-div window (Tier 2: $6.25–$6.40).", "manual"),
    ]

    # ── Write to DB ───────────────────────────────────────────────────────────

    with sqlite3.connect(os.path.join(BASE_DIR, "rockefeller_state.db"), timeout=10.0) as conn:
        cur = conn.cursor()

        # Cycles — INSERT OR REPLACE (idempotent re-runs)
        for c in cycles:
            cur.execute("""
                INSERT OR REPLACE INTO ro_cycle_log (
                    ticker, cycle_year, status, formula,
                    n2_filed_date, n2_price, n2_nav, n2_premium_pct, n2_avg_vol_ratio,
                    pre_n2_warning_date, pre_n2_warning_type, n2a_effective_date,
                    record_date_est, record_date_actual,
                    expiration_date_est, expiration_date_actual,
                    exdiv_date, exdiv_drop,
                    sub_price_estimated, sub_price_actual,
                    cycle_low_price, cycle_low_date, cycle_low_days_from_n2,
                    price_at_record_date, nav_at_record_date, premium_at_record_date,
                    post_exp_1mo_price, recovery_from_low_pct,
                    tier1_low, tier1_high, tier1_start_date, tier1_end_date,
                    tier2_low, tier2_high, tier2_start_date, tier2_end_date,
                    tier3_low, tier3_high, tier3_start_date, tier3_end_date,
                    tier4_low, tier4_high,
                    actual_sell_price, actual_sell_date,
                    actual_reentry_price, actual_reentry_date, reentry_path,
                    bottom_at_record_date, lessons_json, created_at, updated_at
                ) VALUES (
                    :ticker, :cycle_year, :status, :formula,
                    :n2_filed_date, :n2_price, :n2_nav, :n2_premium_pct, :n2_avg_vol_ratio,
                    :pre_n2_warning_date, :pre_n2_warning_type, :n2a_effective_date,
                    :record_date_est, :record_date_actual,
                    :expiration_date_est, :expiration_date_actual,
                    :exdiv_date, :exdiv_drop,
                    :sub_price_estimated, :sub_price_actual,
                    :cycle_low_price, :cycle_low_date, :cycle_low_days_from_n2,
                    :price_at_record_date, :nav_at_record_date, :premium_at_record_date,
                    :post_exp_1mo_price, :recovery_from_low_pct,
                    :tier1_low, :tier1_high, :tier1_start_date, :tier1_end_date,
                    :tier2_low, :tier2_high, :tier2_start_date, :tier2_end_date,
                    :tier3_low, :tier3_high, :tier3_start_date, :tier3_end_date,
                    :tier4_low, :tier4_high,
                    :actual_sell_price, :actual_sell_date,
                    :actual_reentry_price, :actual_reentry_date, :reentry_path,
                    :bottom_at_record_date, :lessons_json, :created_at, :updated_at
                )
            """, {k: c.get(k) for k in [
                "ticker","cycle_year","status","formula",
                "n2_filed_date","n2_price","n2_nav","n2_premium_pct","n2_avg_vol_ratio",
                "pre_n2_warning_date","pre_n2_warning_type","n2a_effective_date",
                "record_date_est","record_date_actual",
                "expiration_date_est","expiration_date_actual",
                "exdiv_date","exdiv_drop",
                "sub_price_estimated","sub_price_actual",
                "cycle_low_price","cycle_low_date","cycle_low_days_from_n2",
                "price_at_record_date","nav_at_record_date","premium_at_record_date",
                "post_exp_1mo_price","recovery_from_low_pct",
                "tier1_low","tier1_high","tier1_start_date","tier1_end_date",
                "tier2_low","tier2_high","tier2_start_date","tier2_end_date",
                "tier3_low","tier3_high","tier3_start_date","tier3_end_date",
                "tier4_low","tier4_high",
                "actual_sell_price","actual_sell_date",
                "actual_reentry_price","actual_reentry_date","reentry_path",
                "bottom_at_record_date","lessons_json","created_at","updated_at",
            ]})

        # Events — look up cycle IDs, then insert
        cur.execute("SELECT id, ticker, cycle_year FROM ro_cycle_log")
        cycle_id_map = {(r[1], r[2]): r[0] for r in cur.fetchall()}

        for (ticker, yr, edate, etype, price, nav, prem, vol, vol_r, spy,
             desc, src) in events:
            cid = cycle_id_map.get((ticker, yr))
            cur.execute("""
                INSERT OR IGNORE INTO ro_cycle_events
                    (ro_cycle_id, ticker, cycle_year, event_date, event_type,
                     price, nav, premium_pct, volume, volume_ratio, spy_change_pct,
                     description, signal_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (cid, ticker, yr, edate, etype, price, nav, prem, vol, vol_r, spy, desc, src))

        conn.commit()

    # Verify
    with sqlite3.connect(os.path.join(BASE_DIR, "rockefeller_state.db"), timeout=10.0) as conn:
        cur = conn.cursor()
        cur.execute("SELECT ticker, cycle_year, status, cycle_low_price FROM ro_cycle_log ORDER BY cycle_year, ticker")
        cycle_rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM ro_cycle_events")
        event_count = cur.fetchone()[0]

    logger.info(f"RO history seeded — {len(cycle_rows)} cycles, {event_count} events:")
    for r in cycle_rows:
        logger.info(f"  {r[1]} {r[0]}: status={r[2]}  cycle_low=${r[3]}")
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
    parser.add_argument("--seed-ro-history",   action="store_true",
                        help="Seed ro_cycle_log + ro_cycle_events with all known RO cycles (2025, 2026)")
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
    elif args.seed_ro_history:
        success = seed_ro_history()
    else:
        success = run_daily_maintenance()

    sys.exit(0 if success else 1)
