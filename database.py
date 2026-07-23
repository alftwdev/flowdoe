import sqlite3
import threading
import json
import os
import logging
import hashlib
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("Database_Engine")

class EcosystemDatabase:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path="rockefeller_state.db"):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(EcosystemDatabase, cls).__new__(cls)
                cls._instance.db_path = os.path.join(BASE_DIR, db_path)
                cls._instance._initialize_tables()
            return cls._instance

    def __init__(self, db_path='rockefeller_state.db'):
        pass

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute('PRAGMA journal_mode=DELETE;')
        conn.execute('PRAGMA synchronous=NORMAL;')
        return conn

    def _initialize_tables(self):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS global_state (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS audit_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        level TEXT,
                        message TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS alert_state_manager (
                        alert_id TEXT PRIMARY KEY,
                        last_state TEXT,
                        last_trigger REAL,
                        broadcast_count INTEGER DEFAULT 0,
                        last_alert_time TIMESTAMP
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS market_data_cache (
                        cache_key TEXT PRIMARY KEY,
                        response_json TEXT NOT NULL,
                        cached_at TIMESTAMP NOT NULL
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS wheel_positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        position_type TEXT NOT NULL,
                        strike REAL NOT NULL,
                        expiration TEXT NOT NULL,
                        premium_collected REAL NOT NULL,
                        contracts INTEGER DEFAULT 1,
                        status TEXT NOT NULL DEFAULT 'OPEN',
                        opened_date TEXT DEFAULT CURRENT_TIMESTAMP,
                        closed_date TEXT,
                        close_note TEXT,
                        last_alert_dte INTEGER
                    )
                """)
                # Daily IV snapshot table — feeds real IVR after 30+ trading days.
                # Populated via scheduler.py --mode store_daily_iv at 21:30 UTC.
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS iv_daily (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        iv_value REAL NOT NULL,
                        recorded_date TEXT NOT NULL,
                        UNIQUE(symbol, recorded_date)
                    )
                """)
                conn.commit()

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS youtube_videos (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        video_id TEXT NOT NULL UNIQUE,
                        playlist_id TEXT NOT NULL,
                        title TEXT NOT NULL,
                        transcript_fetched INTEGER DEFAULT 0,
                        processed_date TEXT,
                        added_date TEXT DEFAULT CURRENT_DATE
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS youtube_key_points (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        video_id TEXT NOT NULL,
                        point_number INTEGER NOT NULL,
                        content TEXT NOT NULL,
                        approved INTEGER DEFAULT 0,
                        integration_notes TEXT,
                        created_date TEXT DEFAULT CURRENT_DATE,
                        FOREIGN KEY(video_id) REFERENCES youtube_videos(video_id)
                    )
                """)
                conn.commit()

                # CEF daily premium log — accumulates from monitor.py runs.
                # Replaces CEFConnect dependency (API retired). After 30+ trading days
                # this table provides a real empirical baseline for z-score calibration.
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cef_premium_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ticker TEXT NOT NULL,
                        log_date TEXT NOT NULL,
                        nav REAL NOT NULL,
                        price REAL NOT NULL,
                        premium_pct REAL NOT NULL,
                        UNIQUE(ticker, log_date)
                    )
                """)
                conn.commit()

                # Signal ledger — logs predictions from all three strategies for accuracy scoring.
                # Graded weekly by announcements.py; published to #announcements as a free-tier hook.
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS signal_ledger (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        signal_type TEXT NOT NULL,
                        ticker TEXT,
                        predicted_direction TEXT NOT NULL,
                        entry_price REAL,
                        prediction_date TEXT NOT NULL,
                        target_date TEXT NOT NULL,
                        exit_price REAL,
                        outcome TEXT DEFAULT 'PENDING',
                        score_contribution REAL DEFAULT 0,
                        graded_date TEXT,
                        notes TEXT
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_ledger_outcome ON signal_ledger(outcome)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_ledger_target ON signal_ledger(target_date)")
                conn.commit()

                # Graceful column migrations — try each; OperationalError means already exists.
                for col_sql in [
                    # Lot-engine (session 1)
                    "ALTER TABLE wheel_positions ADD COLUMN cost_basis REAL DEFAULT 0",
                    "ALTER TABLE wheel_positions ADD COLUMN accumulated_premiums REAL DEFAULT 0",
                    # Wheelhouse upgrade (session 2): accurate retained premium + cycle tracking
                    "ALTER TABLE wheel_positions ADD COLUMN open_fees REAL DEFAULT 0",
                    "ALTER TABLE wheel_positions ADD COLUMN close_fees REAL DEFAULT 0",
                    "ALTER TABLE wheel_positions ADD COLUMN close_price_per_share REAL",
                    "ALTER TABLE wheel_positions ADD COLUMN roll_group_id TEXT",
                ]:
                    try:
                        cursor.execute(col_sql)
                        conn.commit()
                    except sqlite3.OperationalError:
                        pass  # column already exists
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to initialize tables: {e}")

    def track_and_limit_alerts(self, alert_id, current_state, current_trigger, max_broadcasts=3, threshold_pct=0.001):
        """
        Universal 3-Strike Gatekeeper: Accounts for negative-integer tracking math
        during sharp market declines by normalizing baseline variations.
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT last_state, last_trigger, broadcast_count FROM alert_state_manager WHERE alert_id = ?", (alert_id,))
                row = cursor.fetchone()
                now_str = datetime.now().isoformat()

                if row is None:
                    cursor.execute("INSERT INTO alert_state_manager VALUES (?, ?, ?, ?, ?)",
                                   (alert_id, current_state, current_trigger, 1, now_str))
                    conn.commit()
                    return True

                last_state, last_trigger, broadcast_count = row
                
                # Math Correction: wrap last_trigger in abs() to prevent negative threshold calculation locks
                trigger_delta = abs(current_trigger - last_trigger)
                allowed_variance = abs(last_trigger) * threshold_pct if last_trigger != 0 else threshold_pct

                if current_state != last_state or trigger_delta > allowed_variance:
                    cursor.execute("""
                        UPDATE alert_state_manager 
                        SET last_state = ?, last_trigger = ?, broadcast_count = 1, last_alert_time = ? 
                        WHERE alert_id = ?
                    """, (current_state, current_trigger, now_str, alert_id))
                    conn.commit()
                    return True

                if broadcast_count < max_broadcasts:
                    cursor.execute("""
                        UPDATE alert_state_manager 
                        SET broadcast_count = broadcast_count + 1, last_alert_time = ? 
                        WHERE alert_id = ?
                    """, (now_str, alert_id))
                    conn.commit()
                    return True

                return False
        except sqlite3.OperationalError as e:
            logger.warning(f"Database lock in alert manager: {e}")
            return False

    def log_prediction(self, signal_type: str, ticker: str, predicted_direction: str,
                       entry_price: float, target_days: int, notes: str = "") -> bool:
        """
        Logs a signal prediction to the ledger for later grading.
        Deduplicates by signal_type + ticker + prediction_date — one log per signal per day.
        signal_type: 'market_direction' | 'tqqq_call' | 'tqqq_put' | 'clm_floor' | 'btc_sentiment'
        predicted_direction: 'BULLISH' | 'BEARISH' | 'NEUTRAL'
        target_days: trading days until grading (1=next day, 5=one week, 14=two weeks, 30=month)
        """
        from datetime import date, timedelta
        today = date.today().isoformat()
        target = (date.today() + timedelta(days=target_days)).isoformat()
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM signal_ledger WHERE signal_type=? AND ticker=? AND prediction_date=?",
                    (signal_type, ticker, today)
                )
                if cursor.fetchone():
                    return False  # already logged today
                cursor.execute("""
                    INSERT INTO signal_ledger
                        (signal_type, ticker, predicted_direction, entry_price,
                         prediction_date, target_date, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (signal_type, ticker, predicted_direction, entry_price, today, target, notes))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"log_prediction failed: {e}")
            return False

    def get_pending_predictions(self, max_target_date: str = None) -> list:
        """Returns PENDING predictions whose target_date <= max_target_date (default: today)."""
        from datetime import date
        cutoff = max_target_date or date.today().isoformat()
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, signal_type, ticker, predicted_direction, entry_price,
                           prediction_date, target_date, notes
                    FROM signal_ledger
                    WHERE outcome = 'PENDING' AND target_date <= ?
                    ORDER BY target_date ASC
                """, (cutoff,))
                cols = ["id", "signal_type", "ticker", "predicted_direction", "entry_price",
                        "prediction_date", "target_date", "notes"]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_pending_predictions failed: {e}")
            return []

    def grade_prediction(self, pred_id: int, exit_price: float, outcome: str,
                         score: float, notes: str = ""):
        """Records the graded outcome for a ledger entry. outcome: WIN | LOSS | NEUTRAL"""
        from datetime import date
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE signal_ledger
                    SET exit_price=?, outcome=?, score_contribution=?, graded_date=?, notes=?
                    WHERE id=?
                """, (exit_price, outcome, score, date.today().isoformat(), notes, pred_id))
                conn.commit()
        except Exception as e:
            logger.error(f"grade_prediction failed: {e}")

    def get_scorecard_window(self, days_back: int = 7) -> list:
        """Returns all graded predictions from the last N days for the weekly scorecard."""
        from datetime import date, timedelta
        since = (date.today() - timedelta(days=days_back)).isoformat()
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT signal_type, ticker, predicted_direction, entry_price, exit_price,
                           outcome, score_contribution, prediction_date, target_date, notes
                    FROM signal_ledger
                    WHERE graded_date >= ? AND outcome != 'PENDING'
                    ORDER BY prediction_date DESC
                """, (since,))
                cols = ["signal_type", "ticker", "predicted_direction", "entry_price", "exit_price",
                        "outcome", "score_contribution", "prediction_date", "target_date", "notes"]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_scorecard_window failed: {e}")
            return []

    def get_mtd_accuracy(self) -> tuple:
        """Returns (wins, total) for the current calendar month."""
        from datetime import date
        month_start = date.today().replace(day=1).isoformat()
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT
                        SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END),
                        COUNT(*)
                    FROM signal_ledger
                    WHERE graded_date >= ? AND outcome != 'PENDING'
                """, (month_start,))
                row = cursor.fetchone()
                return (row[0] or 0, row[1] or 0)
        except Exception as e:
            logger.error(f"get_mtd_accuracy failed: {e}")
            return (0, 0)

    def open_wheel_position(self, symbol, position_type, strike, expiration, premium_collected,
                             contracts=1, cost_basis=None, open_fees=0.0, roll_group_id=None):
        """
        Logs a newly opened CSP or CC position.
        premium_collected: per-contract total in dollars (mid * 100).
        cost_basis: per-share cost basis — defaults to strike (what you pay if assigned on a CSP).
        open_fees: commission paid to open, in dollars total (e.g. 2 contracts × $0.65 = $1.30).
        roll_group_id: shared UUID string linking all legs of a roll chain together.
        """
        cb = cost_basis if cost_basis is not None else strike
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO wheel_positions
                        (symbol, position_type, strike, expiration, premium_collected,
                         contracts, cost_basis, open_fees, roll_group_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (symbol, position_type, strike, expiration, premium_collected,
                      contracts, cb, open_fees or 0.0, roll_group_id))
                conn.commit()
                return cursor.lastrowid
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to open wheel position: {e}")
            return None

    def add_position_premium(self, position_id, premium_amount):
        """
        Accumulate additional premium against an open position (e.g. a CC sold on an assigned lot).
        Adds to accumulated_premiums so net_cost calculations reflect the full premium reduction.
        """
        try:
            with self._get_connection() as conn:
                conn.execute(
                    "UPDATE wheel_positions SET accumulated_premiums = accumulated_premiums + ? WHERE id = ? AND status = 'OPEN'",
                    (premium_amount, position_id)
                )
                conn.commit()
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to accumulate premium for position {position_id}: {e}")

    def close_wheel_position(self, position_id, status="CLOSED", close_note="",
                              close_price_per_share=None, close_fees=0.0):
        """
        Closes a position (CLOSED/ASSIGNED/EXPIRED/ROLLED) and adds actual retained premium
        to the margin-paydown ledger.

        Retained premium calculation:
          EXPIRED  → full premium_collected (no buyback, kept everything)
          ROLLED   → full premium_collected (credit received, continuation)
          ASSIGNED → full premium_collected (assignment at agreed strike, no BTC cost)
          CLOSED   → premium_collected - (close_price_per_share * contracts * 100) - close_fees
                     i.e. what you actually kept after buying it back early

        close_price_per_share: the per-share price you paid to BTC (e.g. $0.45 if sold at $0.90 and
                               closed at 50% profit). Only needed for CLOSED status.
        close_fees: total commission paid to close, in dollars.
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT premium_collected, contracts, open_fees FROM wheel_positions WHERE id = ? AND status = 'OPEN'",
                    (position_id,)
                )
                row = cursor.fetchone()
                if row is None:
                    return False
                prem_collected = float(row[0])
                contracts      = int(row[1])
                open_fees_val  = float(row[2] or 0.0)

                if status == "CLOSED" and close_price_per_share is not None:
                    buyback_cost = float(close_price_per_share) * contracts * 100
                    retained = prem_collected - buyback_cost - (close_fees or 0.0) - open_fees_val
                else:
                    # EXPIRED / ROLLED / ASSIGNED — no buyback, full premium minus fees
                    retained = prem_collected - (close_fees or 0.0) - open_fees_val

                cursor.execute("""
                    UPDATE wheel_positions
                    SET status = ?, closed_date = CURRENT_TIMESTAMP, close_note = ?,
                        close_price_per_share = ?, close_fees = ?
                    WHERE id = ?
                """, (status, close_note, close_price_per_share, close_fees or 0.0, position_id))
                conn.commit()
            self._add_to_premium_ledger(max(retained, 0.0))
            return True
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to close wheel position: {e}")
            return False

    def get_open_wheel_positions(self):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, symbol, position_type, strike, expiration,
                           premium_collected, contracts, last_alert_dte,
                           cost_basis, accumulated_premiums,
                           open_fees, roll_group_id
                    FROM wheel_positions WHERE status = 'OPEN'
                    ORDER BY opened_date ASC
                """)
                cols = [c[0] for c in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to fetch open wheel positions: {e}")
            return []

    def get_wheel_outcome_distribution(self, lookback_days=90):
        """
        Outcome breakdown for closed positions in the lookback window.
        retained_premium uses actual close_price_per_share for CLOSED positions so early
        buy-to-close at 50% profit isn't counted as full premium in the ledger.
        Also returns annualized_roc_avg per outcome group for scorecard context.
        """
        try:
            cutoff = (datetime.now() - timedelta(days=lookback_days)).isoformat()
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT status,
                           COUNT(*) as count,
                           SUM(premium_collected * contracts) as gross_premium,
                           SUM(
                               CASE
                                   WHEN status = 'CLOSED' AND close_price_per_share IS NOT NULL
                                       THEN (premium_collected
                                             - (close_price_per_share * contracts * 100)
                                             - COALESCE(close_fees, 0)
                                             - COALESCE(open_fees, 0))
                                   ELSE (premium_collected
                                         - COALESCE(close_fees, 0)
                                         - COALESCE(open_fees, 0))
                               END
                           ) as retained_premium,
                           AVG(
                               CASE
                                   WHEN strike > 0 AND julianday(COALESCE(closed_date, expiration)) > julianday(opened_date)
                                       THEN (
                                           (premium_collected - COALESCE(open_fees,0) - COALESCE(close_fees,0))
                                           / (strike * contracts * 100)
                                       ) * 365.0
                                         / (julianday(COALESCE(closed_date, expiration)) - julianday(opened_date))
                                   ELSE NULL
                               END
                           ) as avg_annualized_roc
                    FROM wheel_positions
                    WHERE status != 'OPEN'
                      AND closed_date >= ?
                    GROUP BY status
                    ORDER BY count DESC
                """, (cutoff,))
                rows = cursor.fetchall()
                return [
                    {
                        "outcome":           r[0],
                        "count":             r[1],
                        "total_premium":     r[2] or 0.0,   # gross (for reference)
                        "retained_premium":  r[3] or 0.0,   # what actually hit the ledger
                        "avg_annualized_roc": r[4],         # None if insufficient data
                    }
                    for r in rows
                ]
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to fetch wheel outcome distribution: {e}")
            return []

    def mark_wheel_position_alerted(self, position_id, dte):
        self.update_state(f"wheel_pos_{position_id}_last_alert_dte", dte)
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE wheel_positions SET last_alert_dte = ? WHERE id = ?", (dte, position_id))
                conn.commit()
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to mark wheel position alerted: {e}")

    def _add_to_premium_ledger(self, amount):
        """Margin-paydown ledger: cumulative wheel premium collected, same bucket the dividends pay into."""
        current = float(self.get_state("wheel_premium_collected_total", 0.0))
        self.update_state("wheel_premium_collected_total", current + amount)

    def get_total_premium_collected(self):
        return float(self.get_state("wheel_premium_collected_total", 0.0))

    def update_state(self, key, value):
        val_str = json.dumps(value)
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO global_state (key, value) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value, last_updated=CURRENT_TIMESTAMP
                """, (key, val_str))
                conn.commit()
        except sqlite3.OperationalError:
            pass

    # ------------------------------------------------------------------
    # Market Data Cache — cross-process, cross-script TD response cache.
    # Eliminates redundant API calls when multiple cron jobs run within
    # the same session window (e.g., the 13:28-13:50 premarket cluster).
    # ------------------------------------------------------------------

    _CACHE_TTL = {
        "dividends":   86400,   # 24h — payout schedule doesn't change intraday
        "statistics":  86400,   # 24h — fundamentals are daily at best
        "time_series_1day": 3600,  # 1h — daily bars are stable within a session
        "time_series_1week": 86400,
        "time_series_4h":    300,
        "quote":        300,    # 5 min — fresh enough for any cron in the cluster
        "default":      300,
    }

    @staticmethod
    def _cache_key(endpoint, params):
        """Deterministic cache key from endpoint + sorted params (api key excluded)."""
        filtered = {k: v for k, v in sorted(params.items()) if k != "apikey"}
        raw = f"{endpoint}|" + "&".join(f"{k}={v}" for k, v in filtered.items())
        return hashlib.sha1(raw.encode()).hexdigest()

    @staticmethod
    def _cache_ttl(endpoint, params):
        if endpoint == "time_series":
            interval = params.get("interval", "1day")
            key = f"time_series_{interval}"
            return EcosystemDatabase._CACHE_TTL.get(key, EcosystemDatabase._CACHE_TTL["default"])
        return EcosystemDatabase._CACHE_TTL.get(endpoint, EcosystemDatabase._CACHE_TTL["default"])

    def get_cached_response(self, endpoint, params):
        """Return cached API response dict if still fresh, else None."""
        key = self._cache_key(endpoint, params)
        ttl  = self._cache_ttl(endpoint, params)
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT response_json, cached_at FROM market_data_cache WHERE cache_key = ?", (key,)
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                age = (datetime.now() - datetime.fromisoformat(row[1])).total_seconds()
                if age > ttl:
                    return None
                return json.loads(row[0])
        except Exception:
            return None

    def set_cached_response(self, endpoint, params, data):
        """Write an API response to the cache."""
        if data is None:
            return
        key = self._cache_key(endpoint, params)
        try:
            with self._get_connection() as conn:
                conn.execute(
                    """INSERT INTO market_data_cache (cache_key, response_json, cached_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(cache_key) DO UPDATE
                       SET response_json=excluded.response_json, cached_at=excluded.cached_at""",
                    (key, json.dumps(data), datetime.now().isoformat())
                )
                conn.commit()
        except Exception:
            pass

    def purge_expired_cache(self):
        """Remove all cache entries older than their TTL. Called by audit.py daily."""
        try:
            cutoff = (datetime.now() - timedelta(seconds=max(self._CACHE_TTL.values()))).isoformat()
            with self._get_connection() as conn:
                conn.execute("DELETE FROM market_data_cache WHERE cached_at < ?", (cutoff,))
                conn.commit()
        except Exception:
            pass

    def get_state(self, key, default=None):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM global_state WHERE key = ?", (key,))
                result = cursor.fetchone()
                if result:
                    try:
                        value = json.loads(result[0])
                    except json.JSONDecodeError:
                        return result[0]
                    # A stored explicit null is treated the same as "key absent" — every caller's
                    # default exists precisely to handle "no usable value yet," and an explicit
                    # None (often written by a save/restore guard for a key that never existed)
                    # is exactly that case, not a meaningfully different one.
                    return value if value is not None else default
                return default
        except sqlite3.OperationalError:
            return default

    # ── IV Daily snapshot — feeds real IVR after 30+ trading days ─────────────

    def store_daily_iv(self, symbol: str, iv_value: float):
        """Insert today's ATM IV for symbol (UPSERT — safe to call multiple times)."""
        today = __import__("datetime").date.today().isoformat()
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO iv_daily (symbol, iv_value, recorded_date) VALUES (?, ?, ?) "
                    "ON CONFLICT(symbol, recorded_date) DO UPDATE SET iv_value=excluded.iv_value",
                    (symbol, iv_value, today),
                )
                conn.commit()
        except sqlite3.OperationalError as e:
            logger.error(f"store_daily_iv failed for {symbol}: {e}")

    def get_iv_history(self, symbol: str, days: int = 252) -> list:
        """Return list of (iv_value,) rows for symbol, newest first, up to `days` rows."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT iv_value FROM iv_daily WHERE symbol = ? "
                    "ORDER BY recorded_date DESC LIMIT ?",
                    (symbol, days),
                )
                return cursor.fetchall()
        except sqlite3.OperationalError:
            return []

    def get_iv_rank(self, symbol: str) -> dict:
        """
        Compute IVR from stored history. Returns dict with ivr, days_history, reliable.
        Reliable once ≥30 rows exist (roughly 6 trading weeks).
        """
        rows = self.get_iv_history(symbol, 252)
        days = len(rows)
        if days < 2:
            return {"ivr": 0.0, "days_history": days, "reliable": False, "tag": f"BUILDING ({days}/30 days)"}
        values = [float(r[0]) for r in rows]
        # Current IV is the most recent stored value (not a live fetch — caller provides live IV)
        current = values[0]
        low = min(values)
        high = max(values)
        ivr = ((current - low) / (high - low) * 100) if high > low else 0.0
        ivr = round(max(0.0, min(100.0, ivr)), 1)
        tag = "LOW IVR" if ivr < 35 else ("ELEVATED IVR" if ivr > 60 else "MID IVR")
        return {"ivr": ivr, "days_history": days, "reliable": days >= 30, "tag": tag, "current_iv": current}

    def store_cef_premium(self, ticker: str, nav: float, price: float, premium_pct: float,
                          log_date: str = None) -> bool:
        """
        Log today's CEF premium observation (upsert — safe to call every monitor.py tick).
        Builds up the rolling history that replaces the retired CEFConnect API.
        Returns True on new insert, False if the date already existed (skipped).
        """
        date_str = log_date or datetime.now().strftime("%Y-%m-%d")
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO cef_premium_log (ticker, log_date, nav, price, premium_pct) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (ticker.upper(), date_str, nav, price, round(premium_pct, 4)),
                )
                inserted = cursor.rowcount > 0
                conn.commit()
                return inserted
        except Exception as e:
            logger.error(f"store_cef_premium failed for {ticker}: {e}")
            return False

    def get_cef_premium_history(self, ticker: str, days: int = 252) -> list:
        """
        Return stored CEF premium observations newest-first, up to `days` rows.
        Each row is a dict: {log_date, nav, price, premium_pct}.
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT log_date, nav, price, premium_pct FROM cef_premium_log "
                    "WHERE ticker = ? ORDER BY log_date DESC LIMIT ?",
                    (ticker.upper(), days),
                )
                cols = ["log_date", "nav", "price", "premium_pct"]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_cef_premium_history failed for {ticker}: {e}")
            return []
