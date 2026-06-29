import sqlite3
import threading
import json
import os
import logging
from datetime import datetime

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
                conn.commit()
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

    def open_wheel_position(self, symbol, position_type, strike, expiration, premium_collected, contracts=1):
        """Logs a newly opened CSP or CC position. premium_collected is per-contract, in dollars (mid*100)."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO wheel_positions (symbol, position_type, strike, expiration, premium_collected, contracts)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (symbol, position_type, strike, expiration, premium_collected, contracts))
                conn.commit()
                return cursor.lastrowid
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to open wheel position: {e}")
            return None

    def close_wheel_position(self, position_id, status="CLOSED", close_note=""):
        """Closes a position (CLOSED/ASSIGNED/EXPIRED/ROLLED) and adds its premium to the margin-paydown ledger."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT premium_collected, contracts FROM wheel_positions WHERE id = ? AND status = 'OPEN'", (position_id,))
                row = cursor.fetchone()
                if row is None:
                    return False
                premium_total = float(row[0]) * int(row[1])
                cursor.execute("""
                    UPDATE wheel_positions
                    SET status = ?, closed_date = CURRENT_TIMESTAMP, close_note = ?
                    WHERE id = ?
                """, (status, close_note, position_id))
                conn.commit()
            self._add_to_premium_ledger(premium_total)
            return True
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to close wheel position: {e}")
            return False

    def get_open_wheel_positions(self):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, symbol, position_type, strike, expiration, premium_collected, contracts, last_alert_dte
                    FROM wheel_positions WHERE status = 'OPEN'
                """)
                cols = [c[0] for c in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to fetch open wheel positions: {e}")
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
