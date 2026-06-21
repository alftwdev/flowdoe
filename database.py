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
