import sqlite3
import threading
import json
import os
import logging

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
        # CRITICAL UPGRADE: We strictly FORBID persistent connections (self.conn) here.
        # Every method opens and cleanly closes its own connection to prevent NFS file locking.
        pass

    def _get_connection(self):
        """Helper to safely open an ephemeral connection with the right pragmas."""
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
                    CREATE TABLE IF NOT EXISTS users (
                        user_id TEXT PRIMARY KEY,
                        months_active INTEGER DEFAULT 0,
                        has_insider_role BOOLEAN DEFAULT 0
                    )
                """)
                # Standardized trade context table added by metrics.py
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS trade_context_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TIMESTAMP,
                        symbol TEXT,
                        side TEXT,
                        vrp_reading REAL
                    )
                """)
                conn.commit()
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to initialize tables. Lock detected: {e}")

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
        except sqlite3.OperationalError as e:
            logger.warning(f"Could not update state for {key}. DB Locked: {e}")

    def get_state(self, key, default=None):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM global_state WHERE key = ?", (key,))
                result = cursor.fetchone()
                if result:
                    try:
                        return json.loads(result[0])
                    except json.JSONDecodeError:
                        return result[0]
                return default
        except sqlite3.OperationalError as e:
            logger.warning(f"Could not read state for {key}. DB Locked: {e}")
            return default

    def log_event(self, message, level="INFO"):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO audit_logs (level, message) VALUES (?, ?)", (level, message))
                conn.commit()
        except sqlite3.OperationalError:
            pass # Silent fail to prevent log cascades from crashing the main system

    def get_all_users(self):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id, months_active, has_insider_role FROM users")
                return [{"user_id": r[0], "months_active": r[1], "has_insider_role": bool(r[2])} for r in cursor.fetchall()]
        except sqlite3.OperationalError as e:
            logger.warning(f"Could not retrieve users. Error: {e}")
            return []

    def update_user_role(self, user_id, has_role):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE users SET has_insider_role = ? WHERE user_id = ?", (int(has_role), user_id))
                conn.commit()
        except sqlite3.OperationalError as e:
            logger.warning(f"Could not update user role. Error: {e}")
