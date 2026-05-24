import sqlite3
import threading
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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

    def _initialize_tables(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Dynamic Key-Value store (Replaces market_regime.json and last_alert.json)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS global_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Ledger (Replaces loyalty_ledger.json)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_ledger (
                    user_id TEXT PRIMARY KEY,
                    months_active INTEGER DEFAULT 0,
                    has_insider_role BOOLEAN DEFAULT 0
                )
            """)
            # System Audit Trail (Replaces ecosystem.log logic)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    level TEXT,
                    message TEXT
                )
            """)
            conn.commit()

    # --- STATE MANAGEMENT ---
    def update_state(self, key, value):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            val_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
            cursor.execute("""
                INSERT INTO global_state (key, value, last_updated) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET 
                value=excluded.value, last_updated=CURRENT_TIMESTAMP
            """, (key, val_str))
            conn.commit()

    def get_state(self, key, default=None):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM global_state WHERE key = ?", (key,))
            result = cursor.fetchone()
            if result:
                try: return json.loads(result[0])
                except json.JSONDecodeError: return result[0]
            return default

    # --- SRE AUDIT LOGGING ---
    def log_event(self, message, level="INFO"):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO audit_logs (level, message) VALUES (?, ?)", (level, message))
            conn.commit()

    # --- LOYALTY LEDGER METRICS ---
    def get_all_users(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, months_active, has_insider_role FROM user_ledger")
            return [{"user_id": r[0], "months_active": r[1], "has_insider_role": bool(r[2])} for r in cursor.fetchall()]

    def update_user_role(self, user_id, has_insider):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE user_ledger SET has_insider_role = ? WHERE user_id = ?", (int(has_insider), user_id))
            conn.commit()
