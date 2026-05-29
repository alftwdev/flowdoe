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
                cls._instance._initialized = False
                cls._instance._initialize_tables()
            return cls._instance

    def __init__(self, db_path='rockefeller_state.db'):
        if getattr(self, '_initialized', False):
            return
            
        # Added a 15-second timeout to handle high-concurrency locks gracefully
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=15.0)
        self.cursor = self.conn.cursor()
        
        # Switched to DELETE mode: Safe for PythonAnywhere's Network File System (NFS)
        self.cursor.execute('PRAGMA journal_mode=DELETE;')
        self.cursor.execute('PRAGMA synchronous=NORMAL;')
        self.conn.commit() 
        self._initialized = True

    def _initialize_tables(self):
        with sqlite3.connect(self.db_path, timeout=15.0) as conn:
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
            conn.commit()

    def update_state(self, key, value):
        val_str = json.dumps(value)
        with sqlite3.connect(self.db_path, timeout=15.0) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO global_state (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, last_updated=CURRENT_TIMESTAMP
            """, (key, val_str))
            conn.commit()

    def get_state(self, key, default=None):
        with sqlite3.connect(self.db_path, timeout=15.0) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM global_state WHERE key = ?", (key,))
            result = cursor.fetchone()
            if result:
                try:
                    return json.loads(result[0])
                except json.JSONDecodeError:
                    return result[0]
            return default

    def log_event(self, message, level="INFO"):
        with sqlite3.connect(self.db_path, timeout=15.0) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO audit_logs (level, message) VALUES (?, ?)", (level, message))
            conn.commit()

    def get_all_users(self):
        with sqlite3.connect(self.db_path, timeout=15.0) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, months_active, has_insider_role FROM users")
            return [{"user_id": r[0], "months_active": r[1], "has_insider_role": bool(r[2])} for r in cursor.fetchall()]

    def update_user_role(self, user_id, has_role):
        with sqlite3.connect(self.db_path, timeout=15.0) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET has_insider_role = ? WHERE user_id = ?", (int(has_role), user_id))
            conn.commit()
