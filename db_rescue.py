import sqlite3
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DB_Rescue")

def rescue_database():
    corrupted_db = "rockefeller_state.db"
    rescued_db = "rockefeller_state_clean.db"

    if not os.path.exists(corrupted_db):
        logger.error(f"Could not find {corrupted_db}. Ensure you are in the correct directory.")
        return

    logger.info("Initiating surgical data extraction from malformed database...")

    try:
        # Connect to both the broken database and the fresh blank slate
        conn_bad = sqlite3.connect(corrupted_db)
        cursor_bad = conn_bad.cursor()

        conn_good = sqlite3.connect(rescued_db)
        cursor_good = conn_good.cursor()

        # Step 1: Rebuild tables with the correct schema
        cursor_good.execute("CREATE TABLE IF NOT EXISTS global_state (key TEXT PRIMARY KEY, value TEXT, last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        cursor_good.execute("CREATE TABLE IF NOT EXISTS audit_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT, message TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        cursor_good.execute("CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY, months_active INTEGER DEFAULT 0, has_insider_role BOOLEAN DEFAULT 0)")

        # Step 2: Extract and Transplant Global State
        try:
            cursor_bad.execute("SELECT key, value, last_updated FROM global_state")
            rows = cursor_bad.fetchall()
            cursor_good.executemany("INSERT OR IGNORE INTO global_state (key, value, last_updated) VALUES (?, ?, ?)", rows)
            logger.info(f"✅ Successfully rescued {len(rows)} memory states.")
        except Exception as e:
            logger.warning(f"Partial state loss: {e}")

        # Step 3: Extract and Transplant User Ledgers (Critical Data)
        try:
            cursor_bad.execute("SELECT user_id, months_active, has_insider_role FROM users")
            rows = cursor_bad.fetchall()
            cursor_good.executemany("INSERT OR IGNORE INTO users (user_id, months_active, has_insider_role) VALUES (?, ?, ?)", rows)
            logger.info(f"✅ Successfully rescued {len(rows)} user profiles.")
        except Exception as e:
            logger.warning(f"Partial user loss: {e}")

        # Commit the salvaged data
        conn_good.commit()
        conn_bad.close()
        conn_good.close()

        # Step 4: Safely swap the corrupted file for the clean one
        os.rename(corrupted_db, "rockefeller_state_corrupted_backup.db")
        os.rename(rescued_db, corrupted_db)

        logger.info("🎯 Rescue complete. The clean, uncorrupted database is now active.")

    except Exception as e:
        logger.error(f"Critical failure during extraction: {e}")

if __name__ == "__main__":
    rescue_database()
