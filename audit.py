import os
import sqlite3
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

logger = logging.getLogger("Phase_C_Auditor")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

class EcosystemAuditor:
    def __init__(self, db_path="rockefeller_state.db"):
        self.db_path = os.path.join(BASE_DIR, db_path)

    def optimize_database_performance(self):
        """
        Phase C: Database Gatekeeper Maintenance & Lean CPU Optimization.
        Prevents infinite state-bloat and ensures the 3-Strike rule remains mathematically rigid.
        """
        logger.info("Initiating Phase C Systemic Integrity Audit...")
        try:
            # Block 1: Standard transactions for pruning stale gatekeepers
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                
                purge_threshold = (datetime.now() - timedelta(hours=24)).isoformat()
                cursor.execute("DELETE FROM alert_state_manager WHERE last_alert_time < ?", (purge_threshold,))
                purged_alerts = cursor.rowcount
                
                cursor.execute("DELETE FROM audit_logs WHERE id NOT IN (SELECT id FROM audit_logs ORDER BY id DESC LIMIT 500)")
                purged_logs = cursor.rowcount
                
                conn.commit()
                logger.info(f"Ecosystem Audit Complete. Purged {purged_alerts} stale strike locks and {purged_logs} historical logs.")
            
            # Block 2: Isolated Auto-Commit execution for VACUUM to prevent transaction crashes
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                conn.isolation_level = None  # Force auto-commit mode required for VACUUM
                conn.execute("VACUUM")
                logger.info("Database VACUUM completed successfully. Ecosystem CPU execution optimized.")
                
            return True
        except Exception as e:
            logger.critical(f"Database Audit Failed (Lock Contention / Concurrency Error): {e}")
            return False

if __name__ == "__main__":
    auditor = EcosystemAuditor()
    auditor.optimize_database_performance()
