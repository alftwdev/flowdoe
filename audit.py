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
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                
                # 1. Purge stale gatekeeper locks (older than 24 hours)
                purge_threshold = (datetime.now() - timedelta(hours=24)).isoformat()
                cursor.execute("DELETE FROM alert_state_manager WHERE last_alert_time < ?", (purge_threshold,))
                purged_alerts = cursor.rowcount
                
                # 2. Prune Audit Logs to keep query times in the millisecond range
                cursor.execute("DELETE FROM audit_logs WHERE id NOT IN (SELECT id FROM audit_logs ORDER BY id DESC LIMIT 500)")
                purged_logs = cursor.rowcount
                
                # 3. VACUUM the database to reclaim space and optimize CPU execution
                cursor.execute("VACUUM")
                conn.commit()
                
                logger.info(f"Ecosystem Audit Complete. Purged {purged_alerts} stale strike locks and {purged_logs} historical logs.")
                return True
        except Exception as e:
            logger.critical(f"Database Audit Failed (Lock Contention / Concurrency Error): {e}")
            return False

if __name__ == "__main__":
    auditor = EcosystemAuditor()
    auditor.optimize_database_performance()
