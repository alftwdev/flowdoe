import os
import json
import fcntl
import logging
import sys
from datetime import datetime

# 1. Global Logger Configuration
logger = logging.getLogger("Ecosystem")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "market_regime.json")
LOG_FILE = os.path.join(BASE_DIR, "ecosystem.log")

class EcosystemState:
    """Thread-safe and Process-safe memory ledger using POSIX locks."""
    def __init__(self, filepath=STATE_FILE):
        self.filepath = filepath
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w") as f:
                json.dump({"vix_status": "STABLE", "regime": "BULLISH", "last_updated": str(datetime.utcnow())}, f)

    def read(self):
        try:
            with open(self.filepath, "r") as f:
                # Acquire shared lock for reading
                fcntl.flock(f, fcntl.LOCK_SH)
                data = json.load(f)
                fcntl.flock(f, fcntl.LOCK_UN)
                return data
        except Exception as e:
            logger.error(f"State Read Error: {e}")
            return {}

    def get(self, key, default=None):
        data = self.read()
        return data.get(key, default)

    def update(self, key, value):
        try:
            with open(self.filepath, "r+") as f:
                # Acquire exclusive lock for writing to prevent race conditions
                fcntl.flock(f, fcntl.LOCK_EX)
                data = json.load(f)
                data[key] = value
                data["last_updated"] = str(datetime.utcnow())
                
                f.seek(0)
                json.dump(data, f, indent=4)
                f.truncate()
                fcntl.flock(f, fcntl.LOCK_UN)
            logger.info(f"Ecosystem State Updated: [{key}] -> [{value}]")
            return True
        except Exception as e:
            logger.error(f"State Write Error: {e}")
            return False

def log_event(message, level="INFO"):
    """Persistent audit trail for SRE review."""
    timestamp = datetime.utcnow().isoformat()
    log_entry = f"[{timestamp}] [{level}] {message}\n"
    with open(LOG_FILE, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(log_entry)
        fcntl.flock(f, fcntl.LOCK_UN)
