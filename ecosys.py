import os
import json
import threading
import logging

logging.basicConfig(
    filename='ecosystem.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# Create a logger specific to the ecosystem
logger = logging.getLogger("Rockefeller")

def log_event(message, level="INFO"):
    """Redirects all non-critical metrics to a file stream to save CPU and disk space."""
    if level.upper() == "ERROR":
        logging.error(message)
    else:
        logging.info(message)

class EcosystemState:
    """
    Shared Memory State Engine.
    Eliminates disk I/O bottlenecks by keeping the macro ledger in RAM,
    only writing changes to disk when values diverge from historical metrics.
    """
    _instance = None
    _lock = threading.Lock()
    _state = {}
    _filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "market_regime.json")

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._load_from_disk()
        return cls._instance

    @classmethod
    def _load_from_disk(cls):
        if os.path.exists(cls._filepath):
            try:
                with open(cls._filepath, "r") as f:
                    cls._state = json.load(f)
            except Exception as e:
                log_event(f"Error loading state ledger from disk: {e}", "ERROR")
                cls._state = {}

    def get(self, key, default=None):
        with self._lock:
            return self._state.get(key, default)

    def update(self, new_data):
        with self._lock:
            changed = False
            for k, v in new_data.items():
                if self._state.get(k) != v:
                    self._state[k] = v
                    changed = True
            
            if changed:
                try:
                    with open(self._filepath, "w") as f:
                        json.dump(self._state, f, indent=4)
                    log_event(f"Ecosystem State safely persisted to disk ledger.")
                except Exception as e:
                    log_event(f"Error persisting state matrix: {e}", "ERROR")
