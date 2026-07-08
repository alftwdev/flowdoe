"""
shared_ws.py — Unified TD WebSocket price stream (Grow tier, 8 trial slots).

Single connection slot subscribing CLM, CRF, VIXY, QQQ, TQQQ, SPY.
monitor.py and tqqq.py both import this module; they register callbacks
and read `get_price()` without opening separate connections.

Connection slot usage: 1 of 8 trial WS slots (7 remain for future expansion).
Self-healing: if the WS drops, the background thread reconnects automatically.
"""

import os
import threading
import logging
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

logger = logging.getLogger("SharedWS")

# All symbols needed across both monitor.py and tqqq.py — one subscription covers all.
WS_SYMBOLS = ["CLM", "CRF", "VIXY", "QQQ", "TQQQ", "SPY"]

# Minimum price move to trigger registered callbacks — avoids callback storms on
# sub-cent quote ticks that carry no actionable information.
CALLBACK_THRESHOLD_PCT = 0.05  # 0.05% move triggers callbacks


class TDWebSocketManager:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.latest_prices: dict[str, float] = {}
        self.latest_timestamps: dict[str, datetime] = {}
        self._callbacks: list = []
        self._ws = None
        self._lock = threading.Lock()
        self._connected = False
        self._connected_at = 0.0
        self._bg_thread = None

    def register_callback(self, fn):
        """
        Register a callback fn(symbol, price, event) that fires on every meaningful
        price update. Callbacks run in the WS receive thread — keep them fast;
        spawn a thread if you need to do expensive work (like compute_cornerstone_reports).
        """
        self._callbacks.append(fn)

    def get_price(self, symbol: str) -> float:
        """Return latest WebSocket price for symbol, or 0.0 if not yet received."""
        return self.latest_prices.get(symbol, 0.0)

    def get_age_seconds(self, symbol: str) -> float:
        """Seconds since last price update for this symbol. Returns inf if never received."""
        ts = self.latest_timestamps.get(symbol)
        if ts is None:
            return float("inf")
        return (datetime.utcnow() - ts).total_seconds()

    def is_fresh(self, symbol: str, max_age_seconds: float = 60.0) -> bool:
        """True if we have a price and it arrived within max_age_seconds."""
        return self.get_price(symbol) > 0.0 and self.get_age_seconds(symbol) <= max_age_seconds

    def _on_event(self, event: dict):
        if event.get("event") != "price":
            return
        symbol = event.get("symbol", "")
        try:
            price = float(event.get("price", 0.0))
        except (ValueError, TypeError):
            return
        if price <= 0:
            return

        with self._lock:
            prev_price = self.latest_prices.get(symbol, 0.0)
            self.latest_prices[symbol] = price
            self.latest_timestamps[symbol] = datetime.utcnow()

        # Only fire callbacks when price moves enough to matter
        if prev_price > 0:
            move_pct = abs((price - prev_price) / prev_price * 100)
            if move_pct < CALLBACK_THRESHOLD_PCT:
                return

        for cb in self._callbacks:
            try:
                cb(symbol, price, event)
            except Exception as e:
                logger.error(f"WS callback error for {symbol}: {e}")

    def _close_existing(self):
        """
        Attempt a clean disconnect before replacing the ws instance.
        The TD SDK spawns internal threads on connect() — skipping this leaks them.
        Enough reconnect storms (equity close cycling) exhaust the OS thread limit
        and produce 'can't start new thread' from the SDK's own logger.
        """
        if self._ws is not None:
            try:
                self._ws.disconnect()
            except Exception:
                pass
            self._ws = None
        self._connected = False

    def connect(self):
        self._close_existing()
        try:
            from twelvedata import TDClient
            td = TDClient(apikey=self.api_key)
            self._ws = td.websocket(symbols=WS_SYMBOLS, on_event=self._on_event)
            self._ws.connect()
            self._connected = True
            self._connected_at = time.time()
            logger.info(f"WebSocket connected — subscribed: {WS_SYMBOLS}")
        except Exception as e:
            logger.error(f"WebSocket connect failed: {e}")
            self._connected = False
            self._ws = None

    def keep_alive(self):
        """Blocking receive loop. Call in a dedicated thread."""
        if self._ws and self._connected:
            try:
                self._ws.keep_alive()
            except Exception as e:
                logger.error(f"WebSocket keep_alive error: {e}")
            finally:
                self._connected = False

    def _run_forever(self):
        """
        Auto-reconnecting loop — runs in the background daemon thread.
        Backoff only resets after a connection stable for >= 60s.
        Short-lived drops (TD cycling at equity close) escalate: 30→60→120→300s.
        """
        backoff = 30.0
        while True:
            try:
                self.connect()
                if self._connected:
                    self.keep_alive()
            except Exception as e:
                logger.error(f"WS run loop error: {e}")
            stable = self._connected_at > 0 and (time.time() - self._connected_at) >= 60
            if stable:
                backoff = 30.0
            else:
                backoff = min(backoff * 2, 300.0)
            logger.info(f"WebSocket disconnected — reconnecting in {backoff:.0f}s")
            time.sleep(backoff)

    def start_background(self) -> threading.Thread:
        """
        Starts the WebSocket background thread — idempotent. Safe to call from
        multiple processes sharing this singleton; the thread is only started once.
        Both monitor.py and tqqq.py call this; the second call is a no-op.
        """
        with self._lock:
            if self._bg_thread is not None and self._bg_thread.is_alive():
                logger.info("WebSocket background thread already running — skipping duplicate start.")
                return self._bg_thread
            t = threading.Thread(target=self._run_forever, daemon=True, name="TDWebSocket")
            t.start()
            self._bg_thread = t
        logger.info("WebSocket background thread started.")
        return t


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton — both monitor.py and tqqq.py share one instance.
# ─────────────────────────────────────────────────────────────────────────────

_manager = None  # TDWebSocketManager singleton
_manager_lock = threading.Lock()


def get_ws_manager(api_key: str = None) -> TDWebSocketManager:
    """
    Returns the shared WebSocket manager, creating it on first call.
    Both monitor.py and tqqq.py call this — they always get the same instance,
    so only one WebSocket connection slot is consumed regardless of import order.
    """
    global _manager
    if _manager is None:
        with _manager_lock:
            if _manager is None:
                key = api_key or os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
                if not key:
                    raise RuntimeError("TD API key not found — set TWELVE_DATA_API_KEY in .env")
                _manager = TDWebSocketManager(key)
    return _manager
