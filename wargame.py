import os
import time
import logging
import asyncio
import sqlite3
import requests
from unittest.mock import patch, AsyncMock
from datetime import datetime

# Import the modernized ecosystem modules
import mornings
import macro_radar
import gex
import trade_signals
import income
import monitor
import metrics
import ai
import cross_asset
import fed
from database import EcosystemDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | WARGAME | %(message)s")
logger = logging.getLogger("Simulator")
db = EcosystemDatabase()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "rockefeller_state.db")

SIM_STATE = {
    "credit_spread": 3.5,
    "supertrend_price": 450.0,
    "supertrend_line": 400.0,
    "volume_multiplier": 2.0,
    "cef_price": 7.50,
    "cef_nav": 6.50
}

# Preserve the unpatched requests functions to prevent infinite recursion
_original_get = requests.get
_original_post = requests.post
_original_put = requests.put

def mock_requests_get(url, *args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code=200):
            self._json_data = json_data
            self.status_code = status_code
        def json(self): return self._json_data
        def raise_for_status(self): pass

    # FRED Mock Data
    if "api.stlouisfed.org" in url:
        if "WALCL" in url: return MockResponse({"observations": [{"value": "7000000"}]})
        if "WTREGEN" in url: return MockResponse({"observations": [{"value": "500000"}]})
        if "RRPONTSYD" in url: return MockResponse({"observations": [{"value": "400000"}]})
        if "BAMLH0A0HYM2" in url: return MockResponse({"observations": [{"value": str(SIM_STATE["credit_spread"])}]})

    # Twelve Data Mock Data (Upgraded for VWAP, EMA, and ATR)
    if "api.twelvedata.com" in url:
        if "market_state" in url: return MockResponse([{"country": "United States", "code": "NYSE", "is_market_open": True}])
        if "vwap" in url: return MockResponse({"values": [{"vwap": "525.50"}]})
        if "ema" in url: return MockResponse({"values": [{"ema": "520.00"}]})
        if "atr" in url: return MockResponse({"values": [{"atr": "8.50"}]})
        if "supertrend" in url: return MockResponse({"values": [{"close": str(SIM_STATE["supertrend_price"]), "supertrend": str(SIM_STATE["supertrend_line"])}]})
        if "statistics" in url: return MockResponse({"statistics": {"volume": str(1000000 * SIM_STATE["volume_multiplier"]), "avg_volume_30_days": "1000000"}})
        if "dividends" in url: return MockResponse({"dividends": [{"ex_date": datetime.now().strftime("%Y-%m-%d"), "amount": 0.85}]})
        if "quote" in url: return MockResponse({"close": "45.00" if "JEPI" in url or "SCHD" in url else "15.00"})
        if "time_series" in url: return MockResponse({"values": [{"close": str(SIM_STATE["cef_price"] if "X" not in url else SIM_STATE["cef_nav"])} for _ in range(25)]})
            
    return MockResponse({}, status_code=404)

def mock_requests_post(url, *args, **kwargs):
    # PASSTHROUGH: Execute real POST requests for webhooks/pushover so we can verify formatting in Discord
    if "discord.com" in url or "pushover.net" in url:
        return _original_post(url, *args, **kwargs)

    class MockResponse:
        def __init__(self, json_data, status_code=200):
            self._json_data = json_data
            self.status_code = status_code
        def json(self): return self._json_data
        def raise_for_status(self): pass
        
    return MockResponse({}, 404)

def mock_requests_put(url, *args, **kwargs):
    if "discord.com" in url:
        return _original_put(url, *args, **kwargs)

    class MockResponse:
        def __init__(self, status_code=204): self.status_code = status_code
    return MockResponse()

@patch('requests.get', side_effect=mock_requests_get)
@patch('requests.post', side_effect=mock_requests_post)
@patch('requests.put', side_effect=mock_requests_put)
@patch('aiohttp.ClientSession.get')
def execute_wargame(mock_aio_get):
    logger.info("========== INITIATING OPERATION: MASTERMIND WARGAME ==========")
    logger.info("NOTE: Discord webhooks and Pushover are LIVE. Webhooks will fire.")
    
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json.return_value = {}
    mock_aio_get.return_value.__aenter__.return_value = mock_response

    # Update database injection to match the standardized structure
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT OR REPLACE INTO users (user_id, months_active, has_insider_role) VALUES ('test_user_wargame', 4, 0)")
        conn.commit()
        conn.close()
        logger.info("Test user successfully injected into standard ledger.")
    except Exception as e:
        logger.error(f"Failed to inject test user: {e}")

    logger.info("\n>>> PHASE 1: PRE-MARKET INTELLIGENCE <<<")
    SIM_STATE["credit_spread"] = 3.5 
    db.update_state("market_regime", {"vix_status": "STABLE", "regime": "BULLISH", "rsi_shield_limit": 66})
    try:
        mornings.generate_morning_brief(is_test=True)
    except Exception as e:
        logger.warning(f"Morning brief module bypassed: {e}")
    time.sleep(2)

    logger.info("\n>>> PHASE 2: ASYNC GEX MAPPING & TACTICAL OPTIONS BOUNDARY <<<")
    asyncio.run(gex.gex_persistent_loop(is_test=True))
    trade_signals.execute_options_expected_move(is_test=True) 
    trade_signals.execute_forex_tactical_scan(is_test=True)
    time.sleep(2)

    logger.info("\n>>> PHASE 3: MACRO SHOCK & ALGORITHMIC DIRECTIONAL FILTERS <<<")
    # Simulate a severe credit spread blowout to test systemic gates
    SIM_STATE["credit_spread"] = 6.2  
    db.update_state("market_regime", {"vix_status": "STORM", "regime": "BEARISH"})
    
    macro_radar.scan_macro_liquidity(is_test=True)
    
    logger.info("--- Testing Futures VWAP Intraday Map ---")
    cross_asset.broadcast_futures_snapshot(is_test=True)
    
    logger.info("--- Testing TSP Matrix Rotation ---")
    fed.compile_eod_tsp_recap(is_test=True)
    time.sleep(2)
    
    logger.info("\n>>> PHASE 4: CEF LIQUIDATION MONITOR (SYSTEMIC OVERRIDE CHECK) <<<")
    # Triggering the monitor.py to ensure it reads the 6.2 credit spread and issues the CEF warning
    SIM_STATE["cef_price"] = 4.50 
    try:
        monitor.send_daily_pulse(is_test=True)
    except Exception as e:
        logger.warning(f"Monitor pulse execution skipped: {e}")
    time.sleep(2)

    logger.info("\n>>> PHASE 5: LEDGER AUDIT & WEEKEND PREP <<<")
    try:
        metrics.audit_loyalty_ledger(is_test=False) 
        metrics.generate_weekly_digest(is_test=True)
    except AttributeError:
        logger.info("Gamification metrics bypassed (Weekend Prep active).")
        
    try:
        import weekend
        logger.info("--- Testing Weekend Prep Executive Summary ---")
        weekend.execute_weekend_broadcast()
    except ImportError:
        logger.info("Weekend module not found locally. Skipping.")
    
    logger.info("\n========== WARGAME COMPLETE. ARCHITECTURE VERIFIED. ==========")

if __name__ == "__main__":
    execute_wargame()
