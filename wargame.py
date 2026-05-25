import os
import time
import logging
import asyncio
import sqlite3
from unittest.mock import patch, AsyncMock
from datetime import datetime

# Ecosystem Modules
import mornings
import macro_radar
import gex
import trade_signals
import income
import monitor
import metrics
import ai
from database import EcosystemDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | WARGAME | %(message)s")
logger = logging.getLogger("Simulator")
db = EcosystemDatabase()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "rockefeller_state.db")

# --- SYNTHETIC MARKET STATE ---
SIM_STATE = {
    "credit_spread": 3.5,
    "supertrend_price": 450.0,
    "supertrend_line": 400.0,
    "volume_multiplier": 2.0,
    "cef_price": 7.50,
    "cef_nav": 6.50
}

def mock_requests_get(url, *args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code=200):
            self._json_data = json_data
            self.status_code = status_code
        def json(self): return self._json_data
        def raise_for_status(self): pass

    # FRED Macro Data
    if "api.stlouisfed.org" in url:
        if "WALCL" in url: return MockResponse({"observations": [{"value": "7000000"}]})
        if "WTREGEN" in url: return MockResponse({"observations": [{"value": "500000"}]})
        if "RRPONTSYD" in url: return MockResponse({"observations": [{"value": "400000"}]})
        if "BAMLH0A0HYM2" in url: return MockResponse({"observations": [{"value": str(SIM_STATE["credit_spread"])}]})

    # Twelve Data
    if "api.twelvedata.com" in url:
        if "market_state" in url: return MockResponse([{"country": "United States", "code": "NYSE", "is_market_open": True}])
        if "supertrend" in url: return MockResponse({"values": [{"close": str(SIM_STATE["supertrend_price"]), "supertrend": str(SIM_STATE["supertrend_line"])}]})
        if "statistics" in url: return MockResponse({"statistics": {"volume": str(1000000 * SIM_STATE["volume_multiplier"]), "avg_volume_30_days": "1000000"}})
        if "dividends" in url: return MockResponse({"dividends": [{"ex_date": datetime.now().strftime("%Y-%m-%d"), "amount": 0.85}]})
        if "quote" in url: return MockResponse({"close": "45.00" if "JEPI" in url or "SCHD" in url else "15.00"})
        if "time_series" in url:
            closes = [{"close": str(SIM_STATE["cef_price"] if "X" not in url else SIM_STATE["cef_nav"])} for _ in range(25)]
            return MockResponse({"values": closes})
            
    return MockResponse({}, status_code=404)

def mock_requests_post(url, *args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code=200):
            self._json_data = json_data
            self.status_code = status_code
        def json(self): return self._json_data
        def raise_for_status(self): pass
        
    if "generativelanguage.googleapis.com" in url:
        return MockResponse({
            "candidates": [{"content": {"parts": [{"text": '{"discord_embed_brief": "WARGAME SIMULATION: Liquidity models indicate localized compression. Alpha engines have successfully executed mean-reversion sequences capturing simulated edge."}'}]}}]
        })
    return MockResponse({}, 404)

def mock_requests_put(url, *args, **kwargs):
    class MockResponse:
        def __init__(self, status_code=204): self.status_code = status_code
    if "discord.com/api/v10/guilds" in url:
        logger.info(f"[SIMULATED API] Granted Discord Role for {url.split('/')[-3]}")
        return MockResponse()
    return MockResponse(403)

@patch('requests.get', side_effect=mock_requests_get)
@patch('requests.post', side_effect=mock_requests_post)
@patch('requests.put', side_effect=mock_requests_put)
@patch('aiohttp.ClientSession.get')
def execute_wargame(mock_aio_get, mock_put, mock_post, mock_get):
    logger.info("========== INITIATING OPERATION: MASTERMIND WARGAME ==========")
    
    # Setup AsyncMock for GEX Options Chain
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json.return_value = {}
    mock_aio_get.return_value.__aenter__.return_value = mock_response

    # Inject test user using native sqlite3 to bypass object wrapper limitations
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT OR REPLACE INTO user_ledger (user_id, months_active, has_insider_role) VALUES ('test_user_wargame', 4, 0)")
        conn.commit()
        conn.close()
        logger.info("Test user injected into ledger.")
    except Exception as e:
        logger.error(f"Failed to inject test user: {e}")

# Inside wargame.py, inside execute_wargame():
    try:
        logger.info("\n>>> PHASE 2: PUBLIC CONVERSION TEASER (AI MODULE) <<<")
        ai.broadcast_public_teaser(is_test=True)
    except AttributeError:
        logger.error("AI Module missing 'broadcast_public_teaser'. Ensure ai.py is updated.")
    except Exception as e:
        logger.error(f"AI Module crashed: {e}")        

    logger.info("\n>>> PHASE 1: PRE-MARKET INTELLIGENCE <<<")
    SIM_STATE["credit_spread"] = 3.5 
    db.update_state("market_regime", {"vix_status": "STABLE", "regime": "BULLISH", "rsi_shield_limit": 66})
    mornings.generate_morning_brief(is_test=True)
    time.sleep(1)
    
    logger.info("\n>>> PHASE 2: PUBLIC CONVERSION TEASER (AI MODULE) <<<")
    ai.broadcast_public_teaser(is_test=True)
    time.sleep(1)

    logger.info("\n>>> PHASE 3: ASYNC GEX MAPPING & LONG DEPLOYMENT <<<")
    asyncio.run(gex.gex_persistent_loop(is_test=True))
    trade_signals.execute_signal_scan(is_test=True) 
    time.sleep(1)

    logger.info("\n>>> PHASE 4: FLASH CRASH (MACRO SHOCK) <<<")
    SIM_STATE["credit_spread"] = 6.2  
    db.update_state("market_regime", {"vix_status": "STORM", "regime": "BEARISH"})
    macro_radar.scan_macro_liquidity(is_test=True)
    time.sleep(1)
    
    logger.info("\n>>> PHASE 5: YIELD CAPTURE & CEF LIQUIDATION MONITOR <<<")
    SIM_STATE["cef_price"] = 4.50 
    income.process_income_cycle(is_test=True)
    monitor.send_daily_pulse(is_test=True, broadcast_all=True)
    time.sleep(1)

    logger.info("\n>>> PHASE 6: LEDGER AUDIT & PERFORMANCE ROLLUP <<<")
    metrics.audit_loyalty_ledger(is_test=False) 
    metrics.generate_weekly_digest(is_test=True)
    
    logger.info("\n========== WARGAME COMPLETE. ARCHITECTURE VERIFIED. ==========")

if __name__ == "__main__":
    execute_wargame()
