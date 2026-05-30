import os
import sys
import asyncio
import aiohttp
import logging
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("GEX_Engine")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

db = EcosystemDatabase()

async def fetch_options_data(session, symbol):
    url = f"https://api.twelvedata.com/options/chain?symbol={symbol}&apikey={TD_API_KEY}"
    async with session.get(url, timeout=12) as response:
        if response.status != 200:
            return {"data": []}
        return await response.json()
        
async def fetch_spot_price(session, symbol):
    url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}"
    async with session.get(url, timeout=10) as response:
        if response.status != 200:
            return 0.0
        res = await response.json()
        return float(res.get("price", 0.0))

async def perform_gex_calculations():
    logger.info("Executing asynchronous Gamma & Institutional OI sweep...")
    try:
        async with aiohttp.ClientSession() as session:
            raw_data = await fetch_options_data(session, "SPY")
            spot_price = await fetch_spot_price(session, "SPY")
            
            contracts = raw_data.get("data", [])
            if not contracts:
                target_strike = db.get_state("spy_gex_flip", 500.0)
            else:
                total_call_oi = 0
                total_put_oi = 0
                weighted_strike_sum = 0
                
                # Metrics for "Massive API" Replication
                strike_oi_map = {}

                for contract in contracts:
                    strike = float(contract.get("strike", 0))
                    call_oi = int(contract.get("call_open_interest", 0) or 0)
                    put_oi = int(contract.get("put_open_interest", 0) or 0)
                    
                    total_call_oi += call_oi
                    total_put_oi += put_oi
                    weighted_strike_sum += strike * (call_oi + put_oi)
                    
                    # Track Open Interest Clustering
                    if strike not in strike_oi_map:
                        strike_oi_map[strike] = 0
                    strike_oi_map[strike] += (call_oi + put_oi)

                total_oi = total_call_oi + total_put_oi
                target_strike = (weighted_strike_sum / total_oi) if total_oi > 0 else 520.0
                
                # Compute Put/Call Ratio and OI Walls
                put_call_ratio = total_put_oi / total_call_oi if total_call_oi > 0 else 1.0
                db.update_state("spy_put_call_ratio", round(put_call_ratio, 3))
                
                if strike_oi_map:
                    top_strike = max(strike_oi_map, key=strike_oi_map.get)
                    db.update_state("spy_highest_oi_strike", top_strike)

            db.update_state("spy_gex_flip", target_strike)
            logger.info(f"GEX Sweep Complete. Gamma Flip: ${target_strike:.2f}")
            
            vrp_score = db.get_state("SPY_vrp_latest", 0.0)
            friction_state = "🔄 NEUTRAL FRICTION REGIME"
            
            if spot_price > 0:
                if spot_price < target_strike and vrp_score > 0:
                    friction_state = "⚠️ SHORT GAMMA LIQUIDITY TRAP (Explosive RV)"
                elif spot_price > target_strike and vrp_score < 0:
                    friction_state = "🟢 LONG GAMMA BUFFER REGIME (Mean Reverting Quiet)"
            
            db.update_state("gamma_friction_state", friction_state)
            
    except Exception as e:
        logger.error(f"GEX Calculation Exception: {e}")

async def gex_persistent_loop(is_test=False):
    logger.info("Async GEX Engine initialized.")
    backoff = 60
    while True:
        try:
            await perform_gex_calculations()
            if is_test: break
            await asyncio.sleep(900)
        except Exception as e:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 900)

if __name__ == "__main__":
    is_test_mode = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    asyncio.run(gex_persistent_loop(is_test=is_test_mode))
