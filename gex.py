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
    # Venture Tier format query endpoint
    url = f"https://api.twelvedata.com/options/chain?symbol={symbol}&apikey={TD_API_KEY}"
    async with session.get(url, timeout=12) as response:
        if response.status != 200:
            raise Exception(f"Twelve Data API Link Exception: Status {response.status}")
        return await response.json()

async def perform_gex_calculations():
    logger.info("Executing asynchronous Gamma Exposure sweep...")
    try:
        async with aiohttp.ClientSession() as session:
            raw_data = await fetch_options_data(session, "SPY")
            
            contracts = raw_data.get("data", [])
            if not contracts:
                logger.warning("Empty options matrix fetched. Defaulting back to baseline levels.")
                target_strike = db.get_state("spy_gex_flip", 500.0)
            else:
                total_call_oi = 0
                total_put_oi = 0
                weighted_strike_sum = 0
                
                # Math Engine: Parse open interest profiles to locate structural imbalance boundaries
                for contract in contracts:
                    strike = float(contract.get("strike", 0))
                    call_oi = int(contract.get("call_open_interest", 0) or 0)
                    put_oi = int(contract.get("put_open_interest", 0) or 0)
                    
                    total_call_oi += call_oi
                    total_put_oi += put_oi
                    weighted_strike_sum += strike * (call_oi + put_oi)

                total_oi = total_call_oi + total_put_oi
                if total_oi > 0:
                    target_strike = weighted_strike_sum / total_oi
                else:
                    target_strike = 520.0 # Standard mathematical default anchor

            db.update_state("spy_gex_flip", target_strike)
            logger.info(f"GEX Sweep Complete. SPY Gamma Flip mapped to memory at ${target_strike:.2f}")
            
    except asyncio.TimeoutError:
        logger.error("API timing boundary breached due to high structural latency.")
        raise
    except Exception as e:
        logger.error(f"GEX Calculation Exception: {e}")
        raise

async def gex_persistent_loop(is_test=False):
    logger.info("Async GEX Engine initialized. Entering persistent watch state.")
    backoff = 60
    
    while True:
        try:
            await perform_gex_calculations()
            backoff = 60 # Reset circuit breaker
            if is_test: 
                break
            await asyncio.sleep(900)
            
        except asyncio.CancelledError:
            logger.info("GEX Engine gracefully shutting down.")
            break
        except Exception as e:
            logger.warning(f"GEX Engine fault. Retrying in {backoff} seconds.")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 900)

if __name__ == "__main__":
    is_test_mode = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    try:
        asyncio.run(gex_persistent_loop(is_test=is_test_mode))
    except KeyboardInterrupt:
        sys.exit(0)
