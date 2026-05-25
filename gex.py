import os
import sys
import asyncio
import aiohttp
import logging
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("GEX_Engine")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

db = EcosystemDatabase()

async def fetch_options_data(session, symbol):
    """Non-blocking network request using aiohttp."""
    url = f"https://api.twelvedata.com/options/chain?symbol={symbol}&apikey={TD_API_KEY}"
    async with session.get(url, timeout=10) as response:
        if response.status != 200:
            raise Exception(f"HTTP Error {response.status}")
        return await response.json()

async def perform_gex_calculations():
    logger.info("Executing asynchronous Gamma Exposure sweep...")
    try:
        async with aiohttp.ClientSession() as session:
            # 1. Non-blocking fetch
            # data = await fetch_options_data(session, "SPY")
            
            # 2. GEX Math (Placeholder for options chain processing)
            await asyncio.sleep(1) # Simulating heavy math computation
            target_strike = 530.00 # Simulated Gamma Flip Line
            
            # 3. Write to state
            db.update_state("spy_gex_flip", target_strike)
            logger.info(f"GEX Sweep Complete. SPY Gamma Flip calculated at ${target_strike:.2f} and mapped to memory.")
            
    except asyncio.TimeoutError:
        logger.error("API request timed out. Network latency high.")
        raise
    except Exception as e:
        logger.error(f"GEX Calculation Error: {e}")
        raise

async def gex_persistent_loop(is_test=False):
    logger.info("Async GEX Engine initialized. Entering persistent watch state.")
    backoff = 60 # Starting backoff timer
    
    while True:
        try:
            await perform_gex_calculations()
            backoff = 60 # Reset backoff on success
            
            if is_test: 
                break
                
            await asyncio.sleep(900) # 15-minute standard loop
            
        except asyncio.CancelledError:
            logger.info("GEX Engine gracefully shutting down.")
            break
        except Exception as e:
            logger.warning(f"GEX Engine fault. Engaging circuit breaker. Retrying in {backoff} seconds.")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 900) # Exponential backoff capped at 15 minutes

if __name__ == "__main__":
    is_test_mode = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    try:
        asyncio.run(gex_persistent_loop(is_test=is_test_mode))
    except KeyboardInterrupt:
        logger.info("Process terminated by user.")
