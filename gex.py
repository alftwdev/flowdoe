import os
import sys
import asyncio
import logging
from ecosys import EcosystemState

logger = logging.getLogger("GEX_Engine")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)
logger.setLevel(logging.INFO)

async def perform_gex_calculations():
    """Async execution block to offload network wait times."""
    logger.info("Executing asynchronous Gamma Exposure sweep...")
    # Insert Twelve Data options chain retrieval logic here
    # state = EcosystemState()
    # state.update("spy_gex_flip", target_strike)
    
    # Mocking execution for architectural layout
    await asyncio.sleep(2) 
    logger.info("GEX Sweep Complete. SPY Gamma Flip calculated and mapped to memory.")

async def gex_persistent_loop():
    logger.info("Async GEX Engine initialized. Entering persistent watch state.")
    while True:
        try:
            await perform_gex_calculations()
            # Non-blocking sleep for exactly 15 minutes
            await asyncio.sleep(900)
        except asyncio.CancelledError:
            logger.info("GEX Engine gracefully shutting down.")
            break
        except Exception as e:
            logger.error(f"GEX Async loop crashed: {e}. Backing off.")
            await asyncio.sleep(60)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        asyncio.run(perform_gex_calculations())
    else:
        # Run this in the PythonAnywhere Always-on tab
        try:
            asyncio.run(gex_persistent_loop())
        except KeyboardInterrupt:
            logger.info("Process terminated by user.")
