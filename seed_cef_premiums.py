"""
seed_cef_premiums.py — CEF premium z-score initialization.

CEFConnect's v3 API was deprecated (all endpoints return 404 as of Jul 2026).
This script now:
  1. Sets informed priors for CLM/CRF in DB (mu/sigma derived from 5-year
     historical premium range — meaningfully better than the 15/4 hardcoded defaults)
  2. Creates the cef_premium_log table (auto-created by EcosystemDatabase on first run,
     but this forces initialization explicitly)
  3. Prints what to expect going forward (daily accumulation via monitor.py)

After running this once, monitor.py logs each day's real premium automatically.
The cef_calibrate mode (22:30 UTC daily) updates mu/sigma as data accumulates.
After 20 trading days → empirical data takes over from priors.
After 90 trading days → solid rolling baseline.

Usage:
  python seed_cef_premiums.py
"""

import sys
import logging
from analytics import HighFidelityAnalyticsEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("CEFSeed")

def main():
    engine = HighFidelityAnalyticsEngine()
    results = {}
    for ticker in ["CLM", "CRF"]:
        logger.info(f"Initializing {ticker} premium z-score baseline...")
        result = engine.calibrate_cef_premium_zscore(ticker)
        if result:
            results[ticker] = result
            logger.info(
                f"  {ticker}: mu={result['mu']:.2f}% | sigma={result['sigma']:.2f}% "
                f"| source={result['source']} — DB updated."
            )
        else:
            logger.warning(f"  {ticker}: calibration returned empty — check DB connectivity.")

    logger.info("")
    logger.info("Done. What happens next:")
    logger.info("  • monitor.py logs today's real CLM/CRF premium to cef_premium_log on each run")
    logger.info("  • market_scheduler.py fires cef_calibrate at 22:30 UTC daily")
    logger.info("  • After 20 trading days: empirical data replaces the informed priors")
    logger.info("  • After 90 trading days: solid rolling baseline for production z-scores")

if __name__ == "__main__":
    main()
