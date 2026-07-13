"""
seed_cef_premiums.py — One-time CEFConnect premium z-score calibration.

Run once on PythonAnywhere to anchor monitor.py's CLM/CRF z-score calculation
with real 252-day empirical data instead of the hardcoded defaults (mu=15, sigma=4).

After seeding, run daily via market_scheduler.py at 22:30 UTC to keep rolling:
  python seed_cef_premiums.py --daily

Usage:
  python seed_cef_premiums.py          # seed both CLM and CRF
  python seed_cef_premiums.py --daily  # refresh (same logic, stable to re-run)
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
        logger.info(f"Calibrating {ticker} premium z-score from CEFConnect...")
        result = engine.calibrate_cef_premium_zscore(ticker)
        if result:
            results[ticker] = result
            logger.info(
                f"  {ticker}: mu={result['mu']:.2f}% | sigma={result['sigma']:.2f}% | "
                f"n={result['n']} days — DB updated."
            )
        else:
            logger.warning(f"  {ticker}: calibration failed — DB unchanged, defaults remain.")

    if results:
        logger.info(
            "Done. monitor.py will now use empirical z-scores on next loop tick. "
            "Re-run weekly (or add to market_scheduler.py at 22:30 UTC) to keep rolling."
        )
    else:
        logger.error("Both calibrations failed. Check CEFConnect connectivity.")
        sys.exit(1)

if __name__ == "__main__":
    main()
