import os
import sys
import logging
import time
import requests
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("Macro_Radar")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

db = EcosystemDatabase()

load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY") 
WEBHOOK_MACRO = os.getenv("WEBHOOK_MARKET_ANALYSIS")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError: 
    HAS_ESSENTIALS = False

def validate_environment():
    """Gatekeeper to ensure crucial API keys exist before execution."""
    required_keys = ["FRED_API_KEY"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        db.log_event(f"CRITICAL: Macro Radar missing environment variables: {missing}", "ERROR")
        sys.exit(1)

def fetch_fred_metric(series_id):
    """Fetches latest metric value from St. Louis Federal Reserve API."""
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit=1"
    try:
        res = requests.get(url, timeout=10).json()
        return float(res['observations'][0]['value'])
    except Exception as e:
        logger.warning(f"FRED fetch failed for {series_id}: {e}")
        return 0.0

def scan_macro_liquidity(is_test=False):
    logger.info("Initiating Macro Liquidity Deep Scan...")

    # FRED Series IDs: 
    # WALCL (Total Assets), WTREGEN (TGA), RRPONTSYD (Reverse Repo), BAMLH0A0HYM2 (High Yield Credit Spread)
    fed_assets = fetch_fred_metric("WALCL") / 1000 # Convert millions to billions
    tga = fetch_fred_metric("WTREGEN")
    rev_repo = fetch_fred_metric("RRPONTSYD")
    credit_spread = fetch_fred_metric("BAMLH0A0HYM2")

    # Formula for true institutional stock market liquidity
    net_liquidity = fed_assets - tga - rev_repo
    
    # Store directly into SQLite memory
    db.update_state("net_liquidity", net_liquidity)
    db.update_state("credit_spread", credit_spread)
    db.log_event(f"Macro Radar Scan: Net Liq ${net_liquidity:,.0f}B | Spread {credit_spread}%")

    if credit_spread > 4.5:
        risk_emoji, regime_alert = "🚨", "CREDIT STRESS DETECTED: Restricting aggressive equities signals."
    else:
        risk_emoji, regime_alert = "🟢", "Credit markets stable. Standard flow operations authorized."

    payload = (
        f"**Federal Reserve System Liquidity Snapshot**\n"
        f"┣ **Fed Balance Sheet:** `${fed_assets:,.0f}B`\n"
        f"┣ **Treasury General Account:** `${tga:,.0f}B`\n"
        f"┣ **Reverse Repo Facility:** `${rev_repo:,.0f}B`\n"
        f"┣ **Global Net Liquidity:** `${net_liquidity:,.0f}B`\n"
        f"┗ **High Yield Credit Spread:** `{credit_spread:.2f}%`\n\n"
        f"**System Interpretation:**\n{risk_emoji} *{regime_alert}*"
    )
    
    logger.info(f"Scan complete. Net Liq: ${net_liquidity:,.0f}B | Spread: {credit_spread}%")

    if HAS_ESSENTIALS and WEBHOOK_MACRO:
        title = "🏦 Institutional Liquidity Radar" + (" [TEST]" if is_test else "")
        send_essentials_embed(WEBHOOK_MACRO, title, payload, 0x3498db)

if __name__ == "__main__":
    validate_environment()
    logger.info("Macro Radar execution cycle initiated.")
    
    is_test = False
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Mode Initiated.")
        is_test = True
    
    try:
        scan_macro_liquidity(is_test=is_test)
        logger.info("Macro Radar execution complete. Shutting down cleanly.")
    except Exception as e:
        logger.error(f"Macro Radar fatal execution error: {e}")
        db.log_event(f"Macro Radar fatal execution error: {e}", "ERROR")
        sys.exit(1)
