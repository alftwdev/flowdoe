import os
import sys
import time
import requests
import logging
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

# Setup institutional-grade logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("Rockefeller_Income")

db = EcosystemDatabase()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
# Webhook Priority: Dividend specific > Market Analysis > Fallback
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

def fetch_twelvedata_yield(symbol):
    """Fetches real-time price and dividend history from Twelve Data Venture tier."""
    base_url = "https://api.twelvedata.com"
    try:
        # 1. Fetch Quote
        q_res = requests.get(f"{base_url}/quote?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        price = float(q_res.get("close", 0))
        
        # 2. Fetch Dividends
        d_res = requests.get(f"{base_url}/dividends?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        dividends = d_res.get("dividends", [])
        
        # 3. Calculate Annualized Yield
        now = datetime.now()
        one_year_ago = now - timedelta(days=365)
        
        annual_div = sum(
            float(d['amount']) for d in dividends 
            if datetime.strptime(d['date'], "%Y-%m-%d") > one_year_ago
        )
        
        yield_pct = (annual_div / price) * 100 if price > 0 else 0
        return {"price": price, "yield": yield_pct, "annual_div": annual_div}
        
    except Exception as e:
        logger.error(f"TwelveData extraction failed for {symbol}: {e}")
        return None

def process_income_cycle(is_test=False):
    target_assets = ["JEPI", "SCHD", "O", "ARCC"]
    results = {}
    
    logger.info("Initiating Twelve Data Yield Analysis...")
    
    for symbol in target_assets:
        data = fetch_twelvedata_yield(symbol)
        if data:
            results[symbol] = data
            logger.info(f"Captured {symbol}: ${data['price']:.2f} | Yield: {data['yield']:.2f}%")
    
    # Store to Database so metrics.py can access it
    db.update_state("income_alpha_data", results)
    
    # Dispatch Broadcast
    if WEBHOOK_INCOME and HAS_ESSENTIALS:
        report = "\n".join([f"┣ {s}: {v['yield']:.2f}% Yield (${v['price']:.2f})" for s, v in results.items()])
        title = "🏦 Institutional Yield Monitor" + (" [TEST]" if is_test else "")
        send_essentials_embed(WEBHOOK_INCOME, title, report, 0xf1c40f)
        logger.info("Report dispatched successfully.")

if __name__ == "__main__":
    is_test = len(sys.argv) > 1 and sys.argv[1].lower() == "test"
    process_income_cycle(is_test=is_test)
