import os
import sys
import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from database import EcosystemDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("Rockefeller_Income")

db = EcosystemDatabase()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

def fetch_risk_free_rate():
    """Fetches the US 10-Year Treasury Yield as the baseline Risk-Free Rate proxy."""
    try:
        url = f"https://api.twelvedata.com/price?symbol=US10Y&apikey={TD_API_KEY}"
        res = requests.get(url, timeout=10).json()
        price = float(res.get("price", 4.50))
        return price if price < 10.0 else price / 10.0
    except Exception:
        return 4.50  

def fetch_twelvedata_yield(symbol):
    """Calculates robust Trailing Twelve Month (TTM) Dividend Yield with frequency fallbacks."""
    base_url = "https://api.twelvedata.com"
    try:
        q_res = requests.get(f"{base_url}/quote?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        d_res = requests.get(f"{base_url}/dividends?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        
        price = float(q_res.get("close", 0) or q_res.get("previous_close", 0))
        dividends = d_res.get("dividends", [])
        
        if price <= 0: return None

        one_year_ago = datetime.now() - timedelta(days=365)
        ttm_dividend_sum = 0.0
        
        for div in dividends:
            div_date_str = div.get("date")
            if div_date_str:
                div_date = datetime.strptime(div_date_str, "%Y-%m-%d")
                if div_date >= one_year_ago:
                    ttm_dividend_sum += float(div.get("amount", 0))
        
        # UPGRADE: Intelligent Fallback Frequency Multiplier
        if ttm_dividend_sum == 0.0 and dividends:
            latest_amount = float(dividends[0].get("amount", 0))
            if symbol in ["JEPI", "O"]:  # Known monthly payers
                ttm_dividend_sum = latest_amount * 12
            else:
                ttm_dividend_sum = latest_amount * 4  # Standard quarterly assumption
            
        div_yield = (ttm_dividend_sum / price) * 100 if ttm_dividend_sum > 0 else 0.0
        return {"price": price, "yield": div_yield}
        
    except Exception as e:
        logger.error(f"TwelveData extraction failed for {symbol}: {e}")
        return None

def process_income_cycle(is_test=False):
    target_assets = ["JEPI", "SCHD", "O", "ARCC"]
    results = {}
    
    logger.info("Initiating Twelve Data TTM Yield Analysis...")
    vix_iv = db.get_state("vix_iv_index", 20.0)
    is_yield_trap = vix_iv > 25.0
    rf_rate = fetch_risk_free_rate()
    
    for symbol in target_assets:
        data = fetch_twelvedata_yield(symbol)
        if data and data['price'] > 0:
            results[symbol] = data
        else:
            logger.warning(f"Failed to capture valid data for {symbol}.")
    
    if not results:
        logger.error("No valid yield data captured. Aborting broadcast.")
        return

    db.update_state("income_alpha_data", results)
    
    if WEBHOOK_INCOME and HAS_ESSENTIALS:
        report = ""
        if is_yield_trap:
            report += f"⚠️ **YIELD TRAP PROTECTION ACTIVE**\nImplied Volatility (VIX) currently reads `{vix_iv}`. High yields may reflect collapsing equity valuations rather than sustainable cash flow.\n\n"
        
        for sym, v in results.items():
            alpha_spread = v['yield'] - rf_rate
            report += f"┣ **{sym}**: `{v['yield']:.2f}%` Yield (${v['price']:.2f}) [Spread vs RF: `{alpha_spread:+.2f}%`]\n"
            
        # UPGRADE: Restored Discord formatting line breaks
        report += f"\n\n📈 *Baseline Risk-Free Rate Proxy (10Y/Treasury): {rf_rate:.2f}%*"
        
        title = "💰 Institutional Yield Monitor" + (" [TEST]" if is_test else "")
        send_essentials_embed(WEBHOOK_INCOME, title, report, 0xe67e22 if is_yield_trap else 0x1f8b4c)

if __name__ == "__main__":
    is_test_mode = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    process_income_cycle(is_test=is_test_mode)
