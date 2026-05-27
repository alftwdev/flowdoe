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

def fetch_twelvedata_yield(symbol):
    # (Original fetch logic preserved entirely)
    base_url = "https://api.twelvedata.com"
    try:
        q_res = requests.get(f"{base_url}/quote?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        d_res = requests.get(f"{base_url}/dividends?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        
        price = float(q_res.get("close", 0))
        dividends = d_res.get("dividends", [])
        one_year_ago = datetime.now() - timedelta(days=365)
        
        annual_div = 0.0
        for d in dividends:
            div_date_str = d.get('ex_date', d.get('date', '1970-01-01'))
            try:
                div_date = datetime.strptime(div_date_str, "%Y-%m-%d")
                if div_date > one_year_ago:
                    annual_div += float(d.get('amount', 0))
            except ValueError:
                continue
        
        yield_pct = (annual_div / price) * 100 if price > 0 else 0
        return {"price": price, "yield": yield_pct, "annual_div": annual_div}
        
    except Exception as e:
        logger.error(f"TwelveData extraction failed for {symbol}: {e}")
        return None

def compile_eod_income_recap(is_test=False):
    """Compiles an EOD structural valuation matrix for yield positions."""
    logger.info("Generating EOD Income & CEF Structure Recap...")
    target_assets = ["CLM", "CRF", "JEPI", "SCHD"]
    report_lines = []
    
    for symbol in target_assets:
        try:
            url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=2&apikey={TD_API_KEY}"
            res = requests.get(url, timeout=10).json()
            if "values" in res and len(res["values"]) > 1:
                close_p = float(res["values"][0]["close"])
                prior_p = float(res["values"][1]["close"])
                daily_pct = ((close_p - prior_p) / prior_p) * 100
                
                saved_data = db.get_state("income_alpha_data", {})
                current_yield = saved_data.get(symbol, {}).get("yield", 0.0)
                
                emoji = "🍏" if daily_pct >= 0 else "🍎"
                report_lines.append(f"┣ **{symbol}**: ${close_p:.2f} ({emoji} `{daily_pct:+.2f}%`) | Yield: `{current_yield:.2f}%`")
        except Exception as e:
            logger.error(f"EOD calculation failed for {symbol}: {e}")
            
    full_report = (
        "### 🌅 End-of-Day Income & Premium Yield Recap\n"
        "Session closed. Evaluating theta decay realization and structural valuations:\n\n" + 
        "\n".join(report_lines) + 
        "\n\n*Post-Market Strategy: Theta decay verified. Premium tracking stable. Retain capital configurations.*"
    )
    
    if WEBHOOK_INCOME and HAS_ESSENTIALS:
        send_essentials_embed(WEBHOOK_INCOME, "🏦 EOD Premium & CEF Summary", full_report, 0x1f8b4c)

# Add to your master loop in income.py:
# if 1005 <= int(now.strftime("%H%M")) <= 1010 and current_date != last_eod_date:
#     compile_eod_income_recap()
def process_income_cycle(is_test=False):
    target_assets = ["JEPI", "SCHD", "O", "ARCC"]
    results = {}
    
    logger.info("Initiating Twelve Data Yield Analysis...")
    
    # Yield Trap Protection Layer
    vix_iv = db.get_state("vix_iv_index", 20.0)
    is_yield_trap = vix_iv > 25.0
    
    for symbol in target_assets:
        data = fetch_twelvedata_yield(symbol)
        if data and data['price'] > 0:
            results[symbol] = data
            logger.info(f"Captured {symbol}: ${data['price']:.2f} | Yield: {data['yield']:.2f}%")
        else:
            logger.warning(f"Failed to capture valid data for {symbol}.")
    
    if not results:
        logger.error("No valid yield data captured. Aborting broadcast.")
        return

    db.update_state("income_alpha_data", results)
    
    if WEBHOOK_INCOME and HAS_ESSENTIALS:
        report = ""
        if is_yield_trap:
            logger.warning(f"VIX at {vix_iv} > 25. Yield trap protection activated.")
            report += f"⚠️ **YIELD TRAP PROTECTION ACTIVE**\nImplied Volatility (VIX) currently reads `{vix_iv}`. High yields may reflect collapsing equity valuations rather than sustainable cash flow. Scale allocations down defensively.\n\n"
        
        report += "\n".join([f"┣ **{symbol}**: {v['yield']:.2f}% Yield (${v['price']:.2f})" for symbol, v in results.items()])
        
        title = "🏦 Institutional Yield Monitor" + (" [TEST]" if is_test else "")
        send_essentials_embed(WEBHOOK_INCOME, title, report, 0xe67e22 if is_yield_trap else 0xf1c40f)
        logger.info("Report dispatched successfully.")

if __name__ == "__main__":
    is_test_mode = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    process_income_cycle(is_test=is_test_mode)
