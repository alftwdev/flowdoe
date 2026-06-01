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

# --- ELITE ASSET UNIVERSES ---
GIANT_STOCKS = ["AAPL", "MSFT", "JNJ", "PG", "JPM", "KO", "XOM", "CVX", "ABBV", "HD", "COST", "WMT", "MCD", "VZ", "T", "O"]
POPULAR_CC = ["JEPI", "JEPQ", "DIVO", "SCHD", "SPYI", "QQQI", "XYLD", "QYLD", "RYLD", "BST", "GPIX"]
SPECULATIVE_CC = ["QDTE", "XDTE", "FEPI", "AIPI", "YMAX", "MSTY", "CONY", "NVDY", "IWMY", "AMZY", "FBY"]

def fetch_risk_free_rate():
    try:
        url = f"https://api.twelvedata.com/price?symbol=US10Y&apikey={TD_API_KEY}"
        res = requests.get(url, timeout=10).json()
        price = float(res.get("price", 4.50))
        return price if price < 10.0 else price / 10.0
    except Exception:
        return 4.50  

def fetch_twelvedata_yield(symbol):
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
        
        # Intelligent Fallback Frequency Multiplier
        if ttm_dividend_sum == 0.0 and dividends:
            latest_amount = float(dividends[0].get("amount", 0))
            if symbol in POPULAR_CC or symbol in SPECULATIVE_CC or symbol == "O": 
                ttm_dividend_sum = latest_amount * 12 # Monthly assumption
            else:
                ttm_dividend_sum = latest_amount * 4  # Quarterly assumption
            
        div_yield = (ttm_dividend_sum / price) * 100 if ttm_dividend_sum > 0 else 0.0
        return {"price": price, "yield": div_yield}
    except Exception:
        return None

def fetch_dynamic_calendar():
    """Scans TwelveData for today's dividend actions."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    url = f"https://api.twelvedata.com/dividends_calendar?start_date={today_str}&end_date={today_str}&apikey={TD_API_KEY}"
    
    ex_div_today = []
    pay_today = []
    
    try:
        res = requests.get(url, timeout=10).json()
        data = res.get("data", [])
        for item in data:
            sym = item.get("symbol")
            if item.get("ex_dividend_date") == today_str:
                ex_div_today.append(sym)
            if item.get("payment_date") == today_str:
                pay_today.append(sym)
    except Exception as e:
        logger.error(f"Dividend calendar fetch failed: {e}")
        
    return ex_div_today, pay_today

def format_asset_line(symbol, data, rf_rate):
    if not data: return f"┣ **{symbol}**: Awaiting Pricing Telemetry"
    spread = data['yield'] - rf_rate
    return f"┣ **{symbol}**: `{data['yield']:.2f}%` Yield (${data['price']:.2f}) [Spread vs RF: `{spread:+.2f}%`]"

def process_dynamic_income_cycle(is_test=False):
    logger.info("Initiating Dynamic Yield & Dividend Calendar Scan...")
    
    rf_rate = fetch_risk_free_rate()
    vix_iv = db.get_state("vix_iv_index", 20.0)
    is_yield_trap = float(vix_iv) > 25.0
    
    # 1. Fetch Dynamic Dates
    ex_div_raw, pay_raw = fetch_dynamic_calendar()
    
    # 2. Intersect with Ecosystem Universes
    ex_div_giants = [s for s in ex_div_raw if s in GIANT_STOCKS][:3]
    ex_div_cc = [s for s in ex_div_raw if s in POPULAR_CC][:3]
    ex_div_spec = [s for s in ex_div_raw if s in SPECULATIVE_CC][:2]
    
    pay_giants = [s for s in pay_raw if s in GIANT_STOCKS][:3]
    pay_cc = [s for s in pay_raw if s in POPULAR_CC][:3]
    pay_spec = [s for s in pay_raw if s in SPECULATIVE_CC][:2]

    # 3. Graceful Watchlist Fallbacks (If nothing happens today)
    if not ex_div_giants and not pay_giants: ex_div_giants = ["SCHD", "O"] 
    if not ex_div_cc and not pay_cc: ex_div_cc = ["JEPI", "JEPQ"]
    if not ex_div_spec and not pay_spec: ex_div_spec = ["XDTE", "QDTE"]

    report_lines = []
    
    if is_yield_trap:
        report_lines.append(f"⚠️ **YIELD TRAP PROTECTION ACTIVE**\nImplied Volatility (VIX) currently reads `{vix_iv}`. High yields may reflect collapsing equity valuations rather than sustainable cash flow.\n")

    report_lines.append("📅 **GOING EX-DIVIDEND TODAY (Capture Flow)**")
    for sym in ex_div_giants + ex_div_cc:
        data = fetch_twelvedata_yield(sym)
        report_lines.append(format_asset_line(sym, data, rf_rate))
    if not ex_div_giants and not ex_div_cc: 
        report_lines.append("┣ *No major ecosystem assets going ex-dividend today.*")
        
    report_lines.append("\n💸 **PAYMENT DATE TODAY (Liquidity Distribution)**")
    for sym in pay_giants + pay_cc:
        data = fetch_twelvedata_yield(sym)
        report_lines.append(format_asset_line(sym, data, rf_rate))
    if not pay_giants and not pay_cc: 
        report_lines.append("┣ *No major ecosystem assets distributing liquidity today.*")
        
    if ex_div_spec or pay_spec:
        report_lines.append("\n🔥 **SPECULATIVE YIELD RADAR (New/High-Risk CC ETFs)**")
        for sym in list(set(ex_div_spec + pay_spec)):
            data = fetch_twelvedata_yield(sym)
            report_lines.append(format_asset_line(sym, data, rf_rate))

    report_lines.append(f"\n📈 *Baseline Risk-Free Rate Proxy (10Y/Treasury): {rf_rate:.2f}%*")
    
    final_payload = "\n".join(report_lines)
    
    if WEBHOOK_INCOME and HAS_ESSENTIALS:
        title = "🏦 Institutional Yield & Distribution Terminal" + (" [TEST]" if is_test else "")
        send_essentials_embed(WEBHOOK_INCOME, title, final_payload, 0xe67e22 if is_yield_trap else 0x1f8b4c)
        logger.info("Income report successfully routed.")

if __name__ == "__main__":
    is_test_mode = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    process_dynamic_income_cycle(is_test=is_test_mode)
