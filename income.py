import os
import requests
import json
import pytz
import sys
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- 1. ECOSYSTEM INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# Environment Variables
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS")

# Technical Thresholds for "Popular/Trending" Assets
MIN_YIELD = 4.0
MIN_VOLUME = 500000 # Only report on liquid, high-volume assets
MAX_DISPATCH = 8    # Maximum signals per alert to prevent spam

# --- 2. THE DYNAMIC INTELLIGENCE ENGINE ---

def get_risk_rating(yield_val, volume):
    """Surgical Risk Assessment based on yield extremity and liquidity."""
    if yield_val > 50: return "⚠️ ULTRA-HIGH (Yield Trap)"
    if yield_val > 18: return "⚖️ ELEVATED (High Income)"
    if volume < 100000: return "🔍 LOW LIQUIDITY (Caution)"
    return "✅ STABLE (Institutional)"

def run_dynamic_income_scan(mode="DAILY"):
    """
    Scans the entire market via Twelve Data for trending high-yield setups.
    Does NOT use a hardcoded watchlist.
    """
    print(f"    [INCOME] Initiating Dynamic Market Scan ({mode})...")
    
    # We scan a 14-day window to find the most relevant upcoming "Main Events"
    start_date = datetime.now().strftime('%Y-%m-%d')
    end_date = (datetime.now() + timedelta(days=14)).strftime('%Y-%m-%d')
    
    # Twelve Data Venture: Dividend Calendar endpoint
    url = f"https://api.twelvedata.com/dividends_calendar?start_date={start_date}&end_date={end_date}&apikey={TD_API_KEY}"
    
    try:
        r = requests.get(url, timeout=20)
        events = r.json()
        if not isinstance(events, list):
            events = events.get("rows", [])

        print(f"    [SENTRY] Analyzing {len(events)} market events for quality...")
        
        processed_candidates = []
        
        for e in events:
            if len(processed_candidates) >= MAX_DISPATCH: break
            
            ticker = e['symbol']
            
            # Fetch real-time market data to check Volume and Price
            quote_res = requests.get(f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={TD_API_KEY}").json()
            
            try:
                price = float(quote_res.get('close', 0))
                vol = int(quote_res.get('volume', 0))
                div_amt = float(e.get('amount', 0))
                
                # Dynamic Yield Calculation (Estimated Annual)
                # We assume monthly for high-yielders > $0.20/mo, quarterly otherwise
                freq = 12 if div_amt > 0.15 else 4 
                annual_yield = (div_amt * freq / price) * 100 if price > 0 else 0
                
                # THE FILTER: Must be high volume AND meet yield threshold
                if annual_yield >= MIN_YIELD and vol >= MIN_VOLUME:
                    risk = get_risk_rating(annual_yield, vol)
                    processed_candidates.append({
                        "symbol": ticker,
                        "price": price,
                        "yield": round(annual_yield, 2),
                        "amount": div_amt,
                        "ex_date": e['ex_dividend_date'],
                        "pay_date": e.get('payment_date', 'TBD'),
                        "risk": risk,
                        "vol": vol
                    })
                    print(f"    [+] High-Signal Match: {ticker} ({annual_yield:.1f}% Yield)")
            except:
                continue

        # 3. DISPATCH LOGIC
        if mode == "WEEKLY":
            title = "📅 Weekly Institutional Income Outlook"
            header = "Top-Tier Dividend Opportunities (Next 14 Days)"
            color = 0x2980b9
        else:
            title = f"💰 Daily Income Scan: {datetime.now().strftime('%b %d')}"
            header = "Tactical High-Volume Dividend Pulse"
            color = 0x27ae60

        if not processed_candidates:
            desc = "### **System Stable: Market Quiet**\nNo trending high-volume dividend events met the institutional threshold today."
        else:
            desc = f"### **{header}**\n"
            # Sort by Yield (Highest first)
            processed_candidates.sort(key=lambda x: x['yield'], reverse=True)
            
            for p in processed_candidates:
                desc += (
                    f"**{p['symbol']}** | Price: `${p['price']}`\n"
                    f"┣ Yield: `{p['yield']}%` | Risk: `{p['risk']}`\n"
                    f"┣ Ex-Date: `{p['ex_date']}` | Amt: `${p['amount']}`\n"
                    f"┗ Vol: `{p['vol']:,}` | Pay: `{p['pay_date']}`\n\n"
                )

        print(f"    [BROADCAST] Dispatching to Discord...")
        if HAS_ESSENTIALS and WEBHOOK_INCOME:
            send_essentials_embed(WEBHOOK_INCOME, title, desc, color)

    except Exception as e:
        print(f"    [CRITICAL ERROR] Scan Failed: {e}")

# --- 3. SCHEDULER ---

def run_scheduler():
    tz_est = pytz.timezone('US/Eastern')
    print(f"--- 🏛️ DYNAMIC INCOME PATROL ACTIVE ---")

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        run_dynamic_income_scan(mode="WEEKLY")
        time.sleep(2)
        run_dynamic_income_scan(mode="DAILY")
        return

    while True:
        now_est = datetime.now(tz_est)
        if now_est.hour == 8 and now_est.minute == 0:
            if now_est.weekday() == 0:
                run_dynamic_income_scan(mode="WEEKLY")
                time.sleep(10)
                run_dynamic_income_scan(mode="DAILY")
            else:
                run_dynamic_income_scan(mode="DAILY")
            time.sleep(65)
        
        time.sleep(30)

if __name__ == "__main__":
    run_scheduler()
