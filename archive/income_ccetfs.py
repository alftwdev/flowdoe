import os
import requests
import pandas as pd
import sys
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- 1. INITIALIZATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

TD_API_KEY = str(os.getenv("TWELVE_DATA_API_KEY")).strip()
WEBHOOK_URL = os.getenv("WEBHOOK_INCOME_CCETFS")
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

# Core assets we ALWAYS want to check, even if not in "movers"
CORE_WATCHLIST = ["MSTY", "NVDY", "JEPI", "JEPQ"]

def get_market_context():
    try:
        if os.path.exists(HISTORY_FILE):
            df = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
            return df.iloc[-1]['Regime'].upper().strip()
    except:
        return "NEUTRAL"
    return "NEUTRAL"

def get_dynamic_income_universe():
    """Venture Tier: Scans for high-volume ETFs to find new income opportunities."""
    # Scanning for active ETFs that likely have liquid option chains for covered calls
    url = f"https://api.twelvedata.com/etf/list?apikey={TD_API_KEY}"
    try:
        # For the purpose of the 'Hunter', we focus on a subset of known high-yield 
        # or we use the 'market_movers' endpoint to see what's trending.
        movers_url = f"https://api.twelvedata.com/market_movers/stocks?direction=all&apikey={TD_API_KEY}"
        data = requests.get(movers_url).json()
        if data.get("status") == "error": return CORE_WATCHLIST
        
        found_tickers = [item['symbol'] for item in data['values'][:15]]
        return list(dict.fromkeys(CORE_WATCHLIST + found_tickers))
    except:
        return CORE_WATCHLIST

def get_income_intel(symbol, regime):
    try:
        # Quote + RSI + ATR
        quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        atr_url = f"https://api.twelvedata.com/atr?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        
        q = requests.get(quote_url).json()
        r = requests.get(rsi_url).json()
        a = requests.get(atr_url).json()

        if "error" in [q.get("status"), r.get("status"), a.get("status")]: return None

        price = float(q['close'])
        rsi = float(r['values'][0]['rsi'])
        atr = float(a['values'][0]['atr'])
        change = float(q['percent_change'])
        # Yield is often in 'trailing_annual_dividend_yield'
        div_yield = float(q.get('trailing_annual_dividend_yield', 0)) * 100

        status = "HOLD"
        color = 0x95a5a6 # Gray
        
        # LOGIC: The Rockefeller Entry
        if rsi < 35 and regime != "RISK-OFF":
            status = "🏛️ PRIORITY ACCUMULATION (OVERSOLD)"
            color = 0x27ae60 
        elif rsi < 45 and regime != "RISK-OFF":
            status = "CORE ACCUMULATION (VALUE)"
            color = 0x2ecc71
        elif rsi > 70:
            status = "OVEREXTENDED (NO NEW POSITIONS)"
            color = 0xe67e22

        # Yield Trap Filter (ATR Check)
        if abs(price * (change/100)) > (atr * 2.0):
            status = "⚠️ VOLATILITY ALERT (AVOID)"
            color = 0xe74c3c

        return {
            "symbol": symbol,
            "name": q.get("name", symbol),
            "price": price,
            "rsi": rsi,
            "yield": div_yield,
            "status": status,
            "color": color,
            "change": change
        }
    except:
        return None

def run_income_radar():
    print(f"--- 💸 DYNAMIC INCOME HUNTER START ---")
    regime = get_market_context()
    universe = get_dynamic_income_universe()
    
    signals_sent = 0
    for symbol in universe:
        print(f"   [SCANNING] {symbol}...", end="\r")
        intel = get_income_intel(symbol, regime)
        
        if intel:
            # Only alert on high-conviction "Buy" or "Avoid" signals to keep the channel clean
            is_test = "test" in sys.argv
            if intel['status'] != "HOLD" or is_test:
                signals_sent += 1
                msg = (
                    f"### **Income Sentry: {intel['symbol']}**\n"
                    f"**System Status**: `{intel['status']}`\n\n"
                    f"**Tactical Metrics**:\n"
                    f"└ Current Price: `${intel['price']:.2f}` ({intel['change']:+.2f}%)\n"
                    f"└ Relative Strength: `{intel['rsi']:.1f}`\n"
                    f"└ **Market Posture**: `{regime}`\n\n"
                    f"*Note: Strategy utilizes ATR-based volatility filters to avoid high-yield traps.*"
                )
                send_essentials_embed(WEBHOOK_URL, f"Income Intel: {intel['name']}", msg, intel['color'])

    print(f"--- 💸 HUNTER FINISHED: {signals_sent} SIGNALS DISPATCHED ---")

if __name__ == "__main__":
    run_income_radar()
