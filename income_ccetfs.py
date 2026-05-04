import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

load_dotenv()
TD_API_KEY = os.getenv("TD_API_KEY")
WEBHOOK_INCOME = os.getenv("WEBHOOK_INCOME_CCETFS")

def discover_income_etfs():
    """Dynamically finds Covered Call ETFs using Twelve Data Search."""
    print("🔍 Scanning for new Income/Covered Call ETFs...")
    url = f"https://api.twelvedata.com/symbol_search?symbol=Covered%20Call&outputsize=10&apikey={TD_API_KEY}"
    try:
        results = requests.get(url).json().get('data', [])
        # Filter for US ETFs only
        return [item['symbol'] for item in results if item['instrument_type'] == 'ETF' and item['currency'] == 'USD']
    except Exception as e:
        print(f"Discovery Error: {e}")
        return ["JEPQ", "JEPI", "SPYI", "MSTY", "NVDY"] # Fallback to core

def get_premium_metrics(symbol):
    """Fetches high-value FIRE metrics: RSI, Price, and Yield."""
    try:
        # 1. Get Quote & Yield
        quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        quote = requests.get(quote_url).json()
        
        # 2. Get RSI (Daily) for Entry Intelligence
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        rsi_data = requests.get(rsi_url).json()
        
        price = float(quote.get('close', 0))
        # Note: 12Data 'yield' depends on plan; fallback to 0 if unavailable
        div_yield = float(quote.get('trailing_annual_dividend_yield', 0)) * 100
        rsi = float(rsi_data.get('values', [{}])[0].get('rsi', 50))

        # FIRE Entry Logic
        if rsi < 35:
            status = "💎 **Strike Zone** (Oversold)"
            color_mod = "🟢"
        elif rsi > 65:
            status = "⚠️ **Overextended** (Hold)"
            color_mod = "🔴"
        else:
            status = "✅ **Neutral** (Accumulate)"
            color_mod = "🟡"

        return {
            "symbol": symbol,
            "price": price,
            "yield": div_yield,
            "rsi": rsi,
            "status": status,
            "indicator": color_mod
        }
    except:
        return None

def run_income_radar():
    print("--- FIRE INCOME RADAR START ---")
    dynamic_list = discover_income_etfs()
    # Merge with your "Must-Watch" list
    watchlist = list(set(dynamic_list + ["JEPQ", "JEPI", "MSTY", "NVDY", "SPYI"]))
    
    report_lines = []
    for ticker in watchlist:
        m = get_premium_metrics(ticker)
        if m:
            line = (f"{m['indicator']} **{m['symbol']}** | Price: ${m['price']:.2f} | "
                    f"RSI: {m['rsi']:.1f}\n   └ {m['status']}")
            report_lines.append(line)
        time.sleep(1) # Respect Rate Limits

    report_text = "\n\n".join(report_lines)
    
    send_essentials_embed(
        webhook_url=WEBHOOK_INCOME,
        title="🔥 Professional Income Discovery Radar",
        description=f"Automated scan for high-yield covered call opportunities.\n\n{report_text}",
        color=0x2ecc71
    )
    print("--- RADAR DISPATCHED ---")

if __name__ == "__main__":
    run_income_radar()
