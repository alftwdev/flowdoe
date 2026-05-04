import os
import time
import requests
import datetime
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

load_dotenv()

# --- 0. CONFIG ---
TD_API_KEY = os.getenv("TD_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_TRADE_SIGNALS")

def get_active_tickers():
    """Venture Discovery: Finds high-volume targets for options liquidity."""
    url = f"https://api.twelvedata.com/market_movers/stocks?direction=all&outputsize=10&apikey={TD_API_KEY}"
    try:
        data = requests.get(url).json()
        core_list = ["NVDA", "TSLA", "TQQQ", "AAPL", "AMD", "SOFI", "PLTR", "MSFT"]
        discovered = [item['symbol'] for item in data.get('values', []) if item['type'] == 'Stock']
        return list(set(discovered + core_list))[:12]
    except:
        return ["NVDA", "TSLA", "TQQQ", "AAPL", "AMD", "SOFI"]

def get_trade_setup(symbol):
    """Venture Analytics: Pulls VWAP, RSI, and Standard Deviation for Strike placement."""
    vwap_url = f"https://api.twelvedata.com/vwap?symbol={symbol}&interval=15min&apikey={TD_API_KEY}"
    rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=15min&time_period=14&apikey={TD_API_KEY}"
    quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    # Standard Deviation to find a 'realistic' strike offset
    std_url = f"https://api.twelvedata.com/stdev?symbol={symbol}&interval=15min&time_period=20&apikey={TD_API_KEY}"

    try:
        price_data = requests.get(quote_url).json()
        v_data = requests.get(vwap_url).json()
        r_data = requests.get(rsi_url).json()
        s_data = requests.get(std_url).json()

        return {
            "price": float(price_data['close']),
            "vwap": float(v_data['values'][0]['vwap']),
            "rsi": float(r_data['values'][0]['rsi']),
            "stdev": float(s_data['values'][0]['stdev']),
            "symbol": symbol
        }
    except: return None

def run_discovery_scan():
    print(f"--- 🏛️ TRADE DISCOVERY START: {datetime.datetime.now()} ---")
    active_watchlist = get_active_tickers()
    
    for symbol in active_watchlist:
        setup = get_trade_setup(symbol)
        if not setup: continue

        rsi = setup['rsi']
        price = setup['price']
        vwap = setup['vwap']
        sd_offset = setup['stdev'] * 0.5 # 0.5 Sigma offset for a realistic strike target

        # --- ACTIONABLE ALGO LOGIC ---
        # BUY CALLS: Oversold and Reclaiming VWAP
        if rsi < 35 and price >= vwap:
            strike = round(price + sd_offset)
            msg = (
                f"**ACTION**: BUY CALLS\n"
                f"**TICKER**: ${symbol}\n"
                f"**STRIKE**: ${strike} (Targeting Momentum)\n"
                f"**DTE**: 7-14 Days\n"
                f"**LOGIC**: Oversold RSI + VWAP Reclaim (Bullish Pivot)\n"
                f"**EXIT**: You must decide - set a limit to exit if needed."
            )
            send_essentials_embed(WEBHOOK_URL, f"🏛️ SIGNAL: BULLISH ENTRY", msg, 0x2ecc71)

        # BUY PUTS: Overbought and Rejecting VWAP
        elif rsi > 65 and price <= vwap:
            strike = round(price - sd_offset)
            msg = (
                f"**ACTION**: BUY PUTS\n"
                f"**TICKER**: ${symbol}\n"
                f"**STRIKE**: ${strike} (Targeting Mean-Reversion)\n"
                f"**DTE**: 7-14 Days\n"
                f"**LOGIC**: Overbought RSI + VWAP Rejection (Bearish Exhaustion)\n"
                f"**EXIT**: You must decide - set a limit to exit if needed."
            )
            send_essentials_embed(WEBHOOK_URL, f"🏛️ SIGNAL: BEARISH ENTRY", msg, 0xe74c3c)
        
        time.sleep(1.2)

if __name__ == "__main__":
    run_discovery_scan()
