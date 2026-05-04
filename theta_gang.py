import os
import time
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- 0. CONFIG ---
load_dotenv()
# Using the key mapped in your .env
TD_API_KEY = os.getenv("TD_API_KEY")
WEBHOOK_THETAGANG = os.getenv("WEBHOOK_THETA_GANG")

def discover_high_vol_tickers():
    """Dynamically discovers active/high-vol ETFs to ensure the list never goes stale."""
    print("🔍 Scanning for high-velocity Theta candidates...")
    # Search for Leveraged and Tech ETFs which usually carry the best premiums
    url = f"https://api.twelvedata.com/symbol_search?symbol=Leveraged&outputsize=10&apikey={TD_API_KEY}"
    try:
        results = requests.get(url).json().get('data', [])
        found = [item['symbol'] for item in results if item['instrument_type'] == 'ETF' and item['currency'] == 'USD']
        # Merge with your 'Reliable Income' & High-Yield cores
        core_watchlist = ["NVDA", "TSLA", "TQQQ", "SOFI", "MSTY", "NVDY", "IWM"]
        return list(set(found + core_watchlist))
    except:
        return ["NVDA", "TSLA", "TQQQ", "SOFI", "MSTY", "NVDY"]

def get_venture_theta_metrics(symbol):
    """Leverages Venture-tier indicators for precise Expected Move calculations."""
    try:
        # 1. Price Quote
        quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        # 2. ATR for Volatility (14-day)
        atr_url = f"https://api.twelvedata.com/atr?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        
        q_resp = requests.get(quote_url).json()
        a_resp = requests.get(atr_url).json()

        price = float(q_resp.get('close', 0))
        atr = float(a_resp.get('values', [{}])[0].get('atr', 0))
        
        if price == 0 or atr == 0:
            return None

        # --- THE THETA MATH ---
        # 1. Annualized Vol Proxy (ATR * Sqrt(252) / Price)
        ann_vol = (atr * np.sqrt(252)) / price
        
        # 2. Weekly Expected Move (Price * (Vol / Sqrt(52)))
        weekly_move = price * (ann_vol / np.sqrt(52))
        
        # 3. Juice Score: ATR as % of Price (Higher = More Premium)
        juice_score = (atr / price) * 100

        return {
            "symbol": symbol,
            "price": price,
            "vol": ann_vol * 100,
            "move": weekly_move,
            "juice": juice_score,
            "safe_put": price - weekly_move,          # 1.0 SD
            "juicy_put": price - (weekly_move * 0.5)   # 0.5 SD
        }
    except Exception as e:
        print(f"    [!] Error analyzing {symbol}: {e}")
        return None

def run_theta_heatmap():
    print(f"--- 🎡 THETA HEATMAP START: {time.ctime()} ---")
    
    tickers = discover_high_vol_tickers()
    results = []

    for ticker in tickers:
        print(f"    [VENTURE ANALYTICS] Processing {ticker}...")
        metrics = get_td_metrics = get_venture_theta_metrics(ticker)
        if metrics:
            results.append(metrics)
        time.sleep(1.5) # Respect Venture-tier rate limits

    # Sort by Juice Score (High premiums at the top)
    results = sorted(results, key=lambda x: x['juice'], reverse=True)

    report_lines = []
    for m in results[:8]: # Top 8 opportunities
        indicator = "🔥" if m['juice'] > 3.5 else "🧊"
        line = (
            f"{indicator} **{m['symbol']}** | Price: ${m['price']:.2f}\n"
            f"└ **Juice Score**: {m['juice']:.1f}% (Est IV: {m['vol']:.1f}%)\n"
            f"└ 🟢 Safe Strike ($1.0\sigma$): **${m['safe_put']:.2f}**\n"
            f"└ ⚡ Juicy Strike ($0.5\sigma$): **${m['juicy_put']:.2f}**\n"
        )
        report_lines.append(line)

    if report_lines:
        send_essentials_embed(
            webhook_url=WEBHOOK_THETAGANG,
            title="🎡 Theta Gang: High-Premium Heatmap",
            description="Dynamically screened for the highest premium-to-price ratios.\n\n" + "\n".join(report_lines),
            color=0x9b59b6 # Purple
        )
        print("--- HEATMAP DISPATCHED ---")

if __name__ == "__main__":
    run_theta_heatmap()
