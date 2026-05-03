import datetime
import os
import requests
import pandas as pd
import numpy as np
import time
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- 0. CONFIG ---
load_dotenv()
ALPHA_VANTAGE_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
WEBHOOK_THETAGANG = os.getenv("WEBHOOK_THETA_GANG") # Ensure this is in your .env

def get_theta_data(symbol):
    """Fetches compact data for volatility and strike analysis."""
    print(f"    [THETA] Analyzing {symbol}...")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_KEY}&outputsize=compact"
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        data = response.json()
        
        if "Time Series (Daily)" in data:
            df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient='index').astype(float)
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            return df
        return None
    except Exception as e:
        print(f"    [!] Error fetching {symbol}: {e}")
        return None

def calculate_premium_levels(df):
    """Calculates IV Rank proxy and Standard Deviation 'Deltas'."""
    # Calculate Daily Returns and Volatility
    df['returns'] = df['4. close'].pct_change()
    volatility = df['returns'].std() * np.sqrt(252) # Annualized Vol
    
    current_price = df['4. close'].iloc[-1]
    
    # Weekly Expected Move (Annual Vol / Sqrt(52 weeks))
    weekly_move = current_price * (volatility / np.sqrt(52))
    
    # Delta Proxies (Using Standard Deviation)
    # 1.0 SD is roughly 0.16 Delta (Safe)
    # 0.5 SD is roughly 0.30 Delta (Juicy)
    safe_strike = current_price - weekly_move
    juicy_strike = current_price - (weekly_move * 0.5)
    
    return {
        "price": current_price,
        "vol": volatility * 100,
        "weekly_range": weekly_move,
        "safe_put": safe_strike,
        "juicy_put": juicy_strike
    }

def run_theta_report():
    print(f"\n--- THETA GANG ANALYSIS: {datetime.datetime.now()} ---")
    tickers = ["TQQQ", "SOFI", "NVDA"]
    report_lines = []

    for ticker in tickers:
        df = get_theta_data(ticker)
        if df is not None:
            stats = calculate_premium_levels(df)
            
            line = (
                f"**{ticker}**: ${stats['price']:.2f}\n"
                f"└ Est. IV: {stats['vol']:.1f}% | Exp. Move: ±${stats['weekly_range']:.2f}\n"
                f"└ 🟢 Safe Strike (0.16δ): **${stats['safe_put']:.2f}**\n"
                f"└ 🔥 Juicy Strike (0.30δ): **${stats['juicy_put']:.2f}**\n"
            )
            report_lines.append(line)
        time.sleep(15) # Free Tier Safety

    if report_lines:
        full_report = "\n".join(report_lines)
        print("ACTION: Dispatching to Discord #thetagang...")
        send_essentials_embed(
            webhook_url=WEBHOOK_THETAGANG,
            title="Theta Gang: Weekly Premium Radar",
            description=full_report,
            color=0x9b59b6 # Purple for Theta Gang
        )

if __name__ == "__main__":
    run_theta_report()
