import feedparser
import requests
import time
import os
import pandas as pd
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- 1. INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Use the key used in your working macro_radar.py
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_DIVIDEND_WIRE")
HISTORY_FILE = os.path.join(BASE_DIR, "macro_history.csv")

PRIORITY_TICKERS = ["CLM", "CRF"]

def get_market_context():
    """Reads the current regime from your macro_history.csv."""
    try:
        if os.path.exists(HISTORY_FILE):
            df = pd.read_csv(HISTORY_FILE)
            return df.iloc[-1]['Regime'].upper().strip()
    except:
        pass
    return "NEUTRAL"

def get_td_dividend_intel(ticker):
    """Venture Tier: Fetches dividend data + Technicals."""
    try:
        key = str(TD_API_KEY).strip()
        # Fetching Quote
        url = f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={key}"
        r = requests.get(url, timeout=15).json()
        
        if r.get("status") == "error":
            print(f"    [!] TD Error for {ticker}: {r.get('message')}")
            return None

        price = float(r.get('close', 0))
        # Yield handling
        raw_yield = float(r.get('trailing_annual_dividend_yield', 0))
        div_yield = raw_yield * 100 if raw_yield < 1.0 else raw_yield
        
        return {
            "price": price,
            "yield": div_yield,
            "name": r.get('name', ticker),
            "sector": r.get('sector', 'Capital Markets'),
            "ticker": ticker
        }
    except Exception as e:
        print(f"    [!] System Error for {ticker}: {e}")
        return None

def run_dividend_wire():
    print(f"--- 📡 DIVIDEND WIRE: ROCKEFELLER SCAN START ---")
    regime = get_market_context()
    
    # 1. Catalyst Search (RSS)
    feed_url = "https://seekingalpha.com/market-news/dividend-stocks.rss"
    feed = feedparser.parse(feed_url)
    
    processed = set()
    found_news = False

    # Check top 5 headlines
    for entry in feed.entries[:5]:
        words = entry.title.replace("(", " ").replace(")", " ").split()
        found_ticker = next((w for w in words if w.isupper() and 1 <= len(w) <= 5), None)
        
        if found_ticker and found_ticker not in processed:
            print(f"    [NEWS MATCH] {found_ticker}...")
            intel = get_td_dividend_intel(found_ticker)
            if intel:
                found_news = True
                desc = (
                    f"### 🔔 Dividend Catalyst\n"
                    f"**Headline**: {entry.title}\n\n"
                    f"**Venture Intel**:\n"
                    f"└ Price: `${intel['price']:.2f}`\n"
                    f"└ Yield: `{intel['yield']:.2f}%`\n"
                    f"└ Sector: `{intel['sector']}`\n\n"
                    f"**Market Context**: `{regime}`\n"
                    f"🔗 [Full Analysis]({entry.link})"
                )
                send_essentials_embed(WEBHOOK_URL, f"Intel: {intel['name']}", desc, 0x27ae60)
                processed.add(found_ticker)
                time.sleep(2)

    # 2. Rockefeller Fallback (If no news, update Priority Assets)
    if not found_news:
        print("    [INFO] No fresh catalysts. Dispatching Priority Pulse...")
        for ticker in PRIORITY_TICKERS:
            intel = get_td_dividend_intel(ticker)
            if intel:
                desc = (
                    f"### 🏛️ Priority Asset Pulse\n"
                    f"No new headlines detected in the last hour. Current standing:\n\n"
                    f"**{intel['name']} ({ticker})**\n"
                    f"└ Price: `${intel['price']:.2f}`\n"
                    f"└ Yield: `{intel['yield']:.2f}%`\n"
                    f"└ Market Regime: `{regime}`\n\n"
                    f"*System Status: Monitoring for SEC filings and Whale Dumps.*"
                )
                send_essentials_embed(WEBHOOK_URL, f"Daily Pulse: {ticker}", desc, 0xffd700)
                time.sleep(2)

    print(f"--- 📡 DIVIDEND WIRE FINISHED ---")

if __name__ == "__main__":
    run_dividend_wire()
