import feedparser
import requests
import time
import os
import pandas as pd
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- 0. CONFIG ---
load_dotenv()
# Your Twelve Data API Key from .env
TD_API_KEY = os.getenv("TD_API_KEY")
# Your specific Discord Webhook for Dividend Wire
WEBHOOK_URL = os.getenv("WEBHOOK_DIVIDEND_WIRE")

# Priority assets for your specific strategy
PRIORITY_TICKERS = ["CLM", "CRF"]

def get_td_dividend_intel(ticker):
    """Fetches high-tier dividend data and technical safety from Twelve Data."""
    try:
        # 1. Fundamental Quote (Price + Div Yield)
        quote_url = f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={TD_API_KEY}"
        # 2. Technical Overlay (RSI for 'Overbought' alerts on dividend news)
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={ticker}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        
        quote = requests.get(quote_url).json()
        rsi_data = requests.get(rsi_url).json()

        price = float(quote.get('close', 0))
        # Twelve Data trailing yield (Note: some symbols return yield as a decimal, some as percentage)
        div_yield = float(quote.get('trailing_annual_dividend_yield', 0)) * 100
        
        # Get the most recent RSI value
        rsi_list = rsi_data.get('values', [])
        rsi = float(rsi_list[0].get('rsi', 50)) if rsi_list else 50

        # Technical Sentiment Logic for Income Investors
        if rsi > 70:
            safety_signal = "⚠️ **Caution**: Yield Chasing Risk (Overbought)"
        elif rsi < 35:
            safety_signal = "💎 **Value Zone**: Dividend Yield Expansion (Oversold)"
        else:
            safety_signal = "✅ **Stable**: Accumulation Phase"

        return {
            "price": price,
            "yield": div_yield,
            "rsi": rsi,
            "signal": safety_signal,
            "name": quote.get('name', ticker)
        }
    except Exception as e:
        print(f"    [!] Twelve Data Error for {ticker}: {e}")
        return None

def run_dividend_wire():
    print(f"--- 📡 DIVIDEND WIRE START: {time.ctime()} ---")
    
    if not WEBHOOK_URL:
        print("    [!] ERROR: WEBHOOK_DIVIDEND_WIRE missing in .env")
        return

    # Seeking Alpha RSS for real-time dividend catalyst detection
    feed_url = "https://seekingalpha.com/market-news/dividend-stocks.rss"
    feed = feedparser.parse(feed_url)
    
    processed_symbols = set()
    found_any_news = False

    # Scan top 8 recent news items
    for entry in feed.entries[:8]:
        title = entry.title
        # Extract Ticker (Uppercase words 1-5 chars)
        words = title.replace("(", " ").replace(")", " ").replace(":", " ").split()
        found_ticker = next((w for w in words if w.isupper() and 1 <= len(w) <= 5), None)
        
        if found_ticker and found_ticker not in processed_symbols:
            found_any_news = True
            print(f"    [CATALYST] {found_ticker} detected in news feed.")
            intel = get_td_dividend_intel(found_ticker)
            
            if intel and intel['price'] > 0:
                is_priority = found_ticker in PRIORITY_TICKERS
                priority_header = "🚨 **CORE STRATEGY ASSET** 🚨\n" if is_priority else ""
                
                description = (
                    f"{priority_header}"
                    f"**Headline**: {title}\n\n"
                    f"📊 **Venture Analytics**:\n"
                    f"└ Current Price: ${intel['price']:.2f}\n"
                    f"└ Div Yield: {intel['yield']:.2f}%\n"
                    f"└ RSI Analysis: {intel['rsi']:.1f}\n"
                    f"└ **Sentiment**: {intel['signal']}\n\n"
                    f"[View Full Article]({entry.link})"
                )

                # Gold for Priority assets, Green for standard dividend stocks
                color = 0xffd700 if is_priority else 0x1f8b4c 

                send_essentials_embed(
                    webhook_url=WEBHOOK_URL,
                    title=f"🔔 Dividend News: {intel['name']} ({found_ticker})",
                    description=description,
                    color=color
                )
                
                processed_symbols.add(found_ticker)
                time.sleep(2) # Respect rate limits

    if not found_any_news:
        print("    [INFO] No news found in feed. Running Weekend Pulse for Priority Assets...")
        for ticker in PRIORITY_TICKERS:
            intel = get_td_dividend_intel(ticker)
            if intel:
                send_essentials_embed(
                    webhook_url=WEBHOOK_URL,
                    title=f"📡 Weekend Pulse: {intel['name']} ({ticker})",
                    description=(
                        f"No new headlines found today.\n\n"
                        f"**Current Status**:\n"
                        f"└ Price: ${intel['price']:.2f}\n"
                        f"└ Yield: {intel['yield']:.2f}%\n"
                        f"└ RSI: {intel['rsi']:.1f}\n"
                        f"└ **Signal**: {intel['signal']}"
                    ),
                    color=0xffd700
                )
                time.sleep(2)

    print(f"--- 📡 DIVIDEND WIRE FINISHED ---")

if __name__ == "__main__":
    run_dividend_wire()
