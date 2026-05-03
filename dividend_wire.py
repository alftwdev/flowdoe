import feedparser
import yfinance as yf
import requests
import time
import os
from dotenv import load_dotenv

# --- 0. CONFIG ---
load_dotenv()
# Matches your .env variable exactly
WEBHOOK_URL = os.getenv("WEBHOOK_DIVIDEND_WIRE")

# Priority assets for Essentials Algos
PRIORITY_TICKERS = ["CLM", "CRF"]

def get_stock_stats(ticker):
    """Fetches key dividend stats using yfinance."""
    try:
        stock = yf.Ticker(ticker)
        # Use fast_info for efficient price fetching
        fast_info = stock.fast_info
        info = stock.info
        
        return {
            "price": fast_info.get("last_price") or info.get("currentPrice"),
            "yield": (info.get("dividendYield", 0) or 0) * 100,
            "last_div": info.get("lastDividendValue", 0)
        }
    except Exception:
        return None

def run_dividend_wire():
    print(f"--- DIVIDEND WIRE START: {time.ctime()} ---")
    
    if not WEBHOOK_URL:
        print("    [!] ERROR: WEBHOOK_DIVIDEND_WIRE not found in .env")
        return

    feed_url = "https://seekingalpha.com/market-news/dividend-stocks.rss"
    feed = feedparser.parse(feed_url)
    
    if not feed.entries:
        print("    [!] No entries found in feed (Common on weekends).")
        return

    # Scan the 10 most recent news items
    for entry in feed.entries[:10]:
        title = entry.title
        print(f"    [SCANNING] {title[:60]}...")
        
        # Identify Ticker: Clean punctuation and split into words
        clean_title = title.replace("(", " ").replace(")", " ").replace(":", " ")
        words = clean_title.split()
        
        found_ticker = None
        
        # Check priority holdings first
        for p in PRIORITY_TICKERS:
            if p in words:
                found_ticker = p
                break
        
        # Fallback to general uppercase ticker detection (1-5 chars)
        if not found_ticker:
            found_ticker = next((w for w in words if w.isupper() and 1 <= len(w) <= 5), None)
        
        if found_ticker:
            print(f"    [MATCH] Found ticker: {found_ticker}")
            stats = get_stock_stats(found_ticker)
            
            # Special Tagging for Cornerstone strategy
            priority_tag = "🚨 **PRIORITY ASSET UPDATE** 🚨\n" if found_ticker in PRIORITY_TICKERS else ""
            
            stats_text = ""
            if stats and stats['price']:
                stats_text = (
                    f"\n📊 **Essentials Stats for {found_ticker}:**\n"
                    f"└ Price: ${stats['price']:.2f}\n"
                    f"└ Yield: {stats['yield']:.2f}%\n"
                    f"└ Last Payout: ${stats['last_div']:.2f}\n"
                )

            message = f"{priority_tag}📰 **{title}**\n{entry.link}\n{stats_text}"
            
            # Dispatch to Discord
            response = requests.post(WEBHOOK_URL, json={"content": message})
            if response.status_code in [200, 204]:
                print(f"    [SUCCESS] Dispatched {found_ticker}")
            else:
                print(f"    [ERROR] Discord Fail: {response.status_code}")
                
            time.sleep(2) # Respect webhook rate limits

    print(f"--- DIVIDEND WIRE FINISHED ---")

def manual_test():
    """Forces a test notification for CLM to verify Discord and yfinance."""
    print("--- STARTING FORCED TEST ---")
    test_ticker = "CLM"
    stats = get_stock_stats(test_ticker)
    
    if stats:
        priority_tag = "🚨 **PRIORITY ASSET UPDATE (TEST)** 🚨\n"
        stats_text = (
            f"\n📊 **Essentials Stats for {test_ticker}:**\n"
            f"└ Price: ${stats['price']:.2f}\n"
            f"└ Yield: {stats['yield']:.2f}%\n"
            f"└ Last Payout: ${stats['last_div']:.2f}\n"
        )
        
        message = f"{priority_tag}📰 **TEST: Manual Ticker Verification**\nhttps://seekingalpha.com\n{stats_text}"
        
        response = requests.post(WEBHOOK_URL, json={"content": message})
        if response.status_code in [200, 204]:
            print("    [SUCCESS] Test notification sent to Discord!")
        else:
            print(f"    [ERROR] Discord returned status: {response.status_code}")

if __name__ == "__main__":
    # To run the REAL feed:
    run_dividend_wire()
    
    # To run the TEST (uncomment the line below and comment the one above):
    # manual_test()
