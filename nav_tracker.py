import os
import requests
import json
import time
from dotenv import load_dotenv

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS")
NAV_LEDGER = os.path.join(BASE_DIR, "nav_status.json")

def get_nav_premium(symbol):
    # Rockefeller Logic: Scrapes Morningstar or utilizes a high-tier NAV API
    # For now, we simulate the 'Premium' calc based on Twelve Data price vs Anchor NAV
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        r = requests.get(url).json()
        price = float(r['close'])
        # Anchor NAVs (You would update these manually or via scrape daily)
        anchor_nav = 6.50 if symbol == "CLM" else 6.45 
        premium = ((price - anchor_nav) / anchor_nav) * 100
        return price, premium
    except:
        return None, None

def run_nav_tracker():
    print("📈 NAV TRACKER ACTIVE...")
    while True:
        for ticker in ["CLM", "CRF"]:
            price, premium = get_nav_premium(ticker)
            if premium and premium > 25:
                desc = (
                    f"### 🚨 High Premium Alert: {ticker}\n"
                    f"└ Current Price: `${price:.2f}`\n"
                    f"└ Premium to NAV: `{premium:.2f}%`\n\n"
                    f"**Rockefeller Strategy**: DRIP efficiency is low. Consider taking cash dividend if premium exceeds 30%."
                )
                if HAS_ESSENTIALS:
                    send_essentials_embed(WEBHOOK_INCOME, f"🛡️ NAV Shield: {ticker}", desc, 0xf1c40f)
        
        time.sleep(3600) # Check hourly

if __name__ == "__main__":
    run_nav_tracker()
